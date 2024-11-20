package main

import (
	"context"
	"database/sql"
	_ "embed"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	jetstream "github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

type CheckpointResults struct {
	Blocked     int
	Pages       int
	Transferred int
}

type Queue struct {
	lk     sync.Mutex
	events []jetstream.Event
}

func NewQueue(capacity int) *Queue {
	return &Queue{
		events: make([]jetstream.Event, 0, capacity),
	}
}

func (q *Queue) Enqueue(event jetstream.Event) {
	q.lk.Lock()
	defer q.lk.Unlock()

	q.events = append(q.events, event)
}

func (q *Queue) Dequeue() (jetstream.Event, bool) {
	q.lk.Lock()
	defer q.lk.Unlock()

	if len(q.events) == 0 {
		var e jetstream.Event
		return e, false
	}

	event := q.events[0]
	q.events = q.events[1:]
	return event, true
}

func (q *Queue) Size() int {
	q.lk.Lock()
	defer q.lk.Unlock()

	return len(q.events)
}

var AppBskyAllowlist = map[string]bool{
	"app.bsky.actor.profile":      true,
	"app.bsky.feed.generator":     true,
	"app.bsky.feed.like":          true,
	"app.bsky.feed.post":          true,
	"app.bsky.feed.postgate":      true,
	"app.bsky.feed.repost":        true,
	"app.bsky.feed.threadgate":    true,
	"app.bsky.graph.block":        true,
	"app.bsky.graph.follow":       true,
	"app.bsky.graph.list":         true,
	"app.bsky.graph.listblock":    true,
	"app.bsky.graph.listitem":     true,
	"app.bsky.graph.starterpack":  true,
	"app.bsky.labeler.service":    true,
	"chat.bsky.actor.declaration": true,
}

const JetstreamUrl = `wss://jetstream1.us-west.bsky.network/subscribe` // TODO(ejd): attach a reconnect cursor

const userTimestampUpdate = `insert into users (did, ts) values (?, ?) on conflict (did) do update set ts = ?`

//go:embed schema.sql
var ddl string

func handler(ctx context.Context, queue *Queue, dbCnx *sql.DB) {
	if _, err := dbCnx.ExecContext(ctx, ddl); err != nil {
		log.Printf("could not create tables: %v\n", err)
	}
	if _, err := dbCnx.ExecContext(ctx, "PRAGMA wal_autocheckpoint = 0"); err != nil {
		log.Printf("could not set PRAGMA wal_autocheckpoint: %v\n", err)
	}

	var (
		dbTx       *sql.Tx
		err        error
		eventCount int
	)

queueLoop:
	for {
		select {
		case <-ctx.Done():
			break queueLoop
		default:
		}

		event, ok := queue.Dequeue()
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if dbTx == nil {
			dbTx, err = dbCnx.BeginTx(ctx, nil)
			if err != nil {
				log.Printf("failed to begin transaction: %v\n", err)
			}
		}

		if event.Kind != jetstream.EventKindCommit {
			continue
		}
		if event.Commit.Operation != jetstream.CommitOperationCreate {
			// we're missing deletes and updates but this matches how bsky-activity
			// does it so we stay consistent
			continue
		}

		did := event.Did
		commit := *event.Commit
		ts := time.Now().UTC().Unix()

		if _, ok := AppBskyAllowlist[commit.Collection]; !ok {
			continue
		}

		dbTx.ExecContext(ctx, userTimestampUpdate, did, ts, ts)

		eventCount += 1
		if eventCount%100_000 == 0 {
			if err = dbTx.Commit(); err != nil {
				log.Printf("commit failed: %v\n", err)
			} else {
				log.Printf("commit successful\n")
			}

			var results CheckpointResults
			err = dbCnx.QueryRowContext(ctx, "PRAGMA wal_checkpoint(RESTART)").Scan(&results.Blocked, &results.Pages, &results.Transferred)
			switch {
			case err != nil:
				log.Printf("failed checkpoint: %v\n", err)
			case results.Blocked == 1:
				log.Printf("checkpoint: blocked\n")
			case results.Pages == results.Transferred:
				log.Printf("checkpoint: %d pages transferred\n", results.Transferred)
			case results.Pages != results.Transferred:
				log.Printf("checkpoint: %d pages, %d transferred\n", results.Pages, results.Transferred)
			}

			dbTx, err = dbCnx.BeginTx(ctx, nil)
			if err != nil {
				log.Printf("failed to begin transaction: %v\n", err)
			}
		}

		if eventCount%2500 == 0 {
			log.Printf("queue size: %d\n", queue.Size())
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, JetstreamUrl, nil)
	if err != nil {
		log.Fatalf("failed to open websocket: %v\n", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close websocket: %v\n", err)
		}
		log.Printf("websocket closed\n")
	}()

	// TODO(ejd): use more readable URL params for this
	dbCnx, err := sql.Open("sqlite3", "data/bsky-users.db?_journal=WAL&_fk=on&_timeout=5000&_sync=1&_txlock=immediate")
	if err != nil {
		log.Fatalf("failed to open database: %v\n", err)
	}
	defer func() {
		if _, err := dbCnx.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			log.Printf("error doing final WAL checkpoint: %v\n", err)
		}
		if err := dbCnx.Close(); err != nil {
			log.Printf("failed to close db: %v\n", err)
		}
		log.Printf("db closed\n")
	}()

	queue := NewQueue(100_000)
	go handler(ctx, queue, dbCnx)

	log.Printf("starting up\n")
	go func() {
		for {
			var event jetstream.Event
			err := conn.ReadJSON(&event)
			if err != nil {
				log.Printf("ReadJSON error: %v\n", err)
				stop()
				break
			}

			queue.Enqueue(event)
			if queue.Size() < 50 {
				log.Printf("[startup] added event")
			}
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down\n")
}
