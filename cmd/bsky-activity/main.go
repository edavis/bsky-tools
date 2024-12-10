package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	jetstream "github.com/bluesky-social/jetstream/pkg/models"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

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

	var event jetstream.Event

	if len(q.events) == 0 {
		return event, false
	}

	event = q.events[0]
	q.events = q.events[1:]
	return event, true
}

func (q *Queue) Size() int {
	q.lk.Lock()
	defer q.lk.Unlock()

	return len(q.events)
}

const JetstreamUrl = `wss://jetstream1.us-west.bsky.network/subscribe`

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

var AtprotoAllowlist = map[string]bool{
	"social.psky":           true,
	"blue.zio.atfile":       true,
	"com.shinolabs.pinksea": true,
	"com.whtwnd":            true,
	"events.smokesignal":    true,
	"fyi.unravel":           true,
	"xyz.statusphere":       true,
}

func trackedRecordType(collection string) bool {
	for k, _ := range AppBskyAllowlist {
		if collection == k {
			return true
		}
	}
	for k, _ := range AtprotoAllowlist {
		if strings.HasPrefix(collection, k) {
			return true
		}
	}
	return false
}

func handler(ctx context.Context, queue *Queue) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	pipe := rdb.Pipeline()
	var eventCount int

eventLoop:
	for {
		select {
		case <-ctx.Done():
			break eventLoop
		default:
		}

		event, ok := queue.Dequeue()
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if event.Kind != jetstream.EventKindCommit {
			continue
		}
		if event.Commit.Operation != jetstream.CommitOperationCreate {
			continue
		}

		commit := *event.Commit
		collection := commit.Collection

		// if collection doesn't start with either allowlist, continue
		if !trackedRecordType(collection) {
			continue
		}

		// if collection starts with one of the Atproto allowlist keys, incr
		for k, _ := range AtprotoAllowlist {
			if strings.HasPrefix(collection, k) {
				ckey := strings.ReplaceAll(k, ".", "_")
				if err := pipe.Incr(ctx, "dev.edavis.atproto.collection."+ckey).Err(); err != nil {
					log.Printf("failed incrementing an atproto collection: %v\n", err)
				}
			}
		}

		// if a post with an embed, incr that $embed type
		if collection == "app.bsky.feed.post" {
			var post appbsky.FeedPost
			if err := json.Unmarshal(commit.Record, &post); err != nil {
				log.Printf("error parsing appbsky.FeedPost: %v\n", err)
			}
			if post.Embed != nil {
				var ekey string
				switch {
				case post.Embed.EmbedImages != nil:
					ekey = post.Embed.EmbedImages.LexiconTypeID
				case post.Embed.EmbedVideo != nil:
					ekey = post.Embed.EmbedVideo.LexiconTypeID
				case post.Embed.EmbedExternal != nil:
					ekey = post.Embed.EmbedExternal.LexiconTypeID
				case post.Embed.EmbedRecord != nil:
					ekey = post.Embed.EmbedRecord.LexiconTypeID
				case post.Embed.EmbedRecordWithMedia != nil:
					ekey = post.Embed.EmbedRecordWithMedia.LexiconTypeID
				}
				if ekey == "" {
					continue
				}
				if err := pipe.Incr(ctx, "app.bsky.feed.post:embed:"+ekey).Err(); err != nil {
					log.Printf("failed incrementing embed key: %v\n", err)
				}
			}
		}

		// incr the collection and ops
		if err := pipe.Incr(ctx, collection).Err(); err != nil {
			log.Printf("failed incrementing collection: %v\n", err)
		}

		if err := pipe.Incr(ctx, `dev.edavis.muninsky.ops`).Err(); err != nil {
			log.Printf("failed incrementing ops: %v\n", err)
		}

		// add one to the count, every 500 ops execute the piepline
		eventCount += 1
		if eventCount%2500 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("failed to exec pipe\n")
			}
			log.Printf("queue size: %d\n", queue.Size())
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
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

	queue := NewQueue(100_000)
	go handler(ctx, queue)

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
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down\n")
}
