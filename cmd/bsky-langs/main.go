package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
)

const JetstreamUrl = `ws://localhost:6008/subscribe?wantedCollections=app.bsky.feed.post`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	conn, _, err := websocket.DefaultDialer.Dial(JetstreamUrl, nil)
	if err != nil {
		log.Fatalf("failed to open websocket: %v\n", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("failed to close websocket: %v\n", err)
		}
		log.Printf("websocket closed\n")
	}()

	jetstreamEvents := make(chan []byte)
	go processPosts(ctx, jetstreamEvents)

	log.Printf("starting up\n")
	go func() {
		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				stop()
				break
			}
			jetstreamEvents <- message
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down\n")
}
