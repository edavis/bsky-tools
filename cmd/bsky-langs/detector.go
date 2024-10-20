package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	appbsky "github.com/bluesky-social/indigo/api/bsky"
	jetstream "github.com/bluesky-social/jetstream/pkg/models"
	"github.com/pemistahl/lingua-go"
	"github.com/redis/go-redis/v9"
)

func processPosts(ctx context.Context, events <-chan []byte) {
	languages := []lingua.Language{
		lingua.Portuguese,
		lingua.English,
		lingua.Japanese,
		lingua.German,
		lingua.French,
		lingua.Spanish,
		lingua.Korean,
		lingua.Thai,
	}
	detector := lingua.
		NewLanguageDetectorBuilder().
		FromLanguages(languages...).
		WithPreloadedLanguageModels().
		Build()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	pipe := rdb.Pipeline()

	var (
		count   int
		event   jetstream.Event
		post    appbsky.FeedPost
		langKey string
	)

	for message := range events {
		event = jetstream.Event{}
		if err := json.Unmarshal(message, &event); err != nil {
			continue
		}

		if event.Kind != jetstream.EventKindCommit {
			continue
		}

		if event.Commit.Operation != jetstream.CommitOperationCreate {
			continue
		}

		commit := *event.Commit
		post = appbsky.FeedPost{}
		if err := json.Unmarshal(commit.Record, &post); err != nil {
			log.Printf("error parsing appbsky.FeedPost: %v\n", err)
			continue
		}

		if post.Text == "" {
			continue
		}

		language, _ := detector.DetectLanguageOf(post.Text)
		langKey = `bsky-langs:detected:` + strings.ToLower(language.IsoCode639_1().String())

		if err := pipe.Incr(ctx, langKey).Err(); err != nil {
			log.Printf("failed incrementing lang key: %v\n", err)
		}

		count += 1
		if count%1000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Printf("failed to execute pipe\n")
			}
		}
	}

}
