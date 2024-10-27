package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/redis/go-redis/v9"
)

const PlcExportUrl = `https://plc.directory/export`
const PlcOpsCountKey = `dev.edavis.muninsky.plc_ops`

func main() {
	ctx := context.Background()
	client := http.DefaultClient

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	req, err := http.NewRequestWithContext(ctx, "GET", PlcExportUrl, nil)
	if err != nil {
		panic(err)
	}

	var lastCursor string
	var cursor string
	cursor = syntax.DatetimeNow().String()

	q := req.URL.Query()
	q.Add("count", "1000")
	req.URL.RawQuery = q.Encode()

	for {
		q := req.URL.Query()
		if cursor != "" {
			q.Set("after", cursor)
		}
		req.URL.RawQuery = q.Encode()

		log.Printf("requesting %s\n", req.URL.String())
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("error doing PLC request: %v\n", err)
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("PLC request failed status=%d\n", resp.StatusCode)
		}

		respBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response body: %v\n", err)
		}

		lines := strings.Split(string(respBytes), "\n")
		if len(lines) == 0 || (len(lines) == 1 && len(lines[0]) == 0) {
			time.Sleep(5 * time.Second)
			continue
		}

		var opCount int64
		for _, l := range lines {
			if len(l) < 2 {
				break
			}

			var op map[string]interface{}
			err = json.Unmarshal([]byte(l), &op)
			if err != nil {
				log.Printf("error decoding JSON: %v\n", err)
			}

			var ok bool
			cursor, ok = op["createdAt"].(string)
			if !ok {
				log.Printf("missing createdAt")
			}

			if cursor == lastCursor {
				continue
			}

			opCount += 1
			lastCursor = cursor
		}

		log.Printf("fetched %d operations", opCount)
		if _, err := rdb.IncrBy(ctx, PlcOpsCountKey, opCount).Result(); err != nil {
			log.Printf("error incrementing op count in redis: %v\n", err)
		}

		time.Sleep(5 * time.Second)
	}
}
