#!/usr/bin/env bash

cursor="$(redis-cli <<<'get bsky-tools:firehose:subscribe-repos:seq')"

websocat -v -bE \
  ws-l:127.0.0.1:9060 \
  "wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=${cursor}"
