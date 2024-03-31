#!/usr/bin/env bash

websocat -v -bE -B 1048576 \
  ws-l:127.0.0.1:9060 wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos?cursor=0
