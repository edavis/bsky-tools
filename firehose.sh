#!/usr/bin/env bash

websocat -v -bE ws-l:127.0.0.1:9060 wss://bsky.network/xrpc/com.atproto.sync.subscribeRepos
