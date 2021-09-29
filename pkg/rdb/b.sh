#!/bin/bash

export CGO_CFLAGS="-I/opt/homebrew/include"
export CGO_LDFLAGS="-L/opt/homebrew/lib/ -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
# GOOS=darwin GOARCH=arm64 go get github.com/tecbot/gorocksdb
GOOS=darwin GOARCH=arm64 go run pkg/rocksdb/check.go
# /Users/wenlinwu/src/nebula-stresser/pkg/rocksdb/check.go

