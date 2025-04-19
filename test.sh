#!/bin/sh
go test \
	./client \
	./pkg \
	./pkg/flags \
	./pkg/transport \
	./proxy \
	./raft \
	./server \
	./server/deimos_http \
	./snap \
	./store \
	./wait \
	./wal
