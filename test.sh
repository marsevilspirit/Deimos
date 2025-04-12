#!/bin/sh
 go test ./wal \
 	./snap \
	./server \
 	./server/deimos_http \
 	./raft \
	./wait \
 	./store \
	./proxy
