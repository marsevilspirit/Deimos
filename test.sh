#!/bin/sh
 go test ./wal \
 	./snap \
 	./server/... \
 	./raft \
	./wait \
 	./store
