#!/bin/sh
 go test ./wal \
 	./snap \
	./server \
 	./server/marshttp \
 	./raft \
	./wait \
 	./store
