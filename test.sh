#!/bin/sh
 go test ./wal \
 	./snap \
 	./server/... \
 	./raft \
 	./store
