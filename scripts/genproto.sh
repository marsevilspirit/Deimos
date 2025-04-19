#!/bin/sh -e
#
# Generate all Deimos protobuf bindings.
# Run from repository root.
#

DIRS="./wal/walpb ./server/serverpb ./snap/snappb ./raft/raftpb"

if ! protoc --version > /dev/null; then
 	echo "could not find protoc, is it installed + in PATH?"
 	exit 255
fi

go get github.com/gogo/protobuf@v1.3.2

for dir in ${DIRS}; do
	pushd ${dir}
		protoc -I=.:$GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.2/ --gogo_out=. *.proto
	popd
done
