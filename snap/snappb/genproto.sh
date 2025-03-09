set -e
export GOPATH=$HOME/go
protoc -I=.:$GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.2/ --gogo_out=. *.proto
