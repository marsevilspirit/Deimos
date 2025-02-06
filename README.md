# m_raft

### protobuf command
```shell
export GOPATH=$HOME/go

protoc -I . -I $GOPATH/pkg/mod/github.com/gogo/protobuf@v1.3.2/ --gogo_out=. entry.proto
```
