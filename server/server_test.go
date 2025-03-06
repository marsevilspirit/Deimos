package server

import (
	"context"
	"reflect"
	"testing"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	pb "github.com/marsevilspirit/marstore/server/serverpb"
	"github.com/marsevilspirit/marstore/store"
)

func TestServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n := raft.StartNode(1, []int64{1}, 10, 1)
	n.Campaign(ctx)

	srv := &Server{
		Node:  n,
		Store: store.New(),
		Send:  func(_ []raftpb.Message) {},
		Save:  func(_ raftpb.HardState, _ []raftpb.Entry) {},
	}
	Start(srv)
	defer srv.Stop()

	r := pb.Request{
		Method: "PUT",
		Id:     1,
		Path:   "/foo",
		Val:    "bar",
	}
	resp, err := srv.Do(ctx, r)
	if err != nil {
		t.Fatal(err)
	}

	g, w := resp.Event.Node, &store.NodeExtern{
		Key:           "/foo",
		ModifiedIndex: 1,
		CreatedIndex:  1,
		Value:         stringp("bar"),
	}

	if !reflect.DeepEqual(g, w) {
		t.Error("value:", *g.Value)
		t.Errorf("g = %+v, w = %+v", g, w)
	}
}

func stringp(s string) *string {
	return &s
}
