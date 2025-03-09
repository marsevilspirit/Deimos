package server

import (
	"context"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	pb "github.com/marsevilspirit/marstore/server/serverpb"
	"github.com/marsevilspirit/marstore/store"
)

func TestClusterOf1(t *testing.T) { testServer(t, 1) }
func TestClusterOf3(t *testing.T) { testServer(t, 3) }

func testServer(t *testing.T, ns int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := make([]*Server, ns)

	send := func(msgs []raftpb.Message) {
		for _, m := range msgs {
			// t.Logf("m: %#v\n", m)
			ss[m.To-1].Node.Step(ctx, m)
		}
	}

	peers := make([]int64, ns)
	for i := int64(0); i < ns; i++ {
		peers[i] = i + 1
	}

	for i := int64(0); i < ns; i++ {
		n := raft.StartNode(i+1, peers, 10, 1)

		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()

		srv := &Server{
			Node:   n,
			Store:  store.New(),
			Send:   send,
			Save:   func(_ raftpb.HardState, _ []raftpb.Entry) {},
			Ticker: tk.C,
		}
		Start(srv)

		//TODO: randomize election timeout
		// then remove this sleep.
		time.Sleep(1 * time.Millisecond)

		ss[i] = srv
	}

	for i := 1; i <= 10; i++ {
		r := pb.Request{
			Method: "PUT",
			Id:     int64(1),
			Path:   "/foo",
			Val:    "bar",
		}
		j := rand.Intn(len(ss))
		t.Logf("ss = %d", j)
		resp, err := ss[j].Do(ctx, r)
		if err != nil {
			t.Fatal(err)
		}

		g, w := resp.Event.Node, &store.NodeExtern{
			Key:           "/foo",
			ModifiedIndex: uint64(i),
			CreatedIndex:  uint64(i),
			Value:         stringp("bar"),
		}

		if !reflect.DeepEqual(g, w) {
			t.Errorf("g = %#v,\n               w = %#v", g, w)
		}
	}

	time.Sleep(10 * time.Millisecond)

	var last any
	for i, sv := range ss {
		sv.Stop()
		g, _ := sv.Store.Get("/", true, true)
		if last != nil && !reflect.DeepEqual(last, g) {
			t.Errorf("server %d: Root = %#v, want %#v", i, g, last)
		}
		last = g
	}
}

func stringp(s string) *string {
	return &s
}
