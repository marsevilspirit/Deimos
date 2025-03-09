package marshttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	"github.com/marsevilspirit/marstore/server"
	"github.com/marsevilspirit/marstore/store"
)

func nopSave(st raftpb.HardState, ents []raftpb.Entry) {}
func nopSend(m []raftpb.Message)                       {}

func TestSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := store.New()

	n := raft.StartNode(1, []int64{1}, 10, 1)
	n.Campaign(ctx)

	srv := &server.Server{
		Node:  n,
		Store: st,
		Send:  server.SendFunc(nopSend),
		Save:  func(st raftpb.HardState, ents []raftpb.Entry) {},
	}
	server.Start(srv)
	defer srv.Stop()

	h := &Handler{
		Timeout: time.Hour,
		Server:  srv,
	}

	s := httptest.NewServer(h)
	defer s.Close()

	resp, err := http.PostForm(s.URL+"/v2/keys/foo", url.Values{"value": {"bar"}})
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 201 {
		t.Errorf("StatusCode = %d, expected %d", 201, resp.StatusCode)
	}

	g := new(store.Event)
	if err := json.NewDecoder(resp.Body).Decode(&g); err != nil {
		t.Fatal(err)
	}

	w := &store.NodeExtern{
		Key:           "/foo/1",
		Value:         stringp("bar"),
		ModifiedIndex: 1,
		CreatedIndex:  1,
	}
	if !reflect.DeepEqual(g.Node, w) {
		t.Errorf("value = %+v, \nwant %+v", *g.Node.Value, *w.Value)
		t.Errorf("g = %+v, \nwant %+v", g.Node, w)
	}
}

func stringp(s string) *string { return &s }
