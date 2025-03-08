package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	"github.com/marsevilspirit/marstore/server"
	"github.com/marsevilspirit/marstore/server/marshttp"
	"github.com/marsevilspirit/marstore/store"
)

var (
	fid     = flag.String("id", "0xBEEF", "The id of this server")
	timeout = flag.Duration("timeout", 10*time.Second, "Request Timeout")
	laddr   = flag.String("l", ":6618", "HTTP service address (e.g., ':6618')")

	peers = marshttp.Peers{}
)

func init() {
	flag.Var(peers, "peers", "your peers")
}

func main() {
	flag.Parse()

	id, err := strconv.ParseInt(*fid, 0, 64)
	if err != nil {
		log.Fatal(err)
	}

	if peers.Pick(id) == "" {
		log.Fatalf("%#x=<addr> must be specified in peers", id)
	}

	n := raft.StartNode(id, peers.Ids(), 10, 1)

	tk := time.NewTicker(100 * time.Millisecond)

	ctx, _ := context.WithCancel(context.Background())

	n.Campaign(ctx)

	s := &server.Server{
		Store:  store.New(),
		Node:   n,
		Save:   func(st raftpb.HardState, ents []raftpb.Entry) {}, // TODO: use wal
		Send:   marshttp.Sender(peers),
		Ticker: tk.C,
	}

	server.Start(s)

	h := &marshttp.Handler{
		Timeout: *timeout,
		Server:  s,
	}
	http.Handle("/", h)
	log.Fatal(http.ListenAndServe(*laddr, nil))
}
