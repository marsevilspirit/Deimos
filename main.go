package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/server"
	"github.com/marsevilspirit/marstore/server/marshttp"
	"github.com/marsevilspirit/marstore/store"
	"github.com/marsevilspirit/marstore/wal"
)

var (
	fid     = flag.String("id", "0x1", "The id of this server")
	timeout = flag.Duration("timeout", 10*time.Second, "Request Timeout")
	laddr   = flag.String("l", ":6618", "HTTP service address (e.g., ':6618')")
	dir     = flag.String("data-dir", "", "Directry to store wal files and snapshot files")
	peers   = marshttp.Peers{}
)

func init() {
	peers.Set("0x1=localhost:6618")
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

	// n := raft.StartNode(id, peers.Ids(), 10, 1)

	if *dir == "" {
		*dir = fmt.Sprintf("%v", *fid)
		log.Printf("main: no data-dir is given, use default data-dir ./%s", *dir)
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatal(err)
	}

	waldir := path.Join(*dir, "wal")

	var w *wal.WAL
	var n raft.Node
	if wal.Exist(waldir) {
		// TODO: check snapshot; not open from zero
		w, err = wal.OpenFromIndex(waldir, 0)
		if err != nil {
			log.Fatal(err)
		}
		// TODO: save/recovery nodeID?
		_, st, ents, err := w.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		// WARN: warn nil
		n = raft.RestartNode(id, peers.Ids(), 10, 1, nil, st, ents)
	} else {
		w, err = wal.Create(waldir)
		if err != nil {
			log.Fatal(err)
		}
		n = raft.StartNode(id, peers.Ids(), 10, 1)
	}

	tk := time.NewTicker(100 * time.Millisecond)

	ctx, _ := context.WithCancel(context.Background())

	n.Campaign(ctx)

	s := &server.Server{
		Store:  store.New(),
		Node:   n,
		Save:   w.Save,
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
