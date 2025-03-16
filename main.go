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

const (
	// the owner can make/remove files inside the directory
	privateDirMode = 0700
)

var (
	fid     = flag.String("id", "0x1", "The id of this server")
	timeout = flag.Duration("timeout", 10*time.Second, "Request Timeout")
	laddr   = flag.String("l", ":9927", "HTTP service address (e.g., ':9927')")
	dir     = flag.String("data-dir", "", "Directry to store wal files and snapshot files")
	peers   = &marshttp.Peers{}
)

func init() {
	peers.Set("0x1=localhost:9927")
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
		*dir = fmt.Sprintf("%v_mars_data", *fid)
		log.Printf("main: no data-dir is given, uing default data-dir ./%s", *dir)
	}
	if err := os.MkdirAll(*dir, 0700); err != nil {
		log.Fatalf("main: cannot create data directory: %v", err)
	}

	n, w := startRaft(id, peers.Ids(), path.Join(*dir, "wal"))

	tk := time.NewTicker(100 * time.Millisecond)

	ctx, _ := context.WithCancel(context.Background())

	n.Campaign(ctx)

	s := &server.Server{
		Store:  store.New(),
		Node:   n,
		Save:   w.Save,
		Send:   marshttp.Sender(*peers),
		Ticker: tk.C,
	}

	server.Start(s)

	h := &marshttp.Handler{
		Timeout: *timeout,
		Server:  s,
		Peers:   *peers,
	}
	http.Handle("/", h)
	log.Fatal(http.ListenAndServe(*laddr, nil))
}

func startRaft(id int64, perrIds []int64, waldir string) (raft.Node, *wal.WAL) {
	if !wal.Exist(waldir) {
		w, err := wal.Create(waldir)
		if err != nil {
			log.Fatal(err)
		}
		n := raft.StartNode(id, perrIds, 10, 1)
		return n, w
	}
	// restart a node from previous wal
	// TODO: check snapshot; not open from zero
	w, err := wal.OpenFromIndex(waldir, 0)
	if err != nil {
		log.Fatal(err)
	}
	wid, st, ents, err := w.ReadAll()
	// TODO: save/recovery nodeID?
	if wid != 0 {
		log.Fatalf("unexpected nodeid %d: nodeid should always be zero until we save nodeid into wal", wid)
	}
	if err != nil {
		log.Fatal(err)
	}
	// WARN: snapshot replaces nil
	n := raft.RestartNode(id, perrIds, 10, 1, nil, st, ents)
	return n, w
}
