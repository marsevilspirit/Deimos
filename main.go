package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/marsevilspirit/deimos/proxy"
	"github.com/marsevilspirit/deimos/raft"
	"github.com/marsevilspirit/deimos/server"
	"github.com/marsevilspirit/deimos/server/deimos_http"
	"github.com/marsevilspirit/deimos/snap"
	"github.com/marsevilspirit/deimos/store"
	"github.com/marsevilspirit/deimos/wal"
)

const (
	// the owner can make/remove files inside the directory
	privateDirMode = 0700

	proxyFlagValueOff      = "off"
	proxyFlagValueReadonly = "readonly"
	proxyFlagValueOn       = "on"
)

var (
	fid       = flag.String("id", "0x1", "The ID of this server")
	timeout   = flag.Duration("timeout", 10*time.Second, "Request Timeout")
	paddr     = flag.String("peer-bind-addr", ":6666", "Peer service address (e.g., ':6666')")
	dir       = flag.String("data-dir", "", "Directry to store wal files and snapshot files")
	snapCount = flag.Int64("snapshot-count", server.DefaultSnapCount, "Number of committed transactions to trigger a snapshot")

	peers     = &deimos_http.Peers{}
	addrs     = &Addrs{}
	proxyFlag = new(ProxyFlag)

	proxyFlagValues = []string{
		proxyFlagValueOff,
		proxyFlagValueReadonly,
		proxyFlagValueOn,
	}
)

func init() {
	flag.Var(peers, "peers", "your peers")
	flag.Var(addrs, "bind-addr", "List of HTTP service addresses (e.g., '127.0.0.1:4001,10.0.0.1:8080')")
	flag.Var(proxyFlag, "proxy", fmt.Sprintf("Valid values include %s", strings.Join(proxyFlagValues, ", ")))
	peers.Set("0x1=localhost:8080")
	addrs.Set("127.0.0.1:4001")
	proxyFlag.Set(proxyFlagValueOff)
}

func main() {
	flag.Parse()

	setFlagsFromEnv()

	if string(*proxyFlag) == proxyFlagValueOff {
		startDeimos()
	} else {
		startProxy()
	}

	// Block indefinitely
	<-make(chan struct{})
}

// startDeimos launches the etcd server and HTTP handlers for client/server communication.
func startDeimos() {
	id, err := strconv.ParseInt(*fid, 0, 64)
	if err != nil {
		log.Fatal(err)
	}
	if id == raft.None {
		log.Fatalf("Deimos: cannot use None(%d) as Deimos server id", raft.None)
	}

	if peers.Pick(id) == "" {
		log.Fatalf("%#x=<addr> must be specified in peers", id)
	}

	if *snapCount <= 0 {
		log.Fatalf("etcd: snapshot-count must be greater than 0: snapshot-count=%d", *snapCount)
	}

	if *dir == "" {
		*dir = fmt.Sprintf("%v_deimos_data", *fid)
		log.Printf("main: no data-dir is given, uing default data-dir ./%s", *dir)
	}
	if err := os.MkdirAll(*dir, privateDirMode); err != nil {
		log.Fatalf("main: cannot create data directory: %v", err)
	}

	snapdir := path.Join(*dir, "snap")
	if err := os.MkdirAll(snapdir, privateDirMode); err != nil {
		log.Fatalf("etcd: cannot create snapshot directory: %v", err)
	}
	snapshotter := snap.New(snapdir)

	waldir := path.Join(*dir, "wal")
	var w *wal.WAL
	var n raft.Node
	st := store.New()

	if !wal.Exist(waldir) {
		w, err = wal.Create(waldir)
		if err != nil {
			log.Fatal(err)
		}
		n = raft.StartNode(id, peers.IDs(), 10, 1)
	} else {
		var index int64
		snapshot, err := snapshotter.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatal(err)
		}
		if snapshot != nil {
			log.Printf("etcd: restart from snapshot at index %d", snapshot.Index)
			st.Recovery(snapshot.Data)
			index = snapshot.Index
		}

		// restart a node from previous wal
		if w, err = wal.OpenAtIndex(waldir, index); err != nil {
			log.Fatal(err)
		}
		wid, st, ents, err := w.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		// TODO(xiangli): save/recovery nodeID?
		if wid != 0 {
			log.Fatalf("unexpected nodeid %d: nodeid should always be zero until we save nodeid into wal", wid)
		}
		n = raft.RestartNode(id, peers.IDs(), 10, 1, snapshot, st, ents)
	}

	s := &server.DeimosServer{
		Store: st,
		Node:  n,
		Storage: struct {
			*wal.WAL
			*snap.Snapshotter
		}{w, snapshotter},
		Send:       deimos_http.Sender(*peers),
		Ticker:     time.Tick(100 * time.Millisecond),
		SyncTicker: time.Tick(500 * time.Millisecond),
		SnapCount:  *snapCount,
	}

	s.Start()

	ch := deimos_http.NewClientHandler(s, *peers, *timeout)
	ph := deimos_http.NewPeerHandler(s)

	// Start the peer server in a goroutine
	go func() {
		log.Print("Listening for peers on ", *paddr)
		log.Fatal(http.ListenAndServe(*paddr, ph))
	}()

	// Start a client server goroutine for each listen address
	for _, addr := range *addrs {
		addr := addr
		go func() {
			log.Print("Listening for client requests on ", addr)
			log.Fatal(http.ListenAndServe(addr, ch))
		}()
	}
}

// startProxy launches an HTTP proxy for client communication which proxies to other etcd nodes.
func startProxy() {
	ph, err := proxy.NewHandler((*peers).Endpoints())
	if err != nil {
		log.Fatal(err)
	}

	if string(*proxyFlag) == proxyFlagValueReadonly {
		ph = proxy.NewReadonlyHandler(ph)
	}

	// Start a proxy server goroutine for each listen address
	for _, addr := range *addrs {
		addr := addr
		go func() {
			log.Print("Listening for client requests on ", addr)
			log.Fatal(http.ListenAndServe(addr, ph))
		}()
	}
}

// Addrs implements the flag.Value interface to allow users to define multiple
// listen addresses on the command-line
type Addrs []string

// Set parses a command line set of listen addresses, formatted like:
// 127.0.0.1:7001,10.1.1.2:80
func (as *Addrs) Set(s string) error {
	parsed := make([]string, 0)
	for _, in := range strings.Split(s, ",") {
		a := strings.TrimSpace(in)
		if err := validateAddr(a); err != nil {
			return err
		}
		parsed = append(parsed, a)
	}
	if len(parsed) == 0 {
		return errors.New("no valid addresses given!")
	}
	*as = parsed
	return nil
}

func (as *Addrs) String() string {
	return strings.Join(*as, ",")
}

// validateAddr ensures that the provided string is a valid address. Valid
// addresses are of the form IP:port.
// Returns an error if the address is invalid, else nil.
func validateAddr(s string) error {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return errors.New("bad format in address specification")
	}
	if net.ParseIP(parts[0]) == nil {
		return errors.New("bad IP in address specification")
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return errors.New("bad port in address specification")
	}
	return nil
}

// ProxyFlag implements the flag.Value interface.
type ProxyFlag string

// Set verifies the argument to be a valid member of proxyFlagValues
// before setting the underlying flag value.
func (pf *ProxyFlag) Set(s string) error {
	for _, v := range proxyFlagValues {
		if s == v {
			*pf = ProxyFlag(s)
			return nil
		}
	}

	return errors.New("invalid value")
}

func (pf *ProxyFlag) String() string {
	return string(*pf)
}

// setFlagsFromEnv parses all registered flags in the global flagset,
// and if they are not already set it attempts to set their values from
// environment variables. Environment variables take the name of the flag but
// are UPPERCASE, have the prefix "DEIMOS_", and any dashes are replaced by
// underscores - for example: some-flag => DEIMOS_SOME_FLAG
func setFlagsFromEnv() {
	alreadySet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		alreadySet[f.Name] = true
	})
	flag.VisitAll(func(f *flag.Flag) {
		if !alreadySet[f.Name] {
			key := "DEIMOS_" + strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
			val := os.Getenv(key)
			if val != "" {
				flag.Set(f.Name, val)
			}
		}

	})
}
