package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	"github.com/lmittmann/tint"

	"github.com/marsevilspirit/deimos/pkg"
	flagtypes "github.com/marsevilspirit/deimos/pkg/flags"
	"github.com/marsevilspirit/deimos/pkg/transport"
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

	version = "0.0.1-alpha"
)

var (
	name         = flag.String("name", "default", "Unique human-readable name for this node")
	timeout      = flag.Duration("timeout", 10*time.Second, "Request Timeout")
	dir          = flag.String("data-dir", "", "Path to the data directory")
	snapCount    = flag.Int64("snapshot-count", server.DefaultSnapCount, "Number of committed transactions to trigger a snapshot")
	printVersion = flag.Bool("version", false, "Print the version and exit")

	cluster   = &server.Cluster{}
	cors      = &pkg.CORSInfo{}
	proxyFlag = new(flagtypes.Proxy)

	clientTLSInfo = transport.TLSInfo{}
	peerTLSInfo   = transport.TLSInfo{}
)

func init() {
	flag.Var(cluster, "bootstrap-config", "Initial cluster configuration for bootstrapping")
	cluster.Set("default=http://localhost:2380,default=http://localhost:7001")

	flag.Var(flagtypes.NewURLsValue("http://localhost:2380,http://localhost:7001"), "advertise-peer-urls", "List of this member's peer URLs to advertise to the rest of the cluster")
	flag.Var(flagtypes.NewURLsValue("http://localhost:2379,http://localhost:4001"), "advertise-client-urls", "List of this member's client URLs to advertise to the rest of the cluster")
	flag.Var(flagtypes.NewURLsValue("http://localhost:2380,http://localhost:7001"), "listen-peer-urls", "List of this URLs to listen on for peer traffic")
	flag.Var(flagtypes.NewURLsValue("http://localhost:2379,http://localhost:4001"), "listen-client-urls", "List of this URLs to listen on for client traffic")

	flag.Var(cors, "cors", "Comma-separated white list of origins for CORS (cross-origin resource sharing).")

	flag.Var(proxyFlag, "proxy", fmt.Sprintf("Valid values include %s", strings.Join(flagtypes.ProxyValues, ", ")))
	proxyFlag.Set(flagtypes.ProxyValueOff)

	// tls
	flag.StringVar(&clientTLSInfo.CAFile, "ca-file", "", "Path to the client server TLS CA file.")
	flag.StringVar(&clientTLSInfo.CertFile, "cert-file", "", "Path to the client server TLS cert file.")
	flag.StringVar(&clientTLSInfo.KeyFile, "key-file", "", "Path to the client server TLS key file.")

	flag.StringVar(&peerTLSInfo.CAFile, "peer-ca-file", "", "Path to the peer server TLS CA file.")
	flag.StringVar(&peerTLSInfo.CertFile, "peer-cert-file", "", "Path to the peer server TLS cert file.")
	flag.StringVar(&peerTLSInfo.KeyFile, "peer-key-file", "", "Path to the peer server TLS key file.")
}

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println("deimos version", version)
		os.Exit(0)
	}

	slog.SetDefault(slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:     slog.LevelInfo,
			AddSource: false,
		}),
	))

	pkg.SetFlagsFromEnv(flag.CommandLine)

	if string(*proxyFlag) == flagtypes.ProxyValueOff {
		startDeimos()
	} else {
		startProxy()
	}

	// Block indefinitely
	<-make(chan struct{})
}

// startDeimos launches the deimos server and HTTP handlers for client/server communication.
func startDeimos() {
	self := cluster.FindName(*name)
	if self == nil {
		log.Fatalf("deimos: no member with name=%q exists", *name)
	}

	if self.ID == raft.None {
		log.Fatalf("deimos: cannot use None(%d) as member id", raft.None)
	}

	if *snapCount <= 0 {
		log.Fatalf("deimos: snapshot-count must be greater than 0: snapshot-count=%d", *snapCount)
	}

	if *dir == "" {
		*dir = fmt.Sprintf("%v_deimos_data", self.ID)
		log.Printf("main: no data-dir is given, uing default data-dir ./%s", *dir)
	}
	if err := os.MkdirAll(*dir, privateDirMode); err != nil {
		log.Fatalf("main: cannot create data directory: %v", err)
	}

	snapdir := path.Join(*dir, "snap")
	if err := os.MkdirAll(snapdir, privateDirMode); err != nil {
		log.Fatalf("deimos: cannot create snapshot directory: %v", err)
	}
	snapshotter := snap.New(snapdir)

	waldir := path.Join(*dir, "wal")
	var w *wal.WAL
	var n raft.Node
	var err error
	st := store.New()

	if !wal.Exist(waldir) {
		slog.Debug("wal isn't exist")
		w, err = wal.Create(waldir)
		if err != nil {
			log.Fatal(err)
		}
		n = raft.StartNode(self.ID, cluster.IDs(), 10, 1)
	} else {
		slog.Debug("wal exist")
		var index int64
		snapshot, err := snapshotter.Load()
		if err != nil && err != snap.ErrNoSnapshot {
			log.Fatal(err)
		}
		if snapshot != nil {
			slog.Info("deimos: restart from snapshot at", "index", snapshot.Index)
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
		// TODO: save/recovery nodeID?
		if wid != 0 {
			log.Fatalf("unexpected nodeid %d: nodeid should always be zero until we save nodeid into wal", wid)
		}
		n = raft.RestartNode(self.ID, cluster.IDs(), 10, 1, snapshot, st, ents)
	}

	pt, err := transport.NewTransport(peerTLSInfo)
	if err != nil {
		log.Fatal(err)
	}

	cls := server.NewClusterStore(st, *cluster)

	acurls, err := pkg.URLsFromFlags(flag.CommandLine, "advertise-client-urls", "addr", clientTLSInfo)
	if err != nil {
		log.Fatal(err.Error())
	}

	s := &server.DeimosServer{
		Name:       *name,
		ClientURLs: acurls,
		Store:      st,
		Node:       n,
		Storage: struct {
			*wal.WAL
			*snap.Snapshotter
		}{w, snapshotter},
		Send:         server.Sender(pt, cls),
		Ticker:       time.Tick(100 * time.Millisecond),
		SyncTicker:   time.Tick(500 * time.Millisecond),
		SnapCount:    *snapCount,
		ClusterStore: cls,
	}

	s.Start()

	ch := &pkg.CORSHandler{
		Handler: deimos_http.NewClientHandler(s, cls, *timeout),
		Info:    cors,
	}
	ph := deimos_http.NewPeerHandler(s)

	lpurls, err := pkg.URLsFromFlags(flag.CommandLine, "listen-peer-urls", "peer-bind-addr", peerTLSInfo)
	if err != nil {
		log.Fatal(err.Error())
	}

	for _, u := range lpurls {
		l, err := transport.NewListener(u.Host, peerTLSInfo)
		if err != nil {
			log.Fatal(err)
		}

		// Start the peer server in a goroutine
		urlStr := u.String()
		go func() {
			log.Print("Listening for peers on ", urlStr)
			log.Fatal(http.Serve(l, ph))
		}()
	}

	lcurls, err := pkg.URLsFromFlags(flag.CommandLine, "listen-client-urls", "bind-addr", clientTLSInfo)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Start a client server goroutine for each listen address
	for _, u := range lcurls {
		l, err := transport.NewListener(u.Host, clientTLSInfo)
		if err != nil {
			log.Fatal(err)
		}

		urlStr := u.String()
		go func() {
			log.Print("Listening for client requests on ", urlStr)
			log.Fatal(http.Serve(l, ch))
		}()
	}
}

// startProxy launches an HTTP proxy for client communication which proxies to other deimos nodes.
func startProxy() {
	pt, err := transport.NewTransport(clientTLSInfo)
	if err != nil {
		log.Fatal(err)
	}

	ph, err := proxy.NewHandler(pt, (*cluster).PeerURLs())
	if err != nil {
		log.Fatal(err)
	}

	ph = &pkg.CORSHandler{
		Handler: ph,
		Info:    cors,
	}

	if string(*proxyFlag) == flagtypes.ProxyValueReadonly {
		ph = proxy.NewReadonlyHandler(ph)
	}

	lcurls, err := pkg.URLsFromFlags(flag.CommandLine, "listen-client-urls", "bind-addr", clientTLSInfo)
	if err != nil {
		log.Fatal(err.Error())
	}
	// Start a proxy server goroutine for each listen address
	for _, u := range lcurls {
		l, err := transport.NewListener(u.Host, clientTLSInfo)
		if err != nil {
			log.Fatal(err)
		}

		host := u.Host
		go func() {
			log.Print("Listening for client requests on ", host)
			log.Fatal(http.Serve(l, ph))
		}()
	}
}
