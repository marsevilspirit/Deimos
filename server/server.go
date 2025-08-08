package server

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/marsevilspirit/deimos/pkg/types"
	"github.com/marsevilspirit/deimos/raft"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	pb "github.com/marsevilspirit/deimos/server/serverpb"
	"github.com/marsevilspirit/deimos/store"
	"github.com/marsevilspirit/deimos/wait"
)

const (
	defaultSyncTimeout = time.Second
	DefaultSnapCount   = 10000
	// TODO: calculated based on heartbeat interval
	defaultPublishRetryInterval = 5 * time.Second
)

var (
	ErrUnknownMethod = errors.New("Deimos server: unknown method")
	ErrStopped       = errors.New("Deimos server: server stopped")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type SendFunc func(m []raftpb.Message)
type SaveFunc func(st raftpb.HardState, ents []raftpb.Entry)

type Response struct {
	Event   *store.Event
	Watcher store.Watcher
	err     error
}

type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	Save(st raftpb.HardState, ents []raftpb.Entry)
	// SaveSnap function saves snapshot to the underlying stable storage.
	SaveSnap(snap raftpb.Snapshot)

	// TODO: WAL should be able to control cut itself. After implement self-controled cut,
	// remove it in this interface.
	// Cut cuts out a new wal file for saving new state and entries.
	Cut() error
}

type Server interface {
	// Start performs any initialization of the Server necessary for it to
	// begin serving requests. It must be called before Do or Process.
	// Start must be non-blocking; any long-running server functionality
	// should be implemented in goroutines.
	Start()
	// Stop terminates the Server and performs any necessary finalization.
	// Do and Process cannot be called after Stop has been invoked.
	Stop()
	// Do takes a request and attempts to fulfil it, returning a Response.
	Do(ctx context.Context, r pb.Request) (Response, error)
	// Process takes a raft message and applies it to the server's raft state
	// machine, respecting any timeout of the given context.
	Process(ctx context.Context, m raftpb.Message) error
}

type RaftTimer interface {
	Index() int64
	Term() int64
}

type DeimosServer struct {
	w    wait.Wait
	done chan struct{}

	Name       string
	ClientURLs types.URLs

	Node  raft.Node
	Store store.Store

	msgsc chan raftpb.Message

	// Send specifies the send function for sending msgs to members. Send
	// MUST NOT block. It is okay to drop messages, since clients should
	// timeout and reissue their messages.  If Send is nil, server will
	// panic.
	Send SendFunc

	Storage Storage

	Ticker     <-chan time.Time
	SyncTicker <-chan time.Time

	SnapCount int64 // number of entries to trigger a snapshot

	// Cache of the latest raft index and raft term the server has seen
	raftIndex    int64
	raftTerm     int64
	ClusterStore ClusterStore
}

// Start prepares and starts server in a new goroutine. It is no longer safe to
// modify a servers fields after it has been sent to Start.
// It also a goroutine to publish its server infomation.
func (s *DeimosServer) Start() {
	s.start()
	go s.publish(defaultPublishRetryInterval)
}

func (s *DeimosServer) start() {
	if s.SnapCount == 0 {
		log.Printf("server: set snapshot count to default %d", DefaultSnapCount)
		s.SnapCount = DefaultSnapCount
	}
	s.w = wait.New()
	s.done = make(chan struct{})
	// TODO: if this is an empty log, writes all peer infos
	// into the first entry
	go s.run()
}

func (s *DeimosServer) Process(ctx context.Context, m raftpb.Message) error {
	return s.Node.Step(ctx, m)
}

func (s *DeimosServer) run() {
	var syncC <-chan time.Time
	// snapi indicates the index of the last submitted snapshot request
	var snapi, appliedi int64
	for {
		select {
		case <-s.Ticker:
			s.Node.Tick()
		case rd := <-s.Node.Ready():
			s.Storage.Save(rd.HardState, rd.Entries)
			s.Storage.SaveSnap(rd.Snapshot)
			s.Send(rd.Messages)

			// TODO: do this in the background,
			// but take care to apply entries in a single goroutine,
			// and not race them.
			// TODO: apply configuration change into ClusterStore.
			for _, e := range rd.CommittedEntries {
				if e.Data == nil {
					slog.Debug("Server: TODO e.Data is nil")
					continue
				}
				switch e.Type {
				case raftpb.EntryNormal:
					var r pb.Request
					if err := r.Unmarshal(e.Data); err != nil {
						panic("TODO: this is bad, what do we do about it?")
					}
					s.w.Trigger(r.ID, s.apply(r))
				case raftpb.EntryConfChange:
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(e.Data); err != nil {
						panic("TODO: this is bad, what do we do about it?")
					}
					s.Node.ApplyConfChange(cc)
					s.w.Trigger(cc.ID, nil)
				default:
					panic("unexpected entry type")
				}
				atomic.StoreInt64(&s.raftIndex, e.Index)
				atomic.StoreInt64(&s.raftTerm, e.Term)
				appliedi = e.Index
			}

			if rd.Snapshot.Index > snapi {
				snapi = rd.Snapshot.Index
			}

			// recover from snapshot if it is more updated than current applied
			if rd.Snapshot.Index > appliedi {
				if err := s.Store.Recovery(rd.Snapshot.Data); err != nil {
					panic("TODO: this is bad, what do we do about it?")
				}
				appliedi = rd.Snapshot.Index
			}

			if appliedi-snapi > s.SnapCount {
				s.snapshot()
				snapi = appliedi
			}

			if rd.SoftState != nil {
				if rd.RaftState == raft.StateLeader {
					syncC = s.SyncTicker
				} else {
					syncC = nil
				}
				if rd.SoftState.ShouldStop {
					s.Stop()
					return
				}
			}
		case <-syncC:
			s.sync(defaultSyncTimeout)
		case <-s.done:
			return
		}
	}
}

// Stop stops the server, and shuts down the running goroutine.
// Stop should be called after a Start(s), otherwise it block forever.
func (s *DeimosServer) Stop() {
	s.Node.Stop()
	close(s.done)
}

// Do interprets r and performs an operation on s.Store according to r.Method
// and other fields. If r.Method is "POST", "PUT", "DELETE", or a "GET" with
// Quorum == true, r will be sent through consensus before performing its
// respective operation. Do will block until an action is performed or there is
// an error.
func (s *DeimosServer) Do(ctx context.Context, r pb.Request) (Response, error) {
	if r.ID == 0 {
		panic("r.Id cannot be 0")
	}

	if r.Method == "GET" && r.Quorum {
		r.Method = "QGET"
	}

	switch r.Method {
	case "POST", "PUT", "DELETE", "QGET":
		data, err := r.Marshal()
		if err != nil {
			return Response{}, err
		}
		ch := s.w.Register(r.ID)
		s.Node.Propose(ctx, data)
		select {
		case x := <-ch:
			resp := x.(Response)
			return resp, resp.err
		case <-ctx.Done():
			s.w.Trigger(r.ID, nil) // GC wait
			return Response{}, ctx.Err()
		case <-s.done:
			return Response{}, ErrStopped
		}
	case "GET":
		switch {
		case r.Wait:
			wc, err := s.Store.Watch(r.Path, r.Recursive, false, r.Since)
			if err != nil {
				return Response{}, err
			}
			return Response{Watcher: wc}, nil
		default:
			// Check if we can serve this read with lease
			if s.CanReadWithLease() {
				// Serve read directly with lease guarantee
				ev, err := s.Store.Get(r.Path, r.Recursive, r.Sorted)
				if err != nil {
					return Response{}, err
				}
				return Response{Event: ev}, nil
			} else if s.Node == nil || s.w == nil {
				// Server not fully initialized, serve directly from store
				ev, err := s.Store.Get(r.Path, r.Recursive, r.Sorted)
				if err != nil {
					return Response{}, err
				}
				return Response{Event: ev}, nil
			} else {
				// Fallback to consensus-based read (convert to QGET)
				r.Method = "QGET"
				data, err := r.Marshal()
				if err != nil {
					return Response{}, err
				}
				ch := s.w.Register(r.ID)
				s.Node.Propose(ctx, data)
				select {
				case x := <-ch:
					resp := x.(Response)
					return resp, resp.err
				case <-ctx.Done():
					s.w.Trigger(r.ID, nil) // GC wait
					return Response{}, ctx.Err()
				case <-s.done:
					return Response{}, ErrStopped
				}
			}
		}
	default:
		return Response{}, ErrUnknownMethod
	}
}

func (s *DeimosServer) AddNode(ctx context.Context, id int64, context []byte) error {
	cc := raftpb.ConfChange{
		ID:      GenID(),
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: context,
	}
	return s.configure(ctx, cc)
}

func (s *DeimosServer) RemoveNode(ctx context.Context, id int64) error {
	cc := raftpb.ConfChange{
		ID:     GenID(),
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: id,
	}
	return s.configure(ctx, cc)
}

// Implement the RaftTimer interface
func (s *DeimosServer) Index() int64 {
	return atomic.LoadInt64(&s.raftIndex)
}

func (s *DeimosServer) Term() int64 {
	return atomic.LoadInt64(&s.raftTerm)
}

// configure sends configuration change through consensus then performs it.
// It will block until the change is performed or there is an error.
func (s *DeimosServer) configure(ctx context.Context, cc raftpb.ConfChange) error {
	ch := s.w.Register(cc.ID)
	if err := s.Node.ProposeConfChange(ctx, cc); err != nil {
		log.Printf("configure error: %v", err)
		s.w.Trigger(cc.ID, nil)
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		s.w.Trigger(cc.ID, nil) // GC wait
		return ctx.Err()
	}
}

// sync proposes a SYNC request and is non-blocking.
// This makes no guarantee that the request will be proposed or performed.
// The request will be cancelled after the given timeout.
func (s *DeimosServer) sync(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := pb.Request{
		Method: "SYNC",
		ID:     GenID(),
		Time:   time.Now().UnixNano(),
	}
	data, err := req.Marshal()
	if err != nil {
		log.Printf("marshal request %#v error: %v", req, err)
		return
	}
	// There is no promise that node has leader when do SYNC request,
	// so it uses goroutine to propose.
	go func() {
		s.Node.Propose(ctx, data)
		cancel()
	}()
}

// publish registers server information into the cluster. The information
// is the json format of its self member struct, whose ClientURLs may be
// updated.
// The function keeps attempting to register until it succeeds,
// or its server is stopped.
// TODO: take care of info fetched from cluster store after having reconfig.
func (s *DeimosServer) publish(retryInterval time.Duration) {
	m := *s.ClusterStore.Get().FindName(s.Name)
	m.ClientURLs = s.ClientURLs.StringSlice()
	b, err := json.Marshal(m)
	if err != nil {
		log.Printf("Deimosserver: json marshal error: %v", err)
		return
	}
	req := pb.Request{
		ID:     GenID(),
		Method: "PUT",
		Path:   m.storeKey(),
		Val:    string(b),
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), retryInterval)
		_, err := s.Do(ctx, req)
		cancel()
		switch err {
		case nil:
			log.Printf("Deimosserver: published %+v to the cluster", m)
			return
		case ErrStopped:
			log.Printf("Deimosserver: aborting publish because server is stopped")
			return
		default:
			log.Printf("Deimosserver: publish error: %v", err)
		}
	}
}

func getExpirationTime(r *pb.Request) time.Time {
	var t time.Time
	if r.Expiration != 0 {
		t = time.Unix(0, r.Expiration)
	}
	return t
}

// apply interprets r as a call to store.X and returns a
// Response interpreted from store.Event
func (s *DeimosServer) apply(r pb.Request) Response {
	f := func(ev *store.Event, err error) Response {
		return Response{Event: ev, err: err}
	}

	expr := getExpirationTime(&r)

	switch r.Method {
	case "POST":
		return f(s.Store.Create(r.Path, r.Dir, r.Val, true, expr))
	case "PUT":
		exists, existSet := getBool(r.PrevExist)
		switch {
		case existSet:
			if exists {
				return f(s.Store.Update(r.Path, r.Val, expr))
			}
			return f(s.Store.Create(r.Path, r.Dir, r.Val, false, expr))
		case r.PrevIndex > 0 || r.PrevValue != "":
			return f(s.Store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, expr))
		default:
			return f(s.Store.Set(r.Path, r.Dir, r.Val, expr))
		}
	case "DELETE":
		switch {
		case r.PrevIndex > 0 || r.PrevValue != "":
			return f(s.Store.CompareAndDelete(r.Path, r.PrevValue, r.PrevIndex))
		default:
			return f(s.Store.Delete(r.Path, r.Dir, r.Recursive))
		}
	case "QGET":
		return f(s.Store.Get(r.Path, r.Recursive, r.Sorted))
	case "SYNC":
		s.Store.DeleteExpiredKeys(time.Unix(0, r.Time))
		return Response{}
	default:
		// This should never reached, but just in case.
		return Response{err: ErrUnknownMethod}
	}
}

// TODO: non-blocking snapshot
func (s *DeimosServer) snapshot() {
	d, err := s.Store.Save()
	// TODO: current store will never fail to do a snapshot
	// what should we do if the store might fail?
	if err != nil {
		panic("TODO: this is bad, what do we do about it?")
	}
	s.Node.Compact(d)
	s.Storage.Cut()
}

// TODO: move the function to /id pkg maybe?
// GenID generates a random id that is not equal to 0.
func GenID() (n int64) {
	for n == 0 {
		n = rand.Int63()
	}
	return
}

func getBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

// CanReadWithLease implements LeaseReader interface
func (s *DeimosServer) CanReadWithLease() bool {
	if s.Node == nil {
		return false
	}
	return s.Node.CanReadWithLease()
}

// HasValidLease implements LeaseReader interface
func (s *DeimosServer) HasValidLease() bool {
	if s.Node == nil {
		return false
	}
	return s.Node.HasValidLease()
}
