package server

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/marsevilspirit/deimos/raft"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	pb "github.com/marsevilspirit/deimos/server/serverpb"
	"github.com/marsevilspirit/deimos/store"
	"github.com/marsevilspirit/deimos/testutil"
)

func TestGetExpirationTime(t *testing.T) {
	tests := []struct {
		r    pb.Request
		want time.Time
	}{
		{
			pb.Request{Expiration: 0},
			time.Time{},
		},
		{
			pb.Request{Expiration: 60000},
			time.Unix(0, 60000),
		},
		{
			pb.Request{Expiration: -60000},
			time.Unix(0, -60000),
		},
	}

	for i, tt := range tests {
		got := getExpirationTime(&tt.r)
		if !reflect.DeepEqual(tt.want, got) {
			t.Errorf("#%d: incorrect expiration time: want=%v got=%v", i, tt.want, got)
		}
	}
}

// TestDoLocalAction tests requests which do not need to go through raft to be applied,
// and are served through local data.
func TestDoLocalAction(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp   Response
		werr    error
		waction []string
	}{
		{
			pb.Request{Method: "GET", Id: 1, Wait: true},
			Response{Watcher: &stubWatcher{}}, nil, []string{"Watch"},
		},
		{
			pb.Request{Method: "GET", Id: 1},
			Response{Event: &store.Event{}}, nil, []string{"Get"},
		},
		{
			pb.Request{Method: "BADMETHOD", Id: 1},
			Response{}, ErrUnknownMethod, []string{},
		},
	}
	for i, tt := range tests {
		st := &storeRecorder{}
		srv := &DeimosServer{Store: st}
		resp, err := srv.Do(context.TODO(), tt.req)

		if err != tt.werr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		action := st.Action()
		if !reflect.DeepEqual(action, tt.waction) {
			t.Errorf("#%d: action = %+v, want %+v", i, action, tt.waction)
		}
	}
}

// TestDoBadLocalAction tests server requests which do not need to go through consensus,
// and return errors when they fetch from local data.
func TestDoBadLocalAction(t *testing.T) {
	storeErr := fmt.Errorf("bah")
	tests := []struct {
		req pb.Request

		waction []string
	}{
		{
			pb.Request{Method: "GET", Id: 1, Wait: true},
			[]string{"Watch"},
		},
		{
			pb.Request{Method: "GET", Id: 1},
			[]string{"Get"},
		},
	}
	for i, tt := range tests {
		st := &errStoreRecorder{err: storeErr}
		srv := &DeimosServer{Store: st}
		resp, err := srv.Do(context.Background(), tt.req)

		if err != storeErr {
			t.Fatalf("#%d: err = %+v, want %+v", i, err, storeErr)
		}
		if !reflect.DeepEqual(resp, Response{}) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, Response{})
		}
		action := st.Action()
		if !reflect.DeepEqual(action, tt.waction) {
			t.Errorf("#%d: action = %+v, want %+v", i, action, tt.waction)
		}
	}
}

func TestApply(t *testing.T) {
	tests := []struct {
		req pb.Request

		wresp   Response
		waction []string
	}{
		{
			pb.Request{Method: "POST", Id: 1},
			Response{Event: &store.Event{}}, []string{"Create"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevExist: boolp(true), PrevIndex: 1},
			Response{Event: &store.Event{}}, []string{"Update"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevExist: boolp(false), PrevIndex: 1},
			Response{Event: &store.Event{}}, []string{"Create"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevExist: boolp(true)},
			Response{Event: &store.Event{}}, []string{"Update"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevExist: boolp(false)},
			Response{Event: &store.Event{}}, []string{"Create"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevIndex: 1},
			Response{Event: &store.Event{}}, []string{"CompareAndSwap"},
		},
		{
			pb.Request{Method: "PUT", Id: 1, PrevValue: "bar"},
			Response{Event: &store.Event{}}, []string{"CompareAndSwap"},
		},
		{
			pb.Request{Method: "PUT", Id: 1},
			Response{Event: &store.Event{}}, []string{"Set"},
		},
		{
			pb.Request{Method: "DELETE", Id: 1, PrevIndex: 1},
			Response{Event: &store.Event{}}, []string{"CompareAndDelete"},
		},
		{
			pb.Request{Method: "DELETE", Id: 1, PrevValue: "bar"},
			Response{Event: &store.Event{}}, []string{"CompareAndDelete"},
		},
		{
			pb.Request{Method: "DELETE", Id: 1},
			Response{Event: &store.Event{}}, []string{"Delete"},
		},
		{
			pb.Request{Method: "QGET", Id: 1},
			Response{Event: &store.Event{}}, []string{"Get"},
		},
		{
			pb.Request{Method: "SYNC", Id: 1},
			Response{}, []string{"DeleteExpiredKeys"},
		},
		{
			pb.Request{Method: "BADMETHOD", Id: 1},
			Response{err: ErrUnknownMethod}, []string{},
		},
	}

	for i, tt := range tests {
		st := &storeRecorder{}
		srv := &DeimosServer{Store: st}
		resp := srv.apply(tt.req)

		if !reflect.DeepEqual(resp, tt.wresp) {
			t.Errorf("#%d: resp = %+v, want %+v", i, resp, tt.wresp)
		}
		action := st.Action()
		if !reflect.DeepEqual(action, tt.waction) {
			t.Errorf("#%d: action = %+v, want %+v", i, action, tt.waction)
		}
	}
}

func TestClusterOf1(t *testing.T) { testServer(t, 1) }
func TestClusterOf3(t *testing.T) { testServer(t, 3) }

func testServer(t *testing.T, ns int64) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := make([]*DeimosServer, ns)

	send := func(msgs []raftpb.Message) {
		for _, m := range msgs {
			t.Logf("m = %+v\n", m)
			ss[m.To-1].Node.Step(ctx, m)
		}
	}

	peers := make([]int64, ns)
	for i := int64(0); i < ns; i++ {
		peers[i] = i + 1
	}

	for i := int64(0); i < ns; i++ {
		id := i + 1
		n := raft.StartNode(id, peers, 10, 1)
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		srv := &DeimosServer{
			Node:    n,
			Store:   store.New(),
			Send:    send,
			Storage: &storageRecorder{},
			Ticker:  tk.C,
		}
		srv.Start()
		// TODO(xiangli): randomize election timeout
		// then remove this sleep.
		time.Sleep(1 * time.Millisecond)
		ss[i] = srv
	}

	for i := 1; i <= 10; i++ {
		r := pb.Request{
			Method: "PUT",
			Id:     int64(i),
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
			t.Error("value:", *g.Value)
			t.Errorf("g = %+v, w %+v", g, w)
		}
	}

	time.Sleep(10 * time.Millisecond)

	var last interface{}
	for i, sv := range ss {
		sv.Stop()
		g, _ := sv.Store.Get("/", true, true)
		if last != nil && !reflect.DeepEqual(last, g) {
			t.Errorf("server %d: Root = %#v, want %#v", i, g, last)
		}
		last = g
	}
}

func TestDoProposal(t *testing.T) {
	tests := []pb.Request{
		pb.Request{Method: "POST", Id: 1},
		pb.Request{Method: "PUT", Id: 1},
		pb.Request{Method: "DELETE", Id: 1},
		pb.Request{Method: "GET", Id: 1, Quorum: true},
	}

	for i, tt := range tests {
		ctx, _ := context.WithCancel(context.Background())
		n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
		st := &storeRecorder{}
		tk := make(chan time.Time)
		// this makes <-tk always successful, which accelerates internal clock
		close(tk)
		srv := &DeimosServer{
			Node:    n,
			Store:   st,
			Send:    func(_ []raftpb.Message) {},
			Storage: &storageRecorder{},
			Ticker:  tk,
		}
		srv.Start()
		resp, err := srv.Do(ctx, tt)
		srv.Stop()

		action := st.Action()
		if len(action) != 1 {
			t.Errorf("#%d: len(action) = %d, want 1", i, len(action))
		}
		if err != nil {
			t.Fatalf("#%d: err = %v, want nil", i, err)
		}
		wresp := Response{Event: &store.Event{}}
		if !reflect.DeepEqual(resp, wresp) {
			t.Errorf("#%d: resp = %v, want %v", i, resp, wresp)
		}
	}
}

func TestDoProposalCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// node cannot make any progress because there are two nodes
	n := raft.StartNode(0xBAD0, []int64{0xBAD0, 0xBAD1}, 10, 1)
	st := &storeRecorder{}
	wait := &waitRecorder{}
	srv := &DeimosServer{
		// TODO: use fake node for better testability
		Node:  n,
		Store: st,
		w:     wait,
	}

	done := make(chan struct{})
	var err error
	go func() {
		_, err = srv.Do(ctx, pb.Request{Method: "PUT", Id: 1})
		close(done)
	}()
	cancel()
	<-done

	action := st.Action()
	if len(action) != 0 {
		t.Errorf("len(action) = %v, want 0", len(action))
	}
	if err != context.Canceled {
		t.Fatalf("err = %v, want %v", err, context.Canceled)
	}
	w := []string{"Register1", "Trigger1"}
	if !reflect.DeepEqual(wait.action, w) {
		t.Errorf("wait.action = %+v, want %+v", wait.action, w)
	}
}

func TestDoProposalStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// node cannot make any progress because there are two nodes
	n := raft.StartNode(0xBAD0, []int64{0xBAD0, 0xBAD1}, 10, 1)
	st := &storeRecorder{}
	tk := make(chan time.Time)
	// this makes <-tk always successful, which accelarates internal clock
	close(tk)
	srv := &DeimosServer{
		// TODO: use fake node for better testability
		Node:    n,
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
		Ticker:  tk,
	}
	srv.Start()

	done := make(chan struct{})
	var err error
	go func() {
		_, err = srv.Do(ctx, pb.Request{Method: "PUT", Id: 1})
		close(done)
	}()
	srv.Stop()
	<-done

	action := st.Action()
	if len(action) != 0 {
		t.Errorf("len(action) = %v, want 0", len(action))
	}
	if err != ErrStopped {
		t.Errorf("err = %v, want %v", err, ErrStopped)
	}
}

// TestSync tests sync 1. is nonblocking 2. sends out SYNC request.
func TestSync(t *testing.T) {
	n := &nodeProposeDataRecorder{}
	srv := &DeimosServer{
		Node: n,
	}
	start := time.Now()
	srv.sync(defaultSyncTimeout)

	// check that sync is non-blocking
	if d := time.Since(start); d > time.Millisecond {
		t.Errorf("CallSyncTime = %v, want < %v", d, time.Millisecond)
	}

	testutil.ForceGosched()
	data := n.data()
	if len(data) != 1 {
		t.Fatalf("len(proposeData) = %d, want 1", len(data))
	}
	var r pb.Request
	if err := r.Unmarshal(data[0]); err != nil {
		t.Fatalf("unmarshal request error: %v", err)
	}
	if r.Method != "SYNC" {
		t.Errorf("method = %s, want SYNC", r.Method)
	}
}

// TestSyncTimeout tests the case that sync 1. is non-blocking 2. cancel request
// after timeout
func TestSyncTimeout(t *testing.T) {
	n := &nodeProposalBlockerRecorder{}
	srv := &DeimosServer{
		Node: n,
	}
	start := time.Now()
	srv.sync(0)

	// check that sync is non-blocking
	if d := time.Since(start); d > time.Millisecond {
		t.Errorf("CallSyncTime = %v, want < %v", d, time.Millisecond)
	}

	// give time for goroutine in sync to cancel
	// TODO: use fake clock
	testutil.ForceGosched()
	w := []string{"Propose blocked"}
	if g := n.Action(); !reflect.DeepEqual(g, w) {
		t.Errorf("action = %v, want %v", g, w)
	}
}

// TODO: TestNoSyncWhenNoLeader

// blockingNodeProposer implements the node interface to allow users to
// block until Propose has been called and then verify the Proposed data
type blockingNodeProposer struct {
	ch chan []byte
	readyNode
}

func (n *blockingNodeProposer) Propose(_ context.Context, data []byte) error {
	n.ch <- data
	return nil
}

// TestSyncTrigger tests that the server proposes a SYNC request when its sync timer ticks
func TestSyncTrigger(t *testing.T) {
	n := &blockingNodeProposer{
		ch:        make(chan []byte),
		readyNode: *newReadyNode(),
	}
	st := make(chan time.Time, 1)
	srv := &DeimosServer{
		Node:       n,
		Store:      &storeRecorder{},
		Send:       func(_ []raftpb.Message) {},
		Storage:    &storageRecorder{},
		SyncTicker: st,
	}
	srv.Start()
	// trigger the server to become a leader and accept sync requests
	n.readyc <- raft.Ready{
		SoftState: &raft.SoftState{
			RaftState: raft.StateLeader,
		},
	}
	// trigger a sync request
	st <- time.Time{}
	var data []byte
	select {
	case <-time.After(time.Second):
		t.Fatalf("did not receive proposed request as expected!")
	case data = <-n.ch:
	}
	srv.Stop()
	var req pb.Request
	if err := req.Unmarshal(data); err != nil {
		t.Fatalf("error unmarshalling data: %v", err)
	}
	if req.Method != "SYNC" {
		t.Fatalf("unexpected proposed request: %#v", req.Method)
	}
}

// snapshot should snapshot the store and cut the persistent
// TODO: node.Compact is called... we need to make the node an interface
func TestSnapshot(t *testing.T) {
	n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
	defer n.Stop()
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &DeimosServer{
		Store:   st,
		Storage: p,
		Node:    n,
	}

	s.snapshot()
	action := st.Action()
	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0] != "Save" {
		t.Errorf("action = %s, want Save", action[0])
	}

	action = p.Action()
	if len(action) != 1 {
		t.Fatalf("len(action) = %d, want 1", len(action))
	}
	if action[0] != "Cut" {
		t.Errorf("action = %s, want Cut", action[0])
	}
}

// Applied > SnapCount should trigger a SaveSnap event
func TestTriggerSnap(t *testing.T) {
	ctx := context.Background()
	n := raft.StartNode(0xBAD0, []int64{0xBAD0}, 10, 1)
	n.Campaign(ctx)
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &DeimosServer{
		Store:     st,
		Send:      func(_ []raftpb.Message) {},
		Storage:   p,
		Node:      n,
		SnapCount: 10,
	}

	s.Start()
	for i := 0; int64(i) < s.SnapCount; i++ {
		s.Do(ctx, pb.Request{Method: "PUT", Id: 1})
	}
	time.Sleep(time.Millisecond)
	s.Stop()

	action := p.Action()
	// each operation is recorded as a Save
	// Nop + SnapCount * Puts + Cut + SaveSnap = Save + SnapCount * Save + Cut + SaveSnap
	if len(action) != 3+int(s.SnapCount) {
		t.Fatalf("len(action) = %d, want %d", len(action), 3+int(s.SnapCount))
	}
	if action[12] != "SaveSnap" {
		t.Errorf("action = %s, want SaveSnap", action[12])
	}
}

// TestRecvSnapshot tests when it receives a snapshot from raft leader,
// it should trigger storage.SaveSnap and also store.Recover.
func TestRecvSnapshot(t *testing.T) {
	n := newReadyNode()
	st := &storeRecorder{}
	p := &storageRecorder{}
	s := &DeimosServer{
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: p,
		Node:    n,
	}

	s.Start()
	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	testutil.ForceGosched()
	s.Stop()

	waction := []string{"Recovery"}
	if g := st.Action(); !reflect.DeepEqual(g, waction) {
		t.Errorf("store action = %v, want %v", g, waction)
	}
	waction = []string{"Save", "SaveSnap"}
	if g := p.Action(); !reflect.DeepEqual(g, waction) {
		t.Errorf("storage action = %v, want %v", g, waction)
	}
}

// TestRecvSlowSnapshot tests that slow snapshot will not be applied
// to store.
func TestRecvSlowSnapshot(t *testing.T) {
	n := newReadyNode()
	st := &storeRecorder{}
	s := &DeimosServer{
		Store:   st,
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
		Node:    n,
	}

	s.Start()
	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	testutil.ForceGosched()
	action := st.Action()

	n.readyc <- raft.Ready{Snapshot: raftpb.Snapshot{Index: 1}}
	// make goroutines move forward to receive snapshot
	testutil.ForceGosched()
	s.Stop()

	if g := st.Action(); !reflect.DeepEqual(g, action) {
		t.Errorf("store action = %v, want %v", g, action)
	}
}

// TestAddNode tests AddNode can propose and perform node addition.
func TestAddNode(t *testing.T) {
	n := newNodeConfigChangeCommitterRecorder()
	s := &DeimosServer{
		Node:    n,
		Store:   &storeRecorder{},
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
	}
	s.Start()
	s.AddNode(context.TODO(), 1, []byte("foo"))
	action := n.Action()
	s.Stop()

	waction := []string{"ProposeConfigChange:ConfigChangeAddNode", "ApplyConfigChange:ConfigChangeAddNode"}
	if !reflect.DeepEqual(action, waction) {
		t.Errorf("action = %v, want %v", action, waction)
	}
}

// TestRemoveNode tests RemoveNode can propose and perform node removal.
func TestRemoveNode(t *testing.T) {
	n := newNodeConfigChangeCommitterRecorder()
	s := &DeimosServer{
		Node:    n,
		Store:   &storeRecorder{},
		Send:    func(_ []raftpb.Message) {},
		Storage: &storageRecorder{},
	}
	s.Start()
	s.RemoveNode(context.TODO(), 1)
	action := n.Action()
	s.Stop()

	waction := []string{"ProposeConfigChange:ConfigChangeRemoveNode", "ApplyConfigChange:ConfigChangeRemoveNode"}
	if !reflect.DeepEqual(action, waction) {
		t.Errorf("action = %v, want %v", action, waction)
	}
}

// TODO: test wait trigger correctness in multi-server case

func TestGetBool(t *testing.T) {
	tests := []struct {
		b    *bool
		wb   bool
		wset bool
	}{
		{nil, false, false},
		{boolp(true), true, true},
		{boolp(false), false, true},
	}
	for i, tt := range tests {
		b, set := getBool(tt.b)
		if b != tt.wb {
			t.Errorf("#%d: value = %v, want %v", i, b, tt.wb)
		}
		if set != tt.wset {
			t.Errorf("#%d: set = %v, want %v", i, set, tt.wset)
		}
	}
}

func TestGenID(t *testing.T) {
	// Sanity check that the GenID function has been seeded appropriately
	// (math/rand is seeded with 1 by default)
	r := rand.NewSource(int64(1))
	var n int64
	for n == 0 {
		n = r.Int63()
	}
	if n == GenID() {
		t.Fatalf("GenID's rand seeded with 1!")
	}
}

type recorder struct {
	sync.Mutex
	action []string
}

func (r *recorder) record(action string) {
	r.Lock()
	r.action = append(r.action, action)
	r.Unlock()
}
func (r *recorder) Action() []string {
	r.Lock()
	cpy := make([]string, len(r.action))
	copy(cpy, r.action)
	r.Unlock()
	return cpy
}

type storeRecorder struct {
	recorder
}

func (s *storeRecorder) Version() int  { return 0 }
func (s *storeRecorder) Index() uint64 { return 0 }
func (s *storeRecorder) Get(_ string, _, _ bool) (*store.Event, error) {
	s.record("Get")
	return &store.Event{}, nil
}
func (s *storeRecorder) Set(_ string, _ bool, _ string, _ time.Time) (*store.Event, error) {
	s.record("Set")
	return &store.Event{}, nil
}
func (s *storeRecorder) Update(_, _ string, _ time.Time) (*store.Event, error) {
	s.record("Update")
	return &store.Event{}, nil
}
func (s *storeRecorder) Create(_ string, _ bool, _ string, _ bool, _ time.Time) (*store.Event, error) {
	s.record("Create")
	return &store.Event{}, nil
}
func (s *storeRecorder) CompareAndSwap(_, _ string, _ uint64, _ string, _ time.Time) (*store.Event, error) {
	s.record("CompareAndSwap")
	return &store.Event{}, nil
}
func (s *storeRecorder) Delete(_ string, _, _ bool) (*store.Event, error) {
	s.record("Delete")
	return &store.Event{}, nil
}
func (s *storeRecorder) CompareAndDelete(_, _ string, _ uint64) (*store.Event, error) {
	s.record("CompareAndDelete")
	return &store.Event{}, nil
}
func (s *storeRecorder) Watch(_ string, _, _ bool, _ uint64) (store.Watcher, error) {
	s.record("Watch")
	return &stubWatcher{}, nil
}
func (s *storeRecorder) Save() ([]byte, error) {
	s.record("Save")
	return nil, nil
}
func (s *storeRecorder) Recovery(b []byte) error {
	s.record("Recovery")
	return nil
}
func (s *storeRecorder) TotalTransactions() uint64 { return 0 }
func (s *storeRecorder) JsonStats() []byte         { return nil }
func (s *storeRecorder) DeleteExpiredKeys(cutoff time.Time) {
	s.record("DeleteExpiredKeys")
}

type stubWatcher struct{}

func (w *stubWatcher) EventChan() chan *store.Event { return nil }
func (w *stubWatcher) Remove()                      {}

// errStoreRecorder returns an store error on Get, Watch request
type errStoreRecorder struct {
	storeRecorder
	err error
}

func (s *errStoreRecorder) Get(_ string, _, _ bool) (*store.Event, error) {
	s.record("Get")
	return nil, s.err
}
func (s *errStoreRecorder) Watch(_ string, _, _ bool, _ uint64) (store.Watcher, error) {
	s.record("Watch")
	return nil, s.err
}

type waitRecorder struct {
	action []string
}

func (w *waitRecorder) Register(id int64) <-chan interface{} {
	w.action = append(w.action, fmt.Sprint("Register", id))
	return nil
}
func (w *waitRecorder) Trigger(id int64, x interface{}) {
	w.action = append(w.action, fmt.Sprint("Trigger", id))
}

func boolp(b bool) *bool { return &b }

func stringp(s string) *string { return &s }

type storageRecorder struct {
	recorder
}

func (p *storageRecorder) Save(st raftpb.HardState, ents []raftpb.Entry) {
	p.record("Save")
}
func (p *storageRecorder) Cut() error {
	p.record("Cut")
	return nil
}
func (p *storageRecorder) SaveSnap(st raftpb.Snapshot) {
	if raft.IsEmptySnap(st) {
		return
	}
	p.record("SaveSnap")
}

type readyNode struct {
	readyc chan raft.Ready
}

func newReadyNode() *readyNode {
	readyc := make(chan raft.Ready, 1)
	return &readyNode{readyc: readyc}
}
func (n *readyNode) Tick()                                          {}
func (n *readyNode) Campaign(ctx context.Context) error             { return nil }
func (n *readyNode) Propose(ctx context.Context, data []byte) error { return nil }
func (n *readyNode) ProposeConfigChange(ctx context.Context, conf raftpb.ConfChange) error {
	return nil
}
func (n *readyNode) Step(ctx context.Context, msg raftpb.Message) error { return nil }
func (n *readyNode) Ready() <-chan raft.Ready                           { return n.readyc }
func (n *readyNode) ApplyConfigChange(conf raftpb.ConfChange)           {}
func (n *readyNode) Stop()                                              {}
func (n *readyNode) Compact(d []byte)                                   {}

type nodeRecorder struct {
	recorder
}

func (n *nodeRecorder) Tick() {
	n.record("Tick")
}
func (n *nodeRecorder) Campaign(ctx context.Context) error {
	n.record("Campaign")
	return nil
}
func (n *nodeRecorder) Propose(ctx context.Context, data []byte) error {
	n.record("Propose")
	return nil
}
func (n *nodeRecorder) ProposeConfigChange(ctx context.Context, conf raftpb.ConfChange) error {
	n.record("ProposeConfigChange")
	return nil
}
func (n *nodeRecorder) Step(ctx context.Context, msg raftpb.Message) error {
	n.record("Step")
	return nil
}
func (n *nodeRecorder) Ready() <-chan raft.Ready { return nil }
func (n *nodeRecorder) ApplyConfigChange(conf raftpb.ConfChange) {
	n.record("ApplyConfigChange")
}
func (n *nodeRecorder) Stop() {
	n.record("Stop")
}
func (n *nodeRecorder) Compact(d []byte) {
	n.record("Compact")
}

type nodeProposeDataRecorder struct {
	nodeRecorder
	sync.Mutex
	d [][]byte
}

func (n *nodeProposeDataRecorder) data() [][]byte {
	n.Lock()
	d := n.d
	n.Unlock()
	return d
}
func (n *nodeProposeDataRecorder) Propose(ctx context.Context, data []byte) error {
	n.nodeRecorder.Propose(ctx, data)
	n.Lock()
	n.d = append(n.d, data)
	n.Unlock()
	return nil
}

type nodeProposalBlockerRecorder struct {
	nodeRecorder
}

func (n *nodeProposalBlockerRecorder) Propose(ctx context.Context, data []byte) error {
	<-ctx.Done()
	n.record("Propose blocked")
	return nil
}

type nodeConfigChangeCommitterRecorder struct {
	nodeRecorder
	readyc chan raft.Ready
}

func newNodeConfigChangeCommitterRecorder() *nodeConfigChangeCommitterRecorder {
	readyc := make(chan raft.Ready, 1)
	readyc <- raft.Ready{SoftState: &raft.SoftState{RaftState: raft.StateLeader}}
	return &nodeConfigChangeCommitterRecorder{readyc: readyc}
}
func (n *nodeConfigChangeCommitterRecorder) ProposeConfigChange(ctx context.Context, conf raftpb.ConfChange) error {
	data, err := conf.Marshal()
	if err != nil {
		return err
	}
	n.readyc <- raft.Ready{CommittedEntries: []raftpb.Entry{{Type: raftpb.EntryConfChange, Data: data}}}
	n.record("ProposeConfigChange:" + conf.Type.String())
	return nil
}
func (n *nodeConfigChangeCommitterRecorder) Ready() <-chan raft.Ready {
	return n.readyc
}
func (n *nodeConfigChangeCommitterRecorder) ApplyConfigChange(conf raftpb.ConfChange) {
	n.record("ApplyConfigChange:" + conf.Type.String())
}
