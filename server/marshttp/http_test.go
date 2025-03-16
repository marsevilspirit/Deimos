package marshttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	"github.com/marsevilspirit/marstore/server"
	"github.com/marsevilspirit/marstore/server/serverpb"
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
func boolp(b bool) *bool       { return &b }

func mustNewURL(t *testing.T, s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		t.Fatalf("error creating URL from %q: %v", s, err)
	}
	return u
}

func TestBadParseRequest(t *testing.T) {
	tests := []struct {
		in *http.Request
	}{
		{
			// parseForm failure
			&http.Request{
				Body:   nil,
				Method: "PUT",
			},
		},
		{
			// bad key prefix
			&http.Request{
				URL: mustNewURL(t, "/badprefix/"),
			},
		},
	}
	for i, tt := range tests {
		got, err := parseRequest(tt.in, 1234)
		if err == nil {
			t.Errorf("case %d: unexpected nil error!", i)
		}
		if !reflect.DeepEqual(got, serverpb.Request{}) {
			t.Errorf("case %d: unexpected non-empty Request: %#v", i, got)
		}
	}
}

func TestGoodParseRequest(t *testing.T) {
	tests := []struct {
		in *http.Request
		w  serverpb.Request
	}{
		{
			// good prefix, all other values default
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo")),
			},
			serverpb.Request{
				Id:   1234,
				Path: "/foo",
			},
		},
		{
			// value specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?value=some_value")),
			},
			serverpb.Request{
				Id:   1234,
				Val:  "some_value",
				Path: "/foo",
			},
		},
		{
			// prevIndex specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?prevIndex=98765")),
			},
			serverpb.Request{
				Id:        1234,
				PrevIndex: 98765,
				Path:      "/foo",
			},
		},
		{
			// recursive specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?recursive=true")),
			},
			serverpb.Request{
				Id:        1234,
				Recursive: true,
				Path:      "/foo",
			},
		},
		{
			// sorted specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?sorted=true")),
			},
			serverpb.Request{
				Id:     1234,
				Sorted: true,
				Path:   "/foo",
			},
		},
		{
			// wait specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?wait=true")),
			},
			serverpb.Request{
				Id:   1234,
				Wait: true,
				Path: "/foo",
			},
		},
		{
			// prevExists should be non-null if specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?prevExists=true")),
			},
			serverpb.Request{
				Id:         1234,
				PrevExists: boolp(true),
				Path:       "/foo",
			},
		},
		{
			// prevExists should be non-null if specified
			&http.Request{
				URL: mustNewURL(t, path.Join(keysPrefix, "foo?prevExists=false")),
			},
			serverpb.Request{
				Id:         1234,
				PrevExists: boolp(false),
				Path:       "/foo",
			},
		},
	}

	for i, tt := range tests {
		got, err := parseRequest(tt.in, 1234)
		if err != nil {
			t.Errorf("#%d: err = %v, want %v", i, err, nil)
		}
		if !reflect.DeepEqual(got, tt.w) {
			t.Errorf("#%d: bad request: got %#v, want %#v", i, got, tt.w)
		}
	}
}

// eventingWatcher immediately returns a simple event of the given action on its channel
type eventingWatcher struct {
	action string
}

func (w *eventingWatcher) EventChan() chan *store.Event {
	ch := make(chan *store.Event)
	go func() {
		ch <- &store.Event{
			Action: w.action,
			Node:   &store.NodeExtern{},
		}
	}()
	return ch
}

func (w *eventingWatcher) Remove() {}

func TestEncodeResponse(t *testing.T) {
	tests := []struct {
		resp server.Response
		idx  string
		code int
		err  error
	}{
		// standard case, standard 200 response
		{
			server.Response{
				Event: &store.Event{
					Action:   store.Get,
					Node:     &store.NodeExtern{},
					PrevNode: &store.NodeExtern{},
				},
				Watcher: nil,
			},
			"0",
			http.StatusOK,
			nil,
		},
		// check new nodes return StatusCreated
		{
			server.Response{
				Event: &store.Event{
					Action:   store.Create,
					Node:     &store.NodeExtern{},
					PrevNode: &store.NodeExtern{},
				},
				Watcher: nil,
			},
			"0",
			http.StatusCreated,
			nil,
		},
		{
			server.Response{
				Watcher: &eventingWatcher{store.Create},
			},
			"0",
			http.StatusCreated,
			nil,
		},
	}

	for i, tt := range tests {
		rw := httptest.NewRecorder()
		err := encodeResponse(context.Background(), rw, tt.resp)
		if err != tt.err {
			t.Errorf("case %d: unexpected err: got %v, want %v", i, err, tt.err)
			continue
		}

		if gct := rw.Header().Get("Content-Type"); gct != "application/json" {
			t.Errorf("case %d: bad Content-Type: got %q, want application/json", i, gct)
		}

		if gei := rw.Header().Get("X-Mars-Index"); gei != tt.idx {
			t.Errorf("case %d: bad X-Etcd-Index header: got %s, want %s", i, gei, tt.idx)
		}

		if rw.Code != tt.code {
			t.Errorf("case %d: bad response code: got %d, want %v", i, rw.Code, tt.code)
		}

	}
}

type dummyWatcher struct {
	echan chan *store.Event
}

func (w *dummyWatcher) EventChan() chan *store.Event {
	return w.echan
}
func (w *dummyWatcher) Remove() {}

type dummyResponseWriter struct {
	cnchan chan bool
	http.ResponseWriter
}

func (rw *dummyResponseWriter) CloseNotify() <-chan bool {
	return rw.cnchan
}

func TestWaitForEventChan(t *testing.T) {
	ctx := context.Background()
	ec := make(chan *store.Event)
	dw := &dummyWatcher{
		echan: ec,
	}
	w := httptest.NewRecorder()
	var wg sync.WaitGroup
	var ev *store.Event
	var err error
	wg.Add(1)
	go func() {
		ev, err = waitForEvent(ctx, w, dw)
		wg.Done()
	}()
	ec <- &store.Event{
		Action: store.Get,
		Node: &store.NodeExtern{
			Key:           "/foo/bar",
			ModifiedIndex: 12345,
		},
	}
	wg.Wait()
	want := &store.Event{
		Action: store.Get,
		Node: &store.NodeExtern{
			Key:           "/foo/bar",
			ModifiedIndex: 12345,
		},
	}
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("bad event: got %#v, want %#v", ev, want)
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForEventCloseNotify(t *testing.T) {
	ctx := context.Background()
	dw := &dummyWatcher{}
	cnchan := make(chan bool)
	w := &dummyResponseWriter{
		cnchan: cnchan,
	}
	var wg sync.WaitGroup
	var ev *store.Event
	var err error
	wg.Add(1)
	go func() {
		ev, err = waitForEvent(ctx, w, dw)
		wg.Done()
	}()
	close(cnchan)
	wg.Wait()
	if ev != nil {
		t.Fatalf("non-nil Event returned with CloseNotifier: %v", ev)
	}
	if err == nil {
		t.Fatalf("nil err returned with CloseNotifier!")
	}
}

func TestWaitForEventCancelledContext(t *testing.T) {
	cctx, cancel := context.WithCancel(context.Background())
	dw := &dummyWatcher{}
	w := httptest.NewRecorder()
	var wg sync.WaitGroup
	var ev *store.Event
	var err error
	wg.Add(1)
	go func() {
		ev, err = waitForEvent(cctx, w, dw)
		wg.Done()
	}()
	cancel()
	wg.Wait()
	if ev != nil {
		t.Fatalf("non-nil Event returned with cancelled context: %v", ev)
	}
	if err == nil {
		t.Fatalf("nil err returned with cancelled context!")
	}
}

func TestV2MachinesEndpoint(t *testing.T) {
	tests := []struct {
		method string
		wcode  int
	}{
		{"GET", http.StatusOK},
		{"HEAD", http.StatusOK},
		{"POST", http.StatusMethodNotAllowed},
	}

	h := Handler{Peers: Peers{}}
	s := httptest.NewServer(h)
	defer s.Close()

	for _, tt := range tests {
		req, _ := http.NewRequest(tt.method, s.URL+machinesPrefix, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}

		if resp.StatusCode != tt.wcode {
			t.Errorf("StatusCode = %d, expected %d", resp.StatusCode, tt.wcode)
		}
	}
}

func TestServeMachines(t *testing.T) {
	peers := Peers{}
	peers.Set("0xBEEF0=localhost:8080&0xBEEF1=localhost:8081&0xBEEF2=localhost:8082")
	h := Handler{Peers: peers}

	writer := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "", nil)
	h.serveMachines(writer, req)
	w := "http://localhost:8080, http://localhost:8081, http://localhost:8082"
	if g := writer.Body.String(); g != w {
		t.Errorf("data = %s, want %s", g, w)
	}
	if writer.Code != http.StatusOK {
		t.Errorf("header = %d, want %d", writer.Code, http.StatusOK)
	}
}
