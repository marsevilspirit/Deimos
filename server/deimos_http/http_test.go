package deimos_http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	Err "github.com/marsevilspirit/deimos/error"
	"github.com/marsevilspirit/deimos/raft"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	"github.com/marsevilspirit/deimos/server"
	"github.com/marsevilspirit/deimos/server/serverpb"
	"github.com/marsevilspirit/deimos/store"
)

func nopSave(st raftpb.HardState, ents []raftpb.Entry) {}
func nopSend(m []raftpb.Message)                       {}

func TestSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := store.New()

	n := raft.StartNode(1, []int64{1}, 10, 1)
	n.Campaign(ctx)

	srv := &server.DeimosServer{
		Node:  n,
		Store: st,
		Send:  server.SendFunc(nopSend),
		Save:  func(st raftpb.HardState, ents []raftpb.Entry) {},
	}
	srv.Start()
	defer srv.Stop()

	h := NewHandler(srv, nil, time.Hour)
	s := httptest.NewServer(h)
	defer s.Close()

	resp, err := http.PostForm(s.URL+"/v2/keys/foo", url.Values{"value": {"bar"}})
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 201 {
		t.Errorf("StatusCode = %d, expected %d", resp.StatusCode, 201)
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

// mustNewRequest takes a path, appends it to the standard keysPrefix, and constructs
// an *http.Request referencing the resulting URL
func mustNewRequest(t *testing.T, p string) *http.Request {
	return &http.Request{
		URL: mustNewURL(t, path.Join(keysPrefix, p)),
	}
}

// mustNewForm takes a set of Values and constructs a PUT *http.Request,
// with a URL constructed from appending the given path to the standard keysPrefix
func mustNewForm(t *testing.T, p string, vals url.Values) *http.Request {
	u := mustNewURL(t, path.Join(keysPrefix, p))
	req, err := http.NewRequest("PUT", u.String(), strings.NewReader(vals.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err != nil {
		t.Fatalf("error creating new request: %v", err)
	}
	return req
}

func TestBadParseRequest(t *testing.T) {
	tests := []struct {
		in    *http.Request
		wcode int
	}{
		{
			// parseForm failure
			&http.Request{
				Body:   nil,
				Method: "PUT",
			},
			Err.EcodeInvalidForm,
		},
		{
			// bad key prefix
			&http.Request{
				URL: mustNewURL(t, "/badprefix/"),
			},
			Err.EcodeInvalidForm,
		},
		// bad values for prevIndex, waitIndex, ttl
		{
			mustNewForm(t, "foo", url.Values{"prevIndex": []string{"garbage"}}),
			Err.EcodeIndexNaN,
		},
		{
			mustNewForm(t, "foo", url.Values{"prevIndex": []string{"1.5"}}),
			Err.EcodeIndexNaN,
		},
		{
			mustNewForm(t, "foo", url.Values{"prevIndex": []string{"-1"}}),
			Err.EcodeIndexNaN,
		},
		{
			mustNewForm(t, "foo", url.Values{"waitIndex": []string{"garbage"}}),
			Err.EcodeIndexNaN,
		},
		{
			mustNewForm(t, "foo", url.Values{"waitIndex": []string{"??"}}),
			Err.EcodeIndexNaN,
		},
		{
			mustNewForm(t, "foo", url.Values{"ttl": []string{"-1"}}),
			Err.EcodeTTLNaN,
		},
		// bad values for recursive, sorted, wait, prevExists
		{
			mustNewForm(t, "foo", url.Values{"recursive": []string{"hahaha"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"recursive": []string{"1234"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"recursive": []string{"?"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"sorted": []string{"?"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"sorted": []string{"x"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"wait": []string{"?!"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"wait": []string{"yes"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"prevExists": []string{"yes"}}),
			Err.EcodeInvalidField,
		},
		{
			mustNewForm(t, "foo", url.Values{"prevExists": []string{"#2"}}),
			Err.EcodeInvalidField,
		},
		// query values are considered
		{
			mustNewRequest(t, "foo?prevExists=wrong"),
			Err.EcodeInvalidField,
		},
		{
			mustNewRequest(t, "foo?ttl=wrong"),
			Err.EcodeTTLNaN,
		},
		// but body takes precedence if both are specified
		{
			mustNewForm(
				t,
				"foo?ttl=12",
				url.Values{"ttl": []string{"garbage"}},
			),
			Err.EcodeTTLNaN,
		},
		{
			mustNewForm(
				t,
				"foo?prevExists=false",
				url.Values{"prevExists": []string{"yes"}},
			),
			Err.EcodeInvalidField,
		},
	}
	for i, tt := range tests {
		got, err := parseRequest(tt.in, 1234)
		if err == nil {
			t.Errorf("#%d: unexpected nil error!", i)
			continue
		}
		ee, ok := err.(*Err.Error)
		if !ok {
			t.Errorf("#%d: err is not not deimos.Error!", i)
			continue
		}
		if ee.ErrorCode != tt.wcode {
			t.Errorf("#%d: code=%d, want %d", i, ee.ErrorCode, tt.wcode)
			t.Logf("cause: %#v", ee.Cause)
		}
		if !reflect.DeepEqual(got, serverpb.Request{}) {
			t.Errorf("#%d: unexpected non-empty Request: %#v", i, got)
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
			mustNewRequest(t, "foo"),
			serverpb.Request{
				Id:   1234,
				Path: "/foo",
			},
		},
		{
			// value specified
			mustNewForm(
				t,
				"foo",
				url.Values{"value": []string{"some_value"}},
			),
			serverpb.Request{
				Id:     1234,
				Method: "PUT",
				Val:    "some_value",
				Path:   "/foo",
			},
		},
		{
			// prevIndex specified
			mustNewForm(
				t,
				"foo",
				url.Values{"prevIndex": []string{"98765"}},
			),
			serverpb.Request{
				Id:        1234,
				Method:    "PUT",
				PrevIndex: 98765,
				Path:      "/foo",
			},
		},
		{
			// recursive specified
			mustNewForm(
				t,
				"foo",
				url.Values{"recursive": []string{"true"}},
			),
			serverpb.Request{
				Id:        1234,
				Method:    "PUT",
				Recursive: true,
				Path:      "/foo",
			},
		},
		{
			// sorted specified
			mustNewForm(
				t,
				"foo",
				url.Values{"sorted": []string{"true"}},
			),
			serverpb.Request{
				Id:     1234,
				Method: "PUT",
				Sorted: true,
				Path:   "/foo",
			},
		},
		{
			// wait specified
			mustNewForm(
				t,
				"foo",
				url.Values{"wait": []string{"true"}},
			),
			serverpb.Request{
				Id:     1234,
				Method: "PUT",
				Wait:   true,
				Path:   "/foo",
			},
		},
		{
			// prevExists should be non-null if specified
			mustNewForm(
				t,
				"foo",
				url.Values{"prevExists": []string{"true"}},
			),
			serverpb.Request{
				Id:         1234,
				Method:     "PUT",
				PrevExists: boolp(true),
				Path:       "/foo",
			},
		},
		{
			// prevExists should be non-null if specified
			mustNewForm(
				t,
				"foo",
				url.Values{"prevExists": []string{"false"}},
			),
			serverpb.Request{
				Id:         1234,
				Method:     "PUT",
				PrevExists: boolp(false),
				Path:       "/foo",
			},
		},
		// mix various fields
		{
			mustNewForm(
				t,
				"foo",
				url.Values{
					"value":      []string{"some value"},
					"prevExists": []string{"true"},
					"prevValue":  []string{"previous value"},
				},
			),
			serverpb.Request{
				Id:         1234,
				Method:     "PUT",
				PrevExists: boolp(true),
				PrevValue:  "previous value",
				Val:        "some value",
				Path:       "/foo",
			},
		},
		// query parameters should be used if given
		{
			mustNewForm(
				t,
				"foo?prevValue=woof",
				url.Values{},
			),
			serverpb.Request{
				Id:        1234,
				Method:    "PUT",
				PrevValue: "woof",
				Path:      "/foo",
			},
		},
		// but form values should take precedence over query parameters
		{
			mustNewForm(
				t,
				"foo?prevValue=woof",
				url.Values{
					"prevValue": []string{"miaow"},
				},
			),
			serverpb.Request{
				Id:        1234,
				Method:    "PUT",
				PrevValue: "miaow",
				Path:      "/foo",
			},
		},
	}

	for i, tt := range tests {
		got, err := parseRequest(tt.in, 1234)
		if err != nil {
			t.Errorf("#%d: err = %v, want %v", i, err, nil)
		}
		if !reflect.DeepEqual(got, tt.w) {
			t.Errorf("#%d: request=%#v, want %#v", i, got, tt.w)
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

func TestWriteError(t *testing.T) {
	// nil error should not panic
	rw := httptest.NewRecorder()
	writeError(rw, nil)
	h := rw.Header()
	if len(h) > 0 {
		t.Fatalf("unexpected non-empty headers: %#v", h)
	}
	b := rw.Body.String()
	if len(b) > 0 {
		t.Fatalf("unexpected non-empty body: %q", b)
	}

	tests := []struct {
		err   error
		wcode int
		wi    string
	}{
		{
			Err.NewError(Err.EcodeKeyNotFound, "/foo/bar", 123),
			http.StatusNotFound,
			"123",
		},
		{
			Err.NewError(Err.EcodeTestFailed, "/foo/bar", 456),
			http.StatusPreconditionFailed,
			"456",
		},
		{
			err:   errors.New("something went wrong"),
			wcode: http.StatusInternalServerError,
		},
	}

	for i, tt := range tests {
		rw := httptest.NewRecorder()
		writeError(rw, tt.err)
		if code := rw.Code; code != tt.wcode {
			t.Errorf("#%d: got %d, want %d", i, code, tt.wcode)
		}
		if idx := rw.Header().Get("X-Deimos-Index"); idx != tt.wi {
			t.Errorf("#%d: X-Deimos-Index=%q, want %q", i, idx, tt.wi)
		}
	}
}

func TestWriteEvent(t *testing.T) {
	// nil event should not panic
	rw := httptest.NewRecorder()
	writeEvent(rw, nil)
	h := rw.Header()
	if len(h) > 0 {
		t.Fatalf("unexpected non-empty headers: %#v", h)
	}
	b := rw.Body.String()
	if len(b) > 0 {
		t.Fatalf("unexpected non-empty body: %q", b)
	}

	tests := []struct {
		ev   *store.Event
		idx  string
		code int
		err  error
	}{
		// standard case, standard 200 response
		{
			&store.Event{
				Action:   store.Get,
				Node:     &store.NodeExtern{},
				PrevNode: &store.NodeExtern{},
			},
			"0",
			http.StatusOK,
			nil,
		},
		// check new nodes return StatusCreated
		{
			&store.Event{
				Action:   store.Create,
				Node:     &store.NodeExtern{},
				PrevNode: &store.NodeExtern{},
			},
			"0",
			http.StatusCreated,
			nil,
		},
	}

	for i, tt := range tests {
		rw := httptest.NewRecorder()
		writeEvent(rw, tt.ev)
		if gct := rw.Header().Get("Content-Type"); gct != "application/json" {
			t.Errorf("case %d: bad Content-Type: got %q, want application/json", i, gct)
		}
		if gei := rw.Header().Get("X-Deimos-Index"); gei != tt.idx {
			t.Errorf("case %d: bad X-Deimos-Index header: got %s, want %s", i, gei, tt.idx)
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

	h := NewHandler(nil, Peers{}, time.Hour)
	s := httptest.NewServer(h)
	defer s.Close()

	for _, tt := range tests {
		req, err := http.NewRequest(tt.method, s.URL+machinesPrefix, nil)
		if err != nil {
			t.Fatal(err)
		}
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

	writer := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	h := &serverHandler{peers: peers}
	h.serveMachines(writer, req)
	w := "http://localhost:8080, http://localhost:8081, http://localhost:8082"
	if g := writer.Body.String(); g != w {
		t.Errorf("body = %s, want %s", g, w)
	}
	if writer.Code != http.StatusOK {
		t.Errorf("header = %d, want %d", writer.Code, http.StatusOK)
	}
}

func TestPeersEndpoints(t *testing.T) {
	tests := []struct {
		peers     Peers
		endpoints []string
	}{
		// single peer with a single address
		{
			peers: Peers(map[int64][]string{
				1: {"192.0.2.1"},
			}),
			endpoints: []string{"http://192.0.2.1"},
		},
		// single peer with a single address with a port
		{
			peers: Peers(map[int64][]string{
				1: {"192.0.2.1:8001"},
			}),
			endpoints: []string{"http://192.0.2.1:8001"},
		},
		// several peers explicitly unsorted
		{
			peers: Peers(map[int64][]string{
				2: {"192.0.2.3", "192.0.2.4"},
				3: {"192.0.2.5", "192.0.2.6"},
				1: {"192.0.2.1", "192.0.2.2"},
			}),
			endpoints: []string{"http://192.0.2.1", "http://192.0.2.2", "http://192.0.2.3", "http://192.0.2.4", "http://192.0.2.5", "http://192.0.2.6"},
		},
		// no peers
		{
			peers:     Peers(map[int64][]string{}),
			endpoints: []string{},
		},
		// peer with no endpoints
		{
			peers: Peers(map[int64][]string{
				3: {},
			}),
			endpoints: []string{},
		},
	}
	for i, tt := range tests {
		endpoints := tt.peers.Endpoints()
		if !reflect.DeepEqual(tt.endpoints, endpoints) {
			t.Errorf("#%d: peers.Endpoints() incorrect: want=%#v got=%#v", i, tt.endpoints, endpoints)
		}
	}
}

func TestAllowMethod(t *testing.T) {
	tests := []struct {
		m  string
		ms []string
		w  bool
		wh string
	}{
		// Accepted methods
		{
			m:  "GET",
			ms: []string{"GET", "POST", "PUT"},
			w:  true,
		},
		{
			m:  "POST",
			ms: []string{"POST"},
			w:  true,
		},
		// Made-up methods no good
		{
			m:  "FAKE",
			ms: []string{"GET", "POST", "PUT"},
			w:  false,
			wh: "GET,POST,PUT",
		},
		// Empty methods no good
		{
			m:  "",
			ms: []string{"GET", "POST"},
			w:  false,
			wh: "GET,POST",
		},
		// Empty accepted methods no good
		{
			m:  "GET",
			ms: []string{""},
			w:  false,
			wh: "",
		},
		// No methods accepted
		{
			m:  "GET",
			ms: []string{},
			w:  false,
			wh: "",
		},
	}

	for i, tt := range tests {
		rw := httptest.NewRecorder()
		g := allowMethod(rw, tt.m, tt.ms...)
		if g != tt.w {
			t.Errorf("#%d: got allowMethod()=%t, want %t", i, g, tt.w)
		}
		if !tt.w {
			if rw.Code != http.StatusMethodNotAllowed {
				t.Errorf("#%d: code=%d, want %d", i, rw.Code, http.StatusMethodNotAllowed)
			}
			gh := rw.Header().Get("Allow")
			if gh != tt.wh {
				t.Errorf("#%d: Allow header=%q, want %q", i, gh, tt.wh)
			}
		}
	}
}
