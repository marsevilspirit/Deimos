package marshttp

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/marsevilspirit/marstore/elog"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	"github.com/marsevilspirit/marstore/server"
	"github.com/marsevilspirit/marstore/server/serverpb"
	"github.com/marsevilspirit/marstore/store"
)

var errClosed = errors.New("marshttp: client closed connection")

const DefaultTimeout = 500 * time.Millisecond

type Handler struct {
	Timeout time.Duration
	Server  *server.Server
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// TODO: set read/write timeout?

	timeout := h.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	switch {
	case strings.HasPrefix(r.URL.Path, "/raft"):
		h.serveRaft(ctx, w, r)
	case strings.HasPrefix(r.URL.Path, "/keys/"):
		h.serveKeys(ctx, w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *Handler) serveKeys(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	rr, err := parseRequest(r)
	if err != nil {
		log.Println(err) // reading of body failed
		return
	}

	resp, err := h.Server.Do(ctx, rr)
	if err != nil {
		log.Println(err)
		http.Error(w, "Internal Server Error", 500)
	}

	if err := encodeResponse(ctx, w, resp); err != nil {
		http.Error(w, "Timeout while waiting for response", 504)
		return
	}
}

func (h *Handler) serveRaft(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		elog.TODO()
	}
	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		elog.TODO()
	}
	if err := h.Server.Node.Step(ctx, m); err != nil {
		elog.TODO()
	}
}

func genId() int64 {
	b := make([]byte, 8)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err) // really bad stuff happened
	}
	return int64(binary.BigEndian.Uint64(b))
}

func parseRequest(r *http.Request) (serverpb.Request, error) {
	if err := r.ParseForm(); err != nil {
		return serverpb.Request{}, err
	}

	q := r.URL.Query()
	rr := serverpb.Request{
		Id:        genId(),
		Method:    r.Method,
		Val:       r.FormValue("value"),
		Path:      r.URL.Path[len("/keys"):],
		PrevValue: q.Get("prevValue"),
		PrevIndex: parseUint64(q.Get("prevIndex")),
		Recursive: parseBool(q.Get("recursive")),
		Since:     parseUint64(q.Get("waitIndex")),
		Sorted:    parseBool(q.Get("sorted")),
		Wait:      parseBool(q.Get("wait")),
	}

	// PrevExists is nullable, so we leave it null
	// if prevExists wasn't specified.
	_, ok := q["prevExists"]
	if ok {
		bv := parseBool(q.Get("prevExists"))
		rr.PrevExists = &bv
	}

	ttl := parseUint64(q.Get("ttl"))
	if ttl > 0 {
		expr := time.Duration(ttl) * time.Second
		rr.Expiration = time.Now().Add(expr).UnixNano()
	}

	return rr, nil
}

func parseBool(s string) bool {
	v, _ := strconv.ParseBool(s)
	return v
}

func parseUint64(s string) uint64 {
	v, _ := strconv.ParseUint(s, 10, 64)
	return v
}

func encodeResponse(ctx context.Context, w http.ResponseWriter, resp server.Response) (err error) {
	var ev *store.Event
	switch {
	case resp.Event != nil:
		ev = resp.Event
	case resp.Watcher != nil:
		ev, err = waitForEvent(ctx, w, resp.Watcher)
		if err != nil {
			return err
		}
	default:
		panic("should not be reachable")
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("X-Mars-Index", fmt.Sprint(ev.Index()))

	if ev.IsCreated() {
		w.WriteHeader(http.StatusCreated)
	}

	if err := json.NewEncoder(w).Encode(ev); err != nil {
		panic(err) // should never be reached
	}
	return nil
}

func waitForEvent(ctx context.Context, w http.ResponseWriter, wa *store.Watcher) (*store.Event, error) {
	// TODO: support streaming?
	defer wa.Remove()
	var nch <-chan bool
	if x, ok := w.(http.CloseNotifier); ok {
		nch = x.CloseNotify()
	}

	select {
	case ev := <-wa.EventChan:
		return ev, nil
	case <-nch:
		elog.TODO()
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}

}
