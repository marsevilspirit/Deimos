package deimos_http

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/marsevilspirit/deimos/elog"
	Err "github.com/marsevilspirit/deimos/error"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	"github.com/marsevilspirit/deimos/server"
	"github.com/marsevilspirit/deimos/server/serverpb"
	"github.com/marsevilspirit/deimos/store"
)

const (
	keysPrefix     = "/v2/keys"
	machinesPrefix = "/v2/machines"
	DefaultTimeout = 500 * time.Millisecond
)

var errClosed = errors.New("marshttp: client closed connection")

// Handler implements the http.Handler interface and serves mars client and
// raft communication.
type Handler struct {
	Timeout time.Duration
	Server  *server.Server
	// TODO: dynamic configuration may make this outdated. take care of it.
	// TODO: dynamic configuration may introduce race also.
	Peers Peers
}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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
	case strings.HasPrefix(r.URL.Path, keysPrefix):
		h.serveKeys(ctx, w, r)
	case strings.HasPrefix(r.URL.Path, machinesPrefix):
		h.serveMachines(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h Handler) serveKeys(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	rr, err := parseRequest(r, genID())
	if err != nil {
		writeError(w, err)
		return
	}

	resp, err := h.Server.Do(ctx, rr)
	if err != nil {
		writeError(w, err)
	}

	var ev *store.Event
	switch {
	case resp.Event != nil:
		ev = resp.Event
	case resp.Watcher != nil:
		if ev, err = waitForEvent(ctx, w, resp.Watcher); err != nil {
			http.Error(w, err.Error(), http.StatusGatewayTimeout)
			return
		}
	default:
		writeError(w, errors.New("received response with no Event/Watcher"))
		return
	}

	writeEvent(w, ev)
}

// serveMachines responds address list in the format '0.0.0.0, 1.1.1.1'.
// TODO: rethink the format of machine list because it is not json format.
func (h Handler) serveMachines(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "HEAD" {
		allow(w, "GET", "HEAD")
		return
	}
	endpoints := h.Peers.Endpoints()
	w.Write([]byte(strings.Join(endpoints, ", ")))
}

func (h *Handler) serveRaft(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("marshttp: error reading raft message:", err)
	}
	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		log.Println("marshttp: error unmarshaling raft message:", err)
	}
	log.Printf("marshttp: raft recv message from %#x: %+v", m.From, m)
	if err := h.Server.Node.Step(ctx, m); err != nil {
		log.Println("marshttp: error stepping raft messages:", err)
	}
}

// genID generates a random id that is: n < 0 < n.
func genID() int64 {
	for {
		b := make([]byte, 8)
		if _, err := io.ReadFull(crand.Reader, b); err != nil {
			panic(err) // really bad stuff happened
		}
		n := int64(binary.BigEndian.Uint64(b))
		if n != 0 {
			return n
		}
	}
}

// parseRequest convert a received http.Request to a server Request,
// performing validatian of supplied fields as appropriate.
// If any validation fails, an empty Request and non-nil error is returned.
func parseRequest(r *http.Request, id int64) (serverpb.Request, error) {
	var err error
	emptyReq := serverpb.Request{}

	err = r.ParseForm()
	if err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidForm,
			err.Error(),
		)
	}

	if !strings.HasPrefix(r.URL.Path, keysPrefix) {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidForm,
			"incorrect key prefix",
		)
	}

	p := r.URL.Path[len(keysPrefix):]

	var pIdx, wIdx, ttl uint64
	if pIdx, err = getUint64(r.Form, "prevIndex"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeIndexNaN,
			`invalid value for "prevIndex"`,
		)
	}
	if wIdx, err = getUint64(r.Form, "waitIndex"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeIndexNaN,
			`invalid value for "waitIndex"`,
		)
	}
	if ttl, err = getUint64(r.Form, "ttl"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeTTLNaN,
			`invalid value for "ttl"`,
		)
	}

	var rec, sort, wait bool
	if rec, err = getBool(r.Form, "recursive"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidField,
			`invalid value for "recursive"`,
		)
	}
	if sort, err = getBool(r.Form, "sorted"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidField,
			`invalid value for "sorted"`,
		)
	}
	if wait, err = getBool(r.Form, "wait"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidField,
			`invalid value for "wait"`,
		)
	}

	// prevExists is nullable, so leave it null if not specified
	var pe *bool
	if _, ok := r.Form["prevExists"]; ok {
		bv, err := getBool(r.Form, "prevExists")
		if err != nil {
			return emptyReq, Err.NewRequestError(
				Err.EcodeInvalidField,
				"invalid value for prevExists",
			)
		}
		pe = &bv
	}

	rr := serverpb.Request{
		Id:         id,
		Method:     r.Method,
		Path:       p,
		Val:        r.FormValue("value"),
		PrevValue:  r.FormValue("prevValue"),
		PrevIndex:  pIdx,
		PrevExists: pe,
		Recursive:  rec,
		Since:      wIdx,
		Sorted:     sort,
		Wait:       wait,
	}

	if pe != nil {
		rr.PrevExists = pe
	}

	if ttl > 0 {
		expr := time.Duration(ttl) * time.Second
		// TODO: use fake clock instead of time module
		rr.Expiration = time.Now().Add(expr).UnixNano()
	}

	return rr, nil
}

// getUint64 extracts a uint64 by the given key from a Form. If the key does
// not exist in the form, 0 is returned. If the key exists but the value is
// badly formed, an error is returned. If multiple values are present only the
// first is considered.
func getUint64(form url.Values, key string) (i uint64, err error) {
	if vals, ok := form[key]; ok {
		i, err = strconv.ParseUint(vals[0], 10, 64)
	}
	return
}

// getBool extracts a bool by the given key from a Form. If the key does not
// exist in the form, false is returned. If the key exists but the value is
// badly formed, an error is returned. If multiple values are present only the
// first is considered.
func getBool(form url.Values, key string) (b bool, err error) {
	if vals, ok := form[key]; ok {
		b, err = strconv.ParseBool(vals[0])
	}
	return
}

// writeError logs and writes the given Error to the ResponseWriter.
// If Error is an internal error, it is rendered to the ResponseWriter.
func writeError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}
	log.Println(err)
	if e, ok := err.(*Err.Error); ok {
		e.Write(w)
	} else {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func writeEvent(w http.ResponseWriter, ev *store.Event) error {
	if ev == nil {
		return errors.New("cannot write empty Event!")
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Add("X-Deimos-Index", fmt.Sprint(ev.Index()))

	if ev.IsCreated() {
		w.WriteHeader(http.StatusCreated)
	}

	return json.NewEncoder(w).Encode(ev)
}

// waitForEvent waits for a given watcher to return its associated
// event. It returns a non-nil error if the given Context times out
// or the given ResponseWriter triggers a CloseNotify.
func waitForEvent(ctx context.Context, w http.ResponseWriter, wa store.Watcher) (*store.Event, error) {
	// TODO: support streaming?
	defer wa.Remove()
	var nch <-chan bool
	if x, ok := w.(http.CloseNotifier); ok {
		nch = x.CloseNotify()
	}

	select {
	case ev := <-wa.EventChan():
		return ev, nil
	case <-nch:
		elog.TODO()
		return nil, errClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func allow(w http.ResponseWriter, m ...string) {
	w.Header().Set("Allow", strings.Join(m, ","))
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
}
