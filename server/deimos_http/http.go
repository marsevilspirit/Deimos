package deimos_http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	Err "github.com/marsevilspirit/deimos/error"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	"github.com/marsevilspirit/deimos/server"
	"github.com/marsevilspirit/deimos/server/serverpb"
	"github.com/marsevilspirit/deimos/store"
)

const (
	keysPrefix     = "/keys"
	machinesPrefix = "/machines"
	raftPrefix     = "/raft"

	// time to wait for response from DeimosServer requests
	DefaultServerTimeout = 500 * time.Millisecond

	// time to wait for a Watch request
	defaultWatchTimeout = 5 * time.Minute
)

var errClosed = errors.New("marshttp: client closed connection")

// NewClientHandler generates a muxed http.Handler with the given parameters to serve deimos client requests.
func NewClientHandler(server server.Server, peers Peers, timeout time.Duration) http.Handler {
	sh := &serverHandler{
		server:  server,
		peers:   peers,
		timeout: timeout,
	}

	if sh.timeout == 0 {
		sh.timeout = DefaultServerTimeout
	}

	mux := http.NewServeMux()
	mux.HandleFunc(keysPrefix, sh.serveKeys)
	mux.HandleFunc(keysPrefix+"/", sh.serveKeys)
	// TODO: dynamic configuration may make this outdated. take care of it.
	// TODO: dynamic configuration may introduce race also.
	mux.HandleFunc(machinesPrefix, sh.serveMachines)
	mux.HandleFunc("/", http.NotFound)
	return mux
}

// NewPeerHandler generates an http.Handler to handle deimos peer (raft) requests.
func NewPeerHandler(server server.Server) http.Handler {
	sh := &serverHandler{
		server: server,
	}
	mux := http.NewServeMux()
	mux.HandleFunc(raftPrefix, sh.serveRaft)
	mux.HandleFunc("/", http.NotFound)
	return mux
}

// serverHandler provides http.Handlers for deimos client and raft communication.
type serverHandler struct {
	timeout time.Duration
	server  server.Server
	peers   Peers
}

func (h serverHandler) serveKeys(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r.Method, "GET", "PUT", "POST", "DELETE") {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.timeout)
	defer cancel()

	rr, err := parseRequest(r, server.GenID())
	if err != nil {
		writeError(w, err)
		return
	}

	resp, err := h.server.Do(ctx, rr)
	if err != nil {
		writeError(w, err)
	}

	switch {
	case resp.Event != nil:
		if err := writeEvent(w, resp.Event); err != nil {
			// Should never be reached
			log.Printf("error writing event: %v", err)
		}
	case resp.Watcher != nil:
		ctx, cancel := context.WithTimeout(context.Background(), defaultWatchTimeout)
		defer cancel()
		handleWatch(ctx, w, resp.Watcher, rr.Stream)
	default:
		writeError(w, errors.New("received response with no Event/Watcher"))
	}
}

// serveMachines responds address list in the format '0.0.0.0, 1.1.1.1'.
// TODO: rethink the format of machine list because it is not json format.
func (h serverHandler) serveMachines(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r.Method, "GET", "HEAD") {
		return
	}
	endpoints := h.peers.Endpoints()
	w.Write([]byte(strings.Join(endpoints, ", ")))
}

func (h *serverHandler) serveRaft(w http.ResponseWriter, r *http.Request) {
	if !allowMethod(w, r.Method, "POST") {
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println("deimos_http: error reading raft message:", err)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		return
	}
	var m raftpb.Message
	if err := m.Unmarshal(b); err != nil {
		log.Println("deimos_http: error unmarshaling raft message:", err)
		http.Error(w, "error reading raft message", http.StatusBadRequest)
		return
	}
	log.Printf("marshttp: raft recv message from %#x: %+v", m.From, m)
	if err := h.server.Process(context.TODO(), m); err != nil {
		log.Println("deimos_http: error processing raft message:", err)
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	var rec, sort, wait, stream bool
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

	if stream, err = getBool(r.Form, "stream"); err != nil {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidField,
			`invalid value for "stream"`,
		)
	}

	if wait && r.Method != "GET" {
		return emptyReq, Err.NewRequestError(
			Err.EcodeInvalidField,
			`"wait" can only be used with GET requests`,
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
		Id:        id,
		Method:    r.Method,
		Path:      p,
		Val:       r.FormValue("value"),
		PrevValue: r.FormValue("prevValue"),
		PrevIndex: pIdx,
		PrevExist: pe,
		Recursive: rec,
		Since:     wIdx,
		Sorted:    sort,
		Stream:    stream,
		Wait:      wait,
	}

	if pe != nil {
		rr.PrevExist = pe
	}

	// TODO: use fake clock instead of time module
	if ttl > 0 {
		expr := time.Duration(ttl) * time.Second
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

// writeEvent serializes a single Event and writes the resulting
// JSON to the given ResponseWriter, along with the appropriate
// headers
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

func handleWatch(ctx context.Context, w http.ResponseWriter, wa store.Watcher, stream bool) {
	defer wa.Remove()
	ech := wa.EventChan()
	var nch <-chan bool
	if x, ok := w.(http.CloseNotifier); ok {
		nch = x.CloseNotify()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Ensure headers are flushed early, in case of long polling
	w.(http.Flusher).Flush()

	for {
		select {
		case <-nch:
			// Client closed connection. Nothing to do.
			return
		case <-ctx.Done():
			// Timed out. net/http will close the connection for use,
			// so nothing to do.
			return
		case ev, ok := <-ech:
			if !ok {
				// If the channel is closed this may be an indication of
				// that notifications are much more than we are able to
				// send to the client in time. Then we simply end streaming.
				return
			}
			if err := json.NewEncoder(w).Encode(ev); err != nil {
				// Should never be reached
				log.Printf("error writing event: %v", err)
				return
			}
			if !stream {
				return
			}
			w.(http.Flusher).Flush()
		}
	}
}

// allowMethod verifies that the given method is one of the allowed methods,
// and if not, it writes an error to w.  A boolean is returned indicating
// whether or not the method is allowed.
func allowMethod(w http.ResponseWriter, m string, ms ...string) bool {
	for _, meth := range ms {
		if m == meth {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(ms, ","))
	http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
	return false
}
