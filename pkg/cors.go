package pkg

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type CORSInfo map[string]bool

// CORSInfo implements the flag.Value interface to allow users to define a list of CORS origins
func (ci *CORSInfo) Set(s string) error {
	m := make(map[string]bool)
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if v != "*" {
			if _, err := url.Parse(v); err != nil {
				return fmt.Errorf("Invalid CORS origin: %s", err)
			}
		}
		m[v] = true

	}
	*ci = CORSInfo(m)
	return nil
}

func (ci *CORSInfo) String() string {
	o := make([]string, 0)
	for k, _ := range *ci {
		o = append(o, k)
	}
	return strings.Join(o, ",")
}

// OriginAllowed determines whether the server will allow a given CORS origin.
func (c CORSInfo) OriginAllowed(origin string) bool {
	return c["*"] || c[origin]
}

type CORSHandler struct {
	Handler http.Handler
	Info    *CORSInfo
}

// addHeader adds the correct cors headers given an origin
func (h *CORSHandler) addHeader(w http.ResponseWriter, origin string) {
	w.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Add("Access-Control-Allow-Origin", origin)
	w.Header().Add("Access-Control-Allow-Headers", "accept, content-type")
}

// ServeHTTP adds the correct CORS headers based on the origin and returns immediately
// with a 200 OK if the method is OPTIONS.
func (h *CORSHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// It is important to flush before leaving the goroutine.
	// Or it may miss the latest info written.
	defer w.(http.Flusher).Flush()

	// Write CORS header.
	if h.Info.OriginAllowed("*") {
		h.addHeader(w, "*")
	} else if origin := req.Header.Get("Origin"); h.Info.OriginAllowed(origin) {
		h.addHeader(w, origin)
	}

	if req.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	h.Handler.ServeHTTP(w, req)
}
