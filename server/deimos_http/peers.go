package deimos_http

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/marsevilspirit/deimos/elog"
	"github.com/marsevilspirit/deimos/raft/raftpb"
)

// Peers contains a mapping of unique IDs to a
// list of hostnames/IP addresses
type Peers map[int64][]string

// addScheme adds the protocol prefix to a string; currently only HTTP
// TODO: improve this when implementing TLS

func addScheme(addr string) string {
	return fmt.Sprintf("http://%s", addr)
}

// Pick returns a random address from a given Peer's addresses. If the
// given peer does not exist, an empty string is returned.
func (ps Peers) Pick(id int64) string {
	addrs := ps[id]
	if len(addrs) == 0 {
		return ""
	}
	return addrs[rand.Intn(len(addrs))]
}

// Each time set will reset peers.
// Set parses command line sets of names to ips formatted like:
// a=1.1.1.1&a=1.1.1.2&b=2.2.2.2
func (ps *Peers) Set(s string) error {
	m := make(map[int64][]string)
	v, err := url.ParseQuery(s)
	if err != nil {
		return err
	}
	for k, v := range v {
		id, err := strconv.ParseInt(k, 0, 64)
		if err != nil {
			return err
		}
		m[id] = v
	}
	*ps = m
	return nil
}

func (ps Peers) String() string {
	v := url.Values{}
	for k, vv := range ps {
		for i := range vv {
			v.Add(strconv.FormatInt(k, 16), vv[i])
		}
	}
	return v.Encode()
}

func (ps Peers) IDs() []int64 {
	var ids []int64
	for id := range ps {
		ids = append(ids, id)
	}
	return ids
}

// EndPoints returns a list of all peer addresses. Each address is
// prefixed with "http://". The returned list is sorted (asc).
// NOTE: with Scheme.
func (ps Peers) Endpoints() []string {
	endpoints := make([]string, 0)
	for _, addrs := range ps {
		for _, addr := range addrs {
			endpoints = append(endpoints, addScheme(addr))
		}
	}
	sort.Strings(endpoints)
	return endpoints
}

// Addrs returns a list of all peer addresses. The returned list is
// sorted in ascending lexicographical order.
// NOTE: without Scheme.
func (ps Peers) Addrs() []string {
	addrs := make([]string, 0)
	for _, paddrs := range ps {
		for _, paddr := range paddrs {
			addrs = append(addrs, paddr)
		}
	}
	sort.Strings(addrs)
	return addrs
}

func Sender(t *http.Transport, p Peers) func(msgs []raftpb.Message) {
	c := &http.Client{Transport: t}

	scheme := "http"
	if t.TLSClientConfig != nil {
		scheme = "https"
	}

	return func(msgs []raftpb.Message) {
		for _, m := range msgs {
			// TODO: reuse go routines
			// limit the number of outgoing connections for the same receiver
			go send(c, scheme, p, m)
		}
	}
}

func send(c *http.Client, scheme string, p Peers, m raftpb.Message) {
	// TODO: reasonable retry logic
	for i := 0; i < 3; i++ {
		addr := p.Pick(m.To)
		if addr == "" {
			// TODO: unknown peer id.. what do we do? I
			// don't think his should ever happen, need to
			// look into this further.
			log.Printf("marshttp: no addr for %d", m.To)
			break
		}

		url := fmt.Sprintf("%s://%s%s", scheme, addr, raftPrefix)

		// TODO: don't block. we should be able to have 1000s
		// of messages out at a time.
		data, err := m.Marshal()
		if err != nil {
			log.Println("marshttp: dropping message:", err)
			break // drop bad message
		}
		if httpPost(c, url, data) {
			break // success
		}

		// TODO: backoff
	}
}

func httpPost(c *http.Client, url string, data []byte) bool {
	resp, err := http.Post(url, "application/protobuf", bytes.NewBuffer(data))
	if err != nil {
		elog.TODO()
		return false
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		elog.TODO()
		return false
	}
	return true
}
