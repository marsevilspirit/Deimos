package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/marsevilspirit/deimos/raft/raftpb"
	"github.com/marsevilspirit/deimos/store"
)

const (
	raftPrefix = "/raft"
)

type ClusterStore interface {
	Get() Cluster
	Delete(id int64)
}

type clusterStore struct {
	Store store.Store
}

func NewClusterStore(st store.Store, c Cluster) ClusterStore {
	cls := &clusterStore{Store: st}
	for _, m := range c {
		cls.add(*m)
	}
	return cls
}

// add puts a new Member into the store.
// A Member with a matching id must not exist.
func (s *clusterStore) add(m Member) {
	b, err := json.Marshal(m)
	if err != nil {
		log.Panicf("marshal peer info error: %v", err)
	}

	if _, err := s.Store.Create(m.storeKey(), false, string(b), false, store.Permanent); err != nil {
		log.Panicf("add member should never fail: %v", err)
	}
}

// TODO(philips): keep the latest copy without going to the store to avoid the
// lock here.
func (s *clusterStore) Get() Cluster {
	c := &Cluster{}
	e, err := s.Store.Get(machineKVPrefix, true, false)
	if err != nil {
		log.Panicf("get member should never fail: %v", err)
	}
	for _, n := range e.Node.Nodes {
		m := Member{}
		if err := json.Unmarshal([]byte(*n.Value), &m); err != nil {
			log.Panicf("unmarshal peer error: %v", err)
		}
		err := c.Add(m)
		if err != nil {
			log.Panicf("add member to cluster should never fail: %v", err)
		}
	}
	return *c
}

// Delete removes a member from the store.
// The given id MUST exist.
func (s *clusterStore) Delete(id int64) {
	p := s.Get().FindID(id).storeKey()
	if _, err := s.Store.Delete(p, false, false); err != nil {
		log.Panicf("delete peer should never fail: %v", err)
	}
}

// addScheme adds the protocol prefix to a string; currently only HTTP
// TODO: improve this when implementing TLS
func addScheme(addr string) string {
	return fmt.Sprintf("http://%s", addr)
}

func Sender(t *http.Transport, cls ClusterStore) func(msgs []raftpb.Message) {
	c := &http.Client{Transport: t}

	scheme := "http"
	if t.TLSClientConfig != nil {
		scheme = "https"
	}

	return func(msgs []raftpb.Message) {
		for _, m := range msgs {
			// TODO: reuse go routines
			// limit the number of outgoing connections for the same receiver
			go send(c, scheme, cls, m)
		}
	}
}

func send(c *http.Client, scheme string, cls ClusterStore, m raftpb.Message) {
	// TODO (xiangli): reasonable retry logic
	for i := 0; i < 3; i++ {
		addr := cls.Get().Pick(m.To)
		if addr == "" {
			// TODO: unknown peer id.. what do we do? I
			// don't think his should ever happen, need to
			// look into this further.
			log.Printf("deimos http: no addr for %d", m.To)
			return
		}

		url := fmt.Sprintf("%s://%s%s", scheme, addr, raftPrefix)

		// TODO: don't block. we should be able to have 1000s
		// of messages out at a time.
		data, err := m.Marshal()
		if err != nil {
			log.Println("deimos http: dropping message:", err)
			return // drop bad message
		}
		if httpPost(c, url, data) {
			return // success
		}
		// TODO: backoff
	}
}

func httpPost(c *http.Client, url string, data []byte) bool {
	resp, err := c.Post(url, "application/protobuf", bytes.NewBuffer(data))
	if err != nil {
		// TODO: log the error?
		return false
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		// TODO: log the error?
		return false
	}
	return true
}
