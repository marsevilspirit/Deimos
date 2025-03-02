package store

import (
	"time"
)

const (
	Get         = "get"
	Create      = "create"
	Update      = "update"
	Delete      = "delete"
	TestAndSet  = "testAndSet"
	TestIAndSet = "testiAndSet"
	Expire      = "expire"
)

const (
	UndefIndex = 0
	UndefTerm  = 0
)

type Event struct {
	Action     string     `json:"action"`
	Key        string     `json:"key"`
	Dir        bool       `json:"dir,omitempty"`
	PrevValue  string     `json:"prevValue,omitempty"`
	Value      string     `json:"value,omitempty"`
	KVPairs    kvPairs    `json:"kvs,omitempty"`
	Expiration *time.Time `json:"expiration,omitempty"`
	TTL        int64      `json:"ttl,omitempty"` // Time to live in second
	Index      uint64     `json:"index"`
	Term       uint64     `json:"term"`
}

func newEvent(action string, key string, index uint64, term uint64) *Event {
	return &Event{
		Action: action,
		Key:    key,
		Index:  index,
		Term:   term,
	}
}
