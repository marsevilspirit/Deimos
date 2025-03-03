package store

import "time"

type KeyValuePair struct {
	Key        string     `json:"key,omitempty"`
	Value      string     `json:"value,omitempty"`
	Dir        bool       `json:"dir,omitempty"`
	Expiration *time.Time `json:"expiration,omitempty"`
	TTL        int64      `json:"ttl,omitempty"` // time to live in second
	KVPairs    kvPairs    `json:"kvpairs,omitempty"`
}

type kvPairs []KeyValuePair

// interfaces for sorting
func (kvs kvPairs) Len() int {
	return len(kvs)
}

func (kvs kvPairs) Less(i, j int) bool {
	return kvs[i].Key < kvs[j].Key
}

func (kvs kvPairs) Swap(i, j int) {
	kvs[i], kvs[j] = kvs[j], kvs[i]
}
