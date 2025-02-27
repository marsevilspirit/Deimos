package store

import (
	"encoding/json"
)

type StoreStats struct {
	// Number of get requests
	Gets uint64 `json:"gets"`

	// Number of set requests
	Sets uint64 `json:"sets"`

	// Number of delete requests
	Deletes uint64 `json:"deletes"`

	// Number of testAndSet requests
	TestAndSets uint64 `json:"testAndSets"`
}

// Stats returns the basic statistics information of etcd storage
func (s *Store) Stats() []byte {
	b, _ := json.Marshal(s.BasicStats)
	return b
}
