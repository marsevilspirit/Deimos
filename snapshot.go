package raft

type Snapshot struct {
	Data []byte
	// the configuration
	Nodes []int64
	// the index at which the snapshot was taken.
	Index int64
	// the log term of the index
	Term int64
}

var emptySnapshot = Snapshot{}

func (s Snapshot) IsEmpty() bool {
	return s.Term == 0
}
