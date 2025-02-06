package raft

type Snapshot struct {
	Data []byte

	Nodes []int64

	Index int64

	Term int64
}

var emptySnapshot = Snapshot{}

func (s Snapshot) IsEmpty() bool {
	return s.Term == 0
}
