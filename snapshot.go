package raft

type Snapshot struct {
	Data []byte

	Nodes []int64

	Index int

	Term int
}

type Snapshoter interface {
	Snap(index, term int, nodes []int64)
	Restore(snap Snapshot)
	GetSnap() Snapshot
}
