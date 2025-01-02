package raft

type Snapshot struct {
	Data []byte

	Nodes []int64

	Index int64

	Term int64
}

type Snapshoter interface {
	Snap(index, term int64, nodes []int64)
	Restore(snap Snapshot)
	GetSnap() Snapshot
}
