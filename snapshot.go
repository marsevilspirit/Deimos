package raft

type Snapshot struct {
	Data []byte

	Nodes []int

	Index int

	Term int
}

type Snapshoter interface {
	Snap(index, term int, nodes []int)
	Restore(snap Snapshot)
	GetSnap() Snapshot
}
