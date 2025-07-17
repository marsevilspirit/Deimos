package raft

import (
	"context"
	"errors"
	"log"
	"log/slog"

	pb "github.com/marsevilspirit/deimos/raft/raftpb"
)

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: stopped")
)

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead       int64
	RaftState  StateType
	ShouldStop bool
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState && a.ShouldStop == b.ShouldStop
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	Messages []pb.Message
}

func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Index == 0
}

func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

type Node interface {
	// Tick increments the internal logical clock for the node
	// by a single tick.
	// Election timeouts and heartbeat timeouts are in units of ticks.
	Tick()
	// Campaign causes the node to transition to candidate state
	// and start campaigning to become leader.
	Campaign(ctx context.Context) error
	// Propose proposes that data be appended to the log.
	Propose(ctx context.Context, data []byte) error
	// ProposeConfChange proposes config change.
	// At most one ConfChange can be in the process of
	// going through consensus.
	// Application needs to call ApplyConfChange
	// when applying EntryConfChange type entry.
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	// Step advances the state machine using the given message.
	// ctx.Err() will be returned, if any.
	Step(ctx context.Context, m pb.Message) error
	// Ready returns a channel that returns
	// the current point-in-time state.
	Ready() <-chan Ready
	// ApplyConfChange applies config change to the local node.
	// TODO: reject existing node when add node
	// TODO: reject non-existant node when remove node
	ApplyConfChange(cc pb.ConfChange)
	// Stop performs any necessary termination of the Node.
	Stop()
	// Compact triggers a compaction of the log.
	Compact(d []byte)
}

// StartNode returns a new Node given a unique raft id,
// a list of raft peers, and the election and
// heartbeat timeouts in units of ticks.
func StartNode(id int64, peers []int64, election, heartbeat int) Node {
	n := newNode()
	r := newRaft(id, peers, election, heartbeat)
	go n.run(r)
	return &n
}

// RestartNode is identical to StartNode but takes an initial State and
// a slice of entries.
// Generally this is used when restarting from a stable storage log.
func RestartNode(id int64, peers []int64, election, heartbeat int, snapshot *pb.Snapshot, state pb.HardState, ents []pb.Entry) Node {
	n := newNode()
	r := newRaft(id, peers, election, heartbeat)
	if snapshot != nil {
		r.restore(*snapshot)
	}
	r.loadState(state)
	r.loadEnts(ents)
	go n.run(r)
	return &n
}

// node is the canonical implementation of the Node interface
type node struct {
	propc    chan pb.Message
	recvc    chan pb.Message
	compactc chan []byte
	confc    chan pb.ConfChange
	readyc   chan Ready
	tickc    chan struct{}
	done     chan struct{}
}

func newNode() node {
	return node{
		propc:    make(chan pb.Message),
		recvc:    make(chan pb.Message),
		compactc: make(chan []byte),
		confc:    make(chan pb.ConfChange),
		readyc:   make(chan Ready),
		tickc:    make(chan struct{}),
		done:     make(chan struct{}),
	}
}

func (n *node) Stop() {
	close(n.done)
}

func (n *node) run(r *raft) {
	var propc chan pb.Message
	var readyc chan Ready

	lead := None
	prevSoftSt := r.softState()
	prevHardSt := r.HardState
	prevSnapi := r.raftLog.snapshot.Index

	for {
		rd := newReady(r, prevSoftSt, prevHardSt, prevSnapi)
		if rd.containsUpdates() {
			readyc = n.readyc
		} else {
			readyc = nil
		}

		if rd.SoftState != nil && lead != rd.SoftState.Lead {
			log.Printf("raft: leader changed from %#x to %#x", lead, r.lead)
			lead = r.softState().Lead
			// block proposal when don't have a leader.
			if r.hasLeader() {
				propc = n.propc
			} else {
				propc = nil
			}
		}

		select {
		// TODO: maybe buffer the config propose
		// if there exists one (the way
		// described in raft dissertation)
		// Currently it is dropped in Step silently.
		case m := <-propc:
			slog.Debug("<-propc")
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:
			slog.Debug("<-recvc")
			r.Step(m) // raft never returns an error
		case d := <-n.compactc:
			slog.Debug("<-compactc")
			r.compact(d)
		case cc := <-n.confc:
			slog.Debug("<-confc")
			switch cc.Type {
			case pb.ConfChangeAddNode:
				r.addNode(cc.NodeID)
			case pb.ConfChangeRemoveNode:
				r.removeNode(cc.NodeID)
			default:
				panic("unexpected conf type")
			}
		case <-n.tickc:
			slog.Debug("<-tickc")
			r.tick()
		case readyc <- rd:
			slog.Debug("readyc <- rd")
			slog.Debug("newReady", "rd", rd)
			if rd.SoftState != nil {
				prevSoftSt = rd.SoftState
			}
			if !IsEmptyHardState(rd.HardState) {
				prevHardSt = rd.HardState
			}
			if !IsEmptySnap(rd.Snapshot) {
				prevSnapi = rd.Snapshot.Index
			}
			// TODO: we assume that all committed config
			// entries will be applied to make things easy for now.
			// TODO: it may have race because applied is set
			// before entries are applied.
			r.raftLog.resetNextEnts()
			r.raftLog.resetUnstable()
			r.msgs = nil
		case <-n.done:
			log.Println("<-done")
			return
		}
	}
}

// Tick increments the internal logical clock for this Node.
// Election timeouts and heartbeat timeouts are in units of ticks.
func (n *node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	}
}

// Campaign causes this Node to transition to candidate state.
func (n *node) Campaign(ctx context.Context) error {
	return n.step(ctx, pb.Message{Type: msgHup})
}

func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.Step(ctx,
		pb.Message{
			Type: msgProp,
			Entries: []pb.Entry{
				{Data: data},
			},
		},
	)
}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx,
		pb.Message{
			Type: msgProp,
			Entries: []pb.Entry{
				{Type: pb.EntryConfChange, Data: data},
			},
		},
	)
}

func (n *node) Step(ctx context.Context, m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if m.Type == msgHup || m.Type == msgBeat {
		// TODO: return an error?
		return nil
	}
	return n.step(ctx, m)
}

// Step advances the state machine using msgs.
// The ctx.Err() will be returned, if any.
func (n *node) step(ctx context.Context, m pb.Message) error {
	ch := n.recvc
	if m.Type == msgProp {
		ch = n.propc
	}

	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// ReadState returns the current point-in-time state.
func (n *node) Ready() <-chan Ready {
	return n.readyc
}

func (n *node) ApplyConfChange(cc pb.ConfChange) {
	select {
	case n.confc <- cc:
	case <-n.done:
	}
}

func (n *node) Compact(d []byte) {
	select {
	case n.compactc <- d:
	case <-n.done:
	}
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState, prevSnapi int64) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEnts(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}

	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if !isHardStateEqual(r.HardState, prevHardSt) {
		rd.HardState = r.HardState
	}
	if prevSnapi != r.raftLog.snapshot.Index {
		rd.Snapshot = r.raftLog.snapshot
	}
	return rd
}
