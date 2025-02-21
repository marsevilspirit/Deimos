package raft

import (
	"context"
	"errors"
	"log"

	pb "github.com/marsevilspirit/m_raft/raftpb"
)

var (
	emptyState = pb.State{}
	ErrStopped = errors.New("raft: stopped")
)

// Ready encapsulates the entries and messages that are ready to be saved to
// stable storage, committed or sent to other peers.
type Ready struct {
	// The current state of a Node.
	pb.State

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	Messages []pb.Message
}

func isStateEqual(a, b pb.State) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.LastIndex == b.LastIndex
}

func IsEmptyState(st pb.State) bool {
	return isStateEqual(st, emptyState)
}

func (rd Ready) containsUpdates() bool {
	return !IsEmptyState(rd.State) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0
}

type Node struct {
	ctx    context.Context
	propc  chan pb.Message
	recvc  chan pb.Message
	readyc chan Ready
	tickc  chan struct{}
	done   chan struct{}
}

// Start returns a new Node given a unique raft id, a list of raft peers, and
// the election and heartbeat timeouts in units of ticks.
func Start(id int64, peers []int64, election, heartbeat int) Node {
	n := newNode()
	r := newRaft(id, peers, election, heartbeat)
	go n.run(r)
	return n
}

// Restart is identical to Start but takes an initial State and a slice of
// entries. Generally this is used when restarting from a stable storage
// log.
func Restart(id int64, peers []int64, election, heartbeat int, state pb.State, ents []pb.Entry) Node {
	n := newNode()
	r := newRaft(id, peers, election, heartbeat)
	r.loadState(state)
	r.loadEnts(ents)
	go n.run(r)
	return n
}

func newNode() Node {
	return Node{
		propc:  make(chan pb.Message),
		recvc:  make(chan pb.Message),
		readyc: make(chan Ready),
		tickc:  make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (n *Node) Stop() {
	close(n.done)
}

func (n *Node) run(r *raft) {
	propc := n.propc
	readyc := n.readyc

	var lead int64
	prevSt := r.State

	for {
		if lead != r.lead {
			log.Printf("raft: leader changed from %#x to %#x", lead, r.lead)
			lead = r.lead
			if r.hasLeader() {
				propc = n.propc
			} else {
				propc = nil
			}
		}

		rd := newReady(r, prevSt)

		if rd.containsUpdates() {
			readyc = n.readyc
		} else {
			readyc = nil
		}

		select {
		case m := <-propc:
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:
			r.Step(m) // raft never returns an error
		case <-n.tickc:
			r.tick()
		case readyc <- rd:
			r.raftLog.resetNextEnts()
			r.raftLog.resetUnstable()
			if !IsEmptyState(rd.State) {
				prevSt = rd.State
			}
			r.msgs = nil
		case <-n.done:
			return
		}
	}
}

// Tick increments the internal logical clock for this Node. Election timeouts
// and heartbeat timeouts are in units of ticks.
func (n *Node) Tick() error {
	select {
	case n.tickc <- struct{}{}:
		return nil
	case <-n.done:
		return ErrStopped
	}
}

func (n *Node) Campaign(ctx context.Context) error {
	return n.Step(ctx, pb.Message{Type: msgHup})
}

// client sends proposals to the leader using this method. The proposals are
func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.Step(ctx,
		pb.Message{
			Type: msgProp,
			Entries: []pb.Entry{
				{Data: data},
			},
		},
	)
}

// Step advances the state machine using msgs. The ctx.Err() will be returned,
// if any.
func (n *Node) Step(ctx context.Context, m pb.Message) error {
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
func (n *Node) Ready() <-chan Ready {
	return n.readyc
}

func newReady(r *raft, prev pb.State) Ready {
	rd := Ready{
		Entries:          r.raftLog.unstableEnts(),
		CommittedEntries: r.raftLog.nextEnts(),
		Messages:         r.msgs,
	}

	if !isStateEqual(r.State, prev) {
		rd.State = r.State
	}
	return rd
}
