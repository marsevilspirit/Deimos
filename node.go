package raft

import (
	"context"
)

type stateResp struct {
	state State
	ents  []Entry
	cents []Entry
	msgs  []Message
}

func (a State) Equal(b State) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.LastIndex == b.LastIndex
}

func (sr stateResp) containsUpdates(prev stateResp) bool {
	return !prev.state.Equal(sr.state) || len(sr.ents) > 0 ||
		len(sr.cents) > 0 || len(sr.msgs) > 0
}

type Node struct {
	ctx    context.Context
	propc  chan []byte
	recvc  chan Message
	statec chan stateResp
	tickc  chan struct{}
}

func Start(ctx context.Context, id int64, peers []int64) *Node {
	n := &Node{
		ctx:    ctx,
		propc:  make(chan []byte),
		recvc:  make(chan Message),
		statec: make(chan stateResp),
		tickc:  make(chan struct{}),
	}
	r := newRaft(id, peers)
	go n.run(r)
	return n
}

func (n *Node) run(r *raft) {
	propc := n.propc
	statec := n.statec
	var prev stateResp
	for {
		if r.hasLeader() {
			propc = n.propc
		} else {
			propc = nil
		}

		sr := stateResp{
			state: r.State,
			ents:  r.raftLog.unstableEnts(),
			cents: r.raftLog.nextEnts(),
			msgs:  r.msgs,
		}

		if sr.containsUpdates(prev) {
			statec = n.statec
		} else {
			statec = nil
		}

		select {
		case p := <-propc:
			r.propose(p)
		case m := <-n.recvc:
			r.Step(m)
		case <-n.tickc:
			// r.tick()
		case statec <- sr:
			r.raftLog.resetNextEnts()
			r.raftLog.resetUnstable()
			r.msgs = nil
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) Tick() error {
	select {
	case n.tickc <- struct{}{}:
		return nil
	case <-n.ctx.Done():
		return n.ctx.Err()
	}
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	select {
	case n.propc <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.ctx.Done():
		return n.ctx.Err()
	}
}

func (n *Node) Step(m Message) error {
	select {
	case n.recvc <- m:
		return nil
	case <-n.ctx.Done():
		return n.ctx.Err()
	}
}

func (n *Node) ReadState(ctx context.Context) (st State, ents, cents []Entry, msgs []Message, err error) {
	select {
	case sr := <-n.statec:
		return sr.state, sr.ents, sr.cents, sr.msgs, nil
	case <-ctx.Done():
		return State{}, nil, nil, nil, ctx.Err()
	case <-n.ctx.Done():
		return State{}, nil, nil, nil, n.ctx.Err()
	}
}
