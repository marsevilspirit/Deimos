package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	pb "github.com/marsevilspirit/m_raft/raftpb"
)

// nextEnts returns the appliable entries and updates the applied index
func nextEnts(r *raft) (ents []pb.Entry) {
	ents = r.raftLog.nextEnts()
	r.raftLog.resetNextEnts()
	return ents
}

type Interface interface {
	Step(m pb.Message) error
	ReadMessages() []pb.Message
}

// leader选举测试
func TestLeaderElection(t *testing.T) {
	tests := []struct {
		*network
		state stateType
	}{
		{newNetwork(nil, nil, nil), stateLeader},
		{newNetwork(nil, nil, nopStepper), stateLeader},
		{newNetwork(nil, nopStepper, nopStepper), stateCandidate},
		{newNetwork(nil, nopStepper, nopStepper, nil), stateCandidate},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), stateLeader},

		// three logs further along than 0
		{newNetwork(nil, ents(1), ents(2), ents(1, 3), nil), stateFollower},

		// logs converge
		{newNetwork(ents(1), nil, ents(2), ents(1), nil), stateLeader},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
		sm := tt.network.peers[1].(*raft)
		if sm.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.state, tt.state)
		}
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []pb.Message
		wcommitted int64
	}{
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{
				{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]pb.Message{

				{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
				{From: 1, To: 2, Type: msgHup},
				{From: 1, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*raft)

			if sm.raftLog.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.raftLog.committed, tt.wcommitted)
			}

			ents := make([]pb.Entry, 0)
			for _, e := range nextEnts(sm) {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}
			props := make([]pb.Message, 0)
			for _, m := range tt.msgs {
				if m.Type == msgProp {
					props = append(props, m)
				}
			}
			for k, m := range props {
				if !bytes.Equal(ents[k].Data, m.Entries[0].Data) {
					t.Errorf("#%d.%d: data = %d, want %d", i, j, ents[k].Data, m.Entries[0].Data)
				}
			}
		}
	}
}

func TestSingleNodeCommit(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 3)
	}
}

// TestCannotCommitWithoutNewTermEntry tests the entries cannot be committed
// when leader changes, no new proposal comes in and ChangeTerm proposal is
// filtered.
func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	// network recovery
	tt.recover()
	// avoid committing ChangeTerm proposal
	tt.ignore(msgApp)

	// elect 1 as the new leader with term 2
	tt.send(pb.Message{From: 2, To: 2, Type: msgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[2].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	tt.recover()

	// send out a heartbeat
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: 2, To: 2, Type: msgBeat})

	if sm.raftLog.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 4)
	}

	// still be able to append a entry
	tt.send(pb.Message{From: 2, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	if sm.raftLog.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 5)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	// 0 cannot reach 2,3,4
	tt.cut(1, 3)
	tt.cut(1, 4)
	tt.cut(1, 5)

	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})
	tt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: []byte("some data")}}})

	sm := tt.peers[1].(*raft)
	if sm.raftLog.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 1)
	}

	// network recovery
	tt.recover()

	// elect 1 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(pb.Message{From: None, To: 2, Type: msgHup})

	if sm.raftLog.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.raftLog.committed, 4)
	}
}

// 测试在分布式系统中两个候选者节点同时发起选举的情况
func TestDuelingCandidates(t *testing.T) {
	a := newRaft(-1, nil, 0, 0) // k, id are set later
	b := newRaft(-1, nil, 0, 0)
	c := newRaft(-1, nil, 0, 0)

	nt := newNetwork(a, b, c)
	nt.cut(1, 3)

	nt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	nt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	nt.recover()
	nt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	wlog := &raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}}, committed: 1}
	tests := []struct {
		sm      *raft
		state   stateType
		term    int64
		raftLog *raftLog
	}{
		{a, stateFollower, 2, wlog},
		{b, stateFollower, 2, wlog},
		{c, stateFollower, 2, newLog()},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.Term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
		base := ltoa(tt.raftLog)
		if sm, ok := nt.peers[1+int64(i)].(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(1)

	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 3, To: 3, Type: msgHup})

	// heal the partition
	tt.recover()

	data := []byte("force follower")
	// send a proposal to 2 to flush out a msgApp to 0
	tt.send(pb.Message{From: 3, To: 3, Type: msgProp, Entries: []pb.Entry{{Data: data}}})

	a := tt.peers[1].(*raft)
	if g := a.state; g != stateFollower {
		t.Errorf("state = %s, want %s", g, stateFollower)
	}
	if g := a.Term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wantLog := ltoa(&raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}, committed: 2})
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	sm := tt.peers[1].(*raft)
	if sm.state != stateLeader {
		t.Errorf("state = %d, want %d", sm.state, stateLeader)
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	tt.send(pb.Message{From: 2, To: 2, Type: msgHup})
	tt.send(pb.Message{From: 1, To: 1, Type: msgHup})
	// pretend we're an old leader trying to make progress
	tt.send(pb.Message{From: 1, To: 1, Type: msgApp, Term: 1, Entries: []pb.Entry{{Term: 1}}})

	l := &raftLog{
		ents: []pb.Entry{
			{}, {Data: nil, Term: 1, Index: 1},
			{Data: nil, Term: 2, Index: 2}, {Data: nil, Term: 3, Index: 3},
		},
		committed: 3,
	}
	base := ltoa(l)
	for i, p := range tt.peers {
		if sm, ok := p.(*raft); ok {
			l := ltoa(sm.raftLog)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff:\n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestProposal(t *testing.T) {
	tests := []struct {
		*network
		success bool
	}{
		{newNetwork(nil, nil, nil), true},
		{newNetwork(nil, nil, nopStepper), true},
		{newNetwork(nil, nopStepper, nopStepper), false},
		{newNetwork(nil, nopStepper, nopStepper, nil), false},
		{newNetwork(nil, nopStepper, nopStepper, nil, nil), true},
	}

	for i, tt := range tests {
		send := func(m pb.Message) {
			defer func() {
				// only recover is we expect it to panic so
				// panics we don't expect go up.
				if !tt.success {
					e := recover()
					if e != nil {
						t.Logf("#%d: err: %s", i, e)
					}
				}
			}()
			tt.send(m)
		}

		data := []byte("somedata")

		// promote 0 the leader
		send(pb.Message{From: 1, To: 1, Type: msgHup})
		send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{Data: data}}})

		wantLog := newLog()
		if tt.success {
			wantLog = &raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Index: 2, Data: data}}, committed: 2}
		}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.network.peers[1].(*raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

// 测试通过代理节点发起提案, 代理节点会将提案重定向给leader节点
func TestProposalByProxy(t *testing.T) {
	data := []byte("somedata")
	tests := []*network{
		newNetwork(nil, nil, nil),
		newNetwork(nil, nil, nopStepper),
	}

	for i, tt := range tests {
		// promote 0 the leader
		tt.send(pb.Message{From: 1, To: 1, Type: msgHup})

		// propose via follower
		tt.send(pb.Message{From: 2, To: 2, Type: msgProp, Entries: []pb.Entry{{Data: []byte("somedata")}}})

		wantLog := &raftLog{ents: []pb.Entry{{}, {Data: nil, Term: 1, Index: 1}, {Term: 1, Data: data, Index: 2}}, committed: 2}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*raft); ok {
				l := ltoa(sm.raftLog)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.peers[1].(*raft)
		if g := sm.Term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []int64
		logs    []pb.Entry
		smTerm  int64
		w       int64
	}{
		// single
		{[]int64{1}, []pb.Entry{{}, {Term: 1}}, 1, 1},
		{[]int64{1}, []pb.Entry{{}, {Term: 1}}, 2, 0},
		{[]int64{2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int64{1}, []pb.Entry{{}, {Term: 2}}, 2, 1},

		// odd
		{[]int64{2, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int64{2, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int64{2, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int64{2, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},

		// even
		{[]int64{2, 1, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int64{2, 1, 1, 1}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int64{2, 1, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int64{2, 1, 1, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int64{2, 1, 2, 2}, []pb.Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int64{2, 1, 2, 2}, []pb.Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		prs := make(map[int64]*progress)
		for j := 0; j < len(tt.matches); j++ {
			prs[int64(j)] = &progress{tt.matches[j], tt.matches[j] + 1}
		}
		sm := &raft{raftLog: &raftLog{ents: tt.logs}, prs: prs, State: pb.State{Term: tt.smTerm}}
		sm.maybeCommit()
		if g := sm.raftLog.committed; g != tt.w {
			t.Errorf("#%d: committed = %d, want %d", i, g, tt.w)
		}
	}
}

// ensure that the Step function ignores the message from old term and does not pass it to the
// acutal stepX function.
func TestStepIgnoreOldTermMsg(t *testing.T) {
	called := false
	fakeStep := func(r *raft, m pb.Message) {
		called = true
	}
	sm := newRaft(1, []int64{1}, 0, 0)
	sm.step = fakeStep
	sm.Term = 2
	sm.Step(pb.Message{Type: msgApp, Term: sm.Term - 1})
	if called {
		t.Errorf("stepFunc called = %v, want %v", called, false)
	}
}

// TestHandleMsgApp ensures:
//  1. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm.
//  2. If an existing entry conflicts with a new one (same index but different terms),
//     delete the existing entry and all that follow it; append any new entries not already in the log.
//  3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
func TestHandleMsgApp(t *testing.T) {
	tests := []struct {
		m       pb.Message
		wIndex  int64
		wCommit int64
		wAccept bool
	}{
		// Ensure 1
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 3, Index: 2, Commit: 3}, 2, 0, false}, // previous log mismatch
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 3, Index: 3, Commit: 3}, 2, 0, false}, // previous log non-exist

		// Ensure 2
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 1}, 2, 1, true},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 0, Index: 0, Commit: 1, Entries: []pb.Entry{{Term: 2}}}, 1, 1, true},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 3, Entries: []pb.Entry{{Term: 2}, {Term: 2}}}, 4, 3, true},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4, Entries: []pb.Entry{{Term: 2}}}, 3, 3, true},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 1, Index: 1, Commit: 4, Entries: []pb.Entry{{Term: 2}}}, 2, 2, true},

		// Ensure 3
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 2}, 2, 2, true},
		{pb.Message{Type: msgApp, Term: 2, LogTerm: 2, Index: 2, Commit: 4}, 2, 2, true}, // commit upto min(commit, last)
	}

	for i, tt := range tests {
		sm := &raft{
			state:   stateFollower,
			State:   pb.State{Term: 2},
			raftLog: &raftLog{committed: 0, ents: []pb.Entry{{}, {Term: 1}, {Term: 2}}},
		}

		sm.handleAppendEntries(tt.m)
		if sm.raftLog.lastIndex() != tt.wIndex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, sm.raftLog.lastIndex(), tt.wIndex)
		}
		if sm.raftLog.committed != tt.wCommit {
			t.Errorf("#%d: committed = %d, want %d", i, sm.raftLog.committed, tt.wCommit)
		}
		m := sm.ReadMessages()
		if len(m) != 1 {
			t.Errorf("#%d: msg = nil, want 1", i)
		}
		gaccept := true
		if m[0].Index == -1 {
			gaccept = false
		}
		if gaccept != tt.wAccept {
			t.Errorf("#%d: accept = %v, want %v", i, gaccept, tt.wAccept)
		}
	}
}

func TestRecvMsgVote(t *testing.T) {
	tests := []struct {
		state   stateType
		i, term int64
		voteFor int64
		w       int64
	}{
		{stateFollower, 0, 0, None, -1},
		{stateFollower, 0, 1, None, -1},
		{stateFollower, 0, 2, None, -1},
		{stateFollower, 0, 3, None, 2},

		{stateFollower, 1, 0, None, -1},
		{stateFollower, 1, 1, None, -1},
		{stateFollower, 1, 2, None, -1},
		{stateFollower, 1, 3, None, 2},

		{stateFollower, 2, 0, None, -1},
		{stateFollower, 2, 1, None, -1},
		{stateFollower, 2, 2, None, 2},
		{stateFollower, 2, 3, None, 2},

		{stateFollower, 3, 0, None, -1},
		{stateFollower, 3, 1, None, -1},
		{stateFollower, 3, 2, None, 2},
		{stateFollower, 3, 3, None, 2},

		{stateFollower, 3, 2, 2, 2},
		{stateFollower, 3, 2, 1, -1},

		{stateLeader, 3, 3, 1, -1},
		{stateCandidate, 3, 3, 1, -1},
	}

	for i, tt := range tests {
		sm := newRaft(1, []int64{1}, 0, 0)
		sm.state = tt.state
		switch tt.state {
		case stateFollower:
			sm.step = stepFollower
		case stateCandidate:
			sm.step = stepCandidate
		case stateLeader:
			sm.step = stepLeader
		}
		sm.State = pb.State{Vote: tt.voteFor}
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 2}, {Term: 2}}}

		sm.Step(pb.Message{Type: msgVote, From: 2, Index: tt.i, LogTerm: tt.term})

		msgs := sm.ReadMessages()
		if g := len(msgs); g != 1 {
			t.Errorf("#%d: len(msgs) = %d, want 1", i, g)
			continue
		}
		if g := msgs[0].Index; g != tt.w {
			t.Errorf("#%d, m.Index = %d, want %d", i, g, tt.w)
		}
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   stateType
		to     stateType
		wallow bool
		wterm  int64
		wlead  int64
	}{
		{stateFollower, stateFollower, true, 1, None},
		{stateFollower, stateCandidate, true, 1, None},
		{stateFollower, stateLeader, false, -1, None},

		{stateCandidate, stateFollower, true, 0, None},
		{stateCandidate, stateCandidate, true, 1, None},
		{stateCandidate, stateLeader, true, 0, 1},

		{stateLeader, stateFollower, true, 1, None},
		{stateLeader, stateCandidate, false, 1, None},
		{stateLeader, stateLeader, true, 0, 1},
	}

	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if tt.wallow == true {
						t.Errorf("%d: allow = %v, want %v", i, false, true)
					}
				}
			}()

			sm := newRaft(1, []int64{1}, 0, 0)
			sm.state = tt.from

			switch tt.to {
			case stateFollower:
				sm.becomeFollower(tt.wterm, tt.wlead)
			case stateCandidate:
				sm.becomeCandidate()
			case stateLeader:
				sm.becomeLeader()
			}

			if sm.Term != tt.wterm {
				t.Errorf("%d: term = %d, want %d", i, sm.Term, tt.wterm)
			}
			if sm.lead != tt.wlead {
				t.Errorf("%d: lead = %d, want %d", i, sm.lead, tt.wlead)
			}
		}()
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state stateType

		wstate stateType
		wterm  int64
		windex int64
	}{
		{stateFollower, stateFollower, 3, 1},
		{stateCandidate, stateFollower, 3, 1},
		{stateLeader, stateFollower, 3, 2},
	}

	tmsgTypes := [...]int64{msgVote, msgApp}
	tterm := int64(3)

	for i, tt := range tests {
		sm := newRaft(1, []int64{1, 2, 3}, 0, 0)
		switch tt.state {
		case stateFollower:
			sm.becomeFollower(1, None)
		case stateCandidate:
			sm.becomeCandidate()
		case stateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(pb.Message{From: 2, Type: msgType, Term: tterm, LogTerm: tterm})

			if sm.state != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.state, tt.wstate)
			}
			if sm.Term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.Term, tt.wterm)
			}
			if int64(len(sm.raftLog.ents)) != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, len(sm.raftLog.ents), tt.windex)
			}
			wlead := int64(2)
			if msgType == msgVote {
				wlead = None
			}
			if sm.lead != wlead {
				t.Errorf("#%d, sm.lead = %d, want %d", i, sm.lead, None)
			}
		}
	}
}

func TestLeaderAppResp(t *testing.T) {
	tests := []struct {
		index      int64
		wmsgNum    int
		windex     int64
		wcommitted int64
	}{
		{-1, 1, 1, 0}, // bad resp; leader does not commit; reply with log entries
		{2, 2, 2, 2},  // good resp; leader commits; broadcast with commit index
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newRaft(1, []int64{1, 2, 3}, 0, 0)
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 0}, {Term: 1}}}
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.ReadMessages()
		sm.Step(pb.Message{From: 2, Type: msgAppResp, Index: tt.index, Term: sm.Term})
		msgs := sm.ReadMessages()

		if len(msgs) != tt.wmsgNum {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wmsgNum)
		}
		for j, msg := range msgs {
			if msg.Index != tt.windex {
				t.Errorf("#%d.%d index = %d, want %d", i, j, msg.Index, tt.windex)
			}
			if msg.Commit != tt.wcommitted {
				t.Errorf("#%d.%d commit = %d, want %d", i, j, msg.Commit, tt.wcommitted)
			}
		}
	}
}

// When the leader receives a heartbeat tick, it should
// send a msgApp with m.Index = max(progress.next-1,log.offset)
// and empty entries.
func TestBcastBeat(t *testing.T) {
	offset := int64(1000)

	// make a state machine with log.offset = 1000
	s := pb.Snapshot{
		Index: offset,
		Term:  1,
		Nodes: []int64{1, 2},
	}

	sm := newRaft(1, []int64{1, 2}, 0, 0)
	sm.Term = 1
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	for i := 0; i < 10; i++ {
		sm.appendEntry(pb.Entry{})
	}

	tests := []struct {
		pnext  int64
		windex int64
		wterm  int64
		wto    int64
	}{
		{offset + 1, offset, 1, 2},
		{offset + 2, offset + 1, 2, 2},
		//pr.next - 1 < offset
		{offset, offset, 1, 2},
		{offset - 1, offset, 1, 2},
	}

	for i, tt := range tests {
		sm.prs[2].match = 0
		sm.prs[2].next = tt.pnext

		sm.Step(pb.Message{Type: msgBeat})
		msgs := sm.ReadMessages()
		if len(msgs) != 1 {
			t.Fatalf("#%d: len(msgs) = %v, want 1", i, len(msgs))
		}
		m := msgs[0]
		if m.Type != msgApp {
			t.Errorf("#%d: type = %v, want %v", i, m.Type, msgApp)
		}
		if m.Index != tt.windex {
			t.Errorf("#%d: prevIndex = %v, want %v", i, m.Index, tt.windex)
		}
		if m.LogTerm != tt.wterm {
			t.Errorf("#%d: prevTerm = %v, want %v", i, m.LogTerm, tt.wterm)
		}
		if m.To != tt.wto {
			t.Errorf("#%d: to = %v, want %v", i, m.To, tt.wto)
		}
		if len(m.Entries) != 0 {
			t.Errorf("#%d: len(ents) = %v, want 0", i, len(m.Entries))
		}
	}
}

// 测试leader节点接收到心跳消息后的处理
func TestRecvMsgBeat(t *testing.T) {
	tests := []struct {
		state stateType
		wMsg  int
	}{
		{stateLeader, 2},
		// candidate and follower should ignore msgBeat
		{stateCandidate, 0},
		{stateFollower, 0},
	}

	for i, tt := range tests {
		sm := newRaft(1, []int64{1, 2, 3}, 0, 0)
		sm.raftLog = &raftLog{ents: []pb.Entry{{}, {Term: 0}, {Term: 1}}}
		sm.Term = 1
		sm.state = tt.state
		switch tt.state {
		case stateFollower:
			sm.step = stepFollower
		case stateCandidate:
			sm.step = stepCandidate
		case stateLeader:
			sm.step = stepLeader
		}
		sm.Step(pb.Message{From: 1, To: 1, Type: msgBeat})

		msgs := sm.ReadMessages()
		if len(msgs) != tt.wMsg {
			t.Errorf("%d: len(msgs) = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.Type != msgApp {
				t.Errorf("%d: msg.type = %v, want %v", i, m.Type, msgApp)
			}
		}
	}
}

func TestRestore(t *testing.T) {
	s := pb.Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []int64{1, 2, 3},
	}

	sm := newRaft(1, []int64{1, 2}, 0, 0)
	if ok := sm.restore(s); !ok {
		t.Fatal("restore fail, want succeed")
	}

	if sm.raftLog.lastIndex() != s.Index {
		t.Errorf("log.lastIndex = %d, want %d", sm.raftLog.lastIndex(), s.Index)
	}
	if sm.raftLog.term(s.Index) != s.Term {
		t.Errorf("log.lastTerm = %d, want %d", sm.raftLog.term(s.Index), s.Term)
	}
	sg := int64Slice(sm.nodes())
	sw := int64Slice(s.Nodes)
	sort.Sort(sg)
	sort.Sort(sw)
	if !reflect.DeepEqual(sg, sw) {
		t.Errorf("sm.Nodes = %+v, want %+v", sg, sw)
	}
	if !reflect.DeepEqual(sm.raftLog.snapshot, s) {
		t.Errorf("snapshot = %+v, want %+v", sm.raftLog.snapshot, s)
	}

	if ok := sm.restore(s); ok {
		t.Fatal("restore succeed, want fail")
	}
}

func TestProvideSnap(t *testing.T) {
	s := pb.Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []int64{1, 2},
	}
	sm := newRaft(1, []int64{1}, 0, 0)
	// restore the statemachin from a snapshot
	// so it has a compacted log and a snapshot
	sm.restore(s)

	sm.becomeCandidate()
	sm.becomeLeader()

	sm.Step(pb.Message{From: 1, To: 1, Type: msgBeat})
	msgs := sm.ReadMessages()
	if len(msgs) != 1 {
		t.Errorf("len(msgs) = %d, want 1", len(msgs))
	}
	m := msgs[0]
	if m.Type != msgApp {
		t.Errorf("m.Type = %v, want %v", m.Type, msgApp)
	}

	// force set the next of node 1, so that
	// node 1 needs a snapshot
	sm.prs[2].next = sm.raftLog.offset

	sm.Step(pb.Message{From: 2, To: 1, Type: msgAppResp, Index: -1})
	msgs = sm.ReadMessages()
	if len(msgs) != 1 {
		t.Errorf("len(msgs) = %d, want 1", len(msgs))
	}
	m = msgs[0]
	if m.Type != msgSnap {
		t.Errorf("m.Type = %v, want %v", m.Type, msgSnap)
	}
}

func TestRestoreFromSnapMsg(t *testing.T) {
	s := pb.Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []int64{1, 2},
	}
	m := pb.Message{Type: msgSnap, From: 1, Term: 2, Snapshot: s}

	sm := newRaft(2, []int64{1, 2}, 0, 0)
	sm.Step(m)

	if !reflect.DeepEqual(sm.raftLog.snapshot, s) {
		t.Errorf("snapshot = %+v, want %+v", sm.raftLog.snapshot, s)
	}
}

func TestSlowNodeRestore(t *testing.T) {
	nt := newNetwork(nil, nil, nil)
	nt.send(pb.Message{From: 1, To: 1, Type: msgHup})

	nt.isolate(3)
	for j := 0; j < defaultCompactThreshold+1; j++ {
		nt.send(pb.Message{From: None, To: 1, Type: msgProp, Entries: []pb.Entry{{}}})
	}
	lead := nt.peers[1].(*raft)
	nextEnts(lead)
	lead.compact(nil)

	nt.recover()
	nt.send(pb.Message{From: 1, To: 1, Type: msgBeat})

	follower := nt.peers[3].(*raft)
	if !reflect.DeepEqual(follower.raftLog.snapshot, lead.raftLog.snapshot) {
		t.Errorf("follower.snap = %+v, want %+v", follower.raftLog.snapshot, lead.raftLog.snapshot)
	}

	committed := follower.raftLog.lastIndex()
	nt.send(pb.Message{From: 1, To: 1, Type: msgProp, Entries: []pb.Entry{{}}})
	if follower.raftLog.committed != committed+1 {
		t.Errorf("follower.comitted = %d, want %d", follower.raftLog.committed, committed+1)
	}
}

func ents(terms ...int64) *raft {
	ents := []pb.Entry{{}}
	for _, term := range terms {
		ents = append(ents, pb.Entry{Term: term})
	}

	sm := &raft{raftLog: &raftLog{ents: ents}}
	sm.reset(0)
	return sm
}

type network struct {
	peers   map[int64]Interface
	dropm   map[connem]float64
	ignorem map[int64]bool
}

// newNetwork initializes a network from peers.
// A nil node will be replaced with a new *stateMachine.
// A *stateMachine will get its k, id.
// When using stateMachine, the address list is always [0, n).
func newNetwork(peers ...Interface) *network {
	size := len(peers)
	peerAddrs := make([]int64, size)
	for i := 0; i < size; i++ {
		peerAddrs[i] = 1 + int64(i)
	}

	npeers := make(map[int64]Interface, size)

	for i, p := range peers {
		id := peerAddrs[i]
		switch v := p.(type) {
		case nil:
			sm := newRaft(id, peerAddrs, 0, 0)
			npeers[id] = sm
		case *raft:
			v.id = id
			v.prs = make(map[int64]*progress)
			for i := 0; i < size; i++ {
				v.prs[peerAddrs[i]] = &progress{}
			}
			v.reset(0)
			npeers[id] = v
		case *blackHole:
			npeers[id] = v
		default:
			panic(fmt.Sprintf("unexpected state machine type: %T", p))
		}
	}
	return &network{
		peers:   npeers,
		dropm:   make(map[connem]float64),
		ignorem: make(map[int64]bool),
	}
}

func (nw *network) send(msgs ...pb.Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.ReadMessages())...)
	}
}

func (nw *network) drop(from, to int64, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(one, other int64) {
	nw.drop(one, other, 1)
	nw.drop(other, one, 1)
}

// isolate disconnects the specified node (id)
// from all other nodes in the network.
func (nw *network) isolate(id int64) {
	for i := 0; i < len(nw.peers); i++ {
		nid := int64(i) + 1
		if nid != id {
			nw.drop(id, nid, 1.0)
			nw.drop(nid, id, 1.0)
		}
	}
}

func (nw *network) ignore(t int64) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[int64]bool)
}

func (nw *network) filter(msgs []pb.Message) []pb.Message {
	mm := make([]pb.Message, 0)
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case msgHup:
			// hups never go over the network, so don't drop them but panic
			panic("unexpected msgHup")
		default:
			perc := nw.dropm[connem{m.From, m.To}]
			if n := rand.Float64(); n < perc {
				continue
			}
		}
		mm = append(mm, m)
	}
	return mm
}

type connem struct {
	from, to int64
}

type blackHole struct{}

func (blackHole) Step(pb.Message) error      { return nil }
func (blackHole) ReadMessages() []pb.Message { return nil }

var nopStepper = &blackHole{}
