package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

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
		tt.send(Message{To: 0, Type: msgHup})
		sm := tt.network.peers[0].(*stateMachine)
		if sm.state != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, sm.state, tt.state)
		}
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestLogReplication(t *testing.T) {
	tests := []struct {
		*network
		msgs       []Message
		wcommitted int
	}{
		{
			newNetwork(nil, nil, nil),
			[]Message{
				{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}},
			},
			2,
		},
		{
			newNetwork(nil, nil, nil),
			[]Message{
				{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}},
				{To: 1, Type: msgHup},
				{To: 1, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}},
			},
			4,
		},
	}

	for i, tt := range tests {
		tt.send(Message{To: 0, Type: msgHup})

		for _, m := range tt.msgs {
			tt.send(m)
		}

		for j, x := range tt.network.peers {
			sm := x.(*stateMachine)

			if sm.log.committed != tt.wcommitted {
				t.Errorf("#%d.%d: committed = %d, want %d", i, j, sm.log.committed, tt.wcommitted)
			}

			ents := make([]Entry, 0)
			for _, e := range sm.nextEnts() {
				if e.Data != nil {
					ents = append(ents, e)
				}
			}

			props := make([]Message, 0)
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
	tt.send(Message{To: 0, Type: msgHup})
	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})
	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})

	sm := tt.peers[0].(*stateMachine)
	if sm.log.committed != 3 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 3)
	}
}

func TestCannotCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(Message{To: 0, Type: msgHup})

	// 0 cannot reach 2,3,4
	tt.cut(0, 2)
	tt.cut(0, 3)
	tt.cut(0, 4)

	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})
	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})

	sm := tt.peers[0].(*stateMachine)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 0)
	}

	tt.recover()

	tt.ignore(msgApp)

	// elect 1 as the leader with a new term
	tt.send(Message{To: 1, Type: msgHup})

	// no log entries from previous term should be committed
	sm = tt.peers[1].(*stateMachine)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 1)
	}

	tt.recover()

	tt.send(Message{To: 1, Type: msgBeat})

	if sm.log.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 4)
	}

	tt.send(Message{To: 1, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})
	if sm.log.committed != 5 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 3)
	}
}

// TestCommitWithoutNewTermEntry tests the entries could be committed
// when leader changes, no new proposal comes in.
func TestCommitWithoutNewTermEntry(t *testing.T) {
	tt := newNetwork(nil, nil, nil, nil, nil)
	tt.send(Message{To: 0, Type: msgHup})
	// 0 cannot reach 2,3,4
	tt.cut(0, 2)
	tt.cut(0, 3)
	tt.cut(0, 4)
	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})
	tt.send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: []byte("some data")}}})
	sm := tt.peers[0].(*stateMachine)
	if sm.log.committed != 1 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 1)
	}
	// network recovery
	tt.recover()
	// elect 1 as the new leader with term 2
	// after append a ChangeTerm entry from the current term, all entries
	// should be committed
	tt.send(Message{To: 1, Type: msgHup})
	if sm.log.committed != 4 {
		t.Errorf("committed = %d, want %d", sm.log.committed, 4)
	}
}

// 测试在分布式系统中两个候选者节点同时发起选举的情况
func TestDualingCandidates(t *testing.T) {
	a := newStateMachine(0, nil) // k, id are set later
	b := newStateMachine(0, nil)
	c := newStateMachine(0, nil)

	nt := newNetwork(a, b, c)
	nt.cut(0, 2)

	nt.send(Message{To: 0, Type: msgHup})
	nt.send(Message{To: 2, Type: msgHup})

	nt.recover()
	nt.send(Message{To: 2, Type: msgHup})

	wlog := &log{ents: []Entry{{}, {Type: Normal, Data: nil, Term: 1}}, committed: 1}

	tests := []struct {
		sm    *stateMachine
		state stateType
		term  int
		log   *log
	}{
		{a, stateFollower, 2, wlog},
		{b, stateFollower, 2, wlog},
		{c, stateFollower, 2, newLog()},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}

		base := ltoa(tt.log)
		if sm, ok := nt.peers[i].(*stateMachine); ok {
			l := ltoa(sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff: \n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	tt.isolate(0)

	tt.send(Message{To: 0, Type: msgHup})
	tt.send(Message{To: 2, Type: msgHup})

	// heal the partition
	tt.recover()

	data := []byte("force follower")
	// send a proposal to 2 to flush out a msgApp to 0
	tt.send(Message{To: 2, Type: msgProp, Entries: []Entry{{Data: data}}})

	a := tt.peers[0].(*stateMachine)
	if g := a.state; g != stateFollower {
		t.Errorf("state = %s, want %s", g, stateFollower)
	}
	if g := a.term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}

	wantLog := ltoa(&log{ents: []Entry{{}, {Type: Normal, Data: nil, Term: 1}, {Term: 1, Data: data}}, committed: 2})
	for i, p := range tt.peers {
		if sm, ok := p.(*stateMachine); ok {
			l := ltoa(sm.log)
			if g := diffu(wantLog, l); g != "" {
				t.Errorf("#%d: diff: \n%s", i, g)
			}
		} else {
			t.Logf("#%d: empty log", i)
		}
	}
}

func TestSingleNodeCandidate(t *testing.T) {
	tt := newNetwork(nil)
	tt.send(Message{To: 0, Type: msgHup})

	sm := tt.peers[0].(*stateMachine)
	if sm.state != stateLeader {
		t.Errorf("state = %s, want %s", sm.state, stateLeader)
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.send(Message{To: 0, Type: msgHup})
	tt.send(Message{To: 1, Type: msgHup})
	tt.send(Message{To: 0, Type: msgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.send(Message{To: 0, Type: msgApp, Term: 1, Entries: []Entry{{Term: 1}}})

	l := &log{
		ents: []Entry{
			{}, {Type: Normal, Data: nil, Term: 1},
			{Type: Normal, Data: nil, Term: 2}, {Type: Normal, Data: nil, Term: 3},
		},
		committed: 3,
	}
	base := ltoa(l)
	for i, p := range tt.peers {
		if sm, ok := p.(*stateMachine); ok {
			l := ltoa(sm.log)
			if g := diffu(base, l); g != "" {
				t.Errorf("#%d: diff: \n%s", i, g)
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
		send := func(m Message) {
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
		send(Message{To: 0, Type: msgHup})
		send(Message{To: 0, Type: msgProp, Entries: []Entry{{Data: data}}})

		wantLog := newLog()
		if tt.success {
			wantLog = &log{ents: []Entry{{}, {Type: Normal, Data: nil, Term: 1}, {Term: 1, Data: data}}, committed: 2}
		}

		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*stateMachine); ok {
				l := ltoa(sm.log)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff: \n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}

		sm := tt.peers[0].(*stateMachine)
		if g := sm.term; g != 1 {
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
		tt.send(Message{To: 0, Type: msgHup})

		// propose via follower
		tt.send(Message{To: 1, Type: msgProp, Entries: []Entry{{Data: []byte("somedata")}}})

		wantLog := &log{ents: []Entry{{}, {Type: Normal, Data: nil, Term: 1}, {Term: 1, Data: data}}, committed: 2}
		base := ltoa(wantLog)
		for i, p := range tt.peers {
			if sm, ok := p.(*stateMachine); ok {
				l := ltoa(sm.log)
				if g := diffu(base, l); g != "" {
					t.Errorf("#%d: diff:\n%s", i, g)
				}
			} else {
				t.Logf("#%d: empty log", i)
			}
		}
		sm := tt.peers[0].(*stateMachine)
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestCommit(t *testing.T) {
	tests := []struct {
		matches []int
		logs    []Entry
		smTerm  int
		w       int
	}{
		// single
		{[]int{1}, []Entry{{}, {Term: 1}}, 1, 1},
		{[]int{1}, []Entry{{}, {Term: 1}}, 2, 0},
		{[]int{2}, []Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int{2}, []Entry{{}, {Term: 1}, {Term: 2}}, 1, 0},
		{[]int{1}, []Entry{{}, {Term: 2}}, 2, 1},

		// odd
		{[]int{2, 1, 1}, []Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int{2, 1, 1}, []Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int{2, 1, 2}, []Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int{2, 1, 2}, []Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},

		// even
		{[]int{2, 1, 1, 1}, []Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int{2, 1, 1, 1}, []Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int{2, 1, 1, 2}, []Entry{{}, {Term: 1}, {Term: 2}}, 1, 1},
		{[]int{2, 1, 1, 2}, []Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
		{[]int{2, 1, 2, 2}, []Entry{{}, {Term: 1}, {Term: 2}}, 2, 2},
		{[]int{2, 1, 2, 2}, []Entry{{}, {Term: 1}, {Term: 1}}, 2, 0},
	}

	for i, tt := range tests {
		ins := make(map[int]*index)
		for j := 0; j < len(tt.matches); j++ {
			ins[j] = &index{tt.matches[j], tt.matches[j] + 1}
		}
		sm := &stateMachine{log: &log{ents: tt.logs}, indexs: ins, term: tt.smTerm}
		sm.maybeCommit()
		if g := sm.log.committed; g != tt.w {
			t.Errorf("#%d: commit = %d, want %d", i, g, tt.w)
		}
	}
}

func TestRecvMsgVote(t *testing.T) {
	tests := []struct {
		state   stateType
		i, term int
		voteFor int
		w       int
	}{
		{stateFollower, 0, 0, none, -1},
		{stateFollower, 0, 1, none, -1},
		{stateFollower, 0, 2, none, -1},
		{stateFollower, 0, 3, none, 2},

		{stateFollower, 1, 0, none, -1},
		{stateFollower, 1, 1, none, -1},
		{stateFollower, 1, 2, none, -1},
		{stateFollower, 1, 3, none, 2},

		{stateFollower, 2, 0, none, -1},
		{stateFollower, 2, 1, none, -1},
		{stateFollower, 2, 2, none, 2},
		{stateFollower, 2, 3, none, 2},

		{stateFollower, 3, 0, none, -1},
		{stateFollower, 3, 1, none, -1},
		{stateFollower, 3, 2, none, 2},
		{stateFollower, 3, 3, none, 2},

		{stateFollower, 3, 2, 1, 2},
		{stateFollower, 3, 2, 0, -1},

		{stateLeader, 3, 3, 0, -1},
		{stateCandidate, 3, 3, 0, -1},
	}

	for i, tt := range tests {
		sm := &stateMachine{
			state: tt.state,
			vote:  tt.voteFor,
			log:   &log{ents: []Entry{{}, {Term: 2}, {Term: 2}}},
		}

		sm.Step(Message{Type: msgVote, From: 1, Index: tt.i, LogTerm: tt.term})

		msgs := sm.Msgs()
		if g := len(msgs); g != 1 {
			t.Errorf("#%d: msg count = %d, want %d", i, len(msgs), g)
			continue
		}

		if g := msgs[0].Index; g != tt.w {
			t.Errorf("#%d: m.Index = %d, want %d", i, g, tt.w)
		}
	}
}

func TestMaybeCompact(t *testing.T) {
	tests := []struct {
		snapshoter Snapshoter
		applied    int
		wCompact   bool
	}{
		{nil, defaultCompactThreshold + 1, false},
		{new(logSnapshoter), defaultCompactThreshold - 1, false},
		{new(logSnapshoter), defaultCompactThreshold + 1, true},
	}
	for i, tt := range tests {
		sm := newStateMachine(0, []int{0, 1, 2})
		sm.setSnapshoter(tt.snapshoter)
		for i := 0; i < defaultCompactThreshold*2; i++ {
			sm.log.append(i, Entry{Term: i + 1})
		}
		sm.log.applied = tt.applied
		sm.log.committed = tt.applied
		if g := sm.maybeCompact(); g != tt.wCompact {
			t.Errorf("#%d: compact = %v, want %v", i, g, tt.wCompact)
		}
		if tt.wCompact {
			s := sm.snapshoter.GetSnap()
			if s.Index != tt.applied {
				t.Errorf("#%d: snap.Index = %v, want %v", i, s.Index, tt.applied)
			}
			if s.Term != tt.applied {
				t.Errorf("#%d: snap.Term = %v, want %v", i, s.Index, tt.applied)
			}
			w := sm.nodes()
			sort.Ints(w)
			sort.Ints(s.Nodes)
			if !reflect.DeepEqual(s.Nodes, w) {
				t.Errorf("#%d: snap.Nodes = %+v, want %+v", i, s.Nodes, w)
			}
		}
	}
}
func TestRestore(t *testing.T) {
	s := Snapshot{
		Index: defaultCompactThreshold + 1,
		Term:  defaultCompactThreshold + 1,
		Nodes: []int{0, 1, 2},
	}
	tests := []struct {
		snapshoter Snapshoter
		wallow     bool
	}{
		{nil, false},
		{new(logSnapshoter), true},
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
			sm := newStateMachine(0, []int{0, 1})
			sm.setSnapshoter(tt.snapshoter)
			sm.restore(s)
			if sm.log.lastIndex() != s.Index {
				t.Errorf("#%d: log.lastIndex = %d, want %d", i, sm.log.lastIndex(), s.Index)
			}
			if sm.log.term(s.Index) != s.Term {
				t.Errorf("#%d: log.lastTerm = %d, want %d", i, sm.log.term(s.Index), s.Term)
			}
			g := sm.nodes()
			sort.Ints(g)
			sort.Ints(s.Nodes)
			if !reflect.DeepEqual(g, s.Nodes) {
				t.Errorf("#%d: sm.Nodes = %+v, want %+v", i, g, s.Nodes)
			}
			if !reflect.DeepEqual(sm.snapshoter.GetSnap(), s) {
				t.Errorf("%d: snapshoter.getSnap = %+v, want %+v", i, sm.snapshoter.GetSnap(), s)
			}
		}()
	}
}

func TestStateTransition(t *testing.T) {
	tests := []struct {
		from   stateType
		to     stateType
		wallow bool
		wterm  int
		wlead  int
	}{
		{stateFollower, stateFollower, true, 1, none},
		{stateFollower, stateCandidate, true, 1, none},
		{stateFollower, stateLeader, false, -1, none},

		{stateCandidate, stateFollower, true, 0, none},
		{stateCandidate, stateCandidate, true, 1, none},
		{stateCandidate, stateLeader, true, 0, 0},

		{stateLeader, stateFollower, true, 1, none},
		{stateLeader, stateCandidate, false, 1, none},
		{stateLeader, stateLeader, true, 0, 0},
	}

	for i, tt := range tests {
		defer func() {
			if r := recover(); r != nil {
				if tt.wallow == true {
					t.Errorf("%d: allow = %v, want %v", i, false, true)
				}
			}
		}()

		sm := newStateMachine(1, []int{0})
		sm.state = tt.from

		switch tt.to {
		case stateFollower:
			sm.becomeFollower(tt.wterm, tt.wlead)
		case stateCandidate:
			sm.becomeCandidate()
		case stateLeader:
			sm.becomeLeader()
		}

		if sm.term != tt.wterm {
			t.Errorf("#%d: term = %d, want %d", i, sm.term, tt.wterm)
		}

		if sm.lead != tt.wlead {
			t.Errorf("#%d: lead = %d, want %d", i, sm.lead, tt.wlead)
		}
	}
}

func TestConf(t *testing.T) {
	sm := newStateMachine(0, []int{0})
	sm.becomeCandidate()
	sm.becomeLeader()
	sm.Step(Message{Type: msgProp, Entries: []Entry{{Type: AddNode}}})
	if sm.log.lastIndex() != 2 {
		t.Errorf("lastindex = %d, want %d", sm.log.lastIndex(), 1)
	}
	if !sm.pendingConf {
		t.Errorf("pendingConf = %v, want %v", sm.pendingConf, true)
	}
	if sm.log.ents[2].Type != AddNode {
		t.Errorf("type = %d, want %d", sm.log.ents[1].Type, AddNode)
	}
	// deny the second configuration change request if there is a pending one
	sm.Step(Message{Type: msgProp, Entries: []Entry{{Type: AddNode}}})
	if sm.log.lastIndex() != 2 {
		t.Errorf("lastindex = %d, want %d", sm.log.lastIndex(), 1)
	}
}

func TestConfChangeLeader(t *testing.T) {
	tests := []struct {
		et       int
		wPending bool
	}{
		{Normal, false},
		{AddNode, true},
		{RemoveNode, true},
	}

	for i, tt := range tests {
		sm := newStateMachine(0, []int{0})
		sm.log = &log{ents: []Entry{{}, {Type: tt.et}}}

		sm.becomeCandidate()
		sm.becomeLeader()

		if sm.pendingConf != tt.wPending {
			t.Errorf("#%d: pendingConf = %v, want %v", i, sm.pendingConf, tt.wPending)
		}
	}
}

func TestAllServerStepdown(t *testing.T) {
	tests := []struct {
		state  stateType
		wstate stateType
		wterm  int
		windex int
	}{
		{stateFollower, stateFollower, 3, 1},
		{stateCandidate, stateFollower, 3, 1},
		{stateLeader, stateFollower, 3, 2},
	}

	tmsgTypes := [...]messageType{msgVote, msgApp}
	tterm := 3

	for i, tt := range tests {
		sm := newStateMachine(0, []int{0, 1, 2})
		switch tt.state {
		case stateFollower:
			sm.becomeFollower(1, 0)
		case stateCandidate:
			sm.becomeCandidate()
		case stateLeader:
			sm.becomeCandidate()
			sm.becomeLeader()
		}

		for j, msgType := range tmsgTypes {
			sm.Step(Message{Type: msgType, Term: tterm, LogTerm: tterm})

			if sm.state != tt.wstate {
				t.Errorf("#%d.%d state = %v , want %v", i, j, sm.state, tt.wstate)
			}
			if sm.term != tt.wterm {
				t.Errorf("#%d.%d term = %v , want %v", i, j, sm.term, tt.wterm)
			}
			if len(sm.log.ents) != tt.windex {
				t.Errorf("#%d.%d index = %v , want %v", i, j, len(sm.log.ents), tt.windex)
			}
		}
	}
}

func TestLeaderAppResp(t *testing.T) {
	tests := []struct {
		index      int
		wmsgNum    int
		windex     int
		wcommitted int
	}{
		{-1, 1, 1, 0}, // bad resp; leader does not commit; reply with log entries
		{2, 2, 2, 2},  // good resp; leader commits; broadcast with commit index
	}

	for i, tt := range tests {
		// sm term is 1 after it becomes the leader.
		// thus the last log term must be 1 to be committed.
		sm := newStateMachine(0, []int{0, 1, 2})
		sm.log = &log{ents: []Entry{{}, {Term: 0}, {Term: 1}}}
		sm.becomeCandidate()
		sm.becomeLeader()
		sm.Msgs()
		sm.Step(Message{From: 1, Type: msgAppResp, Index: tt.index, Term: sm.term})
		msgs := sm.Msgs()

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

// 测试leader节点接收到心跳消息后的处理
func TestRecvMsgBeat(t *testing.T) {
	tests := []struct {
		state stateType
		wMsg  int
	}{
		{stateLeader, 2},
		{stateCandidate, 0},
		{stateFollower, 0},
	}

	for i, tt := range tests {
		sm := newStateMachine(0, []int{0, 1, 2})
		sm.log = &log{ents: []Entry{{}, {Term: 0}, {Term: 1}}}
		sm.term = 1
		sm.state = tt.state
		sm.Step(Message{Type: msgBeat})

		msgs := sm.Msgs()
		if len(msgs) != tt.wMsg {
			t.Errorf("#%d msgNum = %d, want %d", i, len(msgs), tt.wMsg)
		}
		for _, m := range msgs {
			if m.Type != msgApp {
				t.Errorf("#%d type = %v, want %v", i, m.Type, msgApp)
			}
		}
	}
}

func ents(terms ...int) *stateMachine {
	ents := []Entry{{}}
	for _, term := range terms {
		ents = append(ents, Entry{Term: term})
	}

	sm := &stateMachine{log: &log{ents: ents}}
	sm.reset(0)
	return sm
}

type network struct {
	peers   map[int]Interface
	dropm   map[connem]float64
	ignorem map[messageType]bool
}

func newNetwork(peers ...Interface) *network {
	size := len(peers)
	defaultPeerAddrs := make([]int, size)
	for i := 0; i < size; i++ {
		defaultPeerAddrs[i] = i
	}

	npeers := make(map[int]Interface, size)

	for id, p := range peers {
		switch v := p.(type) {
		case nil:
			sm := newStateMachine(id, defaultPeerAddrs)
			npeers[id] = sm
		case *stateMachine:
			v.id = id
			v.indexs = make(map[int]*index)
			for i := 0; i < size; i++ {
				v.indexs[i] = &index{}
			}
			v.reset(0)
			npeers[id] = v
		case *Node:
			npeers[v.sm.id] = v
		default:
			npeers[id] = v
		}
	}
	return &network{
		peers:   npeers,
		dropm:   make(map[connem]float64),
		ignorem: make(map[messageType]bool),
	}
}

func (nw *network) send(msgs ...Message) {
	for len(msgs) > 0 {
		m := msgs[0]
		p := nw.peers[m.To]
		p.Step(m)
		msgs = append(msgs[1:], nw.filter(p.Msgs())...)
	}
}

func (nw *network) drop(from, to int, perc float64) {
	nw.dropm[connem{from, to}] = perc
}

func (nw *network) cut(from, to int) {
	nw.drop(from, to, 1)
	nw.drop(to, from, 1)
}

func (nw *network) isolate(id int) {
	for i := 0; i < len(nw.peers); i++ {
		if i != id {
			nw.drop(id, i, 1.0)
			nw.drop(i, id, 1.0)
		}
	}
}

func (nw *network) ignore(t messageType) {
	nw.ignorem[t] = true
}

func (nw *network) recover() {
	nw.dropm = make(map[connem]float64)
	nw.ignorem = make(map[messageType]bool)
}

func (nw *network) filter(msgs []Message) []Message {
	mm := make([]Message, 0)
	for _, m := range msgs {
		if nw.ignorem[m.Type] {
			continue
		}
		switch m.Type {
		case msgHup:
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
	from int
	to   int
}

type blackHole struct{}

func (blackHole) Step(Message) bool { return true }
func (blackHole) Msgs() []Message   { return nil }

var nopStepper = &blackHole{}

type logSnapshoter struct {
	snapshot Snapshot
}

func (s *logSnapshoter) Snap(index, term int, nodes []int) {
	s.snapshot = Snapshot{
		Index: index,
		Term:  term,
		Nodes: nodes,
		Data:  []byte(fmt.Sprintf("%d-%d", term, index)),
	}
}

func (s *logSnapshoter) Restore(ss Snapshot) {
	s.snapshot = ss
}

func (s *logSnapshoter) GetSnap() Snapshot {
	return s.snapshot
}
