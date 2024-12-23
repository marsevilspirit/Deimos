package raft

import (
	"fmt"
	"reflect"
	"testing"
)

var defaultLog = []Entry{{}}

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

		{
			newNetwork(
				nil,
				&stateMachine{log: []Entry{{}, {Term: 1}}},
				&stateMachine{log: []Entry{{}, {Term: 2}}},
				&stateMachine{log: []Entry{{}, {Term: 1}, {Term: 3}}},
			),
			stateFollower,
		},

		{
			newNetwork(
				&stateMachine{log: []Entry{{}, {Term: 1}}},
				nil,
				&stateMachine{log: []Entry{{}, {Term: 2}}},
				&stateMachine{log: []Entry{{}, {Term: 1}}},
				nil,
			),
			stateLeader,
		},
	}

	for i, tt := range tests {
		tt.step(Message{To: 0, Type: msgHup})
		sm := tt.network.ss[0].(*stateMachine)
		if sm.state != tt.state {
			t.Errorf("#%d: state = %v, want %v", i, sm.state, tt.state)
		}
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want 1", i, g)
		}
	}
}

// 测试在分布式系统中两个候选者节点同时发起选举的情况
func TestDualingCandidates(t *testing.T) {
	a := &stateMachine{log: defaultLog}
	c := &stateMachine{log: defaultLog}

	tt := newNetwork(a, nil, c)

	heal := false
	next := stepperFunc(func(m Message) {
		if heal {
			tt.step(m)
		}
	})
	a.next = next
	c.next = next

	tt.tee = stepperFunc(func(m Message) {
		t.Logf("m = %+v", m)
	})
	tt.step(Message{To: 0, Type: msgHup})
	tt.step(Message{To: 2, Type: msgHup})

	t.Log("healing")
	heal = true
	tt.step(Message{To: 2, Type: msgHup})

	tests := []struct {
		sm    *stateMachine
		state stateType
		term  int
	}{
		{a, stateFollower, 2},
		{c, stateLeader, 2},
	}

	for i, tt := range tests {
		if g := tt.sm.state; g != tt.state {
			t.Errorf("#%d: state = %s, want %s", i, g, tt.state)
		}
		if g := tt.sm.term; g != tt.term {
			t.Errorf("#%d: term = %d, want %d", i, g, tt.term)
		}
	}
	if g := diffLogs(defaultLog, tt.logs()); g != nil {
		for _, diff := range g {
			t.Errorf("bag log:\n%s", diff)
		}
	}
}

func TestCandidateConcede(t *testing.T) {
	a := &stateMachine{log: defaultLog}

	tt := newNetwork(a, nil, nil)
	tt.tee = stepperFunc(func(m Message) {
		t.Logf("m = %+v", m)
	})

	a.next = nopStepper

	tt.step(Message{To: 0, Type: msgHup})
	tt.step(Message{To: 2, Type: msgHup})

	// heal the partition
	a.next = tt

	data := []byte("force follower")
	// send a proposal to 2 to flush out a msgApp to 0
	tt.step(Message{To: 2, Type: msgProp, Data: data})

	if g := a.state; g != stateFollower {
		t.Errorf("state = %s, want %s", g, stateFollower)
	}
	if g := a.term; g != 1 {
		t.Errorf("term = %d, want %d", g, 1)
	}
	wantLog := []Entry{{}, {Term: 1, Data: data}}
	if g := diffLogs(wantLog, tt.logs()); g != nil {
		for _, diff := range g {
			t.Errorf("bag log:\n%s", diff)
		}
	}
}

func TestOldMessages(t *testing.T) {
	tt := newNetwork(nil, nil, nil)
	// make 0 leader @ term 3
	tt.step(Message{To: 0, Type: msgHup})
	tt.step(Message{To: 0, Type: msgHup})
	tt.step(Message{To: 0, Type: msgHup})
	// pretend we're an old leader trying to make progress; this entry is expected to be ignored.
	tt.step(Message{To: 0, Type: msgApp, Index: 1, Term: 1, Entries: []Entry{{Term: 1}}})
	if g := diffLogs(defaultLog, tt.logs()); g != nil {
		for _, diff := range g {
			t.Errorf("bad log:\n%s", diff)
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
		tt.tee = stepperFunc(func(m Message) {
			t.Logf("#%d: m = %+v", i, m)
		})

		step := stepperFunc(func(m Message) {
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
			tt.step(m)
		})

		data := []byte("somedata")

		// promote 0 the leader
		step(Message{To: 0, Type: msgHup})
		step(Message{To: 0, Type: msgProp, Data: data})

		var wantLog []Entry
		if tt.success {
			wantLog = []Entry{{}, {Term: 1, Data: data}}
		} else {
			wantLog = defaultLog
		}
		if g := diffLogs(wantLog, tt.logs()); g != nil {
			for _, diff := range g {
				t.Errorf("#%d: diff:%s", i, diff)
			}
		}
		sm := tt.network.ss[0].(*stateMachine)
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
		tt.tee = stepperFunc(func(m Message) {
			t.Logf("#%d: m = %+v", i, m)
		})

		// promote 0 the leader
		tt.step(Message{To: 0, Type: msgHup})

		// propose via follower
		tt.step(Message{To: 1, Type: msgProp, Data: []byte("somedata")})

		wantLog := []Entry{{}, {Term: 1, Data: data}}
		if g := diffLogs(wantLog, tt.logs()); g != nil {
			for _, diff := range g {
				t.Errorf("#%d: bad entry: %s", i, diff)
			}
		}
		sm := tt.ss[0].(*stateMachine)
		if g := sm.term; g != 1 {
			t.Errorf("#%d: term = %d, want %d", i, g, 1)
		}
	}
}

func TestVote(t *testing.T) {
	tests := []struct {
		i    int
		term int
		want int
	}{
		{0, 0, -1},
		{0, 1, -1},
		{0, 2, -1},
		{0, 3, 2},

		{1, 0, -1},
		{1, 1, -1},
		{1, 2, -1},
		{1, 3, 2},

		{2, 0, -1},
		{2, 1, -1},
		{2, 2, 2},
		{2, 3, 2},

		{3, 0, -1},
		{3, 1, -1},
		{3, 2, 2},
		{3, 3, 2},
	}

	for i, tt := range tests {
		called := false
		sm := &stateMachine{log: []Entry{{}, {Term: 2}, {Term: 2}}}
		sm.next = stepperFunc(func(m Message) {
			called = true
			if m.Index != tt.want {
				t.Errorf("#%d, m.Index = %d, want %d", i, m.Index, tt.want)
			}
		})
		sm.step(Message{Type: msgVote, Index: tt.i, LogTerm: tt.term})
		if !called {
			t.Fatalf("#%d: not called", i)
		}
	}
}

func TestLogDiff(t *testing.T) {
	a := []Entry{{}, {Term: 1}, {Term: 2}}
	b := []Entry{{}, {Term: 1}, {Term: 2}}
	c := []Entry{{}, {Term: 2}}
	d := []Entry(nil)

	w := []diff{
		{1, []*Entry{{Term: 1}, {Term: 1}, {Term: 2}, nilLogEntry}},
		{2, []*Entry{{Term: 2}, {Term: 2}, noEntry, nilLogEntry}},
	}

	t.Logf("diff log:\n%s", diffLogs(a, [][]Entry{b, c, d}))

	if g := diffLogs(a, [][]Entry{b, c, d}); !reflect.DeepEqual(w, g) {
		t.Errorf("g = %s", g)
		t.Errorf("want %s", w)
	}
}

type network struct {
	tee stepper
	ss  []stepper
}

func newNetwork(nodes ...stepper) *network {
	nt := &network{ss: nodes}
	for i, n := range nodes {
		switch v := n.(type) {
		case nil:
			nt.ss[i] = newStateMachine(len(nodes), i, nt)
		case *stateMachine:
			v.k = len(nodes)
			v.addr = i
			if v.next == nil {
				v.next = nt
			}
		default:
			nt.ss[i] = v
		}
	}
	return nt
}

// 实现stepper接口
func (nt network) step(m Message) {
	if nt.tee != nil {
		nt.tee.step(m)
	}
	nt.ss[m.To].step(m)
}

// 获取当前节点的日志
// 如何不是stateMachine类型的节点，log为nil
func (nt network) logs() [][]Entry {
	ls := make([][]Entry, len(nt.ss))
	for i, node := range nt.ss {
		if sm, ok := node.(*stateMachine); ok {
			ls[i] = sm.log
		}
	}
	return ls
}

type diff struct {
	i    int
	ents []*Entry
}

var noEntry = &Entry{}
var nilLogEntry = &Entry{}

func (d diff) String() string {
	s := fmt.Sprintf("[%d] ", d.i)
	for i, e := range d.ents {
		switch e {
		case nilLogEntry:
			s += fmt.Sprintf("o")
		case noEntry:
			s += fmt.Sprintf("-")
		case nil:
			s += fmt.Sprintf("<nil>")
		default:
			s += fmt.Sprintf("<%d:%q>", e.Term, string(e.Data))
		}
		if i != len(d.ents)-1 {
			s += "\t\t"
		}
	}
	return s
}

func diffLogs(base []Entry, logs [][]Entry) []diff {
	var (
		d   []diff
		max int
	)
	logs = append([][]Entry{base}, logs...)
	for _, log := range logs {
		if l := len(log); l > max {
			max = l
		}
	}
	ediff := func(i int) (result []*Entry) {
		e := make([]*Entry, len(logs))
		found := false
		for j, log := range logs {
			if log == nil {
				e[j] = nilLogEntry
				continue
			}
			if len(log) <= i {
				e[j] = noEntry
				found = true
				continue
			}
			e[j] = &log[i]
			if j > 0 {
				switch prev := e[j-1]; {
				case prev == nilLogEntry:
				case prev == noEntry:
				case !reflect.DeepEqual(prev, e[j]):
					found = true
				}
			}
		}
		if found {
			return e
		}
		return nil
	}
	for i := 0; i < max; i++ {
		if e := ediff(i); e != nil {
			d = append(d, diff{i, e})
		}
	}
	return d
}

type stepperFunc func(Message)

func (f stepperFunc) step(m Message) { f(m) }

var nopStepper = stepperFunc(func(Message) {})

type nextStepper func(Message, stepper)
