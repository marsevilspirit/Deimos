package raft

import "testing"

const (
	defaultHeartbeat = 1
	defaultElection  = 5
)

func TestTickMsgHup(t *testing.T) {
	n := New(0, defaultHeartbeat, defaultElection)
	n.sm = newStateMachine(0, []int64{0, 1, 2})

	// simulate to patch the join log
	n.Step(Message{Type: msgApp, Commit: 1, Entries: []Entry{{}}})

	for i := 0; i < defaultElection+1; i++ {
		n.Tick()
	}

	called := false

	for _, m := range n.Msgs() {
		if m.Type == msgVote {
			called = true
		}
	}

	if !called {
		t.Errorf("called = %v, want true", called)
	}
}

func TestTickMsgBeat(t *testing.T) {
	k := 3

	n := dictate(New(0, defaultHeartbeat, defaultElection))
	n.Next()
	for i := 1; i < k; i++ {
		n.Add(int64(i), "", nil)
		for _, m := range n.Msgs() {
			if m.Type == msgApp {
				n.Step(Message{From: m.To, Type: msgAppResp, Index: m.Index + int64(len(m.Entries))})
			}
		}
		n.Msgs()
		n.Next()
	}

	for i := 0; i < defaultHeartbeat+1; i++ {
		n.Tick()
	}

	called := 0
	for _, m := range n.Msgs() {
		if m.Type == msgApp {
			called++
		}
	}
	// msgBeat -> k-1 append
	w := k - 1
	if called != w {
		t.Errorf("called = %v, want %v", called, w)
	}
}

func TestResetElapse(t *testing.T) {
	tests := []struct {
		msg      Message
		welapsed tick
	}{
		{Message{From: 0, To: 1, Type: msgApp, Term: 2, Entries: []Entry{{Term: 1}}}, 0},
		{Message{From: 0, To: 1, Type: msgApp, Term: 1, Entries: []Entry{{Term: 1}}}, 1},
		{Message{From: 0, To: 1, Type: msgVote, Term: 2}, 0},
		{Message{From: 0, To: 1, Type: msgVote, Term: 1}, 1},
	}

	for i, tt := range tests {
		n := New(0, defaultHeartbeat, defaultElection)
		n.sm = newStateMachine(0, []int64{0, 1, 2})
		n.sm.term = 2
		n.sm.log.committed = 1

		n.Tick()
		if n.elapsed != 1 {
			t.Errorf("#%d: elapsed = %d, want 1", i, n.elapsed)
		}

		n.Step(tt.msg)
		if n.elapsed != tt.welapsed {
			t.Errorf("#%d: elapsed = %d, want %d", i, n.elapsed, tt.welapsed)
		}
	}
}

func TestStartCluster(t *testing.T) {
	n := dictate(New(0, defaultHeartbeat, defaultElection))
	n.Next()

	if len(n.sm.indexs) != 1 {
		t.Errorf("k = %d, want 1", len(n.sm.indexs))
	}
	if n.sm.id != 0 {
		t.Errorf("id = %d, want 0", n.sm.id)
	}
	if n.sm.state != stateLeader {
		t.Errorf("state = %d, want %d", n.sm.state, stateLeader)
	}
}

func TestAdd(t *testing.T) {
	n := dictate(New(0, defaultHeartbeat, defaultElection))
	n.Next()

	if len(n.sm.indexs) != 1 {
		t.Errorf("k = %d, want 1", len(n.sm.indexs))
	}
	if n.sm.id != 0 {
		t.Errorf("id = %d, want 0", n.sm.id)
	}

	n.Add(1, "", nil)
	n.Next()

	if len(n.sm.indexs) != 2 {
		t.Errorf("k = %d, want 2", len(n.sm.indexs))
	}
	if n.sm.id != 0 {
		t.Errorf("id = %d, want 0", n.sm.id)
	}
}

func TestRemove(t *testing.T) {
	n := dictate(New(0, defaultHeartbeat, defaultElection))
	n.Next()

	n.Add(1, "", nil)
	n.Next()
	n.Remove(0)
	n.Step(Message{Type: msgAppResp, From: 1, Term: 1, Index: 4})
	n.Next()

	if len(n.sm.indexs) != 1 {
		t.Errorf("k = %d, want 1", len(n.sm.indexs))
	}

	if n.sm.id != 0 {
		t.Errorf("id = %d, want 0", n.sm.id)
	}
}

func dictate(n *Node) *Node {
	n.Step(Message{Type: msgHup})
	n.Add(n.Id(), "", nil)
	return n
}
