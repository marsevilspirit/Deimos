package raft

import "sync"

type Interface interface {
	Step(m Message)
}

type Node struct {
	sm *stateMachine
	mu sync.Mutex
}

func New(k, addr int, next Interface) Interface {
	n := &Node{
		sm: newStateMachine(k, addr, next),
	}
	return n
}

// 并发安全
func (n *Node) Step(m Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.sm.Step(m)
}

func (n *Node) Propose(data []byte) {
	m := Message{
		Type: msgHup,
		Data: data,
	}
	n.Step(m)
}
