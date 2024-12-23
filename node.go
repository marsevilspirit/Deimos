package raft

import "sync"

type Interface interface {
	Step(m Message)
}

type Node struct {
	sm *stateMachine
	mu sync.Mutex
}

func New(k, addr int, next Interface) *Node {
	n := &Node{
		sm: newStateMachine(k, addr),
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
		Type: msgProp,
		Data: data,
	}
	n.Step(m)
}

// Next 方法推进提交索引并返回任何新的可提交条目
func (n *Node) Next() []Entry {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.sm.nextEnts()
}
