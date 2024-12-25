package raft

type tick int

type Interface interface {
	Step(m Message)
}

type Node struct {
	election  tick
	heartbeat tick

	elapsed tick // 用来跟踪选举超时时间的计数器
	sm      *stateMachine

	next Interface
}

func New(k, addr int, heartbeat, election tick, next Interface) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		sm:        newStateMachine(k, addr),
		next:      next,
		heartbeat: heartbeat,
		election:  election,
	}

	return n
}

func (n *Node) Step(m Message) {
	n.sm.Step(m)
	ms := n.sm.Msgs()
	for _, m := range ms {
		switch m.Type {
		case msgAppResp:
			n.elapsed = 0
		case msgVoteResp:
			if m.Index >= 0 {
				n.elapsed = 0
			}
		}
		n.next.Step(m)
	}
}

// Propose 方法向集群提议一条数据
func (n *Node) Propose(data []byte) {
	m := Message{
		Type: msgProp,
		Data: data,
	}
	n.Step(m)
}

// Next 方法推进提交索引并返回任何新的可提交条目
func (n *Node) Next() []Entry {
	return n.sm.nextEnts()
}

func (n *Node) Tick() {
	timeout, msgType := n.election, msgHup
	if n.sm.state == stateLeader {
		timeout, msgType = n.heartbeat, msgBeat
	}
	if n.elapsed >= timeout {
		n.elapsed = 0
		n.Step(Message{Type: msgType})
	} else {
		n.elapsed++
	}
}
