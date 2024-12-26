package raft

type Interface interface {
	Step(m Message)
	Msgs() []Message
}

type tick int

type Node struct {
	election  tick
	heartbeat tick

	elapsed tick // 用来跟踪选举超时时间的计数器
	sm      *stateMachine
}

func New(addr int, peers []int, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		sm:        newStateMachine(addr, peers),
		heartbeat: heartbeat,
		election:  election,
	}

	return n
}

func (n *Node) Msgs() []Message {
	return n.sm.Msgs()
}

func (n *Node) Step(m Message) {
	l := len(n.sm.msgs)
	n.sm.Step(m)
	for _, m := range n.sm.msgs[l:] {
		switch m.Type {
		case msgAppResp:
			n.elapsed = 0
		case msgVoteResp:
			if m.Index >= 0 {
				n.elapsed = 0
			}
		}
	}
}

// Propose 方法向集群提议一条数据
func (n *Node) Propose(data []byte) {
	m := Message{Type: msgProp, Entries: []Entry{{Data: data}}}
	n.Step(m)
}

// Next 方法推进提交索引并返回任何新的可提交条目
func (n *Node) Next() []Entry {
	return n.sm.nextEnts()
}

// Tick 方法推进时间，检查是否需要发送选举超时或心跳消息
func (n *Node) Tick() {
	timeout, msgType := n.election, msgHup
	if n.sm.state == stateLeader {
		timeout, msgType = n.heartbeat, msgBeat
	}
	if n.elapsed >= timeout {
		n.Step(Message{Type: msgType})
		n.elapsed = 0
	} else {
		n.elapsed++
	}
}
