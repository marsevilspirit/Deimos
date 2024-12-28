package raft

import (
	"encoding/json"
	golog "log"
)

type Interface interface {
	Step(m Message)
	Msgs() []Message
}

type tick int

type Config struct {
	NodeId    int
	ClusterId int
	Address   int
}

type Node struct {
	election  tick
	heartbeat tick

	// 用来跟踪选举超时时间的计数器
	elapsed tick
	sm      *stateMachine

	addr int
}

func New(addr int, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		heartbeat: heartbeat,
		election:  election,
		addr:      addr,
	}

	return n
}

func (n *Node) StartCluster() {
	if n.sm != nil {
		panic("node is started")
	}

	n.sm = newStateMachine(n.addr, []int{n.addr})
	n.Step(Message{Type: msgHup})
	n.Step(n.newConfMessage(configAdd, &Config{NodeId: n.addr}))
	n.Next()
}

func (n *Node) Start() {
	if n.sm != nil {
		panic("node is started")
	}
	n.sm = newStateMachine(n.addr, nil)
}

func (n *Node) Add(addr int) {
	n.Step(n.newConfMessage(configAdd, &Config{NodeId: addr}))
}

func (n *Node) Remove(addr int) {
	n.Step(n.newConfMessage(configRemove, &Config{NodeId: addr}))
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

// Next applies all available committed commands.
func (n *Node) Next() []Entry {
	ents := n.sm.nextEnts()
	nents := make([]Entry, 0)
	for i := range ents {
		switch ents[i].Type {
		case normal:
			// dispatch to the application state machine
			nents = append(nents, ents[i])
		case configAdd:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.Add(c.NodeId)
		case configRemove:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.Remove(c.NodeId)
		default:
			panic("unexpected entry type")
		}
	}
	return nents
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

func (n *Node) newConfMessage(t int, c *Config) Message {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return Message{
		Type:    msgProp,
		To:      n.addr,
		Entries: []Entry{{Type: t, Data: data}},
	}
}
