package raft

import (
	"encoding/json"
)

type Interface interface {
	Step(m Message)
	Msgs() []Message
}

type tick int

type ConfigCmd struct {
	Type string
	Addr int
}

type Node struct {
	election  tick
	heartbeat tick

	// 用来跟踪选举超时时间的计数器
	elapsed tick
	sm      *stateMachine

	addr int
}

func New(addr int, peers []int, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		sm:        newStateMachine(addr, peers),
		heartbeat: heartbeat,
		election:  election,
		addr:      addr,
	}

	return n
}

func (n *Node) Add(addr int) {
	n.Step(n.confMessage(&ConfigCmd{Type: "add", Addr: addr}))
}

func (n *Node) Remove(addr int) {
	n.Step(n.confMessage(&ConfigCmd{Type: "remove", Addr: addr}))
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
func (n *Node) Next() {
	ents := n.sm.nextEnts()
	for i := range ents {
		switch ents[i].Type {
		case normal:
			// dispatch to the application state machine
		case config:
			c := new(ConfigCmd)
			err := json.Unmarshal(ents[i].Data, c)
			if err != nil {
				// warning
				continue
			}
			n.updateConf(c)
		default:
			panic("unexpected entry type")
		}
	}
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

func (n *Node) confMessage(c *ConfigCmd) Message {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	return Message{
		Type:    msgProp,
		Entries: []Entry{{Type: config, Data: data}},
	}
}

func (n *Node) updateConf(c *ConfigCmd) {
	switch c.Type {
	case "add":
		n.sm.Add(c.Addr)
	case "remove":
		n.sm.Remove(c.Addr)
	default:
		// warn
	}
}
