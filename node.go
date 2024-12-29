package raft

import (
	"encoding/json"
	golog "log"
)

type Interface interface {
	Step(m Message) bool
	Msgs() []Message
}

type tick int

type Config struct {
	NodeId int
	Addr   string
}

type Node struct {
	sm *stateMachine

	// 用来跟踪选举超时时间的计数器
	elapsed   tick
	election  tick
	heartbeat tick
}

func New(id int, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		heartbeat: heartbeat,
		election:  election,
		sm:        newStateMachine(id, []int{id}),
	}

	return n
}

func (n *Node) Id() int { return n.sm.id }

func (n *Node) HasLeader() bool { return n.sm.lead != none }

func (n *Node) Campaign() { n.Step(Message{Type: msgHup}) }

func (n *Node) Add(id int, addr string) { n.updateConf(AddNode, &Config{NodeId: id, Addr: addr}) }

func (n *Node) Remove(id int) { n.updateConf(RemoveNode, &Config{NodeId: id}) }

func (n *Node) Msgs() []Message { return n.sm.Msgs() }

func (n *Node) Step(m Message) bool {
	l := len(n.sm.msgs)
	if !n.sm.Step(m) {
		return false
	}
	for _, m := range n.sm.msgs[l:] {
		switch m.Type {
		case msgAppResp:
			// we just heard from the leader of the same term
			n.elapsed = 0
		case msgVoteResp:
			// we just heard from the candidate the node voted for
			if m.Index >= 0 {
				n.elapsed = 0
			}
		}
	}
	return true
}

// Propose 方法向集群提议一条数据
func (n *Node) Propose(t int, data []byte) {
	n.Step(Message{Type: msgProp, Entries: []Entry{{Type: t, Data: data}}})
}

// Next return all available entries.
func (n *Node) Next() []Entry {
	ents := n.sm.nextEnts()
	for i := range ents {
		switch ents[i].Type {
		case Normal:
		case AddNode:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.addNode(c.NodeId)
		case RemoveNode:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.removeNode(c.NodeId)
		default:
			panic("unexpected entry type")
		}
	}
	return ents
}

// Tick 方法推进时间，检查是否需要发送选举超时或心跳消息
func (n *Node) Tick() {
	if !n.sm.promotable() {
		return
	}

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

func (n *Node) updateConf(t int, c *Config) {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	n.Propose(t, data)
}
