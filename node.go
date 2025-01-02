package raft

import (
	"encoding/json"
	golog "log"
	"sync/atomic"
)

type Interface interface {
	Step(m Message) bool
	Msgs() []Message
}

type tick int64

type Config struct {
	NodeId  int64
	Addr    string
	Context []byte
}

type Node struct {
	sm *stateMachine

	// 用来跟踪选举超时时间的计数器
	elapsed   tick
	election  tick
	heartbeat tick
}

func New(id int64, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	n := &Node{
		heartbeat: heartbeat,
		election:  election,
		sm:        newStateMachine(id, []int64{id}),
	}

	return n
}

func (n *Node) Id() int64 {
	return atomic.LoadInt64(&n.sm.id)
}

func (n *Node) Index() int64 { return n.sm.log.lastIndex() }

func (n *Node) Term() int64 { return n.sm.term }

func (n *Node) Applied() int64 { return n.sm.log.committed }

func (n *Node) HasLeader() bool { return n.Leader() != none }

func (n *Node) IsLeader() bool { return n.Leader() == n.Id() }

func (n *Node) Leader() int64 { return n.sm.lead.Get() }

func (n *Node) Campaign() { n.Step(Message{Type: msgHup}) }

func (n *Node) Add(id int64, addr string, context []byte) {
	n.updateConf(AddNode, &Config{NodeId: id, Addr: addr, Context: context})
}

func (n *Node) Remove(id int64) { n.updateConf(RemoveNode, &Config{NodeId: id}) }

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
func (n *Node) Propose(t int64, data []byte) {
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

func (n *Node) updateConf(t int64, c *Config) {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	n.Propose(t, data)
}
