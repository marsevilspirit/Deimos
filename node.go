package raft

import (
	"encoding/binary"
	"encoding/json"
	golog "log"
	"math/rand"
	"time"
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

	// TODO: it needs garbage collection later
	rmNodes map[int64]struct{}
	removed bool
}

func New(id int64, heartbeat, election tick) *Node {
	if election < heartbeat*3 {
		panic("election is least three times as heartbeat [election: %d, heartbeat: %d]")
	}

	// 创建一个局部随机数生成器
	localRand := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := &Node{
		heartbeat: heartbeat,
		election:  election + tick(localRand.Int31())%election,
		sm:        newStateMachine(id, []int64{id}),
		rmNodes:   make(map[int64]struct{}),
	}

	return n
}

func Recover(id int64, ents []Entry, state State, heartbeat, election tick) *Node {
	n := New(id, heartbeat, election)
	n.sm.loadEnts(ents)
	n.sm.loadState(state)
	return n
}

func (n *Node) Id() int64 { return n.sm.id }

func (n *Node) ClusterId() int64 { return n.sm.clusterId }

func (n *Node) Index() int64 { return n.sm.index.Get() }

func (n *Node) Term() int64 { return n.sm.term.Get() }

func (n *Node) Applied() int64 { return n.sm.raftLog.committed }

func (n *Node) HasLeader() bool { return n.Leader() != none }

func (n *Node) IsLeader() bool { return n.Leader() == n.Id() }

func (n *Node) Leader() int64 { return n.sm.lead.Get() }

func (n *Node) IsRemoved() bool { return n.removed }

func (n *Node) Campaign() { n.Step(Message{From: n.sm.id, ClusterId: n.ClusterId(), Type: msgHup}) }

func (n *Node) InitCluster(clusterId int64) {
	d := make([]byte, 10)
	wn := binary.PutVarint(d, clusterId)
	n.Propose(ClusterInit, d[:wn])
}

func (n *Node) Add(id int64, addr string, context []byte) {
	n.UpdateConf(AddNode, &Config{NodeId: id, Addr: addr, Context: context})
}

func (n *Node) Remove(id int64) {
	n.UpdateConf(RemoveNode, &Config{NodeId: id})
}

func (n *Node) Msgs() []Message { return n.sm.Msgs() }

func (n *Node) Step(m Message) bool {
	if m.Type == msgDenied {
		n.removed = true
		return false
	}

	if n.ClusterId() != none && m.ClusterId != none && m.ClusterId != n.ClusterId() {
		golog.Printf("denied a message from node %d, cluster %d, accept cluster %d\n", m.From, m.ClusterId, n.ClusterId())
		n.sm.send(Message{To: m.From, ClusterId: n.ClusterId(), Type: msgDenied})
		return true
	}

	if _, ok := n.rmNodes[m.From]; ok {
		if m.From != n.sm.id {
			n.sm.send(Message{To: m.From, ClusterId: n.ClusterId(), Type: msgDenied})
		}
		return true
	}

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
	n.Step(Message{From: n.sm.id, ClusterId: n.ClusterId(), Type: msgProp, Entries: []Entry{{Type: t, Data: data}}})
}

// Next return all available entries.
func (n *Node) Next() []Entry {
	ents := n.sm.nextEnts()
	for i := range ents {
		switch ents[i].Type {
		case Normal:
		case ClusterInit:
			cid, nr := binary.Varint(ents[i].Data)
			if nr <= 0 {
				panic("init cluster failed: cannot read cluster id")
			}
			if n.ClusterId() != -1 {
				panic("cannot init a started cluster")
			}
			n.sm.clusterId = cid
		case AddNode:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.addNode(c.NodeId)
			delete(n.rmNodes, c.NodeId)
		case RemoveNode:
			c := new(Config)
			if err := json.Unmarshal(ents[i].Data, c); err != nil {
				golog.Println(err)
				continue
			}
			n.sm.removeNode(c.NodeId)
			n.rmNodes[c.NodeId] = struct{}{}
			if c.NodeId == n.sm.id {
				n.removed = true
			}
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
		n.Step(Message{From: n.sm.id, ClusterId: n.ClusterId(), Type: msgType})
		n.elapsed = 0
	} else {
		n.elapsed++
	}
}

// IsEmpty returns ture if the log of the node is empty.
func (n *Node) IsEmpty() bool {
	return n.sm.raftLog.isEmpty()
}

func (n *Node) UpdateConf(t int64, c *Config) {
	data, err := json.Marshal(c)
	if err != nil {
		panic(err)
	}

	n.Propose(t, data)
}

// int64 is offset, []Entry is entries
// UnstableEnts retuens all the entries that need to be persistent.
func (n *Node) UnstableEnts() []Entry {
	return n.sm.raftLog.unstableEnts()
}

func (n *Node) UnstableState() State {
	if n.sm.unstableState == EmptyState {
		return EmptyState
	}
	s := n.sm.unstableState
	n.sm.clearState()
	return s
}
