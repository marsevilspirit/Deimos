package raft

import (
	"errors"
	"fmt"
	golog "log"
	"sort"
	"sync/atomic"
)

// 表示缺失的领导者
const none = -1

type messageType int64

const (
	msgHup      messageType = iota // 开始选举
	msgBeat                        // 心跳
	msgProp                        // 提议
	msgApp                         // 附加日志
	msgAppResp                     // 附加日志响应
	msgVote                        // 请求投票
	msgVoteResp                    // 请求投票响应
	msgSnap                        // 快照
	msgDenied                      // 拒绝
)

// 消息类型的字符串表示
var mtmap = [...]string{
	msgHup:      "msgHup",
	msgBeat:     "msgBeat",
	msgProp:     "msgProp",
	msgApp:      "msgApp",
	msgAppResp:  "msgAppResp",
	msgVote:     "msgVote",
	msgVoteResp: "msgVoteResp",
	msgSnap:     "msgSnap",
	msgDenied:   "msgDenied",
}

func (mt messageType) String() string {
	return mtmap[int64(mt)]
}

var errNoLeader = errors.New("no leader")

type stateType int64

const (
	stateFollower  stateType = iota // 跟随者
	stateCandidate                  // 候选人
	stateLeader                     // 领导者
)

// 状态类型的字符串表示
var stmap = [...]string{
	stateFollower:  "Follower",
	stateCandidate: "Candidate",
	stateLeader:    "Leader",
}

var stepmap = [...]stepFunc{
	stateFollower:  stepFollower,
	stateCandidate: stepCandidate,
	stateLeader:    stepLeader,
}

func (st stateType) String() string {
	return stmap[int64(st)]
}

type Message struct {
	Type      messageType // 消息类型
	ClusterId int64       // 集群ID
	From      int64       // 发送者
	To        int64       // 接收者
	Term      int64       // 任期
	LogTerm   int64       // 日志条目的任期
	Index     int64       // 日志条目的索引
	PrevTerm  int64       // 前一个日志条目的任期
	Entries   []Entry     // 日志条目
	Commit    int64       // 已提交的日志条目索引
	Snapshot  Snapshot    // 快照
}

func (m Message) String() string {
	return fmt.Sprintf("type=%v from=%x to=%x term=%d logTerm=%d i=%d ci=%d len(ents)=%d",
		m.Type, m.From, m.To, m.Term, m.LogTerm, m.Index, m.Commit, len(m.Entries))
}

type stepper interface {
	step(m Message)
}

type index struct {
	match int64 // 已匹配的日志条目索引
	next  int64 // 下一个要发送的日志条目索引
}

// 更新已匹配的日志条目索引和下一个要发送的日志条目索引
func (in *index) update(n int64) {
	in.match = n
	in.next = n + 1
}

// 减少下一个要发送的日志条目索引
func (in *index) decr() {
	if in.next--; in.next < 1 {
		in.next = 1
	}
}

func (in *index) String() string {
	return fmt.Sprintf("n=%d m=%d", in.next, in.match)
}

type atomicInt int64

func (i *atomicInt) Set(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

func (i *atomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

// int64Slice implements sort interface
type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type stateMachine struct {
	clusterId int64
	id        int64

	term atomicInt

	index atomicInt

	vote int64

	raftLog *raftLog

	// 每个节点的日志同步状态
	indexs map[int64]*index

	state stateType

	// 收到的投票记录
	votes map[int64]bool

	msgs []Message

	// the leader id
	lead atomicInt

	// peding reconfiguration
	pendingConf bool

	snapshoter Snapshoter
}

func newStateMachine(id int64, peers []int64) *stateMachine {
	if id == none {
		panic("cannot use none id")
	}

	sm := &stateMachine{
		id:        id,
		clusterId: none,
		lead:      none,
		raftLog:   newLog(),
		indexs:    make(map[int64]*index),
	}
	for _, p := range peers {
		sm.indexs[p] = &index{}
	}
	sm.reset(0)
	return sm
}

func (sm *stateMachine) String() string {
	s := fmt.Sprintf(`state=%v term=%d`, sm.state, sm.term)
	switch sm.state {
	case stateFollower:
		s += fmt.Sprintf(" vote=%v lead=%v", sm.vote, sm.lead)
	case stateCandidate:
		s += fmt.Sprintf(` votes="%v"`, sm.votes)
	case stateLeader:
		s += fmt.Sprintf(` ins="%v"`, sm.indexs)
	}
	return s
}

func (sm *stateMachine) setSnapshoter(s Snapshoter) {
	sm.snapshoter = s
}

// 记录投票结果并计算票数
func (sm *stateMachine) poll(id int64, v bool) (granted int) {
	if _, ok := sm.votes[id]; !ok {
		sm.votes[id] = v
	}

	for _, vv := range sm.votes {
		if vv {
			granted++
		}
	}

	return granted
}

// 发送消息
func (sm *stateMachine) send(m Message) {
	m.ClusterId = sm.clusterId
	m.From = sm.id
	m.Term = sm.term.Get()
	golog.Printf("raft.send.msg %v\n", m)
	sm.msgs = append(sm.msgs, m)
}

func (sm *stateMachine) sendAppend(to int64) {
	index := sm.indexs[to]
	m := Message{
		To:    to,
		Index: index.next - 1,
	}

	if sm.needSnapshot(m.Index) {
		m.Type = msgSnap
		m.Snapshot = sm.snapshoter.GetSnap()
	} else {
		m.Type = msgApp
		m.LogTerm = sm.raftLog.term(index.next - 1)
		m.Entries = sm.raftLog.entries(index.next)
		m.Commit = sm.raftLog.committed
	}

	sm.send(m)
}

func (sm *stateMachine) sendHeartbeat(to int64) {
	in := sm.indexs[to]
	index := max(in.next-1, sm.raftLog.lastIndex())
	m := Message{
		To:      to,
		Type:    msgApp,
		Index:   index,
		LogTerm: sm.raftLog.term(index),
		Commit:  sm.raftLog.committed,
	}
	sm.send(m)
}

// 广播附加日志消息
func (sm *stateMachine) bcastAppend() {
	for i := range sm.indexs {
		if i == sm.id {
			continue
		}
		sm.sendAppend(i)
	}
}

func (sm *stateMachine) bcastHeartbeat() {
	for i := range sm.indexs {
		if i == sm.id {
			continue
		}
		sm.sendHeartbeat(i)
	}
}

// 判断是否可以提交日志
// 同时更新commit
func (sm *stateMachine) maybeCommit() bool {
	// 不需要0初始化，所以使用0
	matchIndexs := make(int64Slice, 0, len(sm.indexs))
	for i := range sm.indexs {
		matchIndexs = append(matchIndexs, sm.indexs[i].match)
	}
	sort.Sort(sort.Reverse(matchIndexs))
	matchIndex := matchIndexs[sm.q()-1]

	return sm.raftLog.maybeCommit(matchIndex, sm.term.Get())
}

// return the applied entries and update applied index
func (sm *stateMachine) nextEnts() (ents []Entry) {
	return sm.raftLog.nextEnts()
}

func (sm *stateMachine) reset(term int64) {
	sm.term.Set(term)
	sm.lead.Set(none)
	sm.vote = none
	sm.votes = make(map[int64]bool)
	for i := range sm.indexs {
		sm.indexs[i] = &index{next: sm.raftLog.lastIndex() + 1}
		if i == sm.id {
			sm.indexs[i].match = sm.raftLog.lastIndex()
		}
	}
}

// 计算多数所需的节点数
func (sm *stateMachine) q() int {
	return len(sm.indexs)/2 + 1
}

func (sm *stateMachine) appendEntry(e Entry) {
	e.Term = sm.term.Get()
	sm.index.Set(sm.raftLog.append(sm.raftLog.lastIndex(), e))
	sm.indexs[sm.id].update(sm.raftLog.lastIndex())
	sm.maybeCommit()
}

// promotable indicates whether state machine could be promoted.
// New machine has to wait for the first log entry to be committed, or it will
// always start as a one-node cluster.
func (sm *stateMachine) promotable() bool {
	return sm.raftLog.committed != 0
}

func (sm *stateMachine) becomeFollower(term int64, lead int64) {
	sm.reset(term)
	sm.lead.Set(lead)
	sm.state = stateFollower
	sm.pendingConf = false
}

func (sm *stateMachine) becomeCandidate() {
	if sm.state == stateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	sm.reset(sm.term.Get() + 1)
	sm.vote = sm.id
	sm.state = stateCandidate
}

func (sm *stateMachine) becomeLeader() {
	if sm.state == stateFollower {
		panic("invalid transition [follower -> leader]")
	}
	sm.reset(sm.term.Get())
	sm.lead.Set(sm.id)
	sm.state = stateLeader

	for _, e := range sm.raftLog.entries(sm.raftLog.committed + 1) {
		if e.Type == AddNode || e.Type == RemoveNode {
			sm.pendingConf = true
		}
	}
	sm.appendEntry(Entry{Type: Normal, Data: nil})
}

func (sm *stateMachine) Msgs() []Message {
	msgs := sm.msgs
	sm.msgs = make([]Message, 0)
	return msgs
}

func (sm *stateMachine) Step(m Message) (ok bool) {
	// fmt.Printf("%s node %d receive %+v\n", sm.state.String(), sm.id, m)
	golog.Printf("raft.step beforeState %v\n", sm)
	golog.Printf("raft.step beforeLog %v\n", sm.raftLog)
	defer golog.Printf("raft.step afterLog %v\n", sm.raftLog)
	defer golog.Printf("raft.step afterState %v\n", sm)
	golog.Printf("raft.step msg %v\n", m)
	if m.Type == msgHup {
		sm.becomeCandidate()
		if sm.q() == sm.poll(sm.id, true) {
			sm.becomeLeader()
			return true
		}
		for i := range sm.indexs {
			if i == sm.id {
				continue
			}
			lasti := sm.raftLog.lastIndex()
			sm.send(Message{To: i, Type: msgVote, Index: lasti, LogTerm: sm.raftLog.term(lasti)})
		}
		return true
	}

	switch {
	case m.Term == 0:
		// local message
	case m.Term > sm.term.Get():
		lead := m.From
		if m.Type == msgVote {
			lead = none
		}
		sm.becomeFollower(m.Term, lead)
	case m.Term < sm.term.Get():
		// ignore
		return true
	}

	return stepmap[sm.state](sm, m)
}

func (sm *stateMachine) handleAppendEntries(m Message) {
	if sm.raftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...) {
		sm.index.Set(sm.raftLog.lastIndex())
		sm.send(Message{To: m.From, Type: msgAppResp, Index: sm.raftLog.lastIndex()})
	} else {
		sm.send(Message{To: m.From, Type: msgAppResp, Index: -1})
	}
}

func (sm *stateMachine) handleSnapshot(m Message) {
	sm.restore(m.Snapshot)
	sm.send(Message{To: m.From, Type: msgAppResp, Index: sm.raftLog.lastIndex()})
}

func (sm *stateMachine) addNode(id int64) {
	sm.indexs[id] = &index{next: sm.raftLog.lastIndex() + 1}
	sm.pendingConf = false
}

func (sm *stateMachine) removeNode(id int64) {
	delete(sm.indexs, id)
	sm.pendingConf = false
}

type stepFunc func(sm *stateMachine, m Message) bool

func stepLeader(sm *stateMachine, m Message) bool {
	switch m.Type {
	case msgBeat:
		sm.bcastHeartbeat()
	case msgProp:
		if len(m.Entries) != 1 {
			panic("unexpected length(entries) of a msgProp")
		}
		e := m.Entries[0]
		if e.isConfig() {
			if sm.pendingConf {
				return false
			}
			sm.pendingConf = true
		}
		sm.appendEntry(e)
		sm.bcastAppend()
	case msgAppResp:
		if m.Index < 0 {
			sm.indexs[m.From].decr()
			sm.sendAppend(m.From)
		} else {
			sm.indexs[m.From].update(m.Index)
			if sm.maybeCommit() {
				sm.bcastAppend()
			}
		}
	case msgVote:
		sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
	}
	return true
}

func stepCandidate(sm *stateMachine, m Message) bool {
	switch m.Type {
	case msgProp:
		return false
	case msgApp:
		sm.becomeFollower(sm.term.Get(), m.From)
		sm.handleAppendEntries(m)
	case msgSnap:
		sm.becomeFollower(m.Term, m.From)
		sm.handleSnapshot(m)
	case msgVote:
		sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
	case msgVoteResp:
		gr := sm.poll(m.From, m.Index >= 0)
		switch sm.q() {
		case gr:
			sm.becomeLeader()
			sm.bcastAppend()
		case len(sm.votes) - gr:
			sm.becomeFollower(sm.term.Get(), none)
		}
	}
	return true
}

func stepFollower(sm *stateMachine, m Message) bool {
	switch m.Type {
	case msgProp:
		if sm.lead.Get() == none {
			return false
		}
		m.To = sm.lead.Get()
		sm.send(m)
	case msgApp:
		sm.lead.Set(m.From)
		sm.handleAppendEntries(m)
	case msgSnap:
		sm.handleSnapshot(m)
	case msgVote:
		if (sm.vote == none || sm.vote == m.From) && sm.raftLog.isUpToDate(m.Index, m.LogTerm) {
			sm.vote = m.From
			sm.send(Message{To: m.From, Type: msgVoteResp, Index: sm.raftLog.lastIndex()})
		} else {
			sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
		}
	}
	return true
}

func (sm *stateMachine) maybeCompact() bool {
	if sm.snapshoter == nil || !sm.raftLog.shouldCompact() {
		return false
	}
	sm.snapshoter.Snap(sm.raftLog.applied, sm.raftLog.term(sm.raftLog.applied), sm.nodes())
	sm.raftLog.compact(sm.raftLog.applied)
	return true
}

func (sm *stateMachine) restore(s Snapshot) {
	if sm.snapshoter == nil {
		panic("try to restore from snapshot, but snapshoter is nil")
	}

	sm.raftLog.restore(s.Index, s.Term)
	sm.index.Set(sm.raftLog.lastIndex())
	sm.indexs = make(map[int64]*index)
	for _, n := range s.Nodes {
		sm.indexs[n] = &index{next: sm.raftLog.lastIndex() + 1}
		if n == sm.id {
			sm.indexs[n].match = sm.raftLog.lastIndex()
		}
	}

	sm.pendingConf = false
	sm.snapshoter.Restore(s)
}

func (sm *stateMachine) needSnapshot(i int64) bool {
	if i < sm.raftLog.offset {
		if sm.snapshoter == nil {
			panic("try to generate snapshot, but snapshoter is nil")
		}
		return true
	}
	return false
}

func (sm *stateMachine) nodes() []int64 {
	nodes := make([]int64, 0, len(sm.indexs))
	for k := range sm.indexs {
		nodes = append(nodes, k)
	}
	return nodes
}
