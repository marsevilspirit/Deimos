package raft

import (
	"errors"
	"sort"
)

// 表示缺失的领导者
const none = -1

type messageType int

const (
	msgHup      messageType = iota // 开始选举
	msgBeat                        // 心跳
	msgProp                        // 提议
	msgApp                         // 附加日志
	msgAppResp                     // 附加日志响应
	msgVote                        // 请求投票
	msgVoteResp                    // 请求投票响应
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
}

func (mt messageType) String() string {
	return mtmap[mt]
}

var errNoLeader = errors.New("no leader")

type stateType int

const (
	stateFollower  stateType = iota // 跟随者
	stateCandidate                  // 候选人
	stateLeader                     // 领导者
)

// 状态类型的字符串表示
var stmap = [...]string{
	stateFollower:  "stateFollower",
	stateCandidate: "stateCandidate",
	stateLeader:    "stateLeader",
}

func (st stateType) String() string {
	return stmap[st]
}

type Message struct {
	Type     messageType // 消息类型
	From     int         // 发送者
	To       int         // 接收者
	Term     int         // 任期
	LogTerm  int         // 日志条目的任期
	Index    int         // 日志条目的索引
	PrevTerm int         // 前一个日志条目的任期
	Entries  []Entry     // 日志条目
	Commit   int         // 已提交的日志条目索引
}

type stepper interface {
	step(m Message)
}

type index struct {
	match int // 已匹配的日志条目索引
	next  int // 下一个要发送的日志条目索引
}

// 更新已匹配的日志条目索引和下一个要发送的日志条目索引
func (in *index) update(n int) {
	in.match = n
	in.next = n + 1
}

// 减少下一个要发送的日志条目索引
func (in *index) decr() {
	if in.next--; in.next < 1 {
		in.next = 1
	}
}

type stateMachine struct {
	id int

	term int

	vote int

	log *log

	// 每个节点的日志同步状态
	indexs map[int]*index

	state stateType

	// 收到的投票记录
	votes map[int]bool

	msgs []Message

	// the leader id
	lead int

	// peding reconfiguration
	pendingConf bool
}

func newStateMachine(id int, peers []int) *stateMachine {
	sm := &stateMachine{
		id:     id,
		log:    newLog(),
		indexs: make(map[int]*index),
	}
	for p := range peers {
		sm.indexs[p] = &index{}
	}
	sm.reset()
	return sm
}

// 判断是否可以处理消息
func (sm *stateMachine) canStep(m Message) bool {
	if m.Type == msgProp {
		return sm.lead != none
	}
	return true
}

// 记录投票结果并计算票数
func (sm *stateMachine) poll(id int, v bool) (granted int) {
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
	m.From = sm.id
	m.Term = sm.term
	sm.msgs = append(sm.msgs, m)
}

func (sm *stateMachine) sendAppend(to int) {
	index := sm.indexs[to]
	m := Message{
		Type:    msgApp,
		To:      to,
		Index:   index.next - 1,
		LogTerm: sm.log.term(index.next - 1),
		Entries: sm.log.entries(index.next),
		Commit:  sm.log.committed,
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

// 判断是否可以提交日志
// 同时更新commit
func (sm *stateMachine) maybeCommit() bool {
	matchIndexs := make([]int, len(sm.indexs))
	for i := range sm.indexs {
		matchIndexs[i] = sm.indexs[i].match
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndexs)))
	matchIndex := matchIndexs[sm.q()-1]

	return sm.log.maybeCommit(matchIndex, sm.term)
}

// return the applied entries and update applied index
func (sm *stateMachine) nextEnts() (ents []Entry) {
	return sm.log.nextEnts()
}

func (sm *stateMachine) reset() {
	sm.lead = none
	sm.vote = none
	sm.votes = make(map[int]bool)
	for i := range sm.indexs {
		sm.indexs[i] = &index{next: sm.log.lastIndex() + 1}
		if i == sm.id {
			sm.indexs[i].match = sm.log.lastIndex()
		}
	}
}

// 计算多数所需的节点数
func (sm *stateMachine) q() int {
	return len(sm.indexs)/2 + 1
}

func (sm *stateMachine) becomeFollower(term, lead int) {
	sm.reset()
	sm.term = term
	sm.lead = lead
	sm.state = stateFollower
	sm.pendingConf = false
}

func (sm *stateMachine) becomeCandidate() {
	if sm.state == stateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	sm.reset()
	sm.term++
	sm.vote = sm.id
	sm.state = stateCandidate
}

func (sm *stateMachine) becomeLeader() {
	if sm.state == stateFollower {
		panic("invalid transition [follower -> leader]")
	}
	sm.reset()
	sm.lead = sm.id
	sm.state = stateLeader

	for _, e := range sm.log.ents[sm.log.committed:] {
		if e.Type == configAdd || e.Type == configRemove {
			sm.pendingConf = true
		}
	}
}

func (sm *stateMachine) Msgs() []Message {
	msgs := sm.msgs
	sm.msgs = make([]Message, 0)
	return msgs
}

func (sm *stateMachine) Step(m Message) {
	switch m.Type {
	case msgHup:
		sm.becomeCandidate()
		if sm.q() == sm.poll(sm.id, true) {
			sm.becomeLeader()
			return
		}
		for i := range sm.indexs {
			if i == sm.id {
				continue
			}
			lasti := sm.log.lastIndex()
			sm.send(Message{To: i, Type: msgVote, Index: lasti, LogTerm: sm.log.term(lasti)})
		}
		return
	case msgBeat:
		if sm.state != stateLeader {
			return
		}
		sm.bcastAppend()
		return
	case msgProp:
		if len(m.Entries) == 0 {
			panic("unexpected length(entries) of a msgProp")
		}

		switch sm.lead {
		case sm.id:
			e := m.Entries[0]
			if e.Type == configAdd || e.Type == configRemove {
				if sm.pendingConf {
					// TODO: deny
					return
				}
				sm.pendingConf = true
			}
			e.Term = sm.term
			sm.log.append(sm.log.lastIndex(), e)
			sm.indexs[sm.id].update(sm.log.lastIndex())
			sm.maybeCommit()
			sm.bcastAppend()
		case none:
			panic("msgProp given without leader")
		default:
			m.To = sm.lead
			sm.send(m)
		}
		return
	}

	switch {
	case m.Term > sm.term:
		sm.becomeFollower(m.Term, m.From)
	case m.Term < sm.term:
		return
	}

	// 以下的的处理逻辑都是在当前任期下进行的

	handleAppendEntries := func() {
		if sm.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries...) {
			sm.send(Message{To: m.From, Type: msgAppResp, Index: sm.log.lastIndex()})
		} else {
			sm.send(Message{To: m.From, Type: msgAppResp, Index: -1})
		}
	}

	switch sm.state {
	case stateLeader:
		switch m.Type {
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
			// leader在相同任期下收到投票请求拒接
			sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
		}
	case stateCandidate:
		switch m.Type {
		case msgApp:
			sm.becomeFollower(sm.term, m.From)
			handleAppendEntries()
		case msgVote:
			sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
		case msgVoteResp:
			gr := sm.poll(m.From, m.Index >= 0)
			switch sm.q() {
			case gr:
				sm.becomeLeader()
				sm.bcastAppend()
			case len(sm.votes) - gr:
				sm.becomeFollower(sm.term, none)
			}
		}
	case stateFollower:
		switch m.Type {
		case msgApp:
			handleAppendEntries()
		case msgVote:
			if (sm.vote == none || sm.vote == m.From) && sm.log.isUpToDate(m.Index, m.LogTerm) {
				sm.vote = m.From
				sm.send(Message{To: m.From, Type: msgVoteResp, Index: sm.log.lastIndex()})
			} else {
				sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
			}
		}
	}
}

func (sm *stateMachine) Add(id int) {
	sm.indexs[id] = &index{next: sm.log.lastIndex() + 1}
	sm.pendingConf = false
}

func (sm *stateMachine) Remove(id int) {
	delete(sm.indexs, id)
	sm.pendingConf = false
}
