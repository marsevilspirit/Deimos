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
	msgProp                        // 提议
	msgApp                         // 附加日志
	msgAppResp                     // 附加日志响应
	msgVote                        // 请求投票
	msgVoteResp                    // 请求投票响应
)

// 消息类型的字符串表示
var mtmap = [...]string{
	msgHup:      "msgHup",
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

// 日志条目
type Entry struct {
	Term int    // 任期
	Data []byte // 数据
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
	Data     []byte      // 数据
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
	k      int          // 节点总数
	addr   int          // 节点地址
	term   int          // 任期
	vote   int          // 投票给谁
	log    []Entry      // 日志条目
	ins    []*index     // 每个节点的日志同步状态
	state  stateType    // 状态
	commit int          // 已提交的日志条目索引
	votes  map[int]bool // 收到的投票记录
	next   Interface    // 下一步处理
	lead   int          // 领导者
}

func newStateMachine(k, addr int, next Interface) *stateMachine {
	sm := &stateMachine{
		k:    k,
		addr: addr,
		next: next,
		log:  make([]Entry, 1, 1024),
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
func (sm *stateMachine) poll(addr int, v bool) (granted int) {
	if _, ok := sm.votes[addr]; !ok {
		sm.votes[addr] = v
	}

	for _, vv := range sm.votes {
		if vv {
			granted++
		}
	}

	return granted
}

// 在日志中追加新的条目
func (sm *stateMachine) append(after int, ents ...Entry) int {
	sm.log = append(sm.log[:after+1], ents...)
	return len(sm.log) - 1
}

func (sm *stateMachine) isLogOk(i, term int) bool {
	if i > sm.li() {
		return false
	}
	return sm.log[i].Term == term
}

// 发送消息
func (sm *stateMachine) send(m Message) {
	m.From = sm.addr
	m.Term = sm.term
	sm.next.Step(m)
}

// 发送附加日志消息
func (sm *stateMachine) sendAppend() {
	for i := 0; i < sm.k; i++ {
		if i == sm.addr {
			continue
		}
		in := sm.ins[i]
		m := Message{
			Type:    msgApp,
			To:      i,
			Index:   in.next - 1,
			LogTerm: sm.log[in.next-1].Term,
			Entries: sm.log[in.next:],
		}
		sm.send(m)
	}
}

// 找到在当前任期中复制到多数节点的最大日志索引
func (sm *stateMachine) theN() int {
	mis := make([]int, len(sm.ins))
	for i := range sm.ins {
		mis[i] = sm.ins[i].match
	}
	sort.Ints(mis)
	for _, mi := range mis[sm.k/2+1:] {
		if sm.log[mi].Term == sm.term {
			return mi
		}
	}

	return -1
}

// 更新已提交的日志索引
func (sm *stateMachine) maybeAdvanceCommit() int {
	ci := sm.theN()
	if ci > sm.commit {
		sm.commit = ci
	}
	return sm.commit
}

func (sm *stateMachine) reset() {
	sm.lead = none
	sm.vote = none
	sm.votes = make(map[int]bool)
	sm.ins = make([]*index, sm.k)
	for i := range sm.ins {
		sm.ins[i] = &index{next: len(sm.log)}
	}
}

// 计算多数所需的节点数
func (sm *stateMachine) q() int {
	return sm.k/2 + 1
}

// 判断投票请求是否值得投票
func (sm *stateMachine) voteWorthy(i, term int) bool {
	e := sm.log[sm.li()]
	return term > e.Term || (term == e.Term && i >= sm.li())
}

// 获取最后一个日志条目的索引
func (sm *stateMachine) li() int {
	return len(sm.log) - 1
}

func (sm *stateMachine) becomeFollower(term, lead int) {
	sm.reset()
	sm.term = term
	sm.lead = lead
	sm.state = stateFollower
}

func (sm *stateMachine) Step(m Message) {
	switch m.Type {
	case msgHup:
		sm.term++
		sm.reset()
		sm.state = stateCandidate
		sm.vote = sm.addr
		sm.poll(sm.addr, true)
		for i := 0; i < sm.k; i++ {
			if i == sm.addr {
				continue
			}
			lasti := sm.li()
			sm.send(Message{To: i, Type: msgVote, Index: lasti, LogTerm: sm.log[lasti].Term})
		}
		return
	case msgProp:
		switch sm.lead {
		case sm.addr:
			sm.append(sm.li(), Entry{Term: sm.term, Data: m.Data})
			sm.sendAppend()
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

	handleAppendEntries := func() {
		if sm.isLogOk(m.Index, m.LogTerm) {
			sm.append(m.Index, m.Entries...)
			sm.send(Message{To: m.From, Type: msgAppResp, Index: sm.li()})
		} else {
			sm.send(Message{To: m.From, Type: msgAppResp, Index: -1})
		}
	}

	switch sm.state {
	case stateLeader:
		switch m.Type {
		case msgAppResp:
			in := sm.ins[m.From]
			if m.Index < 0 {
				in.decr()
				sm.sendAppend()
			} else {
				in.update(m.Index)
			}
		}
	case stateCandidate:
		switch m.Type {
		case msgApp:
			sm.becomeFollower(sm.term, m.From)
			handleAppendEntries()
		case msgVoteResp:
			gr := sm.poll(m.From, m.Index >= 0)
			switch sm.q() {
			case gr:
				sm.state = stateLeader
				sm.lead = sm.addr
				sm.sendAppend()
			case len(sm.votes) - gr:
				sm.state = stateFollower
			}
		}
	case stateFollower:
		switch m.Type {
		case msgApp:
			handleAppendEntries()
		case msgVote:
			if sm.voteWorthy(m.Index, m.LogTerm) {
				sm.send(Message{To: m.From, Type: msgVoteResp, Index: sm.li()})
			} else {
				sm.send(Message{To: m.From, Type: msgVoteResp, Index: -1})
			}
		}
	}
}
