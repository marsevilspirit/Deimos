package raft

type Entry struct {
	Term int
	Data []byte
}

type log struct {
	ents    []Entry
	commit  int
	applied int
}

func newLog() *log {
	return &log{
		ents:    make([]Entry, 1, 1024),
		commit:  0,
		applied: 0,
	}
}

func (l *log) append(after int, ents ...Entry) int {
	l.ents = append(l.ents[:after+1], ents...)
	return len(l.ents) - 1
}

func (l *log) len() int {
	return len(l.ents) - 1
}

func (l *log) term(i int) int {
	if i > l.len() {
		return -1
	}
	return l.ents[i].Term
}

func (l *log) entries(i int) []Entry {
	if i > l.len() {
		return nil
	}
	return l.ents[i:]
}

func (l *log) isOk(index, term int) bool {
	if index > l.len() {
		return false
	}
	return l.term(index) == term
}

func (l *log) mayAppend(index, logTerm int, ents ...Entry) bool {
	if l.isOk(index, logTerm) {
		l.append(index, ents...)
		return true
	}
	return false
}

func (l *log) isUpToDate(index, term int) bool {
	entry := l.ents[l.len()]
	return term > entry.Term || (term == entry.Term && index >= l.len())
}

func (l *log) nextEnts() (ents []Entry) {
	if l.commit > l.applied {
		ents = l.ents[l.applied+1 : l.commit+1]
		l.applied = l.commit
	}
	return ents
}
