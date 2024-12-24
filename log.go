package raft

type Entry struct {
	Term int
	Data []byte
}

type log struct {
	ents      []Entry
	committed int
	applied   int
}

func newLog() *log {
	return &log{
		ents:      make([]Entry, 1),
		committed: 0,
		applied:   0,
	}
}

func (l *log) lastIndex() int {
	return len(l.ents) - 1
}

func (l *log) append(after int, ents ...Entry) int {
	l.ents = append(l.ents[:after+1], ents...)
	return l.lastIndex()
}

func (l *log) term(i int) int {
	if i > l.lastIndex() {
		return -1
	}
	return l.ents[i].Term
}

func (l *log) entries(i int) []Entry {
	if i > l.lastIndex() {
		return nil
	}
	return l.ents[i:]
}

func (l *log) matchTerm(index, term int) bool {
	if index > l.lastIndex() {
		return false
	}
	return l.term(index) == term
}

func (l *log) mayAppend(index, logTerm int, commit int, ents ...Entry) bool {
	if l.matchTerm(index, logTerm) {
		l.append(index, ents...)
		l.committed = commit
		return true
	}
	return false
}

func (l *log) isUpToDate(index, term int) bool {
	entry := l.ents[l.lastIndex()]
	return term > entry.Term || (term == entry.Term && index >= l.lastIndex())
}

func (l *log) maybeCommit(maxIndex, term int) bool {
	if maxIndex > l.committed && l.matchTerm(maxIndex, term) {
		l.committed = maxIndex
		return true
	}
	return false
}

// nextEnts returns all the avaliable entries for execution.
// all the returned entries will be marked as applied.
func (l *log) nextEnts() (ents []Entry) {
	if l.committed > l.applied {
		ents = l.ents[l.applied+1 : l.committed+1]
		l.applied = l.committed
	}
	return ents
}
