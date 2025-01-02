package raft

import "fmt"

const (
	Normal int64 = iota

	AddNode
	RemoveNode
)

const (
	// when the size of the raft log becomes
	// larger than the compact threshold,
	// the log will be truncated by a compaction.
	defaultCompactThreshold = 10000
)

type Entry struct {
	Type int64
	Term int64
	Data []byte
}

func (e *Entry) isConfig() bool {
	return e.Type == AddNode || e.Type == RemoveNode
}

type log struct {
	ents      []Entry
	committed int64
	applied   int64
	offset    int64

	// want a compact after the number of log entries
	// exceeds the compact threshold
	compactThreshold int64
}

func newLog() *log {
	return &log{
		ents:             make([]Entry, 1),
		committed:        0,
		applied:          0,
		compactThreshold: defaultCompactThreshold,
	}
}

func (l *log) lastIndex() int64 {
	return int64(len(l.ents)) - 1 + l.offset
}

func (l *log) append(after int64, ents ...Entry) int64 {
	l.ents = append(l.slice(l.offset, after+1), ents...)
	return l.lastIndex()
}

func (l *log) term(i int64) int64 {
	if e := l.at(i); e != nil {
		return e.Term
	}
	return -1
}

func (l *log) entries(i int64) []Entry {
	// never send out the first entry
	// first entry is only used for matching
	// prevLogTerm
	if i == l.offset {
		panic("cannot return the first entry in log")
	}

	return l.slice(i, l.lastIndex()+1)
}

func (l *log) matchTerm(index, term int64) bool {
	if e := l.at(index); e != nil {
		return e.Term == term
	}
	return false
}

func (l *log) maybeAppend(index, logTerm, committed int64, ents ...Entry) bool {
	if l.matchTerm(index, logTerm) {
		l.append(index, ents...)
		l.committed = committed
		return true
	}
	return false
}

func (l *log) isUpToDate(index, term int64) bool {
	entry := l.at(l.lastIndex())
	return term > entry.Term || (term == entry.Term && index >= l.lastIndex())
}

func (l *log) maybeCommit(maxIndex, term int64) bool {
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
		ents = l.slice(l.applied+1, l.committed+1)
		l.applied = l.committed
	}
	return ents
}

// return the number of emtries after the compaction
func (l *log) compact(i int64) int64 {
	if l.isOutOfBounds(i) {
		panic(fmt.Sprintf("compact %d out of bounds [%d:%d]", i, l.offset, l.lastIndex()))
	}
	l.ents = l.slice(i, l.lastIndex()+1)
	l.offset = i
	return int64(len(l.ents))
}

func (l *log) shouldCompact() bool {
	return (l.applied - l.offset) > l.compactThreshold
}

func (l *log) restore(index, term int64) {
	l.ents = []Entry{{Term: term}}
	l.committed = index
	l.applied = index
	l.offset = index
}

func (l *log) at(i int64) *Entry {
	if l.isOutOfBounds(i) {
		return nil
	}
	return &l.ents[i-l.offset]
}

// lo(low) is the index of the first possible entry.
// hi(hight) is the index of the last possible entry + 1.
// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *log) slice(lo, hi int64) []Entry {
	if lo >= hi {
		return nil
	}
	if l.isOutOfBounds(lo) || l.isOutOfBounds(hi-1) {
		return nil
	}
	return l.ents[lo-l.offset : hi-l.offset]
}

// isOutOfBounds returns if the given index is out of the bound.
func (l *log) isOutOfBounds(index int64) bool {
	return index < l.offset || index > l.lastIndex()
}
