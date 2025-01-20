package raft

import "fmt"

const (
	Normal int64 = iota

	ClusterInit
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
	Type  int64
	Term  int64
	Index int64
	Data  []byte
}

func (e *Entry) isConfig() bool {
	return e.Type == AddNode || e.Type == RemoveNode
}

type raftLog struct {
	ents      []Entry
	unstable  int64
	committed int64
	applied   int64
	offset    int64

	// want a compact after the number of log entries
	// exceeds the compact threshold
	compactThreshold int64
}

func newLog() *raftLog {
	return &raftLog{
		ents:             make([]Entry, 1),
		unstable:         1,
		committed:        0,
		applied:          0,
		compactThreshold: defaultCompactThreshold,
	}
}

func (l *raftLog) unstableEnts() []Entry {
	ents := l.entries(l.unstable)
	l.unstable = l.lastIndex() + 1
	return ents
}

func (l *raftLog) lastIndex() int64 {
	return int64(len(l.ents)) - 1 + l.offset
}

func (l *raftLog) append(after int64, ents ...Entry) int64 {
	l.ents = append(l.slice(l.offset, after+1), ents...)
	l.unstable = min(l.unstable, after+1)
	return l.lastIndex()
}

func (l *raftLog) term(i int64) int64 {
	if e := l.at(i); e != nil {
		return e.Term
	}
	return -1
}

func (l *raftLog) findConflict(from int64, ents []Entry) int64 {
	for i, ne := range ents {
		if oe := l.at(from + int64(i)); oe == nil || oe.Term != ne.Term {
			return from + int64(i)
		}
	}
	return -1
}

func (l *raftLog) entries(i int64) []Entry {
	// never send out the first entry
	// first entry is only used for matching
	// prevLogTerm
	if i == l.offset {
		panic("cannot return the first entry in log")
	}

	return l.slice(i, l.lastIndex()+1)
}

func (l *raftLog) matchTerm(index, term int64) bool {
	if e := l.at(index); e != nil {
		return e.Term == term
	}
	return false
}

func (l *raftLog) isEmpty() bool {
	return l.offset == 0 && len(l.ents) == 1
}

func (l *raftLog) String() string {
	return fmt.Sprintf("offset=%d, committed=%d, applied=%d, len(ents)=%d", l.offset, l.committed, l.applied, len(l.ents))
}

func (l *raftLog) maybeAppend(index, logTerm, committed int64, ents ...Entry) bool {
	if l.matchTerm(index, logTerm) {
		from := index + 1
		ci := l.findConflict(from, ents)
		switch {
		case ci == -1:
		case ci <= l.committed:
			panic("conflict with committed entry")
		default:
			l.append(ci-1, ents[ci-from:]...)
		}
		if l.committed < committed {
			l.committed = min(committed, l.lastIndex())
		}
		return true
	}
	return false
}

func (l *raftLog) isUpToDate(index, term int64) bool {
	entry := l.at(l.lastIndex())
	return term > entry.Term || (term == entry.Term && index >= l.lastIndex())
}

func (l *raftLog) maybeCommit(maxIndex, term int64) bool {
	if maxIndex > l.committed && l.matchTerm(maxIndex, term) {
		l.committed = maxIndex
		return true
	}
	return false
}

// nextEnts returns all the avaliable entries for execution.
// all the returned entries will be marked as applied.
func (l *raftLog) nextEnts() (ents []Entry) {
	if l.committed > l.applied {
		ents = l.slice(l.applied+1, l.committed+1)
		l.applied = l.committed
	}
	return ents
}

// return the number of emtries after the compaction
func (l *raftLog) compact(i int64) int64 {
	if l.isOutOfBounds(i) {
		panic(fmt.Sprintf("compact %d out of bounds [%d:%d]", i, l.offset, l.lastIndex()))
	}
	l.ents = l.slice(i, l.lastIndex()+1)
	l.unstable = max(i+1, l.unstable)
	l.offset = i
	return int64(len(l.ents))
}

func (l *raftLog) shouldCompact() bool {
	return (l.applied - l.offset) > l.compactThreshold
}

func (l *raftLog) restore(index, term int64) {
	l.ents = []Entry{{Term: term}}
	l.unstable = index + 1
	l.committed = index
	l.applied = index
	l.offset = index
}

func (l *raftLog) at(i int64) *Entry {
	if l.isOutOfBounds(i) {
		return nil
	}
	return &l.ents[i-l.offset]
}

// lo(low) is the index of the first possible entry.
// hi(hight) is the index of the last possible entry + 1.
// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *raftLog) slice(lo, hi int64) []Entry {
	if lo >= hi {
		return nil
	}
	if l.isOutOfBounds(lo) || l.isOutOfBounds(hi-1) {
		return nil
	}
	return l.ents[lo-l.offset : hi-l.offset]
}

// isOutOfBounds returns if the given index is out of the bound.
func (l *raftLog) isOutOfBounds(index int64) bool {
	return index < l.offset || index > l.lastIndex()
}

func min(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
