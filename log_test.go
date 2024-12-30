package raft

import (
	"reflect"
	"testing"
)

// TestCompactionSideEffects ensures that all the log related funcationality works correctly after
// a compaction.
func TestCompactionSideEffects(t *testing.T) {
	lastIndex := 1000
	log := newLog()
	for i := 0; i < lastIndex; i++ {
		log.append(i, Entry{Term: i + 1})
	}
	log.compact(500)
	if log.lastIndex() != lastIndex {
		t.Errorf("lastIndex = %d, want %d", log.lastIndex(), lastIndex)
	}
	for i := log.offset; i <= log.lastIndex(); i++ {
		if log.term(i) != i {
			t.Errorf("term(%d) = %d, want %d", i, log.term(i), i)
		}
	}
	for i := log.offset; i <= log.lastIndex(); i++ {
		if !log.matchTerm(i, i) {
			t.Errorf("matchTerm(%d) = false, want true", i)
		}
	}
	prev := log.lastIndex()
	log.append(log.lastIndex(), Entry{Term: log.lastIndex() + 1})
	if log.lastIndex() != prev+1 {
		t.Errorf("lastIndex = %d, want = %d", log.lastIndex(), prev+1)
	}
	ents := log.entries(log.lastIndex())
	if len(ents) != 1 {
		t.Errorf("len(entries) = %d, want = %d", len(ents), 1)
	}
}

func TestCompaction(t *testing.T) {
	tests := []struct {
		app     int
		compact []int
		wleft   []int
		wallow  bool
	}{
		{1000, []int{1001}, []int{-1}, false},
		{1000, []int{300, 500, 800, 900}, []int{701, 501, 201, 101}, true},
		{1000, []int{300, 299}, []int{701, -1}, false},
	}

	for i, tt := range tests {
		defer func() {
			if r := recover(); r != nil {
				if tt.wallow == true {
					t.Errorf("#%d: allow = %v, want %v", i, false, tt.wallow)
				}
			}
		}()

		log := newLog()
		for i := 0; i < tt.app; i++ {
			log.append(i, Entry{})
		}

		for j := 0; j < len(tt.compact); j++ {
			log.compact(tt.compact[j])
			if len(log.ents) != tt.wleft[j] {
				t.Errorf("#%d.%d: left = %d, want %d", i, j, len(log.ents), tt.wleft[j])
			}
		}
	}
}

func TestIsOutOfBounds(t *testing.T) {
	offset := 100
	num := 100
	// from 100 to 199 is valid
	l := &log{offset: offset, ents: make([]Entry, num)}

	tests := []struct {
		index int
		w     bool
	}{
		{offset - 1, true},
		{offset, false},
		{offset + num - 1, false},
		{offset + num, true},
		{offset + num/2, false},
	}

	for i, tt := range tests {
		if g := l.isOutOfBounds(tt.index); g != tt.w {
			t.Errorf("#%d: isOutOfBounds=%v, want %v", i, g, tt.w)
		}
	}
}

func TestAt(t *testing.T) {
	offset := 100
	num := 100
	// from 100 to 199 is valid
	l := &log{offset: offset}
	for i := 0; i < num; i++ {
		l.ents = append(l.ents, Entry{Term: i})
	}

	tests := []struct {
		index int
		w     *Entry
	}{
		{offset - 1, nil},
		{offset, &Entry{Term: 0}},
		{offset + num - 1, &Entry{Term: num - 1}},
		{offset + num, nil},
		{offset + num/2, &Entry{Term: num / 2}},
	}

	for i, tt := range tests {
		if g := l.at(tt.index); !reflect.DeepEqual(g, tt.w) {
			t.Errorf("#%d: at=%v, want %v", i, g, tt.w)
		}
	}
}

func TestSlice(t *testing.T) {
	offset := 100
	num := 100
	// from 100 to 199 is valid
	l := &log{offset: offset}
	for i := 0; i < num; i++ {
		l.ents = append(l.ents, Entry{Term: i})
	}

	tests := []struct {
		from, to int
		w        []Entry
	}{
		{offset - 1, offset + 1, nil},
		{offset, offset + 1, []Entry{{Term: 0}}},
		{offset + num/2, offset + num/2 + 1, []Entry{{Term: num / 2}}},
		{offset + num - 1, offset + num, []Entry{{Term: num - 1}}},
		{offset + num, offset + num + 1, nil},
		{offset + num/2, offset + num/2, nil},
		{offset + num/2, offset + num/2 - 1, nil},
	}

	for i, tt := range tests {
		if g := l.slice(tt.from, tt.to); !reflect.DeepEqual(g, tt.w) {
			t.Errorf("#%d: slice=%v, want %v", i, g, tt.w)
		}
	}
}
