package raft

import (
	"reflect"
	"testing"
)

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
