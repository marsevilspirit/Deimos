package wal

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/marsevilspirit/deimos/raft/raftpb"
)

var (
	infoData   = []byte("\b\xef\xfd\x02")
	infoRecord = append([]byte("\x0e\x00\x00\x00\x00\x00\x00\x00\b\x01\x10\x99\xb5\xe4\xd0\x03\x1a\x04"), infoData...)
)

func TestNew(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if g := path.Base(w.f.Name()); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	w.Close()
}

func TestNewForInitedDir(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	os.Create(path.Join(p, walName(0, 0)))
	if _, err = Create(p); err == nil || err != os.ErrExist {
		t.Errorf("err = %v, want %v", err, os.ErrExist)
	}
}

func TestOpenAtIndex(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	f, err := os.Create(path.Join(dir, walName(0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	w, err := OpenAtIndex(dir, 0)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if g := path.Base(w.f.Name()); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	if w.seq != 0 {
		t.Errorf("seq = %d, want %d", w.seq, 0)
	}
	w.Close()

	wname := walName(2, 10)
	f, err = os.Create(path.Join(dir, wname))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	w, err = OpenAtIndex(dir, 5)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if g := path.Base(w.f.Name()); g != wname {
		t.Errorf("name = %+v, want %+v", g, wname)
	}
	if w.seq != 2 {
		t.Errorf("seq = %d, want %d", w.seq, 2)
	}
	w.Close()

	emptydir, err := ioutil.TempDir(os.TempDir(), "waltestempty")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(emptydir)
	if _, err = OpenAtIndex(emptydir, 0); err != ErrNotFound {
		t.Errorf("err = %v, want %v", err, ErrNotFound)
	}
}

func TestCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	// TODO(unihorn): remove this when cut can operate on an empty file
	if err := w.SaveEntry(&raftpb.Entry{}); err != nil {
		t.Fatal(err)
	}
	if err := w.Cut(); err != nil {
		t.Fatal(err)
	}
	wname := walName(1, 1)
	if g := path.Base(w.f.Name()); g != wname {
		t.Errorf("name = %s, want %s", g, wname)
	}

	e := &raftpb.Entry{Index: 1, Term: 1, Data: []byte{1}}
	if err := w.SaveEntry(e); err != nil {
		t.Fatal(err)
	}
	if err := w.Cut(); err != nil {
		t.Fatal(err)
	}
	wname = walName(2, 2)
	if g := path.Base(w.f.Name()); g != wname {
		t.Errorf("name = %s, want %s", g, wname)
	}
}

func TestRecover(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	i := &raftpb.Info{Id: int64(0xBAD0)}
	if err = w.SaveInfo(i); err != nil {
		t.Fatal(err)
	}
	ents := []raftpb.Entry{{Index: 0, Term: 0}, {Index: 1, Term: 1, Data: []byte{1}}, {Index: 2, Term: 2, Data: []byte{2}}}
	for _, e := range ents {
		if err = w.SaveEntry(&e); err != nil {
			t.Fatal(err)
		}
	}
	sts := []raftpb.HardState{{Term: 1, Vote: 1, Commit: 1}, {Term: 2, Vote: 2, Commit: 2}}
	for _, s := range sts {
		if err = w.SaveState(&s); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	if w, err = OpenAtIndex(p, 0); err != nil {
		t.Fatal(err)
	}
	id, state, entries, err := w.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if id != i.Id {
		t.Errorf("id = %d, want %d", id, i.Id)
	}
	if !reflect.DeepEqual(entries, ents) {
		t.Errorf("ents = %+v, want %+v", entries, ents)
	}
	// only the latest state is recorded
	s := sts[len(sts)-1]
	if !reflect.DeepEqual(state, s) {
		t.Errorf("state = %+v, want %+v", state, s)
	}
}

func TestSearchIndex(t *testing.T) {
	tests := []struct {
		names []string
		index int64
		widx  int
		wok   bool
	}{
		{
			[]string{
				"0000000000000000-0000000000000000.wal",
				"0000000000000001-0000000000001000.wal",
				"0000000000000002-0000000000002000.wal",
			},
			0x1000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000004000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x4000, 1, true,
		},
		{
			[]string{
				"0000000000000001-0000000000002000.wal",
				"0000000000000002-0000000000003000.wal",
				"0000000000000003-0000000000005000.wal",
			},
			0x1000, -1, false,
		},
	}
	for i, tt := range tests {
		idx, ok := searchIndex(tt.names, tt.index)
		if idx != tt.widx {
			t.Errorf("#%d: idx = %d, want %d", i, idx, tt.widx)
		}
		if ok != tt.wok {
			t.Errorf("#%d: ok = %v, want %v", i, ok, tt.wok)
		}
	}
}

func TestScanWalName(t *testing.T) {
	tests := []struct {
		str          string
		wseq, windex int64
		wok          bool
	}{
		{"0000000000000000-0000000000000000.wal", 0, 0, true},
		{"0000000000000000.wal", 0, 0, false},
		{"0000000000000000-0000000000000000.snap", 0, 0, false},
	}
	for i, tt := range tests {
		s, index, err := parseWalName(tt.str)
		if g := err == nil; g != tt.wok {
			t.Errorf("#%d: ok = %v, want %v", i, g, tt.wok)
		}
		if s != tt.wseq {
			t.Errorf("#%d: seq = %d, want %d", i, s, tt.wseq)
		}
		if index != tt.windex {
			t.Errorf("#%d: index = %d, want %d", i, index, tt.windex)
		}
	}
}

func TestRecoverAfterCut(t *testing.T) {
	p, err := ioutil.TempDir(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(p)

	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	info := &raftpb.Info{Id: int64(0xBAD1)}
	if err = w.SaveInfo(info); err != nil {
		t.Fatal(err)
	}
	// TODO(unihorn): remove this when cut can operate on an empty file
	if err = w.SaveEntry(&raftpb.Entry{}); err != nil {
		t.Fatal(err)
	}
	if err = w.Cut(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 10; i++ {
		e := raftpb.Entry{Index: int64(i)}
		if err = w.SaveEntry(&e); err != nil {
			t.Fatal(err)
		}
		if err = w.Cut(); err != nil {
			t.Fatal(err)
		}
		if err = w.SaveInfo(info); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	if err := os.Remove(path.Join(p, walName(4, 4))); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		w, err := OpenAtIndex(p, int64(i))
		if err != nil {
			if i <= 4 {
				if err != ErrNotFound {
					t.Errorf("#%d: err = %v, want %v", i, err, ErrNotFound)
				}
			} else {
				t.Errorf("#%d: err = %v, want nil", i, err)
			}
			continue
		}
		id, _, entries, err := w.ReadAll()
		if err != nil {
			t.Errorf("#%d: err = %v, want nil", i, err)
			continue
		}
		if id != info.Id {
			t.Errorf("#%d: id = %d, want %d", i, id, info.Id)
		}
		for j, e := range entries {
			if e.Index != int64(j+i) {
				t.Errorf("#%d: ents[%d].Index = %+v, want %+v", i, j, e.Index, j+i)
			}
		}
	}
}

func TestSaveEmpty(t *testing.T) {
	var buf bytes.Buffer
	var est raftpb.HardState
	w := WAL{
		encoder: newEncoder(&buf, 0),
	}
	if err := w.SaveState(&est); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if len(buf.Bytes()) != 0 {
		t.Errorf("buf.Bytes = %d, want 0", len(buf.Bytes()))
	}
}
