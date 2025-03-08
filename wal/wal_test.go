package wal

import (
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/marsevilspirit/marstore/raft/raftpb"
)

var (
	infoData   = []byte("\b\xef\xfd\x02")
	infoRecord = append([]byte("\n\x00\x00\x00\x00\x00\x00\x00\b\x01\x10\x00\x1a\x04"), infoData...)

	stateData   = []byte("\b\x01\x10\x01\x18\x01")
	stateRecord = append([]byte("\f\x00\x00\x00\x00\x00\x00\x00\b\x03\x10\x00\x1a\x06"), stateData...)

	entryData   = []byte("\b\x01\x10\x01\x18\x01\x22\x01\x01")
	entryRecord = append([]byte("\x0f\x00\x00\x00\x00\x00\x00\x00\b\x02\x10\x00\x1a\t"), entryData...)
)

func TestNew(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "waltest")
	if err != nil {
		t.Fatal(err)
	}
	p := f.Name()
	_, err = New(p)
	if err == nil || err != os.ErrExist {
		t.Errorf("err = %v, want %v", err, os.ErrExist)
	}
	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(p)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	w.Close()
	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSaveEntry(t *testing.T) {
	p := path.Join(os.TempDir(), "waltest")
	w, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	e := &raftpb.Entry{
		Type:  1,
		Term:  1,
		Index: 1,
		Data:  []byte{1},
	}
	err = w.SaveEntry(e)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	b, err := ioutil.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, entryRecord) {
		t.Errorf("ent = %q,\n            want  %q", b, entryRecord)
	}

	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSaveInfo(t *testing.T) {
	p := path.Join(os.TempDir(), "waltest")
	w, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	i := &raftpb.Info{Id: int64(0xBEEF)}
	err = w.SaveInfo(i)
	if err != nil {
		t.Fatal(err)
	}

	// make sure we can only write info at the head of the wal file
	// still in buffer
	err = w.SaveInfo(i)
	if err == nil || err.Error() != "cannot write info at 18, expect 0" {
		t.Errorf("err = %v, want cannot write info at 8, expect 0", err)
	}

	// sync to disk
	w.Sync()
	err = w.SaveInfo(i)
	if err == nil || err.Error() != "cannot write info at 18, expect 0" {
		t.Errorf("err = %v, want cannot write info at 8, expect 0", err)
	}
	w.Close()

	b, err := ioutil.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, infoRecord) {
		t.Errorf("ent = %q, want %q", b, infoRecord)
	}

	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSaveState(t *testing.T) {
	p := path.Join(os.TempDir(), "waltest")
	w, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	st := &raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	err = w.SaveState(st)
	if err != nil {
		t.Fatal(err)
	}
	w.Close()

	b, err := ioutil.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(b, stateRecord) {
		t.Errorf("ent = %q,\n              want %q", b, stateRecord)
	}

	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoadInfo(t *testing.T) {
	i, err := loadInfo(infoData)
	if err != nil {
		t.Fatal(err)
	}
	if i.Id != 0xBEEF {
		t.Errorf("id = %x, want 0xBEEF", i)
	}
}

func TestLoadEntry(t *testing.T) {
	e, err := loadEntry(entryData)
	if err != nil {
		t.Fatal(err)
	}
	we := raftpb.Entry{
		Type:  1,
		Term:  1,
		Index: 1,
		Data:  []byte{1},
	}
	if !reflect.DeepEqual(e, we) {
		t.Errorf("ent = %v, want %v", e, we)
	}
}

func TestLoadState(t *testing.T) {
	s, err := loadState(stateData)
	if err != nil {
		t.Fatal(err)
	}
	ws := raftpb.HardState{
		Term:   1,
		Vote:   1,
		Commit: 1,
	}
	if !reflect.DeepEqual(s, ws) {
		t.Errorf("state = %v, want %v", s, ws)
	}
}

func TestLoadNode(t *testing.T) {
	p := path.Join(os.TempDir(), "waltest")
	w, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	i := &raftpb.Info{Id: int64(0xBEEF)}
	if err = w.SaveInfo(i); err != nil {
		t.Fatal(err)
	}
	ents := []raftpb.Entry{
		{
			Type:  1,
			Term:  1,
			Index: 1,
			Data:  []byte{1},
		},
		{
			Type:  2,
			Term:  2,
			Index: 2,
			Data:  []byte{2},
		},
	}
	for _, e := range ents {
		if err = w.SaveEntry(&e); err != nil {
			t.Fatal(err)
		}
	}
	sts := []raftpb.HardState{
		{
			Term:   1,
			Vote:   1,
			Commit: 1,
		},
		{
			Term:   2,
			Vote:   2,
			Commit: 2,
		},
	}
	for _, s := range sts {
		if err = w.SaveState(&s); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()

	w, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	n, err := w.LoadNode()
	if err != nil {
		t.Fatal(err)
	}
	if n.Id != i.Id {
		t.Errorf("id = %d, want %d", n.Id, i)
	}
	if !reflect.DeepEqual(n.Ents, ents) {
		t.Errorf("ents = %+v, want %+v", n.Ents, ents)
	}
	// only the latest state is recorded
	s := sts[len(sts)-1]
	if !reflect.DeepEqual(n.State, s) {
		t.Errorf("state = %+v, want %+v", n.State, s)
	}

	err = os.Remove(p)
	if err != nil {
		t.Fatal(err)
	}
}
