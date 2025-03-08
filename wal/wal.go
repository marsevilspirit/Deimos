package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/marsevilspirit/marstore/raft/raftpb"
)

var (
	infoType  = int64(1)
	entryType = int64(2)
	stateType = int64(3)
)

type WAL struct {
	f   *os.File
	bw  *bufio.Writer
	buf *bytes.Buffer
}

func newWAL(f *os.File) *WAL {
	return &WAL{f, bufio.NewWriter(f), new(bytes.Buffer)}
}

func New(path string) (*WAL, error) {
	f, err := os.Open(path)
	if err == nil {
		f.Close()
		return nil, os.ErrExist
	}
	f, err = os.Create(path)
	if err != nil {
		return nil, err
	}
	return newWAL(f), nil
}

func Open(path string) (*WAL, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return newWAL(f), nil
}

func (w *WAL) Close() {
	if w.f != nil {
		w.Flush()
		w.f.Close()
	}
}

// | 8 bytes | 8 bytes | 8 bytes |
// |  type   |   len   |  nodeid |
func (w *WAL) SaveInfo(id int64) error {
	if err := w.checkAtHead(); err != nil {
		return err
	}
	w.buf.Reset()
	err := binary.Write(w.buf, binary.LittleEndian, id)
	if err != nil {
		panic(err)
	}
	return writeBlock(w.bw, infoType, w.buf.Bytes())
}

// | 8 bytes | 8 bytes |  variable length |
// | type    |   len   |   entry data     |
func (w *WAL) SaveEntry(e *raftpb.Entry) error {
	// protobuf?
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return writeBlock(w.bw, entryType, b)
}

func (w *WAL) SaveState(s *raftpb.HardState) error {
	// | 8 bytes | 8 bytes |  24 bytes |
	// | type    |   len   |   state   |
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return writeBlock(w.bw, stateType, b)
}

func (w *WAL) Flush() error {
	return w.bw.Flush()
}

func (w *WAL) checkAtHead() error {
	o, err := w.f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if o != 0 || w.bw.Buffered() != 0 {
		return fmt.Errorf("cannot write info at %d, expect 0", max(o, int64(w.bw.Buffered())))
	}
	return nil
}

type Node struct {
	Id    int64
	Ents  []raftpb.Entry
	State raftpb.HardState
}

func (w *WAL) LoadNode() (*Node, error) {
	if err := w.checkAtHead(); err != nil {
		return nil, err
	}

	br := bufio.NewReader(w.f)
	b := &block{}

	err := readBlock(br, b)
	if err != nil {
		return nil, err
	}
	if b.typ != infoType {
		return nil, fmt.Errorf("the first block of wal is not infoType but %d", b.typ)
	}

	id, err := loadInfo(b.data)
	if err != nil {
		return nil, err
	}

	ents := make([]raftpb.Entry, 0)
	var state raftpb.HardState
	for err := readBlock(br, b); err == nil; err = readBlock(br, b) {
		switch b.typ {
		case entryType:
			e, err := loadEntry(b.data)
			if err != nil {
				return nil, err
			}
			ents = append(ents, e)
		case stateType:
			s, err := loadState(b.data)
			if err != nil {
				return nil, err
			}
			state = s
		default:
			return nil, fmt.Errorf("unexpected block type %d", b.typ)
		}
	}
	return &Node{id, ents, state}, nil
}

func loadInfo(d []byte) (int64, error) {
	if len(d) != 8 {
		return 0, fmt.Errorf("len = %d, want 8", len(d))
	}
	buf := bytes.NewBuffer(d)
	return readInt64(buf)
}

func loadEntry(d []byte) (raftpb.Entry, error) {
	var e raftpb.Entry
	err := json.Unmarshal(d, &e)
	return e, err
}

func loadState(d []byte) (raftpb.HardState, error) {
	var s raftpb.HardState
	err := json.Unmarshal(d, &s)
	return s, err
}

func writeInt64(w io.Writer, n int64) error {
	return binary.Write(w, binary.LittleEndian, n)
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func unexpectedEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
