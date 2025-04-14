package wal

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path"
	"sort"

	"github.com/marsevilspirit/deimos/raft"
	"github.com/marsevilspirit/deimos/raft/raftpb"
	"github.com/marsevilspirit/deimos/wal/walpb"
)

const (
	infoType int64 = iota + 1
	entryType
	stateType
	crcType

	// the owner can make/remove files inside the directory
	privateDirMode = 0700
)

var (
	ErrIDMismatch    = errors.New("wal: unmatch id")
	ErrFileNotFound  = errors.New("wal: file not found")
	ErrIndexNotFound = errors.New("wal: index not found in file")
	ErrCRCMismatch   = errors.New("walpb: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical repersentation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
	dir string // the living directory of the underlay files

	ri      int64    // index of entry to start reading
	decoder *decoder // decoder to decode records

	f       *os.File // underlay file opened for appending, sync
	seq     int64    // sequence of the wal file currently used for writes
	enti    int64    // index of the last entry that has been saved to the wal
	encoder *encoder // encoder to encode records
}

// Create creates a WAL ready for appending records.
// The index of first record saved MUST be 0.
func Create(dirpath string) (*WAL, error) {
	log.Printf("path=%s wal.create", dirpath)
	if Exist(dirpath) {
		return nil, os.ErrExist
	}

	if err := os.MkdirAll(dirpath, privateDirMode); err != nil {
		return nil, err
	}

	p := path.Join(dirpath, walName(0, 0))
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		dir:     dirpath,
		seq:     0,
		f:       f,
		encoder: newEncoder(f, 0),
	}
	if err := w.saveCrc(0); err != nil {
		return nil, err
	}
	return w, nil
}

// OpenAtIndex opens the WAL files containing all the entries after
// the given index.
// The index SHOULD have been previously committed to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read.
// The WAL cannot be appended to before
// reading out all of its previous records.
func OpenAtIndex(dirpath string, index int64) (*WAL, error) {
	log.Printf("path=%s wal.load index=%d", dirpath, index)
	names, err := readDir(dirpath)
	if err != nil {
		return nil, err
	}
	names = checkWalNames(names)
	if len(names) == 0 {
		return nil, ErrFileNotFound
	}

	sort.Sort(sort.StringSlice(names))

	nameIndex, ok := searchIndex(names, index)
	if !ok || !isValidSeq(names[nameIndex:]) {
		return nil, ErrFileNotFound
	}

	// open the wal files for reading
	rcs := make([]io.ReadCloser, 0)
	for _, name := range names[nameIndex:] {
		f, err := os.Open(path.Join(dirpath, name))
		if err != nil {
			return nil, err
		}
		rcs = append(rcs, f)
	}
	rc := MultiReadCloser(rcs...)

	// open the lastest wal file for appending
	seq, _, err := parseWalName(names[len(names)-1])
	if err != nil {
		rc.Close()
		return nil, err
	}
	last := path.Join(dirpath, names[len(names)-1])
	f, err := os.OpenFile(last, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		rc.Close()
		return nil, err
	}

	// create a WAL ready for reading
	w := &WAL{
		dir:     dirpath,
		ri:      index,
		decoder: newDecoder(rc),

		f:   f,
		seq: seq,
	}
	return w, nil
}

// ReadAll reads out all records of the current WAL.
// If it cannot read out the expected entry, it will return ErrNotFound.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (id int64, state raftpb.HardState, ents []raftpb.Entry, err error) {
	rec := &walpb.Record{}
	decoder := w.decoder

	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			if e.Index >= w.ri {
				ents = append(ents[:e.Index-w.ri], e)
			}
			w.enti = e.Index
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case infoType:
			i := mustUnmarshalInfo(rec.Data)
			if id != 0 && id != i.Id {
				state.Reset()
				return 0, state, nil, ErrIDMismatch
			}
			id = i.Id
		case crcType:
			crc := decoder.crc.Sum32()
			// current crc of decoder must match the crc of the record.
			// do no need to match 0 crc, since the decoder is a new one at this case.
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return 0, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		default:
			state.Reset()
			return 0, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}
	if err != io.EOF {
		state.Reset()
		return 0, state, nil, err
	}

	if w.enti < w.ri {
		state.Reset()
		return 0, state, nil, ErrIndexNotFound
	}

	// close decoder, disable reading
	w.decoder.close()
	w.ri = 0

	// create encoder (chain crc with the decoder), enable appending
	w.encoder = newEncoder(w.f, w.decoder.lastCRC())
	w.decoder = nil
	return id, state, ents, nil
}

// Cut closes current file written and creates a new one ready to append.
func (w *WAL) Cut() error {
	// create a new wal file with name sequence + 1
	fpath := path.Join(w.dir, walName(w.seq+1, w.enti+1))
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	w.Sync()
	w.f.Close()

	log.Printf("wal.cut index=%d prevfile=%s curfile=%s", w.enti, w.f.Name(), f.Name())

	// update writer and save the previous crc
	w.f = f
	w.seq++
	prevCrc := w.encoder.crc.Sum32()
	w.encoder = newEncoder(w.f, prevCrc)
	return w.saveCrc(prevCrc)
}

func (w *WAL) Sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	return w.f.Sync()
}

func (w *WAL) Close() {
	log.Printf("path=%s wal.close", w.f.Name())
	if w.f != nil {
		w.Sync()
		w.f.Close()
	}
}

func (w *WAL) SaveInfo(i *raftpb.Info) error {
	log.Printf("path=%s wal.saveInfo id=%d", w.f.Name(), i.Id)
	b, err := i.Marshal()
	if err != nil {
		panic(err)
	}
	rec := &walpb.Record{Type: infoType, Data: b}
	return w.encoder.encode(rec)
}

func (w *WAL) SaveEntry(e *raftpb.Entry) error {
	b, err := e.Marshal()
	if err != nil {
		panic(err)
	}
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

func (w *WAL) SaveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	log.Printf("path=%s wal.saveState state=\"%+v\"", w.f.Name(), s)
	b, err := s.Marshal()
	if err != nil {
		panic(err)
	}
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) {
	// TODO: no more reference operator
	w.SaveState(&st)
	for i := range ents {
		w.SaveEntry(&ents[i])
	}
	w.Sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}
