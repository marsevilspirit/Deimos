package wal

import (
	"bytes"
	"hash/crc32"
	"io"
	"reflect"
	"testing"

	"github.com/marsevilspirit/deimos/wal/walpb"
)

func TestReadRecord(t *testing.T) {
	badInfoRecord := make([]byte, len(infoRecord))
	copy(badInfoRecord, infoRecord)
	badInfoRecord[len(badInfoRecord)-1] = 'a'

	tests := []struct {
		data []byte
		wr   *walpb.Record
		we   error
	}{
		{infoRecord, &walpb.Record{Type: 1, Crc: crc32.Checksum(infoData, crcTable), Data: infoData}, nil},
		{[]byte(""), &walpb.Record{}, io.EOF},
		{infoRecord[:len(infoRecord)-len(infoData)-8], &walpb.Record{}, io.ErrUnexpectedEOF},
		{infoRecord[:len(infoRecord)-len(infoData)], &walpb.Record{}, io.ErrUnexpectedEOF},
		{infoRecord[:len(infoRecord)-8], &walpb.Record{}, io.ErrUnexpectedEOF},
		{badInfoRecord, &walpb.Record{}, ErrCRCMismatch},
	}

	rec := &walpb.Record{}
	for i, tt := range tests {
		buf := bytes.NewBuffer(tt.data)
		decoder := newDecoder(io.NopCloser(buf))
		e := decoder.decode(rec)
		if !reflect.DeepEqual(rec, tt.wr) {
			t.Errorf("#%d: block = %v, want %v", i, rec, tt.wr)
		}
		if !reflect.DeepEqual(e, tt.we) {
			t.Errorf("#%d: err = %v, want %v", i, e, tt.we)
		}
		rec = &walpb.Record{}
	}
}

func TestWriteRecord(t *testing.T) {
	b := &walpb.Record{}
	typ := int64(0xABCD)
	d := []byte("Hello world!")
	buf := new(bytes.Buffer)
	e := newEncoder(buf, 0)
	_ = e.encode(&walpb.Record{Type: typ, Data: d})
	_ = e.flush()
	decoder := newDecoder(io.NopCloser(buf))
	err := decoder.decode(b)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if b.Type != typ {
		t.Errorf("type = %d, want %d", b.Type, typ)
	}
	if !reflect.DeepEqual(b.Data, d) {
		t.Errorf("data = %v, want %v", b.Data, d)
	}
}
