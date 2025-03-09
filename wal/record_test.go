package wal

import (
	"bytes"
	"hash/crc32"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestReadRecord(t *testing.T) {
	badInfoRecord := make([]byte, len(infoRecord))
	copy(badInfoRecord, infoRecord)
	badInfoRecord[len(badInfoRecord)-1] = 'a'

	tests := []struct {
		data []byte
		wr   *Record
		we   error
	}{
		{infoRecord, &Record{Type: 1, Crc: crc32.Checksum(infoData, crcTable), Data: infoData}, nil},
		{[]byte(""), &Record{}, io.EOF},
		{infoRecord[:len(infoRecord)-len(infoData)-8], &Record{}, io.ErrUnexpectedEOF},
		{infoRecord[:len(infoRecord)-len(infoData)], &Record{}, io.ErrUnexpectedEOF},
		{infoRecord[:len(infoRecord)-8], &Record{}, io.ErrUnexpectedEOF},
		{badInfoRecord, &Record{}, ErrCRCMismatch},
	}

	rec := &Record{}
	for i, tt := range tests {
		buf := bytes.NewBuffer(tt.data)
		decoder := newDecoder(ioutil.NopCloser(buf))
		e := decoder.decode(rec)
		if !reflect.DeepEqual(rec, tt.wr) {
			t.Errorf("#%d: block = %v, want %v", i, rec, tt.wr)
		}
		if !reflect.DeepEqual(e, tt.we) {
			t.Errorf("#%d: err = %v, want %v", i, e, tt.we)
		}
		rec = &Record{}
	}
}

func TestWriteRecord(t *testing.T) {
	b := &Record{}
	typ := int64(0xABCD)
	d := []byte("Hello world!")
	buf := new(bytes.Buffer)
	e := newEncoder(buf, 0)
	e.encode(&Record{Type: typ, Data: d})
	e.flush()
	decoder := newDecoder(ioutil.NopCloser(buf))
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
