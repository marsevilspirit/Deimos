package wal

import (
	"io"
)

func writeRecord(w io.Writer, rec *Record) error {
	data, err := rec.Marshal()
	if err != nil {
		return err
	}
	if err := writeInt64(w, int64(len(data))); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readRecord(r io.Reader, rec *Record) error {
	rec.Reset()
	len, err := readInt64(r)
	if err != nil {
		return err
	}
	data := make([]byte, len)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return rec.Unmarshal(data)
}
