package wal

import (
	"fmt"
	"io"
)

type block struct {
	typ  int64
	data []byte
}

func writeBlock(w io.Writer, typ int64, data []byte) error {
	if err := writeInt64(w, typ); err != nil {
		return err
	}
	if err := writeInt64(w, int64(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readBlock(r io.Reader, b *block) error {
	typ, err := readInt64(r)
	if err != nil {
		return err
	}
	len, err := readInt64(r)
	if err != nil {
		return unexpectedEOF(err)
	}
	data := make([]byte, len)
	n, err := r.Read(data)
	if err != nil {
		return unexpectedEOF(err)
	}
	if n != int(len) {
		return fmt.Errorf("len(data) = %d, want %d", n, len)
	}
	b.typ = typ
	b.data = data
	return nil
}
