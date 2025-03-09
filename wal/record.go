package wal

func (rec *Record) validate(crc uint32) error {
	if rec.Crc == crc {
		return nil
	}
	rec.Reset()
	return ErrCRCMismatch
}
