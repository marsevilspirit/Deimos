package store

// SaveGzip returns a gzip-compressed protobuf snapshot.
// Directly connects protobuf serialization and gzip compression
func (s *store) SaveGzip() ([]byte, error) {
	return buildSnapshotPBCompressed(s)
}

// RecoveryGzip restores store state from a gzip-compressed protobuf snapshot.
// Directly connects gzip decompression and protobuf deserialization
func (s *store) RecoveryGzip(data []byte) error {
	return applySnapshotPBCompressed(s, data)
}
