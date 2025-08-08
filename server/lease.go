package server

// LeaseReader interface for lease-based read operations
type LeaseReader interface {
	// CanReadWithLease checks if the server can serve reads with lease
	CanReadWithLease() bool

	// HasValidLease checks if the server has a valid leader lease
	HasValidLease() bool
}

// LeaseReadMode represents different read modes
type LeaseReadMode int

const (
	// ReadModeConsensus requires consensus for all reads (QGET behavior)
	ReadModeConsensus LeaseReadMode = iota

	// ReadModeLease allows lease-based reads for GET requests
	ReadModeLease

	// ReadModeLocal allows local reads without any consistency guarantee
	ReadModeLocal
)
