package raft

// CanReadWithLease checks if reads can be served with lease
func (n *node) CanReadWithLease() bool {
	// We need to access the raft instance to check lease status
	// This is a bit tricky since the raft instance is only available in the run() goroutine
	// For now, we'll use a channel-based approach

	// TODO: This is a simplified implementation
	// In a production system, you might want to cache the lease status
	// or use atomic operations for better performance
	return false // Conservative default - always use consensus
}

// HasValidLease checks if this node has a valid leader lease
func (n *node) HasValidLease() bool {
	// Similar to CanReadWithLease, this needs access to the raft instance
	// For now, return false as a conservative default
	return false
}

// LeaseAwareNode extends the basic node with lease awareness
type LeaseAwareNode struct {
	*node
	raft *raft // Direct access to raft for lease queries
}

// NewLeaseAwareNode creates a lease-aware node wrapper
func NewLeaseAwareNode(n *node, r *raft) *LeaseAwareNode {
	return &LeaseAwareNode{
		node: n,
		raft: r,
	}
}

// CanReadWithLease checks if reads can be served with lease
func (ln *LeaseAwareNode) CanReadWithLease() bool {
	return ln.raft.canReadWithLease()
}

// HasValidLease checks if this node has a valid leader lease
func (ln *LeaseAwareNode) HasValidLease() bool {
	return ln.raft.hasValidLease()
}
