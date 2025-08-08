package raft

import (
	"testing"
	"time"
)

func TestLeaderLease(t *testing.T) {
	duration := 100 * time.Millisecond
	lease := NewLeaderLease(duration)

	// Initially no lease
	if lease.IsValid() {
		t.Error("Lease should not be valid initially")
	}

	// Renew lease
	lease.Renew()
	if !lease.IsValid() {
		t.Error("Lease should be valid after renewal")
	}

	// Check time to expiry
	timeToExpiry := lease.TimeToExpiry()
	if timeToExpiry <= 0 || timeToExpiry > duration {
		t.Errorf("Invalid time to expiry: %v", timeToExpiry)
	}

	// Should renew when 1/3 time remains
	time.Sleep(duration * 2 / 3)
	if !lease.ShouldRenew() {
		t.Error("Lease should need renewal")
	}

	// Wait for expiry
	time.Sleep(duration)
	if lease.IsValid() {
		t.Error("Lease should have expired")
	}

	// Revoke lease
	lease.Renew()
	lease.Revoke()
	if lease.IsValid() {
		t.Error("Lease should be revoked")
	}
}

func TestRaftLeaseIntegration(t *testing.T) {
	r := newRaft(1, []int64{1}, 10, 1)

	// Initially no valid lease (not leader)
	if r.hasValidLease() {
		t.Error("Non-leader should not have valid lease")
	}

	// Become leader (proper transition: follower -> candidate -> leader)
	r.becomeCandidate()
	r.becomeLeader()
	if !r.hasValidLease() {
		t.Error("Leader should have valid lease")
	}

	// Can read with lease
	if !r.canReadWithLease() {
		t.Error("Leader should be able to read with lease")
	}

	// Become follower
	r.becomeFollower(2, 2)
	if r.hasValidLease() {
		t.Error("Follower should not have valid lease")
	}

	if r.canReadWithLease() {
		t.Error("Follower should not be able to read with lease")
	}
}

func TestLeaseRenewal(t *testing.T) {
	r := newRaft(1, []int64{1}, 10, 1)
	r.becomeCandidate()
	r.becomeLeader()

	// Initial lease
	if !r.hasValidLease() {
		t.Error("Leader should have valid lease")
	}

	initialExpiry := r.lease.leaseExpiry

	// Renew lease
	time.Sleep(10 * time.Millisecond) // Small delay
	r.renewLease()

	if r.lease.leaseExpiry.Equal(initialExpiry) {
		t.Error("Lease should have been renewed")
	}

	if !r.hasValidLease() {
		t.Error("Renewed lease should be valid")
	}
}
