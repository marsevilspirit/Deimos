package raft

import (
	"time"
)

// LeaderLease represents the leader lease mechanism
type LeaderLease struct {
	// Leader's lease expiration time
	leaseExpiry time.Time

	// Lease duration (typically 3-5 heartbeat intervals)
	leaseDuration time.Duration

	// Last time lease was renewed
	lastRenewal time.Time

	// Whether this node has a valid lease
	hasLease bool

	// Clock skew tolerance
	clockSkewTolerance time.Duration
}

// NewLeaderLease creates a new leader lease
func NewLeaderLease(duration time.Duration) *LeaderLease {
	return &LeaderLease{
		leaseDuration:      duration,
		clockSkewTolerance: duration / 10, // 10% tolerance
		hasLease:           false,
	}
}

// IsValid checks if the lease is still valid
func (l *LeaderLease) IsValid() bool {
	if !l.hasLease {
		return false
	}
	return time.Now().Before(l.leaseExpiry.Add(-l.clockSkewTolerance))
}

// Renew renews the lease for the leader
func (l *LeaderLease) Renew() {
	now := time.Now()
	l.leaseExpiry = now.Add(l.leaseDuration)
	l.lastRenewal = now
	l.hasLease = true
}

// Revoke revokes the lease
func (l *LeaderLease) Revoke() {
	l.hasLease = false
	l.leaseExpiry = time.Time{}
}

// TimeToExpiry returns time until lease expires
func (l *LeaderLease) TimeToExpiry() time.Duration {
	if !l.hasLease {
		return 0
	}
	return time.Until(l.leaseExpiry)
}

// ShouldRenew checks if lease should be renewed
func (l *LeaderLease) ShouldRenew() bool {
	if !l.hasLease {
		return false
	}
	// Renew when 1/3 of lease time remains
	renewThreshold := l.leaseDuration / 3
	return l.TimeToExpiry() <= renewThreshold
}
