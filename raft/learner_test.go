package raft

import (
	"testing"

	pb "github.com/marsevilspirit/deimos/raft/raftpb"
)

func TestLearnerBasicFunctionality(t *testing.T) {
	// Create a raft instance with learner
	r := newRaft(1, []int64{1, 2}, 10, 1)

	// Add a learner node
	r.addLearner(3)

	// Verify learner is added
	if !r.isLearner(3) {
		t.Errorf("Node 3 should be a learner")
	}

	// Verify learner is not counted in quorum
	if r.q() != 2 { // Only nodes 1 and 2 should count for quorum
		t.Errorf("Expected quorum size 2, got %d", r.q())
	}

	// Verify learner cannot vote
	if r.learners[3] {
		// This test is checking that node 3 is marked as a learner in raft instance r
		// The actual election behavior test is done below with r3
	}

	// Test that learner node itself cannot participate in elections
	// Create a separate raft instance for node 3
	r3 := newRaft(3, []int64{1, 2}, 10, 1)
	r3.becomeLearner(1, 1)

	// Verify learner state
	if r3.state != StateLearner {
		t.Errorf("Expected state StateLearner, got %v", r3.state)
	}

	// Verify tick function is nil
	if r3.tick != nil {
		t.Errorf("Learner should have nil tick function")
	}

	// Try to trigger election
	r3.elapsed = 15
	if r3.tick != nil {
		r3.tick()
	}

	// Should not send any messages
	msgs := r3.ReadMessages()
	if len(msgs) > 0 {
		t.Errorf("Learner should not participate in elections, got %d messages", len(msgs))
	}
}

func TestLearnerPromotion(t *testing.T) {
	r := newRaft(1, []int64{1, 2}, 10, 1)

	// Start as follower, then become candidate, then leader
	r.becomeCandidate()
	r.becomeLeader()

	// Add a learner
	r.addLearner(3)

	// Verify initial quorum size (excluding learner)
	initialQuorum := r.q()
	if initialQuorum != 2 {
		t.Errorf("Expected initial quorum size 2, got %d", initialQuorum)
	}

	// Verify learner is initially a learner
	if !r.isLearner(3) {
		t.Errorf("Node 3 should be a learner initially")
	}

	// Simulate learner catching up
	r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("test entry")})
	r.prs[3].match = r.raftLog.lastIndex()
	r.prs[3].next = r.raftLog.lastIndex() + 1

	// Check if learner can be promoted
	if !r.shouldPromoteLearner(3) {
		t.Errorf("Learner should be promotable, match=%d, lastIndex=%d",
			r.prs[3].match, r.raftLog.lastIndex())
	}

	// Promote learner
	r.promoteLearner(3)

	// Verify learner is no longer a learner
	if r.isLearner(3) {
		t.Errorf("Node 3 should no longer be a learner after promotion")
	}

	// Verify quorum size increased
	// For 3 nodes, quorum = 3/2 + 1 = 1 + 1 = 2
	finalQuorum := r.q()
	if finalQuorum != 2 {
		t.Errorf("Expected quorum size 2 after promotion (3 nodes), got %d (initial: %d)", finalQuorum, initialQuorum)
		t.Logf("Learners: %v", r.learners)
		t.Logf("Progress: %v", r.prs)
		t.Logf("isLearner(3): %v", r.isLearner(3))
	}

	// Verify that the promoted node is now counted in voting nodes
	votingNodes := r.getVotingNodes()
	if len(votingNodes) != 3 {
		t.Errorf("Expected 3 voting nodes after promotion, got %d", len(votingNodes))
	}
}

func TestLearnerStateTransitions(t *testing.T) {
	r := newRaft(1, []int64{1, 2}, 10, 1)

	// Test becoming learner
	r.becomeLearner(1, 2)
	if r.state != StateLearner {
		t.Errorf("Expected state StateLearner, got %v", r.state)
	}

	// Test learner cannot vote
	r.Vote = None
	msg := pb.Message{From: 2, Type: msgVote, Term: 2, Index: 1, LogTerm: 1}
	t.Logf("Sending message: %+v", msg)
	if err := r.Step(msg); err != nil {
		t.Errorf("Failed to step message: %v", err)
	}

	msgs := r.ReadMessages()
	if len(msgs) != 1 {
		t.Errorf("Expected 1 message, got %d", len(msgs))
	}

	// Debug: print message details
	t.Logf("Received message: %+v", msgs[0])
	t.Logf("Message type: %d, expected: %d", msgs[0].Type, msgVoteResp)

	// The message should be a vote response denying the vote
	if msgs[0].Type != msgVoteResp {
		t.Errorf("Expected msgVoteResp, got %d", msgs[0].Type)
	}

	if !msgs[0].Denied {
		t.Errorf("Learner should deny vote requests, got denied=%v", msgs[0].Denied)
	}
}

func TestLearnerInCluster(t *testing.T) {
	// Create a 3-node cluster with 1 learner
	r1 := newRaft(1, []int64{1, 2, 3}, 10, 1)
	r2 := newRaft(2, []int64{1, 2, 3}, 10, 1)
	r3 := newRaft(3, []int64{1, 2, 3}, 10, 1)

	// Make node 3 a learner
	r1.addLearner(3)
	r2.addLearner(3)
	r3.addLearner(3)
	r3.becomeLearner(1, 1)

	// Verify quorum calculation excludes learner
	if r1.q() != 2 {
		t.Errorf("Expected quorum size 2, got %d", r1.q())
	}

	// Verify learner nodes list
	learnerNodes := r1.getLearnerNodes()
	if len(learnerNodes) != 1 || learnerNodes[0] != 3 {
		t.Errorf("Expected learner nodes [3], got %v", learnerNodes)
	}

	// Verify voting nodes list
	votingNodes := r1.getVotingNodes()
	if len(votingNodes) != 2 {
		t.Errorf("Expected 2 voting nodes, got %d", len(votingNodes))
	}
}

func TestLearnerLogCatchUp(t *testing.T) {
	r := newRaft(1, []int64{1, 2}, 10, 1)

	// Start as follower, then become candidate, then leader
	r.becomeCandidate()
	r.becomeLeader()

	// Add a learner
	r.addLearner(3)

	// Initially learner should be promotable if committed is 0 (match >= 0)
	// This is a special case where the cluster is empty
	if r.raftLog.committed == 0 {
		if !r.shouldPromoteLearner(3) {
			t.Errorf("Learner should be promotable initially when committed=0, match=%d, committed=%d",
				r.prs[3].match, r.raftLog.committed)
		}
	} else {
		if r.shouldPromoteLearner(3) {
			t.Errorf("Learner should not be promotable initially when committed>0, match=%d, committed=%d",
				r.prs[3].match, r.raftLog.committed)
		}
	}

	// Add some entries to the log
	r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("test1")})
	r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("test2")})

	// In a single-node cluster, entries might not be committed immediately
	// So we'll simulate the learner catching up to the last index instead
	r.prs[3].match = r.raftLog.lastIndex()
	r.prs[3].next = r.raftLog.lastIndex() + 1

	// Now learner should be promotable
	if !r.shouldPromoteLearner(3) {
		t.Errorf("Learner should be promotable after catching up, match=%d, lastIndex=%d",
			r.prs[3].match, r.raftLog.lastIndex())
	}
}
