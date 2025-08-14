package raft

import (
	"fmt"

	pb "github.com/marsevilspirit/deimos/raft/raftpb"
)

// ExampleLearnerUsage demonstrates how to use learner nodes in a Raft cluster
func ExampleLearnerUsage() {
	// Create a 3-node cluster
	r1 := newRaft(1, []int64{1, 2}, 100, 10)
	r2 := newRaft(2, []int64{1, 2}, 100, 10)

	// Start election
	r1.Step(pb.Message{From: 1, Type: msgHup})
	r2.Step(pb.Message{From: 2, Type: msgHup})

	// Process messages
	msgs1 := r1.ReadMessages()
	msgs2 := r2.ReadMessages()

	// Simulate message exchange
	for _, msg := range msgs1 {
		r2.Step(msg)
	}
	for _, msg := range msgs2 {
		r1.Step(msg)
	}

	// One should become leader
	if r1.state == StateLeader {
		fmt.Println("Node 1 became leader")
	} else if r2.state == StateLeader {
		fmt.Println("Node 2 became leader")
	}

	// Add a learner node
	leader := r1
	if r2.state == StateLeader {
		leader = r2
	}

	// Add learner to the cluster
	leader.addLearner(3)
	fmt.Printf("Added learner node %d\n", 3)

	// Verify learner status
	if leader.isLearner(3) {
		fmt.Printf("Node %d is a learner\n", 3)
	}

	// Check quorum size (should exclude learner)
	quorumSize := leader.q()
	fmt.Printf("Quorum size: %d (excluding learner)\n", quorumSize)

	// Simulate learner catching up with log
	leader.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("test entry")})
	leader.prs[3].match = leader.raftLog.committed
	leader.prs[3].next = leader.raftLog.lastIndex() + 1

	// Check if learner can be promoted
	if leader.shouldPromoteLearner(3) {
		fmt.Println("Learner can be promoted")

		// Promote learner to voting member
		leader.promoteLearner(3)
		fmt.Printf("Node %d promoted from learner to voting member\n", 3)

		// Verify promotion
		if !leader.isLearner(3) {
			fmt.Printf("Node %d is no longer a learner\n", 3)
		}

		// Check new quorum size
		newQuorumSize := leader.q()
		fmt.Printf("New quorum size: %d (including promoted node)\n", newQuorumSize)
	}

	// Show cluster status
	fmt.Println("\nCluster Status:")
	fmt.Printf("Leader: Node %d\n", leader.id)
	fmt.Printf("Voting nodes: %v\n", leader.getVotingNodes())
	fmt.Printf("Learner nodes: %v\n", leader.getLearnerNodes())
}

// ExampleLearnerConfiguration demonstrates learner configuration changes
func ExampleLearnerConfiguration() {
	// Create a cluster
	r := newRaft(1, []int64{1, 2}, 100, 10)
	r.becomeLeader()

	// Add a learner
	r.addLearner(3)
	fmt.Printf("Added learner node %d\n", 3)

	// Show initial configuration
	fmt.Printf("Initial voting nodes: %v\n", r.getVotingNodes())
	fmt.Printf("Initial learner nodes: %v\n", r.getLearnerNodes())
	fmt.Printf("Initial quorum size: %d\n", r.q())

	// Simulate learner catching up
	r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("entry1")})
	r.appendEntry(pb.Entry{Type: pb.EntryNormal, Data: []byte("entry2")})
	r.prs[3].match = r.raftLog.committed
	r.prs[3].next = r.raftLog.lastIndex() + 1

	// Promote learner
	if r.shouldPromoteLearner(3) {
		r.promoteLearner(3)
		fmt.Printf("Promoted learner node %d\n", 3)

		// Show final configuration
		fmt.Printf("Final voting nodes: %v\n", r.getVotingNodes())
		fmt.Printf("Final learner nodes: %v\n", r.getLearnerNodes())
		fmt.Printf("Final quorum size: %d\n", r.q())
	}
}
