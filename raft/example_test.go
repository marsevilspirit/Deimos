package raft

import (
	pb "github.com/marsevilspirit/marstore/raft/raftpb"
)

func applyToStore(ents []pb.Entry)    {}
func sendMessages(msgs []pb.Message)  {}
func saveStateToDisk(st pb.HardState) {}
func saveToDisk(ents []pb.Entry)      {}

// func Example_Node() {
// 	n := Start(0, nil, 0, 0)
//
// 	// stuff to n happens in other goroutines
//
// 	// the last known state
// 	var prev pb.HardState
// 	for {
// 		// ReadState blocks until there is new state ready.
// 		rd := <-n.Ready()
// 		if !isHardStateEqual(prev, rd.HardState) {
// 			saveStateToDisk(rd.HardState)
// 			prev = rd.HardState
// 		}
//
// 		saveToDisk(rd.Entries)
// 		go applyToStore(rd.CommittedEntries)
// 		sendMessages(rd.Messages)
// 	}
// }
