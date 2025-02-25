package server

import (
	"context"
	"log"

	"github.com/marsevilspirit/marstore/raft"
	pb "github.com/marsevilspirit/marstore/raft/raftpb"
	"github.com/marsevilspirit/marstore/wait"
)

type Response struct {
	err error
}

type Server struct {
	n raft.Node
	w wait.List

	msgsc chan pb.Message

	// Send specifies the send function for sending to peers.
	// Send MUST NOT block. It is okay to drop messages, since clients
	// should timeout and reissue their messages. If Send is nil, Server
	// Will panic.
	Send func(msgs []pb.Message)
}

func (s *Server) Run(ctx context.Context) {
	for {
		select {
		case rd := <-s.n.Ready():
			// save state to wal
			s.Save(rd.State, rd.Entries)
			// go send messages
			s.Send(rd.Messages)
			go func() {
				for _, e := range rd.CommittedEntries {
					var r Request
					r.Unmarshal(e.Data)
					s.w.Trigger(r.Id, s.apply(r))
				}
			}()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) Do(ctx context.Context, r Request) (Response, error) {
	if r.Id == 0 {
		panic("r.Id cannot be 0")
	}
	switch r.Method {
	case "POST", "PUT", "DELETE":
		data, err := r.Marshal()
		if err != nil {
			return Response{}, err
		}
		ch := s.w.Register(r.Id)
		s.n.Propose(data)
		select {
		case x := <-ch:
			resp := x.(Response)
			return resp, resp.err
		case <-ctx.Done():
			s.w.Trigger(r.Id, nil) // GC wait
			return Response{}, ctx.Err()
		}
	case "GET":
		switch {
		case r.Wait:
			//TODO: store
		}
	}
	panic("not reached") // for some reason the compiler wants this... :/
}

// apply interprets r as a call to store.X and returns an
// Response interpreted from the store.Event
func (s *Server) apply(r Request) Response {
	panic("not implemented")
}
