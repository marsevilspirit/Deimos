package server

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/marsevilspirit/marstore/raft"
	"github.com/marsevilspirit/marstore/raft/raftpb"
	pb "github.com/marsevilspirit/marstore/server/serverpb"
	"github.com/marsevilspirit/marstore/store"
	"github.com/marsevilspirit/marstore/wait"
)

var (
	ErrUnknownMethod = errors.New("Marstore server: unknown method")
	ErrStopped       = errors.New("Marstore server: server stopped")
)

type SendFunc func(m []raftpb.Message)

type Response struct {
	// The last seen term raft was at when this request was built.
	Term int64

	// The last seen index raft was at when this request was built.
	Commit int64

	*store.Event
	*store.Watcher

	err error
}

type Server struct {
	once sync.Once
	w    *wait.List
	done chan struct{}

	Node  raft.Node
	Store store.Store

	msgsc chan raftpb.Message

	// Send specifies the send function for sending msgs to peers. Send
	// MUST NOT block. It is okay to drop messages, since clients should
	// timeout and reissue their messages.  If Send is nil, Server will
	// panic.
	Send SendFunc

	// Save specifies the save function for saving ents to stable storage.
	// Save MUST block until st and ents are on stable storage.  If Send is
	// nil, Server will panic.
	Save func(st raftpb.HardState, ents []raftpb.Entry)
}

// Start prepares and starts the server in a new goroutine.
func Start(s *Server) {
	s.w = wait.New()
	s.done = make(chan struct{})
	go s.run()
}

func (s *Server) run() {
	log.Printf("server.go/run:58 run\n")
	for {
		select {
		case rd := <-s.Node.Ready():
			log.Printf("server.go/run:62 rd: %+v\n", rd)
			s.Save(rd.HardState, rd.Entries)
			s.Send(rd.Messages)
			log.Printf("server.go/run:66 rd.CommittedEntries: %+v\n", rd.CommittedEntries)

			// TODO: do this in the background,
			// but take care to apply entries in a single goroutine,
			// and not race them.
			for _, e := range rd.CommittedEntries {
				log.Printf("server.go/run:71 e: %+v\n", e)
				if e.Data == nil {
					log.Printf("server.go/run:79 e.Data is nil, skip\n")
					continue
				}
				var r pb.Request
				if err := r.Unmarshal(e.Data); err != nil {
					panic("TODO: this is bad, what do we do about it?")
				}
				var resp Response
				resp.Event, resp.err = s.apply(context.TODO(), r)
				resp.Term = rd.Term
				resp.Commit = rd.Commit
				s.w.Trigger(r.Id, resp)
			}
		case <-s.done:
			return
		}
	}
}

func (s *Server) Stop() {
	close(s.done)
}

func (s *Server) Do(ctx context.Context, r pb.Request) (Response, error) {
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
		s.Node.Propose(ctx, data)
		select {
		case x := <-ch:
			resp := x.(Response)
			return resp, resp.err
		case <-ctx.Done():
			s.w.Trigger(r.Id, nil) // GC wait
			return Response{}, ctx.Err()
		case <-s.done:
			return Response{}, ErrStopped
		}
	case "GET":
		switch {
		case r.Wait:
			wc, err := s.Store.Watch(r.Path, r.Recursive, false, r.Since)
			if err != nil {
				return Response{}, err
			}
			return Response{Watcher: wc}, nil
		default:
			ev, err := s.Store.Get(r.Path, r.Recursive, r.Sorted)
			if err != nil {
				return Response{}, err
			}
			return Response{Event: ev}, nil
		}
	default:
		return Response{}, ErrUnknownMethod
	}
}

// apply interprets r as a call to store.X and returns an
// Response interpreted from the store.Event
func (s *Server) apply(ctx context.Context, r pb.Request) (*store.Event, error) {
	expr := time.Unix(0, r.Expiration)

	switch r.Method {
	case "POST":
		return s.Store.Create(r.Path, r.Dir, r.Val, true, expr)
	case "PUT":
		exists, existSet := getBool(r.PrevExists)
		switch {
		case existSet:
			if exists {
				return s.Store.Update(r.Path, r.Val, expr)
			} else {
				return s.Store.Create(r.Path, r.Dir, r.Val, false, expr)
			}
		case r.PrevIndex > 0 || r.PrevValue != "":
			return s.Store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, expr)
		default:
			return s.Store.Set(r.Path, r.Dir, r.Val, expr)
		}
	case "DELETE":
		switch {
		case r.PrevIndex > 0 || r.PrevValue != "":
			return s.Store.CompareAndDelete(r.Path, r.PrevValue, r.PrevIndex)
		default:
			return s.Store.Delete(r.Path, r.Recursive, r.Dir)
		}
	default:
		// This should never reached, but just in case.
		return nil, ErrUnknownMethod
	}
}

func getBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}
