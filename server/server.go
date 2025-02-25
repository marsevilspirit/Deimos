package server

import (
	"github.com/marsevilspirit/m_raft"
)

type Response struct {
	err error
}

type Server struct {
	n raft.Node
}
