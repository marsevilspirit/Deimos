package store

import (
	"path"
	"strings"
)

type tree struct {
	Root *treeNode
}

type treeNode struct {
	Value Node

	Dir bool // for clearity

	NodeMap map[string]*treeNode
}

var emptyNode = Node{".", PERMANENT, nil}

// set the key to value, return the old value if the key exists
func (s *tree) set(key string, value Node) bool {
	key = "/" + key
	key = path.Clean(key)

	nodes := strings.Split(key, "/")
	nodes = nodes[1:]

	nodeMap := s.Root.NodeMap

	i := 0

	for i = 0; i < len(nodes)-1; i++ {
		node := &treeNode{
			Value:   emptyNode,
			Dir:     true,
			NodeMap: make(map[string]*treeNode),
		}
		nodeMap[nodes[i]] = node
		nodeMap = node.NodeMap
		continue
	}

	node, ok := nodeMap[nodes[i]]
	// add new dir
	if !ok {
		node := &treeNode{
			Value:   emptyNode,
			Dir:     true,
			NodeMap: make(map[string]*treeNode),
		}
		nodeMap[nodes[i]] = node
		nodeMap = node.NodeMap
	} else if ok && !node.Dir {
		return false
	} else {
		nodeMap = node.NodeMap
	}

	// add the last node and value
	node, ok = nodeMap[nodes[i]]

	if !ok {
		node := &treeNode{
			Value:   value,
			Dir:     false,
			NodeMap: nil,
		}
		nodeMap[nodes[i]] = node
	} else {
		node.Value = value
	}
	return true
}

// get the node of the key
func (s *tree) get(key string) (Node, bool) {
	key = "/" + key
	key = path.Clean(key)

	nodes := strings.Split(key, "/")
	nodes = nodes[1:]

	nodeMap := s.Root.NodeMap

	var i int

	for i = 0; i < len(nodes)-1; i++ {
		node, ok := nodeMap[nodes[i]]
		if !ok || !node.Dir {
			return emptyNode, false
		}
		nodeMap = node.NodeMap
	}

	treeNode, ok := nodeMap[nodes[i]]
	if ok {
		return treeNode.Value, true
	}
	return emptyNode, false
}

// delete the key, return the old value if the key exists
func (s *tree) delete(key string) bool {
	key = "/" + key
	key = path.Clean(key)

	nodes := strings.Split(key, "/")
	nodes = nodes[1:]

	nodeMap := s.Root.NodeMap

	var i int

	for i = 0; i < len(nodes)-1; i++ {
		node, ok := nodeMap[nodes[i]]
		if !ok || !node.Dir {
			return false
		}
		nodeMap = node.NodeMap
	}

	node, ok := nodeMap[nodes[i]]
	if ok && !node.Dir {
		delete(nodeMap, nodes[i])
		return true
	}
	return false
}
