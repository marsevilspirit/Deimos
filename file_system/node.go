package fileSystem

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	err "github.com/marsevilspirit/marstore/error"
)

var (
	Permanent time.Time
)

const (
	normal = iota
	removed
)

type Node struct {
	Path        string
	CreateIndex uint64
	CreateTerm  uint64
	Parent      *Node
	ExpireTime  time.Time
	ACL         string
	Value       string           // for key-value pair
	Children    map[string]*Node // for directory
	status      int
	mu          sync.Mutex
	removeChan  chan bool // remove channel
}

func newFile(path string, value string, createIndex uint64, createTerm uint64, parent *Node, ACL string, expireTime time.Time) *Node {
	return &Node{
		Path:        path,
		CreateIndex: createIndex,
		CreateTerm:  createTerm,
		Parent:      parent,
		ACL:         ACL,
		removeChan:  make(chan bool, 1),
		ExpireTime:  expireTime,
		Value:       value,
	}
}

func newDir(path string, createIndex uint64, createTerm uint64, parent *Node, ACL string) *Node {
	return &Node{
		Path:        path,
		CreateIndex: createIndex,
		CreateTerm:  createTerm,
		Parent:      parent,
		ACL:         ACL,
		removeChan:  make(chan bool, 1),
		Children:    make(map[string]*Node),
	}
}

// Remove function remove the node.
// If the node is a directory and recursive is true,
// the function will recursively remove
// add nodes under the receiver node.
func (n *Node) Remove(recursive bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.status == removed {
		return nil
	}

	if !n.IsDir() { // key-value pair
		_, name := filepath.Split(n.Path)

		if n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
			n.removeChan <- true
			n.status = removed
		}

		return nil
	}

	if !recursive {
		return err.NewError(102, "")
	}

	for _, n := range n.Children { // delete all children
		n.Remove(true)
	}

	// delete self
	_, name := filepath.Split(n.Path)
	if n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)
		n.removeChan <- true
		n.status = removed
	}

	return nil
}

// Get function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
func (n *Node) Read() (string, error) {
	if n.IsDir() {
		return "", err.NewError(102, "")
	}

	return n.Value, nil
}

// Set function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *Node) Write(value string) error {
	if n.IsDir() {
		return err.NewError(102, "")
	}

	n.Value = value

	return nil
}

// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
func (n *Node) List() ([]*Node, error) {
	n.mu.Lock()
	n.mu.Unlock()
	if !n.IsDir() {
		return nil, err.NewError(104, "")
	}

	nodes := make([]*Node, len(n.Children))

	i := 0
	for _, node := range n.Children {
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is a existing node with the same name under the directory, a "Already Exist"
// error will be returned
func (n *Node) Add(child *Node) error {
	n.mu.Lock()
	n.mu.Unlock()
	if n.status == removed {
		return err.NewError(100, "")
	}

	if !n.IsDir() {
		return err.NewError(104, "")
	}

	_, name := filepath.Split(child.Path)

	_, ok := n.Children[name]

	if ok {
		return err.NewError(105, "")
	}

	n.Children[name] = child

	return nil

}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *Node) Clone() *Node {
	n.mu.Lock()
	n.mu.Unlock()
	if !n.IsDir() {
		return newFile(n.Path, n.Value, n.CreateIndex, n.CreateTerm, n.Parent, n.ACL, n.ExpireTime)
	}

	clone := newDir(n.Path, n.CreateIndex, n.CreateTerm, n.Parent, n.ACL)

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *Node) IsDir() bool {
	if n.Children == nil { // key-value pair
		return false
	}
	return true
}

func (n *Node) Expire() {
	for {
		duration := n.ExpireTime.Sub(time.Now())
		if duration <= 0 {
			n.Remove(true)
			return
		}

		select {
		// if timeout, delete the node
		case <-time.After(duration):
			n.Remove(true)
			return

		// if removed, return
		case <-n.removeChan:
			fmt.Println("node removed")
			return

		}
	}
}
