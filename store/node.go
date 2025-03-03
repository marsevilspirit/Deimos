package store

import (
	"path"
	"path/filepath"
	"sort"
	"time"

	Err "github.com/marsevilspirit/marstore/error"
)

const (
	normal = iota
	removed
)

var Permanent time.Time

// Node is the basic element in the store system.
// A key-value pair will have a string value.
// A directory will have a children map.
type Node struct {
	Path string

	CreateIndex   uint64
	CreateTerm    uint64
	ModifiedIndex uint64
	ModifiedTerm  uint64

	// should not encode this field! avoid cyclical dependency
	Parent *Node `json:"-"`

	ExpireTime time.Time
	ACL        string
	Value      string           // for key-value pair
	Children   map[string]*Node // for directory

	// A reference to the store this node is attached to.
	store *store
}

// newKV creates a Key-Value pair
func newKV(store *store, nodePath string, value string, createIndex uint64,
	createTerm uint64, parent *Node, ACL string, expireTime time.Time) *Node {
	return &Node{
		Path:          nodePath,
		CreateIndex:   createIndex,
		CreateTerm:    createTerm,
		ModifiedIndex: createIndex,
		ModifiedTerm:  createTerm,
		Parent:        parent,
		ACL:           ACL,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// newDir creates a directory
func newDir(store *store, nodePath string, createIndex uint64, createTerm uint64,
	parent *Node, ACL string, expireTime time.Time) *Node {
	return &Node{
		Path:        nodePath,
		CreateIndex: createIndex,
		CreateTerm:  createTerm,
		Parent:      parent,
		ACL:         ACL,
		ExpireTime:  expireTime,
		Children:    make(map[string]*Node),
		store:       store,
	}
}

// IsHidden function checks if the node is a hidden node. A hidden node
// will begin with '_'
// A hidden node will not be shown via get command under a directory
// For example if we have /foo/_hidden and /foo/notHidden, get "/foo"
// will only return /foo/notHidden
func (n *Node) IsHidden() bool {
	_, name := path.Split(n.Path)
	return name[0] == '_'
}

// IsPermanent function checks if the node is a permanent one.
func (n *Node) IsPermanent() bool {
	return n.ExpireTime.IsZero()
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *Node) IsDir() bool {
	return !(n.Children == nil)
}

// Get function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
func (n *Node) Read() (string, *Err.Error) {
	if n.IsDir() {
		return "", Err.NewError(Err.EcodeNotFile, "", UndefIndex, UndefTerm)
	}

	return n.Value, nil
}

// Set function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *Node) Write(value string, index uint64, term uint64) *Err.Error {
	if n.IsDir() {
		return Err.NewError(Err.EcodeNotFile, "", UndefIndex, UndefTerm)
	}

	n.Value = value
	n.ModifiedIndex = index
	n.ModifiedTerm = term

	return nil
}

func (n *Node) ExpirationAndTTL() (*time.Time, int64) {
	if !n.IsPermanent() {
		return &n.ExpireTime, int64(n.ExpireTime.Sub(time.Now())/time.Second) + 1
	}
	return nil, 0
}

// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
func (n *Node) List() ([]*Node, *Err.Error) {
	if !n.IsDir() {
		return nil, Err.NewError(Err.EcodeNotDir, "", UndefIndex, UndefTerm)
	}

	nodes := make([]*Node, len(n.Children))

	i := 0
	for _, node := range n.Children {
		nodes[i] = node
		i++
	}

	return nodes, nil
}

// GetChild function returns the child node under the directory node.
// On success, it returns the file node
// If the node that calls this function is not a directory, it returns
// Not Directory Error
// If the node corresponding to the name string is not file, it returns
// Not File Error
func (n *Node) GetChild(name string) (*Node, *Err.Error) {
	if !n.IsDir() {
		return nil, Err.NewError(Err.EcodeNotDir, n.Path, UndefIndex, UndefTerm)
	}

	child, ok := n.Children[name]

	if ok {
		return child, nil
	}

	return nil, nil

}

// Add function adds a node to the receiver node.
// If the receiver is not a directory, a "Not A Directory" error will be returned.
// If there is a existing node with the same name under the directory, a "Already Exist"
// error will be returned
func (n *Node) Add(child *Node) *Err.Error {
	if !n.IsDir() {
		return Err.NewError(Err.EcodeNotDir, "", UndefIndex, UndefTerm)
	}

	_, name := filepath.Split(child.Path)

	_, ok := n.Children[name]
	if ok {
		return Err.NewError(Err.EcodeNodeExist, "", UndefIndex, UndefTerm)
	}

	n.Children[name] = child

	return nil

}

// Remove function remove the node.
func (n *Node) Remove(recursive bool, callback func(path string)) *Err.Error {
	if n.IsDir() && !recursive {
		// cannot delete a directory without set recursive to true
		return Err.NewError(Err.EcodeNotFile, "", UndefIndex, UndefTerm)
	}

	if !n.IsDir() { // key-value pair
		_, name := path.Split(n.Path)

		// find its parent and remove the node from the map
		// In other words, remove itself
		if n.Parent != nil && n.Parent.Children[name] == n {
			delete(n.Parent.Children, name)
		}

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}

		return nil
	}

	// delete all children
	for _, child := range n.Children {
		child.Remove(true, callback)
	}

	// delete self
	_, name := path.Split(n.Path)
	if n.Parent != nil && n.Parent.Children[name] == n {
		delete(n.Parent.Children, name)

		if callback != nil {
			callback(n.Path)
		}

		if !n.IsPermanent() {
			n.store.ttlKeyHeap.remove(n)
		}
	}
	return nil
}

func (n *Node) Pair(recurisive, sorted bool) KeyValuePair {
	if n.IsDir() {
		pair := KeyValuePair{
			Key: n.Path,
			Dir: true,
		}

		pair.Expiration, pair.TTL = n.ExpirationAndTTL()

		if !recurisive {
			return pair
		}

		children, _ := n.List()
		pair.KVPairs = make([]KeyValuePair, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		for _, child := range children {
			if child.IsHidden() { // get will not list hidden node
				continue
			}
			pair.KVPairs[i] = child.Pair(recurisive, sorted)
			i++
		}

		// eliminate hidden nodes
		pair.KVPairs = pair.KVPairs[:i]

		if sorted {
			sort.Sort(pair.KVPairs)
		}

		return pair
	}
	pair := KeyValuePair{
		Key:   n.Path,
		Value: n.Value,
	}
	pair.Expiration, pair.TTL = n.ExpirationAndTTL()
	return pair
}

func (n *Node) UpdateTTL(expireTime time.Time) {
	if !n.IsPermanent() {
		if expireTime.IsZero() {
			// from ttl to permanent
			// remove from ttl heap
			n.store.ttlKeyHeap.remove(n)
		} else {
			// update ttl
			n.ExpireTime = expireTime
			// update ttl heap
			n.store.ttlKeyHeap.update(n)
		}

	} else {
		if !expireTime.IsZero() {
			// from permanent to ttl
			n.ExpireTime = expireTime
			// push into ttl heap
			n.store.ttlKeyHeap.Push(n)
		}
	}
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *Node) Clone() *Node {
	if !n.IsDir() {
		return newKV(n.store, n.Path, n.Value, n.CreateIndex, n.CreateTerm, n.Parent, n.ACL, n.ExpireTime)
	}

	clone := newDir(n.store, n.Path, n.CreateIndex, n.CreateTerm, n.Parent, n.ACL, n.ExpireTime)

	for key, child := range n.Children {
		clone.Children[key] = child.Clone()
	}

	return clone
}

// recoverAndclean function help to do recovery.
// Two things need to be done: 1. recovery structure; 2. delete expired nodes

// If the node is a directory, it will help recover children's parent pointer and recursively
// call this function on its children.
// We check the expire last since we need to recover the whole structure first and add all the
// notifications into the event history.
func (n *Node) recoverAndclean() {
	if n.IsDir() {
		for _, child := range n.Children {
			child.Parent = n
			child.store = n.store
			child.recoverAndclean()
		}
	}

	if !n.ExpireTime.IsZero() {
		n.store.ttlKeyHeap.push(n)
	}
}
