package store

import (
	"path"
	"path/filepath"
	"sort"
	"time"

	Err "github.com/marsevilspirit/deimos/error"
)

// explanations of Compare function result
const (
	CompareMatch         = 0
	CompareIndexNotMatch = 1
	CompareValueNotMatch = 2
	CompareNotMatch      = 3
)

var Permanent time.Time

// node is the basic element in the store system.
// A key-value pair will have a string value.
// A directory will have a children map.
type node struct {
	Path string

	CreatedIndex  uint64
	ModifiedIndex uint64

	// should not encode this field! avoid circular dependency
	Parent *node `json:"-"`

	ExpireTime time.Time
	ACL        string
	Value      string           // for key-value pair
	Children   map[string]*node // for directory

	// A reference to the store this node is attached to.
	store *store
}

// newKV creates a Key-Value pair
func newKV(store *store, nodePath string, value string, createdIndex uint64,
	parent *node, ACL string, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ACL:           ACL,
		store:         store,
		ExpireTime:    expireTime,
		Value:         value,
	}
}

// newDir creates a directory
func newDir(store *store, nodePath string, createdIndex uint64,
	parent *node, ACL string, expireTime time.Time) *node {
	return &node{
		Path:          nodePath,
		CreatedIndex:  createdIndex,
		ModifiedIndex: createdIndex,
		Parent:        parent,
		ACL:           ACL,
		ExpireTime:    expireTime,
		Children:      make(map[string]*node),
		store:         store,
	}
}

// IsHidden function checks if the node is a hidden node. A hidden node
// will begin with '_'
// A hidden node will not be shown via get command under a directory
// For example if we have /foo/_hidden and /foo/notHidden, get "/foo"
// will only return /foo/notHidden
func (n *node) IsHidden() bool {
	_, name := path.Split(n.Path)
	return name[0] == '_'
}

// IsPermanent function checks if the node is a permanent one.
func (n *node) IsPermanent() bool {
	// we use a uninitialized time.Time to indicate the node is a permanent one.
	// the uninitialized time.Time should equal zero.
	return n.ExpireTime.IsZero()
}

// IsDir function checks whether the node is a directory.
// If the node is a directory, the function will return true.
// Otherwise the function will return false.
func (n *node) IsDir() bool {
	return !(n.Children == nil)
}

// Get function gets the value of the node.
// If the receiver node is not a key-value pair, a "Not A File" error will be returned.
func (n *node) Read() (string, *Err.Error) {
	if n.IsDir() {
		return "", Err.NewError(Err.EcodeNotFile, "", n.store.CurrentIndex)
	}

	return n.Value, nil
}

// Set function set the value of the node to the given value.
// If the receiver node is a directory, a "Not A File" error will be returned.
func (n *node) Write(value string, index uint64) *Err.Error {
	if n.IsDir() {
		return Err.NewError(Err.EcodeNotFile, "", n.store.CurrentIndex)
	}

	n.Value = value
	n.ModifiedIndex = index

	return nil
}

func (n *node) ExpirationAndTTL() (*time.Time, int64) {
	if !n.IsPermanent() {
		/* compute ttl as:
		   ceiling( (expireTime - timeNow) / nanosecondsPerSecond )
		   which ranges from 1..n
		   rather than as:
		   ( (expireTime - timeNow) / nanosecondsPerSecond ) + 1
		   which ranges 1..n+1
		*/
		ttlN := n.ExpireTime.Sub(time.Now())
		ttl := ttlN / time.Second
		if (ttlN % time.Second) > 0 {
			ttl++
		}
		return &n.ExpireTime, int64(ttl)
	}
	return nil, 0
}

// List function return a slice of nodes under the receiver node.
// If the receiver node is not a directory, a "Not A Directory" error will be returned.
func (n *node) List() ([]*node, *Err.Error) {
	if !n.IsDir() {
		return nil, Err.NewError(Err.EcodeNotDir, "", n.store.CurrentIndex)
	}

	nodes := make([]*node, len(n.Children))

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
func (n *node) GetChild(name string) (*node, *Err.Error) {
	if !n.IsDir() {
		return nil, Err.NewError(Err.EcodeNotDir, n.Path, n.store.CurrentIndex)
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
func (n *node) Add(child *node) *Err.Error {
	if !n.IsDir() {
		return Err.NewError(Err.EcodeNotDir, "", n.store.CurrentIndex)
	}

	_, name := filepath.Split(child.Path)

	_, ok := n.Children[name]
	if ok {
		return Err.NewError(Err.EcodeNodeExist, "", n.store.CurrentIndex)
	}

	n.Children[name] = child

	return nil

}

// Remove function remove the node.
func (n *node) Remove(dir, recursive bool, callback func(path string)) *Err.Error {
	if n.IsDir() {
		if !dir {
			return Err.NewError(Err.EcodeNotFile, n.Path, n.store.CurrentIndex)
		}

		if len(n.Children) != 0 && !recursive {
			// cannot delete a directory if it is not empty and the operation
			// is not recursive
			return Err.NewError(Err.EcodeDirNotEmpty, n.Path, n.store.CurrentIndex)
		}
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
		child.Remove(true, true, callback)
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

func (n *node) Repr(recurisive, sorted bool) *NodeExtern {
	if n.IsDir() {
		node := &NodeExtern{
			Key:           n.Path,
			Dir:           true,
			ModifiedIndex: n.ModifiedIndex,
			CreatedIndex:  n.CreatedIndex,
		}

		node.Expiration, node.TTL = n.ExpirationAndTTL()

		if !recurisive {
			return node
		}

		children, _ := n.List()
		node.Nodes = make(NodeExterns, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0

		for _, child := range children {
			if child.IsHidden() { // get will not list hidden node
				continue
			}
			node.Nodes[i] = child.Repr(recurisive, sorted)
			i++
		}

		// eliminate hidden nodes
		node.Nodes = node.Nodes[:i]

		if sorted {
			sort.Sort(node.Nodes)
		}

		return node
	}

	// since n.Value could be changed later, so we need to copy the value out
	value := n.Value
	node := &NodeExtern{
		Key:           n.Path,
		Value:         &value,
		ModifiedIndex: n.ModifiedIndex,
		CreatedIndex:  n.CreatedIndex,
	}
	node.Expiration, node.TTL = n.ExpirationAndTTL()
	return node
}

func (n *node) UpdateTTL(expireTime time.Time) {
	if !n.IsPermanent() {
		if expireTime.IsZero() {
			// from ttl to permanent
			n.ExpireTime = expireTime
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

// Compare function compares node index and value with provided ones.
// second result value explains result and equals to one of Compare.. constants
func (n *node) Compare(prevValue string, prevIndex uint64) (ok bool, which int) {
	indexMatch := (prevIndex == 0 || n.ModifiedIndex == prevIndex)
	valueMatch := (prevValue == "" || n.Value == prevValue)
	ok = valueMatch && indexMatch
	switch {
	case valueMatch && indexMatch:
		which = CompareMatch
	case indexMatch && !valueMatch:
		which = CompareValueNotMatch
	case valueMatch && !indexMatch:
		which = CompareIndexNotMatch
	default:
		which = CompareNotMatch
	}
	return
}

// Clone function clone the node recursively and return the new node.
// If the node is a directory, it will clone all the content under this directory.
// If the node is a key-value pair, it will clone the pair.
func (n *node) Clone() *node {
	if !n.IsDir() {
		return newKV(n.store, n.Path, n.Value, n.CreatedIndex, n.Parent, n.ACL, n.ExpireTime)
	}

	clone := newDir(n.store, n.Path, n.CreatedIndex, n.Parent, n.ACL, n.ExpireTime)

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
func (n *node) recoverAndclean() {
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
