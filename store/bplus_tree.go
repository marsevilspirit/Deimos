package store

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	Err "github.com/marsevilspirit/deimos/error"
)

const (
	// The order of the B+ tree can be adjusted according to actual needs.
	DefaultOrder = 128
)

type BPlusNode interface {
}

// BPlusNode represents a node in a B+ tree.
type InternalNode struct {
	Keys     []string
	Parent   *InternalNode
	Children []BPlusNode

	CreatedIndex  uint64
	ModifiedIndex uint64
	ExpireTime    time.Time
	ACL           string
}

type LeafNode struct {
	Parent *InternalNode
	Keys   []string
	Values []string
	Next   *LeafNode

	CreatedIndex  uint64
	ModifiedIndex uint64
	// Per-entry expiration times aligned with Keys/Values
	ExpireTimes []time.Time
	ACL         string
}

// BPlusTree represents a B+ tree that implements the Store interface.
type BPlusTree struct {
	root  BPlusNode
	order int
	size  int
	mutex sync.RWMutex

	// For TTL management
	ttlHeap *bpTtlKeyHeap

	// Store interface requirements
	CurrentIndex   uint64
	CurrentVersion int
	Stats          *Stats
	WatcherHub     *watcherHub
}

// NewBPlusTree create a new B+ tree
func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		root:           newLeafNode(),
		order:          DefaultOrder,
		size:           0,
		ttlHeap:        newBpTtlKeyHeap(),
		CurrentIndex:   0,
		CurrentVersion: 2, // default version
		Stats:          newStats(),
		WatcherHub:     newWatchHub(1000),
	}
}

// newLeafNode create a new leaf node.
func newLeafNode() *LeafNode {
	return &LeafNode{
		Keys:        make([]string, 0),
		Values:      make([]string, 0),
		ExpireTimes: make([]time.Time, 0),
		Next:        nil,
	}
}

// newInternalNode create a new internal node.
func newInternalNode() *InternalNode {
	return &InternalNode{
		Keys:     make([]string, 0),
		Children: make([]BPlusNode, 0),
	}
}

// Get retrieves a value from the B+ tree (Store interface implementation)
func (t *BPlusTree) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	// For B+ tree, we only support file operations, not directory operations
	// So recursive and sorted are ignored for now
	leaf := t.findLeaf(nodePath)
	if leaf == nil {
		t.Stats.Inc(GetFail)
		return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, 0)
	}

	// Search in the leaf node
	for i, k := range leaf.Keys {
		if k == nodePath {
			// Create event
			e := newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex

			// Set value
			value := leaf.Values[i]
			e.Node.Value = &value

			// Set TTL if exists
			if i < len(leaf.ExpireTimes) && !leaf.ExpireTimes[i].IsZero() {
				e.Node.Expiration = &leaf.ExpireTimes[i]
				ttl := time.Until(leaf.ExpireTimes[i]) / time.Second
				if ttl > 0 {
					e.Node.TTL = int64(ttl)
				}
			}

			t.Stats.Inc(GetSuccess)
			return e, nil
		}
	}

	t.Stats.Inc(GetFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, 0)
}

// findLeaf finds the leaf node containing the specified key
func (t *BPlusTree) findLeaf(key string) *LeafNode {
	if t.root == nil {
		return nil
	}

	node := t.root
	for {
		internalNode, ok := node.(*InternalNode)
		if !ok {
			// If not, it must be a LeafNode, we found it
			return node.(*LeafNode)
		}

		// Find the appropriate child node
		childIndex := t.findChildIndex(internalNode, key)
		if childIndex < 0 || childIndex >= len(internalNode.Children) {
			return nil
		}

		node = internalNode.Children[childIndex]
	}
}

// findChildIndex finds the appropriate child node index in an internal node
func (t *BPlusTree) findChildIndex(node *InternalNode, key string) int {
	if len(node.Children) == 0 {
		return -1
	}
	for i, k := range node.Keys {
		if key < k {
			return i
		}
	}
	return len(node.Children) - 1
}

// Set creates or replaces a key-value pair in the B+ tree (Store interface implementation)
func (t *BPlusTree) Set(nodePath string, dir bool, value string, expireTime time.Time) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// B+ tree only supports files, not directories
	if dir {
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, t.CurrentIndex)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Check if already exists
	leaf := t.findLeaf(nodePath)
	var prevEvent *Event

	if leaf != nil {
		// Check if already exists
		for i, k := range leaf.Keys {
			if k == nodePath {
				// Update existing value
				prevValue := leaf.Values[i]
				prevEvent = newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
				prevEvent.Node.Value = &prevValue

				leaf.Values[i] = value
				leaf.ModifiedIndex = t.CurrentIndex + 1

				// Update TTL
				if i < len(leaf.ExpireTimes) {
					leaf.ExpireTimes[i] = expireTime
				} else {
					// Extend slice if needed
					for len(leaf.ExpireTimes) <= i {
						leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
					}
					leaf.ExpireTimes[i] = expireTime
				}

				// Update TTL heap
				if expireTime.IsZero() {
					t.ttlHeap.removeKey(nodePath)
				} else {
					t.ttlHeap.updateKey(nodePath, expireTime)
				}

				break
			}
		}
	}

	// If not found, insert new
	if prevEvent == nil {
		// Ensure we have a leaf to insert into (tree might be empty)
		if leaf == nil {
			leaf = newLeafNode()
			t.root = leaf
		}
		t.CurrentIndex++
		err := t.insertIntoLeaf(leaf, nodePath, value, t.CurrentIndex, expireTime)
		if err != nil {
			t.Stats.Inc(SetFail)
			return nil, err
		}
		t.size++
	}

	// Create event
	e := newEvent(Set, nodePath, t.CurrentIndex, t.CurrentIndex)
	e.DeimosIndex = t.CurrentIndex
	if prevEvent != nil {
		e.PrevNode = prevEvent.Node
	}
	e.Node.Value = &value

	// Set TTL if exists
	if !expireTime.IsZero() {
		e.Node.Expiration = &expireTime
		ttl := time.Until(expireTime) / time.Second
		if ttl > 0 {
			e.Node.TTL = int64(ttl)
		}
	}

	t.CurrentIndex++
	t.Stats.Inc(SetSuccess)
	t.WatcherHub.notify(e)

	return e, nil
}

// insertIntoLeaf inserts a key-value pair into a leaf node
func (bt *BPlusTree) insertIntoLeaf(leaf *LeafNode, key, value string, createdIndex uint64, expireTime time.Time) error {
	// Find insertion position
	insertIndex := 0
	for i, k := range leaf.Keys {
		if key < k {
			insertIndex = i
			break
		}
		insertIndex = i + 1
	}

	// Insert key-value pair
	leaf.Keys = append(leaf.Keys[:insertIndex], append([]string{key}, leaf.Keys[insertIndex:]...)...)
	leaf.Values = append(leaf.Values[:insertIndex], append([]string{value}, leaf.Values[insertIndex:]...)...)
	// Insert expire time aligned
	if insertIndex < len(leaf.ExpireTimes) {
		leaf.ExpireTimes = append(leaf.ExpireTimes[:insertIndex], append([]time.Time{expireTime}, leaf.ExpireTimes[insertIndex:]...)...)
	} else {
		// pad then append
		for len(leaf.ExpireTimes) < insertIndex {
			leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
		}
		leaf.ExpireTimes = append(leaf.ExpireTimes, expireTime)
	}

	// Update metadata
	leaf.CreatedIndex = createdIndex
	leaf.ModifiedIndex = createdIndex

	// Size has already been incremented in the Insert method

	// Check if splitting is needed
	if len(leaf.Keys) > bt.order {
		bt.splitLeaf(leaf)
	}

	// Add to TTL heap (if expiration time is set)
	if !expireTime.IsZero() {
		bt.ttlHeap.updateKey(key, expireTime)
	}

	return nil
}

// Create creates a new key-value pair in the B+ tree (Store interface implementation)
func (t *BPlusTree) Create(nodePath string, dir bool, value string, unique bool, expireTime time.Time) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// B+ tree only supports files, not directories
	if dir {
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, t.CurrentIndex)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Handle unique flag
	if unique {
		t.CurrentIndex++
		nodePath = nodePath + "/" + fmt.Sprintf("%d", t.CurrentIndex)
	}

	// Check if already exists
	leaf := t.findLeaf(nodePath)
	if leaf != nil {
		for _, k := range leaf.Keys {
			if k == nodePath {
				t.Stats.Inc(CreateFail)
				return nil, Err.NewError(Err.EcodeNodeExist, nodePath, t.CurrentIndex)
			}
		}
	}

	// Create new
	t.CurrentIndex++
	err := t.insertIntoLeaf(leaf, nodePath, value, t.CurrentIndex, expireTime)
	if err != nil {
		t.Stats.Inc(CreateFail)
		return nil, err
	}
	t.size++

	// Create event
	e := newEvent(Create, nodePath, t.CurrentIndex, t.CurrentIndex)
	e.DeimosIndex = t.CurrentIndex
	e.Node.Value = &value

	// Set TTL if exists
	if !expireTime.IsZero() {
		e.Node.Expiration = &expireTime
		ttl := time.Until(expireTime) / time.Second
		if ttl > 0 {
			e.Node.TTL = int64(ttl)
		}
	}

	t.Stats.Inc(CreateSuccess)
	t.WatcherHub.notify(e)

	return e, nil
}

// Update updates a key-value pair in the B+ tree (Store interface implementation)
func (t *BPlusTree) Update(nodePath string, newValue string, expireTime time.Time) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	// Check if exists
	leaf := t.findLeaf(nodePath)
	if leaf == nil {
		t.Stats.Inc(UpdateFail)
		return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
	}

	// Find and update
	for i, k := range leaf.Keys {
		if k == nodePath {
			// Get previous value for event
			prevValue := leaf.Values[i]
			prevEvent := newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
			prevEvent.Node.Value = &prevValue

			// Update value
			leaf.Values[i] = newValue
			t.CurrentIndex++
			leaf.ModifiedIndex = t.CurrentIndex

			// Update TTL
			if i < len(leaf.ExpireTimes) {
				leaf.ExpireTimes[i] = expireTime
			} else {
				// Extend slice if needed
				for len(leaf.ExpireTimes) <= i {
					leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
				}
				leaf.ExpireTimes[i] = expireTime
			}

			// Update TTL heap
			if expireTime.IsZero() {
				t.ttlHeap.removeKey(nodePath)
			} else {
				t.ttlHeap.updateKey(nodePath, expireTime)
			}

			// Create event
			e := newEvent(Update, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node
			e.Node.Value = &newValue

			// Set TTL if exists
			if !expireTime.IsZero() {
				e.Node.Expiration = &expireTime
				ttl := time.Until(expireTime) / time.Second
				if ttl > 0 {
					e.Node.TTL = int64(ttl)
				}
			}

			t.Stats.Inc(UpdateSuccess)
			t.WatcherHub.notify(e)

			return e, nil
		}
	}

	t.Stats.Inc(UpdateFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
}

// Delete removes a key-value pair from the B+ tree (Store interface implementation)
func (t *BPlusTree) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// B+ tree only supports files, not directories
	if dir {
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, t.CurrentIndex)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Check if exists
	leaf := t.findLeaf(nodePath)
	if leaf == nil {
		t.Stats.Inc(DeleteFail)
		return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
	}

	// Find and delete
	for i, k := range leaf.Keys {
		if k == nodePath {
			// Get previous value for event
			prevValue := leaf.Values[i]
			prevEvent := newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
			prevEvent.Node.Value = &prevValue

			// Delete key-value pair
			leaf.Keys = append(leaf.Keys[:i], leaf.Keys[i+1:]...)
			leaf.Values = append(leaf.Values[:i], leaf.Values[i+1:]...)
			if i < len(leaf.ExpireTimes) {
				leaf.ExpireTimes = append(leaf.ExpireTimes[:i], leaf.ExpireTimes[i+1:]...)
			}
			t.size--

			// Remove TTL entry
			t.ttlHeap.removeKey(nodePath)

			// Create event
			e := newEvent(Delete, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node

			t.CurrentIndex++
			t.Stats.Inc(DeleteSuccess)
			t.WatcherHub.notify(e)

			// Check if rebalancing is needed
			if len(leaf.Keys) < t.order/2 && leaf != t.root {
				t.rebalanceLeaf(leaf)
			}

			return e, nil
		}
	}

	t.Stats.Inc(DeleteFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
}

// Watch creates a watcher for the given prefix (Store interface implementation)
func (t *BPlusTree) Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	prefix = path.Clean(path.Join("/", prefix))

	if sinceIndex == 0 {
		sinceIndex = t.CurrentIndex + 1
	}

	// Create watcher using the existing watcherHub
	w, err := t.WatcherHub.watch(prefix, recursive, stream, sinceIndex, t.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// Save saves the current state of the B+ tree (Store interface implementation)
func (t *BPlusTree) Save() ([]byte, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	// For now, return a simple serialization
	// In a real implementation, you might want to serialize the entire tree structure
	data := map[string]interface{}{
		"version": t.CurrentVersion,
		"index":   t.CurrentIndex,
		"size":    t.size,
		"order":   t.order,
	}

	return json.Marshal(data)
}

// Recovery recovers the B+ tree from saved state (Store interface implementation)
func (t *BPlusTree) Recovery(state []byte) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// For now, just parse the basic info
	// In a real implementation, you might want to reconstruct the entire tree
	var data map[string]interface{}
	if err := json.Unmarshal(state, &data); err != nil {
		return err
	}

	if version, ok := data["version"].(float64); ok {
		t.CurrentVersion = int(version)
	}
	if index, ok := data["index"].(float64); ok {
		t.CurrentIndex = uint64(index)
	}
	if size, ok := data["size"].(float64); ok {
		t.size = int(size)
	}
	if order, ok := data["order"].(float64); ok {
		t.order = int(order)
	}

	return nil
}

// TotalTransactions returns the total number of transactions (Store interface implementation)
func (t *BPlusTree) TotalTransactions() uint64 {
	return t.Stats.TotalTranscations()
}

// JsonStats returns statistics in JSON format (Store interface implementation)
func (t *BPlusTree) JsonStats() []byte {
	t.Stats.Watchers = uint64(t.WatcherHub.count)
	return t.Stats.toJson()
}

// CompareAndSwap performs a conditional update (Store interface implementation)
func (t *BPlusTree) CompareAndSwap(nodePath string, prevValue string, prevIndex uint64, value string, expireTime time.Time) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	// Check if exists
	leaf := t.findLeaf(nodePath)
	if leaf == nil {
		t.Stats.Inc(CompareAndSwapFail)
		return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
	}

	// Find and check conditions
	for i, k := range leaf.Keys {
		if k == nodePath {
			// Check previous value and index
			if prevValue != "" && leaf.Values[i] != prevValue {
				t.Stats.Inc(CompareAndSwapFail)
				return nil, Err.NewError(Err.EcodeTestFailed, "value mismatch", t.CurrentIndex)
			}
			if prevIndex != 0 && leaf.ModifiedIndex != prevIndex {
				t.Stats.Inc(CompareAndSwapFail)
				return nil, Err.NewError(Err.EcodeTestFailed, "index mismatch", t.CurrentIndex)
			}

			// Get previous value for event
			prevValue := leaf.Values[i]
			prevEvent := newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
			prevEvent.Node.Value = &prevValue

			// Update value
			leaf.Values[i] = value
			t.CurrentIndex++
			leaf.ModifiedIndex = t.CurrentIndex

			// Update TTL
			if i < len(leaf.ExpireTimes) {
				leaf.ExpireTimes[i] = expireTime
			} else {
				// Extend slice if needed
				for len(leaf.ExpireTimes) <= i {
					leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
				}
				leaf.ExpireTimes[i] = expireTime
			}

			// Update TTL heap
			if expireTime.IsZero() {
				t.ttlHeap.removeKey(nodePath)
			} else {
				t.ttlHeap.updateKey(nodePath, expireTime)
			}

			// Create event
			e := newEvent(CompareAndSwap, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node
			e.Node.Value = &value

			// Set TTL if exists
			if !expireTime.IsZero() {
				e.Node.Expiration = &expireTime
				ttl := time.Until(expireTime) / time.Second
				if ttl > 0 {
					e.Node.TTL = int64(ttl)
				}
			}

			t.Stats.Inc(CompareAndSwapSuccess)
			t.WatcherHub.notify(e)

			return e, nil
		}
	}

	t.Stats.Inc(CompareAndSwapFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
}

// CompareAndDelete performs a conditional deletion (Store interface implementation)
func (t *BPlusTree) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	// Check if exists
	leaf := t.findLeaf(nodePath)
	if leaf == nil {
		t.Stats.Inc(CompareAndDeleteFail)
		return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
	}

	// Find and check conditions
	for i, k := range leaf.Keys {
		if k == nodePath {
			// Check previous value and index
			if prevValue != "" && leaf.Values[i] != prevValue {
				t.Stats.Inc(CompareAndDeleteFail)
				return nil, Err.NewError(Err.EcodeTestFailed, "value mismatch", t.CurrentIndex)
			}
			if prevIndex != 0 && leaf.ModifiedIndex != prevIndex {
				t.Stats.Inc(CompareAndDeleteFail)
				return nil, Err.NewError(Err.EcodeTestFailed, "index mismatch", t.CurrentIndex)
			}

			// Get previous value for event
			prevValue := leaf.Values[i]
			prevEvent := newEvent(Get, nodePath, leaf.ModifiedIndex, leaf.CreatedIndex)
			prevEvent.Node.Value = &prevValue

			// Delete key-value pair
			leaf.Keys = append(leaf.Keys[:i], leaf.Keys[i+1:]...)
			leaf.Values = append(leaf.Values[:i], leaf.Values[i+1:]...)
			if i < len(leaf.ExpireTimes) {
				leaf.ExpireTimes = append(leaf.ExpireTimes[:i], leaf.ExpireTimes[i+1:]...)
			}
			t.size--

			// Remove TTL entry
			t.ttlHeap.removeKey(nodePath)

			// Create event
			e := newEvent(CompareAndDelete, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node

			t.CurrentIndex++
			t.Stats.Inc(CompareAndDeleteSuccess)
			t.WatcherHub.notify(e)

			// Check if rebalancing is needed
			if len(leaf.Keys) < t.order/2 && leaf != t.root {
				t.rebalanceLeaf(leaf)
			}

			return e, nil
		}
	}

	t.Stats.Inc(CompareAndDeleteFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
}

// splitLeaf splits a leaf node
func (t *BPlusTree) splitLeaf(leaf *LeafNode) {
	mid := len(leaf.Keys) / 2

	// Create new leaf node
	newLeaf := newLeafNode()
	newLeaf.Keys = leaf.Keys[mid:]
	newLeaf.Values = leaf.Values[mid:]
	newLeaf.Next = leaf.Next
	// Ensure ExpireTimes array is properly aligned with Keys/Values
	if mid < len(leaf.ExpireTimes) {
		newLeaf.ExpireTimes = leaf.ExpireTimes[mid:]
	} else {
		// If ExpireTimes is shorter, pad with zero times
		for i := 0; i < len(newLeaf.Keys); i++ {
			newLeaf.ExpireTimes = append(newLeaf.ExpireTimes, time.Time{})
		}
	}

	// Update original leaf node
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.Next = newLeaf
	// Ensure ExpireTimes array is properly aligned
	if mid < len(leaf.ExpireTimes) {
		leaf.ExpireTimes = leaf.ExpireTimes[:mid]
	} else {
		// If ExpireTimes is shorter, pad with zero times
		for i := 0; i < len(leaf.Keys); i++ {
			leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
		}
	}

	// If leaf node is root, create new root node
	if leaf == t.root {
		newRoot := newInternalNode()
		newRoot.Keys = []string{newLeaf.Keys[0]}
		newRoot.Children = []BPlusNode{leaf, newLeaf}
		leaf.Parent = newRoot
		newLeaf.Parent = newRoot
		t.root = newRoot
	} else {
		// Insert new key in parent node
		t.insertIntoParent(leaf, newLeaf.Keys[0], newLeaf)
	}
}

// insertIntoParent inserts a new key and child node into the parent node
func (t *BPlusTree) insertIntoParent(leftChild *LeafNode, key string, rightChild *LeafNode) {
	parent := leftChild.Parent
	if parent == nil {
		// If parent node doesn't exist, create new root node
		newRoot := newInternalNode()
		newRoot.Keys = []string{key}
		newRoot.Children = []BPlusNode{leftChild, rightChild}
		leftChild.Parent = newRoot
		rightChild.Parent = newRoot
		t.root = newRoot
		return
	}

	// Find insertion position
	insertIndex := 0
	for i, k := range parent.Keys {
		if key < k {
			insertIndex = i
			break
		}
		insertIndex = i + 1
	}

	// Insert key and child node
	parent.Keys = append(parent.Keys[:insertIndex], append([]string{key}, parent.Keys[insertIndex:]...)...)
	parent.Children = append(parent.Children[:insertIndex+1], append([]BPlusNode{rightChild}, parent.Children[insertIndex+1:]...)...)
	rightChild.Parent = parent

	// Check if parent node splitting is needed
	if len(parent.Keys) > t.order {
		t.splitInternalNode(parent)
	}
}

// splitInternalNode splits an internal node
func (t *BPlusTree) splitInternalNode(node *InternalNode) {
	mid := len(node.Keys) / 2

	// Create new internal node
	newNode := newInternalNode()
	newNode.Keys = node.Keys[mid+1:]
	newNode.Children = node.Children[mid+1:]

	// Update parent references of child nodes
	for _, child := range newNode.Children {
		if leaf, ok := child.(*LeafNode); ok {
			leaf.Parent = newNode
		} else if internal, ok := child.(*InternalNode); ok {
			internal.Parent = newNode
		}
	}

	// Update original node
	promoteKey := node.Keys[mid]
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]

	// If node is root, create new root node
	if node == t.root {
		newRoot := newInternalNode()
		newRoot.Keys = []string{promoteKey}
		newRoot.Children = []BPlusNode{node, newNode}
		node.Parent = newRoot
		newNode.Parent = newRoot
		t.root = newRoot
	} else {
		// Insert new key in parent node
		t.insertIntoParentInternal(node, promoteKey, newNode)
	}
}

// insertIntoParentInternal inserts a new key and child node into the parent of an internal node
func (t *BPlusTree) insertIntoParentInternal(leftChild *InternalNode, key string, rightChild *InternalNode) {
	parent := leftChild.Parent
	if parent == nil {
		// If parent node doesn't exist, create new root node
		newRoot := newInternalNode()
		newRoot.Keys = []string{key}
		newRoot.Children = []BPlusNode{leftChild, rightChild}
		leftChild.Parent = newRoot
		rightChild.Parent = newRoot
		t.root = newRoot
		return
	}

	// Find insertion position
	insertIndex := 0
	for i, k := range parent.Keys {
		if key < k {
			insertIndex = i
			break
		}
		insertIndex = i + 1
	}

	// Insert key and child node
	parent.Keys = append(parent.Keys[:insertIndex], append([]string{key}, parent.Keys[insertIndex:]...)...)
	parent.Children = append(parent.Children[:insertIndex+1], append([]BPlusNode{rightChild}, parent.Children[insertIndex+1:]...)...)
	rightChild.Parent = parent

	// Check if parent node splitting is needed
	if len(parent.Keys) > t.order {
		t.splitInternalNode(parent)
	}
}

// RangeScan performs a range scan
func (t *BPlusTree) RangeScan(start, end string) []string {
	result := make([]string, 0)

	// Find the starting leaf node
	startLeaf := t.findLeaf(start)
	if startLeaf == nil {
		return result
	}

	// Traverse the leaf node linked list
	current := startLeaf
	for current != nil {
		for i, key := range current.Keys {
			if key >= start && key <= end {
				result = append(result, current.Values[i])
			}
			if key > end {
				return result
			}
		}
		current = current.Next
	}

	return result
}

// PrefixScan performs a prefix scan
func (t *BPlusTree) PrefixScan(prefix string) []string {
	result := make([]string, 0)

	// Find the starting leaf node for the prefix
	startLeaf := t.findLeaf(prefix)
	if startLeaf == nil {
		return result
	}

	// Traverse the leaf node linked list
	current := startLeaf
	for current != nil {
		for i, key := range current.Keys {
			if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
				result = append(result, current.Values[i])
			} else if key > prefix && !(len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
				// If key is greater than prefix and doesn't match, we're out of range
				return result
			}
		}
		current = current.Next
	}

	return result
}

// GetAllKeys retrieves all keys
func (t *BPlusTree) GetAllKeys() []string {
	result := make([]string, 0)

	if t.root == nil {
		return result
	}

	// Find the leftmost leaf node
	leftmost := t.findLeftmostLeaf()
	if leftmost == nil {
		return result
	}

	// Traverse all leaf nodes
	current := leftmost
	for current != nil {
		result = append(result, current.Keys...)
		current = current.Next
	}

	return result
}

// findLeftmostLeaf finds the leftmost leaf node
func (t *BPlusTree) findLeftmostLeaf() *LeafNode {
	if t.root == nil {
		return nil
	}

	node := t.root
	for {
		if internalNode, ok := node.(*InternalNode); ok {
			if len(internalNode.Children) == 0 {
				return nil
			}
			node = internalNode.Children[0]
		} else {
			return node.(*LeafNode)
		}
	}
}

// rebalanceLeaf rebalances a leaf node
func (t *BPlusTree) rebalanceLeaf(leaf *LeafNode) {
	parent := leaf.Parent
	if parent == nil {
		return
	}

	// Find the index of the leaf node in the parent node
	childIndex := -1
	for i, child := range parent.Children {
		if child == leaf {
			childIndex = i
			break
		}
	}

	if childIndex == -1 {
		return
	}

	// Try to borrow a key from the left sibling node
	if childIndex > 0 {
		leftSibling := parent.Children[childIndex-1].(*LeafNode)
		if len(leftSibling.Keys) > t.order/2 {
			// Borrow a key from the left sibling
			borrowedKey := leftSibling.Keys[len(leftSibling.Keys)-1]
			borrowedValue := leftSibling.Values[len(leftSibling.Values)-1]
			var borrowedExpire time.Time
			if len(leftSibling.ExpireTimes) > 0 {
				borrowedExpire = leftSibling.ExpireTimes[len(leftSibling.ExpireTimes)-1]
				leftSibling.ExpireTimes = leftSibling.ExpireTimes[:len(leftSibling.ExpireTimes)-1]
			}

			leftSibling.Keys = leftSibling.Keys[:len(leftSibling.Keys)-1]
			leftSibling.Values = leftSibling.Values[:len(leftSibling.Values)-1]

			leaf.Keys = append([]string{borrowedKey}, leaf.Keys...)
			leaf.Values = append([]string{borrowedValue}, leaf.Values...)
			leaf.ExpireTimes = append([]time.Time{borrowedExpire}, leaf.ExpireTimes...)

			// Update the key in the parent node
			parent.Keys[childIndex-1] = leaf.Keys[0]
			return
		}
	}

	// Try to borrow a key from the right sibling node
	if childIndex < len(parent.Children)-1 {
		rightSibling := parent.Children[childIndex+1].(*LeafNode)
		if len(rightSibling.Keys) > t.order/2 {
			// Borrow a key from the right sibling
			borrowedKey := rightSibling.Keys[0]
			borrowedValue := rightSibling.Values[0]
			var borrowedExpire time.Time
			if len(rightSibling.ExpireTimes) > 0 {
				borrowedExpire = rightSibling.ExpireTimes[0]
				rightSibling.ExpireTimes = rightSibling.ExpireTimes[1:]
			}

			rightSibling.Keys = rightSibling.Keys[1:]
			rightSibling.Values = rightSibling.Values[1:]

			leaf.Keys = append(leaf.Keys, borrowedKey)
			leaf.Values = append(leaf.Values, borrowedValue)
			leaf.ExpireTimes = append(leaf.ExpireTimes, borrowedExpire)

			// Update the key in the parent node
			parent.Keys[childIndex] = rightSibling.Keys[0]
			return
		}
	}

	// If unable to borrow keys, merge nodes
	if childIndex > 0 {
		// Merge with left sibling
		t.mergeWithLeftSibling(leaf, childIndex)
	} else if childIndex < len(parent.Children)-1 {
		// Merge with right sibling
		t.mergeWithRightSibling(leaf, childIndex)
	}
}

// mergeWithLeftSibling merges with the left sibling node
func (t *BPlusTree) mergeWithLeftSibling(leaf *LeafNode, childIndex int) {
	parent := leaf.Parent
	leftSibling := parent.Children[childIndex-1].(*LeafNode)

	// Move key-value pairs from current node to left sibling
	leftSibling.Keys = append(leftSibling.Keys, leaf.Keys...)
	leftSibling.Values = append(leftSibling.Values, leaf.Values...)
	leftSibling.ExpireTimes = append(leftSibling.ExpireTimes, leaf.ExpireTimes...)
	leftSibling.Next = leaf.Next

	// Remove current node from parent node
	parent.Keys = append(parent.Keys[:childIndex-1], parent.Keys[childIndex:]...)
	parent.Children = append(parent.Children[:childIndex], parent.Children[childIndex+1:]...)

	// If parent is root and empty, update root node
	if parent == t.root && len(parent.Keys) == 0 {
		t.root = leftSibling
		leftSibling.Parent = nil
	}
}

// mergeWithRightSibling merges with the right sibling node
func (t *BPlusTree) mergeWithRightSibling(leaf *LeafNode, childIndex int) {
	parent := leaf.Parent
	rightSibling := parent.Children[childIndex+1].(*LeafNode)

	// Move key-value pairs from right sibling to current node
	leaf.Keys = append(leaf.Keys, rightSibling.Keys...)
	leaf.Values = append(leaf.Values, rightSibling.Values...)
	leaf.ExpireTimes = append(leaf.ExpireTimes, rightSibling.ExpireTimes...)
	leaf.Next = rightSibling.Next

	// Remove right sibling node from parent node
	parent.Keys = append(parent.Keys[:childIndex], parent.Keys[childIndex+1:]...)
	parent.Children = append(parent.Children[:childIndex+1], parent.Children[childIndex+2:]...)

	// If parent is root and empty, update root node
	if parent == t.root && len(parent.Keys) == 0 {
		t.root = leaf
		leaf.Parent = nil
	}
}

// Exists checks if a key exists
func (t *BPlusTree) Exists(key string) bool {
	leaf := t.findLeaf(key)
	if leaf == nil {
		return false
	}

	for _, k := range leaf.Keys {
		if k == key {
			return true
		}
	}
	return false
}

func (t *BPlusTree) Size() int {
	return t.size
}

// Store interface implementation

// Version retrieves current version of the B+ tree store.
func (t *BPlusTree) Version() int {
	return t.CurrentVersion
}

// Index retrieves current index of the B+ tree store.
func (t *BPlusTree) Index() uint64 {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.CurrentIndex
}

// Height returns the height of the tree
func (t *BPlusTree) Height() int {
	if t.root == nil {
		return 0
	}

	height := 1
	node := t.root
	for {
		if internalNode, ok := node.(*InternalNode); ok {
			if len(internalNode.Children) == 0 {
				break
			}
			node = internalNode.Children[0]
			height++
		} else {
			break
		}
	}
	return height
}

// GetStats retrieves statistics information
func (t *BPlusTree) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"size":   t.Size(),
		"height": t.Height(),
		"order":  t.order,
	}
}

// UpdateTTL updates the TTL for a given key. If expireTime is zero, TTL is removed.
func (t *BPlusTree) UpdateTTL(key string, expireTime time.Time) error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	leaf := t.findLeaf(key)
	if leaf == nil {
		return Err.NewError(Err.EcodeKeyNotFound, key, 0)
	}
	for i, k := range leaf.Keys {
		if k == key {
			if i >= len(leaf.ExpireTimes) {
				// grow slice if necessary
				for len(leaf.ExpireTimes) <= i {
					leaf.ExpireTimes = append(leaf.ExpireTimes, time.Time{})
				}
			}
			leaf.ExpireTimes[i] = expireTime
			if expireTime.IsZero() {
				t.ttlHeap.removeKey(key)
			} else {
				t.ttlHeap.updateKey(key, expireTime)
			}
			return nil
		}
	}
	return Err.NewError(Err.EcodeKeyNotFound, key, 0)
}

// DeleteExpiredKeys removes all keys with expiration before or at cutoff
func (t *BPlusTree) DeleteExpiredKeys(cutoff time.Time) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	for {
		top := t.ttlHeap.top()
		if top == nil || top.expireTime.After(cutoff) {
			break
		}
		// find and delete this key
		key := top.key
		t.ttlHeap.pop()

		leaf := t.findLeaf(key)
		if leaf == nil {
			continue
		}
		deleteIndex := -1
		for i, k := range leaf.Keys {
			if k == key {
				deleteIndex = i
				break
			}
		}
		if deleteIndex == -1 {
			continue
		}
		leaf.Keys = append(leaf.Keys[:deleteIndex], leaf.Keys[deleteIndex+1:]...)
		leaf.Values = append(leaf.Values[:deleteIndex], leaf.Values[deleteIndex+1:]...)
		if deleteIndex < len(leaf.ExpireTimes) {
			leaf.ExpireTimes = append(leaf.ExpireTimes[:deleteIndex], leaf.ExpireTimes[deleteIndex+1:]...)
		}
		t.size--

		if len(leaf.Keys) < t.order/2 && leaf != t.root {
			t.rebalanceLeaf(leaf)
		}
	}
}
