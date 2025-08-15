package store

import (
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
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
	Keys     []string      `json:"keys"`
	Parent   *InternalNode `json:"-"` // Ignore circular references
	Children []BPlusNode   `json:"children"`

	CreatedIndex  uint64    `json:"createdIndex"`
	ModifiedIndex uint64    `json:"modifiedIndex"`
	ExpireTime    time.Time `json:"expireTime,omitempty"`
	ACL           string    `json:"acl,omitempty"`

	// Concurrency control
	mu sync.RWMutex `json:"-"`
}

type LeafNode struct {
	Parent *InternalNode `json:"-"` // Ignore circular references
	Keys   []string      `json:"keys"`
	Values []string      `json:"values"`
	Next   *LeafNode     `json:"-"` // Ignore circular references

	CreatedIndex  uint64 `json:"createdIndex"`
	ModifiedIndex uint64 `json:"modifiedIndex"`
	// Per-entry expiration times aligned with Keys/Values
	ExpireTimes []time.Time `json:"expireTimes,omitempty"`
	ACL         string      `json:"acl,omitempty"`

	// Concurrency control
	mu sync.RWMutex `json:"-"`
}

// BPlusTree represents a B+ tree that implements the Store interface.
//
// Concurrency Notes:
// - This implementation uses a single global mutex for all operations:
//   - All operations (Create, Get, Set, Update, Delete) use the same mutex
//   - This ensures complete serialization and eliminates all race conditions
//   - Split operations are atomic since they're protected by the same mutex
//   - Performance is sacrificed for correctness and simplicity
type BPlusTree struct {
	root     BPlusNode `json:"-"` // Serialized through treeData field
	Order    int       `json:"order"`
	TreeSize int64     `json:"size"` // Changed to int64 for atomic operations

	// Concurrency control - single global mutex for all operations
	mu sync.Mutex `json:"-"`

	// For TTL management
	TTLHeap *bpTtlKeyHeap `json:"ttlHeap"`

	// Store interface requirements
	CurrentIndex   uint64      `json:"currentIndex"`
	CurrentVersion int         `json:"currentVersion"`
	Stats          *Stats      `json:"stats"`
	WatcherHub     *watcherHub `json:"watcherHub"`
}

// NewBPlusTree create a new B+ tree
func NewBPlusTree() *BPlusTree {
	return &BPlusTree{
		root:           newLeafNode(),
		Order:          DefaultOrder,
		TreeSize:       0,
		TTLHeap:        newBpTtlKeyHeap(),
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
// This method uses global mutex for complete serialization
func (t *BPlusTree) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	// Use global mutex for complete serialization
	t.mu.Lock()
	defer t.mu.Unlock()

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
// This method assumes the caller holds the global mutex
func (t *BPlusTree) findLeaf(key string) *LeafNode {
	if t.root == nil {
		return nil
	}

	// Create a snapshot of the tree structure to avoid race conditions
	root := t.root
	node := root

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

		// Get the child node from the snapshot
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
	// B+ tree only supports files, not directories
	if dir {
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, t.CurrentIndex)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Use global mutex for tree modification
	t.mu.Lock()
	defer t.mu.Unlock()

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
					t.TTLHeap.removeKey(nodePath)
				} else {
					t.TTLHeap.updateKey(nodePath, expireTime)
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
		atomic.AddInt64(&t.TreeSize, 1)
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

	t.Stats.Inc(SetSuccess)
	t.WatcherHub.notify(e)

	return e, nil
}

// insertIntoLeaf inserts a key-value pair into a leaf node
// This method is designed to be atomic and concurrency-safe
func (bt *BPlusTree) insertIntoLeaf(leaf *LeafNode, key, value string, createdIndex uint64, expireTime time.Time) error {
	// Find insertion position
	insertIndex := len(leaf.Keys)
	for i, k := range leaf.Keys {
		if key < k {
			insertIndex = i
			break
		}
	}

	// Create new slices to avoid race conditions
	newKeys := make([]string, len(leaf.Keys)+1)
	newValues := make([]string, len(leaf.Values)+1)
	newExpireTimes := make([]time.Time, len(leaf.ExpireTimes)+1)

	// Copy elements before insertion point
	copy(newKeys[:insertIndex], leaf.Keys[:insertIndex])
	copy(newValues[:insertIndex], leaf.Values[:insertIndex])
	if insertIndex < len(leaf.ExpireTimes) {
		copy(newExpireTimes[:insertIndex], leaf.ExpireTimes[:insertIndex])
	}

	// Insert new element
	newKeys[insertIndex] = key
	newValues[insertIndex] = value
	newExpireTimes[insertIndex] = expireTime

	// Copy elements after insertion point
	copy(newKeys[insertIndex+1:], leaf.Keys[insertIndex:])
	copy(newValues[insertIndex+1:], leaf.Values[insertIndex:])
	if insertIndex < len(leaf.ExpireTimes) {
		copy(newExpireTimes[insertIndex+1:], leaf.ExpireTimes[insertIndex:])
	}

	// Update the leaf node atomically
	leaf.Keys = newKeys
	leaf.Values = newValues
	leaf.ExpireTimes = newExpireTimes

	// Update metadata
	leaf.CreatedIndex = createdIndex
	leaf.ModifiedIndex = createdIndex

	// Check if splitting is needed
	if len(leaf.Keys) > bt.Order {
		bt.splitLeaf(leaf)
	}

	// Add to TTL heap (if expiration time is set)
	if !expireTime.IsZero() {
		bt.TTLHeap.updateKey(key, expireTime)
	}

	return nil
}

// Create creates a new key-value pair in the B+ tree (Store interface implementation)
// This method uses strict locking for maximum consistency
func (t *BPlusTree) Create(nodePath string, dir bool, value string, unique bool, expireTime time.Time) (*Event, error) {
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

	// Use global mutex for tree modification - ensure complete atomicity
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already exists - search in the entire tree
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

	// Ensure we have a leaf to insert into (tree might be empty)
	if leaf == nil {
		leaf = newLeafNode()
		t.root = leaf
	}

	// Insert the key-value pair
	err := t.insertIntoLeaf(leaf, nodePath, value, t.CurrentIndex, expireTime)
	if err != nil {
		t.Stats.Inc(CreateFail)
		return nil, err
	}

	// Update size atomically
	atomic.AddInt64(&t.TreeSize, 1)

	// Verify the key was inserted correctly - search in the entire tree after potential splits
	found := false
	verifyLeaf := t.findLeaf(nodePath)
	if verifyLeaf != nil {
		for _, k := range verifyLeaf.Keys {
			if k == nodePath {
				found = true
				break
			}
		}
	}

	if !found {
		t.Stats.Inc(CreateFail)
		return nil, fmt.Errorf("key not found in tree after insertion: %s", nodePath)
	}

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
	nodePath = path.Clean(path.Join("/", nodePath))

	// Use global mutex for tree modification
	t.mu.Lock()
	defer t.mu.Unlock()

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
				t.TTLHeap.removeKey(nodePath)
			} else {
				t.TTLHeap.updateKey(nodePath, expireTime)
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
	// B+ tree only supports files, not directories
	if dir {
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, t.CurrentIndex)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

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
			t.TreeSize--

			// Remove TTL entry
			t.TTLHeap.removeKey(nodePath)

			// Create event
			e := newEvent(Delete, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node

			t.CurrentIndex++
			t.Stats.Inc(DeleteSuccess)
			t.WatcherHub.notify(e)

			// Check if rebalancing is needed
			if len(leaf.Keys) < t.Order/2 && leaf != t.root {
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
	prefix = path.Clean(path.Join("/", prefix))

	if sinceIndex == 0 {
		sinceIndex = t.CurrentIndex + 1
	}

	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

	// Create watcher using the existing watcherHub
	w, err := t.WatcherHub.watch(prefix, recursive, stream, sinceIndex, t.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// Save saves the current state of the B+ tree (Store interface implementation)
func (t *BPlusTree) Save() ([]byte, error) {
	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

	// Create a serializable structure for serialization
	saveStruct := struct {
		Order          int                    `json:"order"`
		Size           int                    `json:"size"`
		CurrentIndex   uint64                 `json:"currentIndex"`
		CurrentVersion int                    `json:"currentVersion"`
		Stats          *Stats                 `json:"stats"`
		WatcherHub     *watcherHub            `json:"watcherHub"`
		TTLHeap        *bpTtlKeyHeap          `json:"ttlHeap"`
		TreeData       map[string]interface{} `json:"treeData"`
	}{
		Order:          t.Order,
		Size:           int(t.TreeSize),
		CurrentIndex:   t.CurrentIndex,
		CurrentVersion: t.CurrentVersion,
		Stats:          t.Stats.clone(),
		WatcherHub:     t.WatcherHub.clone(),
		TTLHeap:        t.TTLHeap.clone(),
		TreeData:       t.serializeTreeData(),
	}

	return json.Marshal(saveStruct)
}

// serializeTreeData converts the tree structure to a serializable format
func (t *BPlusTree) serializeTreeData() map[string]interface{} {
	if t.root == nil {
		return nil
	}

	return t.serializeNode(t.root)
}

// serializeNode recursively serializes a BPlusNode
func (t *BPlusTree) serializeNode(node BPlusNode) map[string]interface{} {
	if node == nil {
		return nil
	}

	switch n := node.(type) {
	case *InternalNode:
		children := make([]map[string]interface{}, len(n.Children))
		for i, child := range n.Children {
			children[i] = t.serializeNode(child)
		}

		return map[string]interface{}{
			"type":          "internal",
			"keys":          n.Keys,
			"children":      children,
			"createdIndex":  n.CreatedIndex,
			"modifiedIndex": n.ModifiedIndex,
			"expireTime":    n.ExpireTime,
			"acl":           n.ACL,
		}

	case *LeafNode:
		return map[string]interface{}{
			"type":          "leaf",
			"keys":          n.Keys,
			"values":        n.Values,
			"expireTimes":   n.ExpireTimes,
			"createdIndex":  n.CreatedIndex,
			"modifiedIndex": n.ModifiedIndex,
			"acl":           n.ACL,
		}

	default:
		return nil
	}
}

// Recovery recovers the B+ tree from saved state (Store interface implementation)
func (t *BPlusTree) Recovery(state []byte) error {
	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

	// Define structure for deserialization
	var saveStruct struct {
		Order          int                    `json:"order"`
		Size           int                    `json:"size"`
		CurrentIndex   uint64                 `json:"currentIndex"`
		CurrentVersion int                    `json:"currentVersion"`
		Stats          *Stats                 `json:"stats"`
		WatcherHub     *watcherHub            `json:"watcherHub"`
		TTLHeap        *bpTtlKeyHeap          `json:"ttlHeap"`
		TreeData       map[string]interface{} `json:"treeData"`
	}

	// Deserialize
	if err := json.Unmarshal(state, &saveStruct); err != nil {
		return err
	}

	// Restore fields
	t.Order = saveStruct.Order
	t.TreeSize = int64(saveStruct.Size)
	t.CurrentIndex = saveStruct.CurrentIndex
	t.CurrentVersion = saveStruct.CurrentVersion
	t.Stats = saveStruct.Stats
	t.WatcherHub = saveStruct.WatcherHub
	t.TTLHeap = saveStruct.TTLHeap

	// Restore tree structure
	if saveStruct.TreeData != nil {
		t.root = t.deserializeNode(saveStruct.TreeData)
	}

	// Ensure TTL heap is properly initialized
	if t.TTLHeap != nil {
		t.TTLHeap.ensureInitialized()
	}

	return nil
}

// deserializeNode reconstructs a BPlusNode from serialized data
func (t *BPlusTree) deserializeNode(data map[string]interface{}) BPlusNode {
	if data == nil {
		return nil
	}

	nodeType, ok := data["type"].(string)
	if !ok {
		return nil
	}

	switch nodeType {
	case "internal":
		node := &InternalNode{
			CreatedIndex:  uint64(data["createdIndex"].(float64)),
			ModifiedIndex: uint64(data["modifiedIndex"].(float64)),
			ACL:           data["acl"].(string),
		}

		// Restore keys
		if keys, ok := data["keys"].([]interface{}); ok {
			node.Keys = make([]string, len(keys))
			for i, key := range keys {
				node.Keys[i] = key.(string)
			}
		}

		// Restore children
		if children, ok := data["children"].([]interface{}); ok {
			node.Children = make([]BPlusNode, len(children))
			for i, childData := range children {
				if childMap, ok := childData.(map[string]interface{}); ok {
					node.Children[i] = t.deserializeNode(childMap)
				}
			}
		}

		// Restore expire time
		if expireTime, ok := data["expireTime"].(string); ok && expireTime != "" {
			if parsed, err := time.Parse(time.RFC3339, expireTime); err == nil {
				node.ExpireTime = parsed
			}
		}

		return node

	case "leaf":
		node := &LeafNode{
			CreatedIndex:  uint64(data["createdIndex"].(float64)),
			ModifiedIndex: uint64(data["modifiedIndex"].(float64)),
			ACL:           data["acl"].(string),
		}

		// Restore keys
		if keys, ok := data["keys"].([]interface{}); ok {
			node.Keys = make([]string, len(keys))
			for i, key := range keys {
				node.Keys[i] = key.(string)
			}
		}

		// Restore values
		if values, ok := data["values"].([]interface{}); ok {
			node.Values = make([]string, len(values))
			for i, value := range values {
				node.Values[i] = value.(string)
			}
		}

		// Restore expire times
		if expireTimes, ok := data["expireTimes"].([]interface{}); ok {
			node.ExpireTimes = make([]time.Time, len(expireTimes))
			for i, expireTime := range expireTimes {
				if expireStr, ok := expireTime.(string); ok && expireStr != "" {
					if parsed, err := time.Parse(time.RFC3339, expireStr); err == nil {
						node.ExpireTimes[i] = parsed
					}
				}
			}
		}

		return node

	default:
		return nil
	}
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
	nodePath = path.Clean(path.Join("/", nodePath))

	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

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
				t.TTLHeap.removeKey(nodePath)
			} else {
				t.TTLHeap.updateKey(nodePath, expireTime)
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
	nodePath = path.Clean(path.Join("/", nodePath))

	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

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
			atomic.AddInt64(&t.TreeSize, -1)

			// Remove TTL entry
			t.TTLHeap.removeKey(nodePath)

			// Create event
			e := newEvent(CompareAndDelete, nodePath, t.CurrentIndex, leaf.CreatedIndex)
			e.DeimosIndex = t.CurrentIndex
			e.PrevNode = prevEvent.Node

			t.CurrentIndex++
			t.Stats.Inc(CompareAndDeleteSuccess)
			t.WatcherHub.notify(e)

			// Check if rebalancing is needed
			if len(leaf.Keys) < t.Order/2 && leaf != t.root {
				t.rebalanceLeaf(leaf)
			}

			return e, nil
		}
	}

	t.Stats.Inc(CompareAndDeleteFail)
	return nil, Err.NewError(Err.EcodeKeyNotFound, nodePath, t.CurrentIndex)
}

// splitLeaf splits a leaf node when it becomes too full
// This method is designed to be atomic and concurrency-safe
func (bt *BPlusTree) splitLeaf(leaf *LeafNode) {
	// For B+ tree, we split at the middle
	mid := len(leaf.Keys) / 2

	// Create new leaf node
	newLeaf := newLeafNode()

	// Copy the second half to the new leaf
	newLeaf.Keys = make([]string, len(leaf.Keys)-mid)
	newLeaf.Values = make([]string, len(leaf.Values)-mid)
	newLeaf.ExpireTimes = make([]time.Time, len(leaf.ExpireTimes)-mid)

	copy(newLeaf.Keys, leaf.Keys[mid:])
	copy(newLeaf.Values, leaf.Values[mid:])
	copy(newLeaf.ExpireTimes, leaf.ExpireTimes[mid:])

	// Truncate the original leaf to keep only the first half
	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]
	leaf.ExpireTimes = leaf.ExpireTimes[:mid]

	// Update metadata
	newLeaf.CreatedIndex = leaf.CreatedIndex
	newLeaf.ModifiedIndex = leaf.ModifiedIndex
	leaf.ModifiedIndex = bt.CurrentIndex

	// Update Next pointers atomically to maintain linked list consistency
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf

	// Update parent pointers
	if leaf.Parent == nil {
		// Create new root
		newRoot := newInternalNode()
		newRoot.Keys = []string{newLeaf.Keys[0]}
		newRoot.Children = []BPlusNode{leaf, newLeaf}
		leaf.Parent = newRoot
		newLeaf.Parent = newRoot
		bt.root = newRoot
	} else {
		// Insert into existing parent
		bt.insertIntoInternal(leaf.Parent, newLeaf.Keys[0], newLeaf)
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
	insertIndex := len(parent.Keys)
	for i, k := range parent.Keys {
		if key < k {
			insertIndex = i
			break
		}
	}

	// Insert key and child node
	parent.Keys = append(parent.Keys[:insertIndex], append([]string{key}, parent.Keys[insertIndex:]...)...)
	parent.Children = append(parent.Children[:insertIndex+1], append([]BPlusNode{rightChild}, parent.Children[insertIndex+1:]...)...)
	rightChild.Parent = parent

	// Check if parent node splitting is needed
	if len(parent.Keys) > t.Order {
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
		if len(leftSibling.Keys) > t.Order/2 {
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
		if len(rightSibling.Keys) > t.Order/2 {
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
	return int(atomic.LoadInt64(&t.TreeSize))
}

// Store interface implementation

// Version retrieves current version of the B+ tree store.
func (t *BPlusTree) Version() int {
	return t.CurrentVersion
}

// Index retrieves current index of the B+ tree store.
func (t *BPlusTree) Index() uint64 {
	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()
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
		"order":  t.Order,
	}
}

// UpdateTTL updates the TTL for a given key. If expireTime is zero, TTL is removed.
func (t *BPlusTree) UpdateTTL(key string, expireTime time.Time) error {
	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

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
				t.TTLHeap.removeKey(key)
			} else {
				t.TTLHeap.updateKey(key, expireTime)
			}
			return nil
		}
	}
	return Err.NewError(Err.EcodeKeyNotFound, key, 0)
}

// DeleteExpiredKeys removes all keys with expiration before or at cutoff
func (t *BPlusTree) DeleteExpiredKeys(cutoff time.Time) {
	// Use single mutex for all operations
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		top := t.TTLHeap.top()
		if top == nil || top.expireTime.After(cutoff) {
			break
		}
		// find and delete this key
		key := top.key
		t.TTLHeap.pop()

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
		atomic.AddInt64(&t.TreeSize, -1)

		if len(leaf.Keys) < t.Order/2 && leaf != t.root {
			t.rebalanceLeaf(leaf)
		}
	}
}

// insertIntoInternal inserts a key and child into an internal node
func (bt *BPlusTree) insertIntoInternal(node *InternalNode, key string, child BPlusNode) {
	// Find insertion position
	insertIndex := len(node.Keys)
	for i, k := range node.Keys {
		if key < k {
			insertIndex = i
			break
		}
	}

	// Create new slices to avoid race conditions
	newKeys := make([]string, len(node.Keys)+1)
	newChildren := make([]BPlusNode, len(node.Children)+1)

	// Copy elements before insertion point
	copy(newKeys[:insertIndex], node.Keys[:insertIndex])
	copy(newChildren[:insertIndex], node.Children[:insertIndex])

	// Insert new element
	newKeys[insertIndex] = key
	newChildren[insertIndex] = child

	// Copy elements after insertion point
	copy(newKeys[insertIndex+1:], node.Keys[insertIndex:])
	copy(newChildren[insertIndex+1:], node.Children[insertIndex:])

	// Update the internal node atomically
	node.Keys = newKeys
	node.Children = newChildren

	// Update parent pointers
	if leaf, ok := child.(*LeafNode); ok {
		leaf.Parent = node
	} else if internal, ok := child.(*InternalNode); ok {
		internal.Parent = node
	}

	// Check if splitting is needed
	if len(node.Keys) > bt.Order {
		bt.splitInternal(node)
	}
}

// splitInternal splits an internal node when it becomes too full
func (bt *BPlusTree) splitInternal(node *InternalNode) {
	mid := len(node.Keys) / 2

	// Create new internal node
	newNode := newInternalNode()
	newNode.Keys = make([]string, len(node.Keys)-mid-1)
	newNode.Children = make([]BPlusNode, len(node.Children)-mid-1)

	// Copy half of the data to the new node
	copy(newNode.Keys, node.Keys[mid+1:])
	copy(newNode.Children, node.Children[mid+1:])

	// Save original data before modifying
	originalKeys := node.Keys[:mid]
	originalChildren := node.Children[:mid+1]

	// Update the original node
	midKey := node.Keys[mid]
	node.Keys = make([]string, mid)
	node.Children = make([]BPlusNode, mid+1)
	copy(node.Keys, originalKeys)
	copy(node.Children, originalChildren)

	// Update parent pointers
	for _, child := range newNode.Children {
		if leaf, ok := child.(*LeafNode); ok {
			leaf.Parent = newNode
		} else if internal, ok := child.(*InternalNode); ok {
			internal.Parent = newNode
		}
	}

	// Insert mid key into parent
	if node.Parent == nil {
		// Create new root
		newRoot := newInternalNode()
		newRoot.Keys = []string{midKey}
		newRoot.Children = []BPlusNode{node, newNode}
		node.Parent = newRoot
		newNode.Parent = newRoot
		bt.root = newRoot
	} else {
		// Insert into existing parent
		bt.insertIntoInternal(node.Parent, midKey, newNode)
	}

	// Update hash table after tree structure changes
	// bt.updateKeyMap() // Removed as per edit hint
}
