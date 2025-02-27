package store

import (
	"path"
	"sort"
	"strings"
	"time"
)

// A file system like tree structure. Each non-leaf node of the tree has a hashmap to
// store its children nodes. Leaf nodes has no hashmap (a nil pointer)
type tree struct {
	Root *treeNode
}

// A treeNode wraps a Node. It has a hashmap to keep records of its children treeNodes.
type treeNode struct {
	InternalNode Node
	Dir          bool
	NodeMap      map[string]*treeNode
}

// TreeNode with its key. We use it when we need to sort the treeNodes.
type tnWithKey struct {
	key string
	tn  *treeNode
}

// Define type and functions to match sort interface
type tnWithKeySlice []tnWithKey

func (t tnWithKeySlice) Len() int           { return len(t) }
func (t tnWithKeySlice) Less(i, j int) bool { return t[i].key < t[j].key }
func (t tnWithKeySlice) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }

// represent an empty node
var emptyNode = Node{"", PERMANENT, nil}

// Set the key to the given value, return true if success
// If any intermidate path of the key is not a directory type, it will fail
// For example if the /foo = Node(bar) exists, set /foo/foo = Node(barbar)
// will fail.
func (t *tree) set(key string, value Node) bool {
	nodesName := split(key)

	// avoid set value to "/"
	if len(nodesName) == 1 && len(nodesName[0]) == 0 {
		return false
	}

	nodeMap := t.Root.NodeMap

	i := 0
	newDir := false

	for i = 0; i < len(nodesName)-1; i++ {
		if newDir {
			tn := &treeNode{
				InternalNode: emptyNode,
				Dir:          true,
				NodeMap:      make(map[string]*treeNode),
			}
			nodeMap[nodesName[i]] = tn
			nodeMap = tn.NodeMap
			continue
		}

		// get the node from nodeMap of the current level
		tn, ok := nodeMap[nodesName[i]]
		if !ok {
			// add a new directory and set newDir to true
			newDir = true
			tn := &treeNode{
				InternalNode: emptyNode,
				Dir:          true,
				NodeMap:      make(map[string]*treeNode),
			}
			nodeMap[nodesName[i]] = tn
			nodeMap = tn.NodeMap
		} else if ok && !tn.Dir {
			// if we meet a non-directory node, we cannot set the key
			return false
		} else {
			nodeMap = tn.NodeMap
		}
	}

	// Add the last tn
	tn, ok := nodeMap[nodesName[i]]
	if !ok {
		// we add a new treeNode
		tn := &treeNode{
			InternalNode: value,
			Dir:          false,
			NodeMap:      nil,
		}
		nodeMap[nodesName[i]] = tn
	} else {
		// we change the value of a old TreeNode
		tn.InternalNode = value
	}
	return true
}

// Get the tree node of the key
func (t *tree) internalGet(key string) (*treeNode, bool) {
	nodesName := split(key)

	// should be able to get root
	if len(nodesName) == 1 && nodesName[0] == "" {
		return t.Root, true
	}

	nodeMap := t.Root.NodeMap

	var i int

	for i = 0; i < len(nodesName)-1; i++ {
		node, ok := nodeMap[nodesName[i]]
		if !ok || !node.Dir {
			return nil, false
		}
		nodeMap = node.NodeMap
	}

	tn, ok := nodeMap[nodesName[i]]
	if ok {
		return tn, true
	}
	return nil, false
}

// get the internalNode of the key
func (t *tree) get(key string) (Node, bool) {
	tn, ok := t.internalGet(key)
	if ok {
		if tn.Dir {
			return emptyNode, false
		}
		return tn.InternalNode, ok
	} else {
		return emptyNode, ok
	}
}

// get the internalNode of the key
// the method can deal with file node
func (t *tree) list(dir string) (any, []string, bool) {
	treeNode, ok := t.internalGet(dir)
	if !ok {
		return nil, nil, ok
	} else {
		if !treeNode.Dir {
			return &treeNode.InternalNode, nil, ok
		}
		length := len(treeNode.NodeMap)
		nodes := make([]*Node, length)
		keys := make([]string, length)

		i := 0
		for key, node := range treeNode.NodeMap {
			nodes[i] = &node.InternalNode
			keys[i] = key
			i++
		}
		return nodes, keys, ok
	}
}

// delete the key, return true if success
func (t *tree) delete(key string) bool {
	nodesName := split(key)

	nodeMap := t.Root.NodeMap

	var i int

	for i = 0; i < len(nodesName)-1; i++ {
		node, ok := nodeMap[nodesName[i]]
		if !ok || !node.Dir {
			return false
		}
		nodeMap = node.NodeMap
	}

	node, ok := nodeMap[nodesName[i]]
	if ok && !node.Dir {
		delete(nodeMap, nodesName[i])
		return true
	}
	return false
}

// traverse wrapper
func (t *tree) traverse(f func(string, *Node), sort bool) {
	if sort {
		sortDfs("", t.Root, f)
	} else {
		dfs("", t.Root, f)
	}
}

// clone() will return a deep cloned tree
func (t *tree) clone() *tree {
	newTree := new(tree)
	newTree.Root = &treeNode{
		InternalNode: Node{
			Value:      "/",
			ExpireTime: time.Unix(0, 0),
			update:     nil,
		},
		Dir:     true,
		NodeMap: make(map[string]*treeNode),
	}
	recursiveClone(t.Root, newTree.Root)
	return newTree
}

// recursiveClone is a helper function for clone()
func recursiveClone(tnSrc *treeNode, tnDes *treeNode) {
	if !tnSrc.Dir {
		tnDes.InternalNode = tnSrc.InternalNode
		return

	} else {
		tnDes.InternalNode = tnSrc.InternalNode
		tnDes.Dir = true
		tnDes.NodeMap = make(map[string]*treeNode)

		for key, tn := range tnSrc.NodeMap {
			newTn := new(treeNode)
			recursiveClone(tn, newTn)
			tnDes.NodeMap[key] = newTn
		}

	}
}

// deep first search to traverse the tree
// apply the func f to each internal node
func dfs(key string, t *treeNode, f func(string, *Node)) {
	// base case
	if len(t.NodeMap) == 0 {
		f(key, &t.InternalNode)
	} else {
		for tnKey, tn := range t.NodeMap {
			tnKey := key + "/" + tnKey
			dfs(tnKey, tn, f)
		}
	}
}

// sort deep first search to traverse the tree
// apply the func f to each internal node
func sortDfs(key string, t *treeNode, f func(string, *Node)) {
	// base case
	if len(t.NodeMap) == 0 {
		f(key, &t.InternalNode)

		// recursion
	} else {

		s := make(tnWithKeySlice, len(t.NodeMap))
		i := 0

		// copy
		for nodeKey, _treeNode := range t.NodeMap {
			newKey := key + "/" + nodeKey
			s[i] = tnWithKey{newKey, _treeNode}
			i++
		}

		// sort
		sort.Sort(s)

		// traverse
		for i = 0; i < len(t.NodeMap); i++ {
			sortDfs(s[i].key, s[i].tn, f)
		}
	}
}

// split the key by '/', get the intermediate node name
func split(key string) []string {
	key = "/" + key
	key = path.Clean(key)

	// get the intermidate nodes name
	nodesName := strings.Split(key, "/")
	// we do not need the root node, since we start with it
	nodesName = nodesName[1:]
	return nodesName
}
