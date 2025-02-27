package store

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
)

func TestStoreGet(t *testing.T) {
	ts := &tree{
		&treeNode{
			InternalNode: NewTestNode("/"),
			Dir:          true,
			NodeMap:      make(map[string]*treeNode),
		},
	}

	// create key
	ts.set("/foo", NewTestNode("bar"))
	// change value
	ts.set("/foo", NewTestNode("barbar"))
	// create key
	ts.set("/hello/foo", NewTestNode("barbarbar"))
	treeNode, ok := ts.get("/foo")
	if !ok {
		t.Fatalf("Expected to get node, got nil")
	}

	if treeNode.Value != "barbar" {
		t.Fatalf("Expect get value barbar, but got %s", treeNode.Value)
	}

	treeNode, ok = ts.get("/hello/foo")
	if !ok {
		t.Fatalf("Expected to get node, got nil")
	}
	if treeNode.Value != "barbarbar" {
		t.Fatalf("Expect get value barbarbar, but got %s", treeNode.Value)
	}

	// create a key under other key
	ok = ts.set("/foo/foo", NewTestNode("bar"))
	if ok {
		t.Fatalf("should not add key under a existing key")
	}

	//delete a key
	ok = ts.delete("/foo")
	if !ok {
		t.Fatalf("cannot delete key")
	}

	// delete a directory
	ok = ts.delete("/hello")
	if ok {
		t.Fatalf("Expect cannot delete /hello, but deleted!")
	}

	// test List
	ts.set("/hello/fooo", NewTestNode("barbarbar"))
	ts.set("/hello/fooo/foo", NewTestNode("barbarbar"))

	nodes, keys, ok := ts.list("/hello")
	if !ok {
		t.Fatalf("cannot list")
	} else {
		nodes, _ := nodes.([]*Node)
		for i := range len(nodes) {
			fmt.Println("List test: ", keys[i], "=", nodes[i].Value)
		}
	}

	// speed test
	keys = GenKeys(100, 10)
	for i := range 100 {
		value := strconv.Itoa(rand.Int())
		ts.set(keys[i], NewTestNode(value))
		treeNode, ok := ts.get(keys[i])

		if !ok {
			continue
		}
		if treeNode.Value != value {
			t.Fatalf("Expect value %s, but got %s", value, treeNode.Value)
		}
	}

	ts.traverse(f, true)
}

func TestTreeClone(t *testing.T) {
	keys := GenKeys(10000, 10)
	ts := &tree{
		&treeNode{
			NewTestNode("/"),
			true,
			make(map[string]*treeNode),
		},
	}
	backTs := &tree{
		&treeNode{
			NewTestNode("/"),
			true,
			make(map[string]*treeNode),
		},
	}

	// generate the first tree
	for _, key := range keys {
		value := strconv.Itoa(rand.Int())
		ts.set(key, NewTestNode(value))
		backTs.set(key, NewTestNode(value))
	}

	copyTs := ts.clone()

	// test if they are identical
	copyTs.traverse(ts.contain, false)

	// remove all the keys from first tree
	for _, key := range keys {
		ts.delete(key)
	}

	// test if they are identical
	// make sure changes in the first tree will affect the copy one
	copyTs.traverse(backTs.contain, false)
}

func BenchmarkTreeStoreSet(b *testing.B) {
	keys := GenKeys(10000, 10)
	b.ResetTimer()
	for b.Loop() {
		ts := &tree{
			&treeNode{
				NewTestNode("/"),
				true,
				make(map[string]*treeNode),
			},
		}

		for _, key := range keys {
			value := strconv.Itoa(rand.Int())
			ts.set(key, NewTestNode(value))
		}
	}
}

func BenchmarkTreeStoreGet(b *testing.B) {
	keys := GenKeys(10000, 10)
	ts := &tree{
		&treeNode{
			NewTestNode("/"),
			true,
			make(map[string]*treeNode),
		},
	}

	for _, key := range keys {
		value := strconv.Itoa(rand.Int())
		ts.set(key, NewTestNode(value))
	}

	b.ResetTimer()
	for b.Loop() {
		for _, key := range keys {
			ts.get(key)
		}
	}
}

func BenchmarkTreeStoreCopy(b *testing.B) {
	keys := GenKeys(10000, 10)

	ts := &tree{
		&treeNode{
			NewTestNode("/"),
			true,
			make(map[string]*treeNode),
		},
	}

	for _, key := range keys {
		value := strconv.Itoa(rand.Int())
		ts.set(key, NewTestNode(value))
	}

	b.ResetTimer()
	for b.Loop() {
		ts.clone()
	}
}

func BenchmarkTreeStoreList(b *testing.B) {
	keys := GenKeys(10000, 10)
	ts := &tree{
		&treeNode{
			NewTestNode("/"),
			true,
			make(map[string]*treeNode),
		},
	}

	for _, key := range keys {
		value := strconv.Itoa(rand.Int())
		ts.set(key, NewTestNode(value))
	}

	b.ResetTimer()
	for b.Loop() {
		for _, key := range keys {
			ts.list(key)
		}
	}
}

func (t *tree) contain(key string, node *Node) {
	_, ok := t.get(key)
	if !ok {
		panic("tree do not contain the given key")
	}
}

func f(key string, n *Node) {
	return
}

func NewTestNode(value string) Node {
	return Node{
		Value:      value,
		ExpireTime: PERMANENT,
		update:     nil,
	}
}
