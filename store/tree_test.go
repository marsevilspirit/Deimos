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
			Value:   CreateTestNode("/"),
			Dir:     true,
			NodeMap: make(map[string]*treeNode),
		},
	}

	// create key
	ts.set("/foo", CreateTestNode("bar"))
	// change value
	ts.set("/foo", CreateTestNode("barbar"))
	// create key
	ts.set("/hello/foo", CreateTestNode("barbarbar"))
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
	ok = ts.set("/foo/foo", CreateTestNode("bar"))
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
	ts.set("/hello/fooo", CreateTestNode("barbarbar"))
	ts.set("/hello/fooo/foo", CreateTestNode("barbarbar"))

	nodes, keys, dirs, ok := ts.list("/hello")
	if !ok {
		t.Fatalf("cannot list")
	} else {
		for i := range len(nodes) {
			fmt.Println("List test: ", keys[i], "=", nodes[i].Value, "[", dirs[i], "]")
		}
	}

	// speed test
	for range 100 {
		key := "/"
		depth := rand.Intn(10)
		for range depth {
			key += "/" + strconv.Itoa(rand.Int()%10)
		}
		value := strconv.Itoa(rand.Int())
		ts.set(key, CreateTestNode(value))
		treeNode, ok := ts.get(key)

		if !ok {
			continue
			// t.Fatalf("Expected to get node, got nil")
		}
		if treeNode.Value != value {
			t.Fatalf("Expect value %s, but got %s", value, treeNode.Value)
		}
	}

	ts.traverse(f, true)
}

func f(key string, n *Node) {
	fmt.Println(key, "=", n.Value)
}

func CreateTestNode(value string) Node {
	return Node{
		Value:      value,
		ExpireTime: PERMANENT,
		update:     nil,
	}
}
