package store

import (
	"testing"
	"time"
)

func TestBPlusTreeSaveAndRecovery(t *testing.T) {
	// Create a new B+ tree
	tree := NewBPlusTree()

	// Add some test data
	testData := map[string]string{
		"/key1": "value1",
		"/key2": "value2",
		"/key3": "value3",
	}

	for key, value := range testData {
		_, err := tree.Set(key, false, value, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, value)
		}
	}

	// Save the tree state
	savedState, err := tree.Save()
	if err != nil {
		t.Fatalf("Failed to save tree: %v", err)
	}

	// Verify saved state is not empty
	if len(savedState) == 0 {
		t.Fatal("Saved state is empty")
	}

	// Create a new tree for recovery
	recoveredTree := NewBPlusTree()

	// Recover from saved state
	err = recoveredTree.Recovery(savedState)
	if err != nil {
		t.Fatalf("Failed to recover tree: %v", err)
	}

	// Verify that all data was recovered
	for key, expectedValue := range testData {
		event, err := recoveredTree.Get(key, false, false)
		if err != nil {
			t.Fatalf("Failed to get key %s after recovery: %v", key, err)
		}
		if event.Node.Value == nil {
			t.Fatalf("Value is nil for key %s after recovery", key)
		}
		if *event.Node.Value != expectedValue {
			t.Fatalf("Value mismatch for key %s: expected %s, got %s", key, expectedValue, *event.Node.Value)
		}
	}

	// Verify metadata was recovered
	if recoveredTree.CurrentIndex != tree.CurrentIndex {
		t.Fatalf("CurrentIndex mismatch: expected %d, got %d", tree.CurrentIndex, recoveredTree.CurrentIndex)
	}
	if recoveredTree.CurrentVersion != tree.CurrentVersion {
		t.Fatalf("CurrentVersion mismatch: expected %d, got %d", tree.CurrentVersion, recoveredTree.CurrentVersion)
	}
	if recoveredTree.TreeSize != tree.TreeSize {
		t.Fatalf("TreeSize mismatch: expected %d, got %d", tree.TreeSize, recoveredTree.TreeSize)
	}
}

func TestBPlusTreeSaveAndRecoveryWithTTL(t *testing.T) {
	// Create a new B+ tree
	tree := NewBPlusTree()

	// Add test data with TTL
	expireTime := time.Now().Add(time.Hour)
	_, err := tree.Set("/ttlkey", false, "ttlvalue", expireTime)
	if err != nil {
		t.Fatalf("Failed to set TTL key: %v", err)
	}

	// Save the tree state
	savedState, err := tree.Save()
	if err != nil {
		t.Fatalf("Failed to save tree: %v", err)
	}

	// Create a new tree for recovery
	recoveredTree := NewBPlusTree()

	// Recover from saved state
	err = recoveredTree.Recovery(savedState)
	if err != nil {
		t.Fatalf("Failed to recover tree: %v", err)
	}

	// Verify TTL data was recovered
	event, err := recoveredTree.Get("/ttlkey", false, false)
	if err != nil {
		t.Fatalf("Failed to get TTL key after recovery: %v", err)
	}
	if event.Node.Value == nil {
		t.Fatal("TTL value is nil after recovery")
	}
	if *event.Node.Value != "ttlvalue" {
		t.Fatalf("TTL value mismatch: expected ttlvalue, got %s", *event.Node.Value)
	}
	if event.Node.Expiration == nil {
		t.Fatal("TTL expiration is nil after recovery")
	}
}
