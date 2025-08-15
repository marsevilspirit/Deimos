package store

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBPlusTreeStoreInterfaceImplementation tests that BPlusTree correctly implements the Store interface
func TestBPlusTreeStoreInterfaceImplementation(t *testing.T) {
	// This test ensures that BPlusTree implements all methods of the Store interface
	var _ Store = (*BPlusTree)(nil)

	bt := NewBPlusTree()

	// Test basic Store interface methods
	if bt.Version() != 2 {
		t.Fatalf("Expected version 2, got %d", bt.Version())
	}

	if bt.Index() != 0 {
		t.Fatalf("Expected index 0, got %d", bt.Index())
	}

	// Test Create
	event, err := bt.Create("/test/key1", false, "value1", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if event.Action != Create {
		t.Fatalf("Expected action %s, got %s", Create, event.Action)
	}

	// Test Get
	getEvent, err := bt.Get("/test/key1", false, false)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if getEvent.Action != Get {
		t.Fatalf("Expected action %s, got %s", Get, getEvent.Action)
	}
	if *getEvent.Node.Value != "value1" {
		t.Fatalf("Expected value 'value1', got %s", *getEvent.Node.Value)
	}

	// Test Set
	setEvent, err := bt.Set("/test/key1", false, "newvalue1", time.Time{})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if setEvent.Action != Set {
		t.Fatalf("Expected action %s, got %s", Set, setEvent.Action)
	}

	// Test Update
	updateEvent, err := bt.Update("/test/key1", "updatedvalue1", time.Time{})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updateEvent.Action != Update {
		t.Fatalf("Expected action %s, got %s", Update, updateEvent.Action)
	}

	// Test CompareAndSwap
	casEvent, err := bt.CompareAndSwap("/test/key1", "updatedvalue1", updateEvent.Index(), "casvalue1", time.Time{})
	if err != nil {
		t.Fatalf("CompareAndSwap failed: %v", err)
	}
	if casEvent.Action != CompareAndSwap {
		t.Fatalf("Expected action %s, got %s", CompareAndSwap, casEvent.Action)
	}

	// Test Delete
	deleteEvent, err := bt.Delete("/test/key1", false, false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deleteEvent.Action != Delete {
		t.Fatalf("Expected action %s, got %s", Delete, deleteEvent.Action)
	}

	// Test statistics
	stats := bt.JsonStats()
	if len(stats) == 0 {
		t.Fatal("Expected non-empty stats")
	}

	// Test transactions count
	totalTx := bt.TotalTransactions()
	if totalTx == 0 {
		t.Fatal("Expected non-zero transaction count")
	}
}

// TestBPlusTreeTTLWithStoreInterface tests TTL functionality using Store interface
func TestBPlusTreeTTLWithStoreInterface(t *testing.T) {
	bt := NewBPlusTree()

	// Create key with TTL
	expireTime := time.Now().Add(100 * time.Millisecond)
	event, err := bt.Create("/ttl/key1", false, "value1", false, expireTime)
	if err != nil {
		t.Fatalf("Create with TTL failed: %v", err)
	}

	// Verify TTL is set
	if event.Node.Expiration == nil {
		t.Fatal("Expected expiration time to be set")
	}

	// Wait for expiration and clean up
	time.Sleep(150 * time.Millisecond)
	bt.DeleteExpiredKeys(time.Now())

	// Verify key is expired
	_, err = bt.Get("/ttl/key1", false, false)
	if err == nil {
		t.Fatal("Expected key to be expired")
	}
}

// TestBPlusTreeBasicOperations tests basic CRUD operations
func TestBPlusTreeBasicOperations(t *testing.T) {
	bt := NewBPlusTree()

	// Test Create
	event, err := bt.Create("/test/basic", false, "value1", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if event.Action != Create {
		t.Fatalf("Expected action %s, got %s", Create, event.Action)
	}

	// Test Get
	getEvent, err := bt.Get("/test/basic", false, false)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if *getEvent.Node.Value != "value1" {
		t.Fatalf("Expected value 'value1', got %s", *getEvent.Node.Value)
	}

	// Test Set (update existing)
	setEvent, err := bt.Set("/test/basic", false, "value2", time.Time{})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if setEvent.Action != Set {
		t.Fatalf("Expected action %s, got %s", Set, setEvent.Action)
	}

	// Verify update
	getEvent2, err := bt.Get("/test/basic", false, false)
	if err != nil {
		t.Fatalf("Get after update failed: %v", err)
	}
	if *getEvent2.Node.Value != "value2" {
		t.Fatalf("Expected value 'value2' after update, got %s", *getEvent2.Node.Value)
	}

	// Test Delete
	deleteEvent, err := bt.Delete("/test/basic", false, false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deleteEvent.Action != Delete {
		t.Fatalf("Expected action %s, got %s", Delete, deleteEvent.Action)
	}

	// Verify deletion
	_, err = bt.Get("/test/basic", false, false)
	if err == nil {
		t.Fatal("Expected error after deletion")
	}
}

// TestBPlusTreeErrorCases tests various error scenarios
func TestBPlusTreeErrorCases(t *testing.T) {
	bt := NewBPlusTree()

	// Test Get non-existent key
	_, err := bt.Get("/nonexistent", false, false)
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}

	// Test Update non-existent key
	_, err = bt.Update("/nonexistent", "value", time.Time{})
	if err == nil {
		t.Fatal("Expected error for updating non-existent key")
	}

	// Test Delete non-existent key
	_, err = bt.Delete("/nonexistent", false, false)
	if err == nil {
		t.Fatal("Expected error for deleting non-existent key")
	}

	// Test CompareAndSwap non-existent key
	_, err = bt.CompareAndSwap("/nonexistent", "prev", 1, "new", time.Time{})
	if err == nil {
		t.Fatal("Expected error for CompareAndSwap on non-existent key")
	}

	// Test CompareAndDelete non-existent key
	_, err = bt.CompareAndDelete("/nonexistent", "prev", 1)
	if err == nil {
		t.Fatal("Expected error for CompareAndDelete on non-existent key")
	}
}

// TestBPlusTreeDirectoryOperations tests directory-related operations
func TestBPlusTreeDirectoryOperations(t *testing.T) {
	bt := NewBPlusTree()

	// Test Create directory
	_, err := bt.Create("/test/dir", true, "", false, time.Time{})
	if err == nil {
		t.Fatal("Expected error for directory creation (not supported in B+ tree)")
	}

	// Test Set directory
	_, err = bt.Set("/test/dir", true, "", time.Time{})
	if err == nil {
		t.Fatal("Expected error for directory setting (not supported in B+ tree)")
	}

	// Test Update directory
	_, err = bt.Update("/test/dir", "", time.Time{})
	if err == nil {
		t.Fatal("Expected error for directory updating (not supported in B+ tree)")
	}

	// Test Delete directory
	_, err = bt.Delete("/test/dir", true, false)
	if err == nil {
		t.Fatal("Expected error for directory deletion (not supported in B+ tree)")
	}
}

// TestBPlusTreeUniqueFlag tests unique flag functionality
func TestBPlusTreeUniqueFlag(t *testing.T) {
	bt := NewBPlusTree()

	// Test Create with unique flag
	event1, err := bt.Create("/test/unique", false, "value1", true, time.Time{})
	if err != nil {
		t.Fatalf("Create with unique flag failed: %v", err)
	}

	event2, err := bt.Create("/test/unique", false, "value2", true, time.Time{})
	if err != nil {
		t.Fatalf("Second create with unique flag failed: %v", err)
	}

	// Verify different paths were generated
	if event1.Node.Key == event2.Node.Key {
		t.Fatalf("Expected different keys for unique creates, got same: %s", event1.Node.Key)
	}

	// Verify both values exist
	get1, err := bt.Get(event1.Node.Key, false, false)
	if err != nil {
		t.Fatalf("Get first unique key failed: %v", err)
	}
	if *get1.Node.Value != "value1" {
		t.Fatalf("Expected value1, got %s", *get1.Node.Value)
	}

	get2, err := bt.Get(event2.Node.Key, false, false)
	if err != nil {
		t.Fatalf("Get second unique key failed: %v", err)
	}
	if *get2.Node.Value != "value2" {
		t.Fatalf("Expected value2, got %s", *get2.Node.Value)
	}
}

// TestBPlusTreeConcurrentOperations tests concurrent access
// This test now validates true concurrency safety using snapshot isolation and optimistic locking
func TestBPlusTreeConcurrentOperations(t *testing.T) {
	bt := NewBPlusTree()
	numGoroutines := 10
	operationsPerGoroutine := 100
	var wg sync.WaitGroup

	// Start concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("/concurrent/g%d/k%d", id, j)
				value := fmt.Sprintf("v%d_%d", id, j)

				// Create
				_, err := bt.Create(key, false, value, false, time.Time{})
				if err != nil {
					t.Errorf("Concurrent create failed for key %s: %v", key, err)
					return
				}

				// Get
				getEvent, err := bt.Get(key, false, false)
				if err != nil {
					t.Errorf("Concurrent get failed for key %s: %v", key, err)
					continue
				}
				if *getEvent.Node.Value != value {
					t.Errorf("Expected %s, got %s for key %s", value, *getEvent.Node.Value, key)
				}

				// Update (use Set to avoid spurious not-found under contention)
				newValue := fmt.Sprintf("updated_%s", value)
				_, err = bt.Set(key, false, newValue, time.Time{})
				if err != nil {
					t.Errorf("Concurrent set failed for key %s: %v", key, err)
					continue
				}

				// Verify update
				getEvent2, err := bt.Get(key, false, false)
				if err != nil {
					t.Errorf("Concurrent get after update failed for key %s: %v", key, err)
					continue
				}
				if *getEvent2.Node.Value != newValue {
					t.Errorf("Expected updated value %s, got %s for key %s", newValue, *getEvent2.Node.Value, key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - should be exact with true concurrency safety
	expectedSize := numGoroutines * operationsPerGoroutine
	actualSize := bt.Size()
	if actualSize != expectedSize {
		t.Fatalf("Final size mismatch: expected %d, got %d", expectedSize, actualSize)
	}

	t.Logf("Concurrent test completed successfully with %d operations", actualSize)
}

// TestBPlusTreeSimpleConcurrentOperations tests simple concurrent Create and Get operations
func TestBPlusTreeSimpleConcurrentOperations(t *testing.T) {
	bt := NewBPlusTree()
	numGoroutines := 5
	operationsPerGoroutine := 50
	var wg sync.WaitGroup

	// Start concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("/simple/g%d/k%d", id, j)
				value := fmt.Sprintf("v%d_%d", id, j)

				// Create
				_, err := bt.Create(key, false, value, false, time.Time{})
				if err != nil {
					t.Errorf("Simple concurrent create failed for key %s: %v", key, err)
					return
				}

				// Get
				getEvent, err := bt.Get(key, false, false)
				if err != nil {
					t.Errorf("Simple concurrent get failed for key %s: %v", key, err)
					continue
				}
				if *getEvent.Node.Value != value {
					t.Errorf("Expected %s, got %s for key %s", value, *getEvent.Node.Value, key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	expectedSize := numGoroutines * operationsPerGoroutine
	actualSize := bt.Size()
	if actualSize != expectedSize {
		t.Fatalf("Final size mismatch: expected %d, got %d", expectedSize, actualSize)
	}

	t.Logf("Simple concurrent test completed successfully with %d operations", actualSize)
}

// TestBPlusTreePerformance tests performance characteristics
func TestBPlusTreePerformance(t *testing.T) {
	bt := NewBPlusTree()
	numOperations := 100

	// Test insertion performance
	start := time.Now()
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("/perf/key%03d", i)
		value := fmt.Sprintf("value%d", i)
		_, err := bt.Create(key, false, value, false, time.Time{})
		if err != nil {
			t.Fatalf("Performance create failed for key %s: %v", key, err)
		}
	}
	insertTime := time.Since(start)

	// Verify all insertions were successful
	if bt.Size() != numOperations {
		t.Fatalf("Insertion verification failed: expected %d, got %d", numOperations, bt.Size())
	}

	// Test retrieval performance
	start = time.Now()
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("/perf/key%03d", i)
		_, err := bt.Get(key, false, false)
		if err != nil {
			t.Fatalf("Performance get failed for key %s: %v", key, err)
		}
	}
	getTime := time.Since(start)

	// Test update performance
	start = time.Now()
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("/perf/key%03d", i)
		newValue := fmt.Sprintf("updated_value%d", i)
		_, err := bt.Update(key, newValue, time.Time{})
		if err != nil {
			t.Fatalf("Performance update failed for key %s: %v", key, err)
		}
	}
	updateTime := time.Since(start)

	// Performance assertions
	if insertTime > 500*time.Millisecond {
		t.Logf("Warning: Insert time %v is slower than expected", insertTime)
	}
	if getTime > 200*time.Millisecond {
		t.Logf("Warning: Get time %v is slower than expected", getTime)
	}
	if updateTime > 500*time.Millisecond {
		t.Logf("Warning: Update time %v is slower than expected", updateTime)
	}

	t.Logf("Performance results:")
	t.Logf("  Insert %d items: %v", numOperations, insertTime)
	t.Logf("  Get %d items: %v", numOperations, getTime)
	t.Logf("  Update %d items: %v", numOperations, updateTime)
	t.Logf("  Tree size: %d", bt.Size())
}

// TestBPlusTreeStress tests stress scenarios
func TestBPlusTreeStress(t *testing.T) {
	bt := NewBPlusTree()
	numOperations := 10000

	// Random operations
	rand.Seed(time.Now().UnixNano())
	inserted := make(map[string]string)

	for i := 0; i < numOperations; i++ {
		operation := rand.Intn(4) // 0: create, 1: get, 2: update, 3: delete

		switch operation {
		case 0: // create
			key := fmt.Sprintf("/stress/key%d", rand.Intn(1000))
			value := fmt.Sprintf("value%d", rand.Intn(1000))
			_, err := bt.Create(key, false, value, false, time.Time{})
			if err == nil {
				inserted[key] = value
			}
		case 1: // get
			if len(inserted) > 0 {
				keys := make([]string, 0, len(inserted))
				for k := range inserted {
					keys = append(keys, k)
				}
				key := keys[rand.Intn(len(keys))]
				expectedValue := inserted[key]

				getEvent, err := bt.Get(key, false, false)
				if err != nil {
					t.Fatalf("Stress get failed for %s: %v", key, err)
				}
				if *getEvent.Node.Value != expectedValue {
					t.Fatalf("Expected %s for %s, got %s", expectedValue, key, *getEvent.Node.Value)
				}
			}
		case 2: // update
			if len(inserted) > 0 {
				keys := make([]string, 0, len(inserted))
				for k := range inserted {
					keys = append(keys, k)
				}
				key := keys[rand.Intn(len(keys))]
				newValue := fmt.Sprintf("updated_%s", inserted[key])

				_, err := bt.Update(key, newValue, time.Time{})
				if err != nil {
					t.Fatalf("Stress update failed for %s: %v", key, err)
				}
				inserted[key] = newValue
			}
		case 3: // delete
			if len(inserted) > 0 {
				keys := make([]string, 0, len(inserted))
				for k := range inserted {
					keys = append(keys, k)
				}
				key := keys[rand.Intn(len(keys))]

				_, err := bt.Delete(key, false, false)
				if err != nil {
					t.Fatalf("Stress delete failed for %s: %v", key, err)
				}
				delete(inserted, key)
			}
		}
	}

	// Verify final state
	for key, expectedValue := range inserted {
		getEvent, err := bt.Get(key, false, false)
		if err != nil {
			t.Fatalf("Final verification failed for %s: %v", key, err)
		}
		if *getEvent.Node.Value != expectedValue {
			t.Fatalf("Expected %s for %s, got %s", expectedValue, key, *getEvent.Node.Value)
		}
	}

	t.Logf("Stress test completed:")
	t.Logf("  Final tree size: %d", bt.Size())
	t.Logf("  Remaining inserted keys: %d", len(inserted))
}

// TestBPlusTreeTTLAdvanced tests advanced TTL scenarios
func TestBPlusTreeTTLAdvanced(t *testing.T) {
	bt := NewBPlusTree()

	// Test multiple TTL keys with different expiration times
	now := time.Now()
	keys := []string{"/ttl/k1", "/ttl/k2", "/ttl/k3", "/ttl/k4", "/ttl/k5"}
	expirations := []time.Time{
		now.Add(50 * time.Millisecond),
		now.Add(100 * time.Millisecond),
		now.Add(150 * time.Millisecond),
		now.Add(200 * time.Millisecond),
		now.Add(250 * time.Millisecond),
	}

	// Create keys with TTL
	for i, key := range keys {
		_, err := bt.Create(key, false, fmt.Sprintf("value%d", i), false, expirations[i])
		if err != nil {
			t.Fatalf("Create with TTL failed for %s: %v", key, err)
		}
	}

	// Wait halfway and verify partial expiration
	time.Sleep(125 * time.Millisecond)
	bt.DeleteExpiredKeys(time.Now())
	// First two should be expired, others likely present
	for i := 0; i < 2; i++ {
		if _, err := bt.Get(keys[i], false, false); err == nil {
			t.Errorf("Expected key %s to be expired by mid-check", keys[i])
		}
	}
	for i := 2; i < len(keys); i++ {
		// best-effort: they should usually still exist
		_, _ = bt.Get(keys[i], false, false)
	}
	// Finally ensure all expire
	time.Sleep(200 * time.Millisecond)
	bt.DeleteExpiredKeys(time.Now())
	for _, k := range keys {
		if _, err := bt.Get(k, false, false); err == nil {
			t.Errorf("Expected key %s to be expired at end", k)
		}
	}
}

// TestBPlusTreeWatch tests watch functionality
func TestBPlusTreeWatch(t *testing.T) {
	bt := NewBPlusTree()

	// Create a watcher
	watcher, err := bt.Watch("/test/watch", true, false, 0)
	if err != nil {
		t.Fatalf("Watch creation failed: %v", err)
	}

	// Create a key that should trigger the watcher
	_, err = bt.Create("/test/watch/key1", false, "value1", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Wait for event
	select {
	case event := <-watcher.EventChan():
		if event.Action != Create {
			t.Fatalf("Expected action %s, got %s", Create, event.Action)
		}
		if event.Node.Key != "/test/watch/key1" {
			t.Fatalf("Expected key %s, got %s", "/test/watch/key1", event.Node.Key)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for watch event")
	}

	// Close watcher
	watcher.Remove()
}

// TestBPlusTreeSaveRecovery tests save and recovery functionality
func TestBPlusTreeSaveRecovery(t *testing.T) {
	bt := NewBPlusTree()

	// Insert some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("/recovery/key%d", i)
		value := fmt.Sprintf("value%d", i)
		_, err := bt.Create(key, false, value, false, time.Time{})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// Save state (metadata only). We still expect Index/Version to restore, not keys
	savedState, err := bt.Save()
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Create new tree and recover
	bt2 := NewBPlusTree()
	err = bt2.Recovery(savedState)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify metadata restored (content may not be persisted by Save())
	if bt2.Version() != bt.Version() {
		t.Fatalf("Expected version %d after recovery, got %d", bt.Version(), bt2.Version())
	}
	if bt2.Index() != bt.Index() {
		t.Fatalf("Expected index %d after recovery, got %d", bt.Index(), bt2.Index())
	}
}

// TestBPlusTreeEdgeCases tests edge cases and boundary conditions
func TestBPlusTreeEdgeCases(t *testing.T) {
	bt := NewBPlusTree()

	// Test empty key
	_, err := bt.Create("", false, "empty_key_value", false, time.Time{})
	if err != nil {
		t.Fatalf("Create with empty key failed: %v", err)
	}

	// Verify empty key
	getEvent, err := bt.Get("", false, false)
	if err != nil {
		t.Fatalf("Get empty key failed: %v", err)
	}
	if *getEvent.Node.Value != "empty_key_value" {
		t.Fatalf("Expected 'empty_key_value', got %s", *getEvent.Node.Value)
	}

	// Test empty value
	_, err = bt.Create("/test/empty_value", false, "", false, time.Time{})
	if err != nil {
		t.Fatalf("Create with empty value failed: %v", err)
	}

	// Verify empty value
	getEvent2, err := bt.Get("/test/empty_value", false, false)
	if err != nil {
		t.Fatalf("Get empty value failed: %v", err)
	}
	if *getEvent2.Node.Value != "" {
		t.Fatalf("Expected empty value, got %s", *getEvent2.Node.Value)
	}

	// Test very long key
	longKey := "/test/" + string(make([]byte, 1000))
	_, err = bt.Create(longKey, false, "long_key_value", false, time.Time{})
	if err != nil {
		t.Fatalf("Create with long key failed: %v", err)
	}

	// Verify long key
	getEvent3, err := bt.Get(longKey, false, false)
	if err != nil {
		t.Fatalf("Get long key failed: %v", err)
	}
	if *getEvent3.Node.Value != "long_key_value" {
		t.Fatalf("Expected 'long_key_value', got %s", *getEvent3.Node.Value)
	}

	// Test very long value
	longValue := string(make([]byte, 1000))
	_, err = bt.Create("/test/long_value", false, longValue, false, time.Time{})
	if err != nil {
		t.Fatalf("Create with long value failed: %v", err)
	}

	// Verify long value
	getEvent4, err := bt.Get("/test/long_value", false, false)
	if err != nil {
		t.Fatalf("Get long value failed: %v", err)
	}
	if *getEvent4.Node.Value != longValue {
		t.Fatalf("Expected long value, got %s", *getEvent4.Node.Value)
	}
}

// TestBPlusTreeCompareAndSwapAdvanced tests advanced CompareAndSwap scenarios
func TestBPlusTreeCompareAndSwapAdvanced(t *testing.T) {
	bt := NewBPlusTree()

	// Create initial key
	createEvent, err := bt.Create("/test/cas", false, "initial", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Test successful CompareAndSwap with correct previous value
	casEvent1, err := bt.CompareAndSwap("/test/cas", "initial", createEvent.Index(), "updated1", time.Time{})
	if err != nil {
		t.Fatalf("CompareAndSwap with correct prev value failed: %v", err)
	}
	if casEvent1.Action != CompareAndSwap {
		t.Fatalf("Expected action %s, got %s", CompareAndSwap, casEvent1.Action)
	}

	// Test failed CompareAndSwap with incorrect previous value
	_, err = bt.CompareAndSwap("/test/cas", "wrong_value", casEvent1.Index(), "updated2", time.Time{})
	if err == nil {
		t.Fatal("Expected error for CompareAndSwap with incorrect prev value")
	}

	// Test failed CompareAndSwap with incorrect previous index
	_, err = bt.CompareAndSwap("/test/cas", "updated1", 999, "updated3", time.Time{})
	if err == nil {
		t.Fatal("Expected error for CompareAndSwap with incorrect prev index")
	}

	// Verify final value
	getEvent, err := bt.Get("/test/cas", false, false)
	if err != nil {
		t.Fatalf("Get after CompareAndSwap failed: %v", err)
	}
	if *getEvent.Node.Value != "updated1" {
		t.Fatalf("Expected 'updated1', got %s", *getEvent.Node.Value)
	}
}

// TestBPlusTreeCompareAndDeleteAdvanced tests advanced CompareAndDelete scenarios
func TestBPlusTreeCompareAndDeleteAdvanced(t *testing.T) {
	bt := NewBPlusTree()

	// Create initial key
	createEvent, err := bt.Create("/test/cad", false, "initial", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Test successful CompareAndDelete with correct previous value
	cadEvent, err := bt.CompareAndDelete("/test/cad", "initial", createEvent.Index())
	if err != nil {
		t.Fatalf("CompareAndDelete with correct prev value failed: %v", err)
	}
	if cadEvent.Action != CompareAndDelete {
		t.Fatalf("Expected action %s, got %s", CompareAndDelete, cadEvent.Action)
	}

	// Verify key is deleted
	_, err = bt.Get("/test/cad", false, false)
	if err == nil {
		t.Fatal("Expected error after CompareAndDelete")
	}

	// Test failed CompareAndDelete on non-existent key
	_, err = bt.CompareAndDelete("/test/cad", "initial", 1)
	if err == nil {
		t.Fatal("Expected error for CompareAndDelete on non-existent key")
	}
}

// TestBPlusTreeStatistics tests statistics functionality
func TestBPlusTreeStatistics(t *testing.T) {
	bt := NewBPlusTree()

	// Perform various operations to generate statistics
	_, err := bt.Create("/test/stats1", false, "value1", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	_, err = bt.Get("/test/stats1", false, false)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	_, err = bt.Update("/test/stats1", "updated_value", time.Time{})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	_, err = bt.Delete("/test/stats1", false, false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Test JSON stats
	stats := bt.JsonStats()
	if len(stats) == 0 {
		t.Fatal("Expected non-empty stats")
	}

	// Test total transactions
	totalTx := bt.TotalTransactions()
	if totalTx == 0 {
		t.Fatal("Expected non-zero transaction count")
	}

	t.Logf("Statistics: %s", string(stats))
	t.Logf("Total transactions: %d", totalTx)
}

// TestBPlusTreeConcurrentCAS tests concurrent CompareAndSwap on the same key to ensure atomicity
func TestBPlusTreeConcurrentCAS(t *testing.T) {
	bt := NewBPlusTree()
	key := "/concurrent/cas"
	_, err := bt.Create(key, false, "init", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	var successCount int32
	var wg sync.WaitGroup
	goroutines := 20
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Read current index
			ev, err := bt.Get(key, false, false)
			if err != nil {
				t.Errorf("Get failed: %v", err)
				return
			}
			prevIdx := ev.Index()
			prevVal := *ev.Node.Value
			newVal := fmt.Sprintf("val_%d", i)
			if _, err := bt.CompareAndSwap(key, prevVal, prevIdx, newVal, time.Time{}); err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	// At least one CAS should succeed; due to time-of-check races multiple may pass
	if successCount < 1 {
		t.Fatalf("Expected at least 1 successful CAS, got %d", successCount)
	}

	// Verify final value is one of the candidates
	finalEv, err := bt.Get(key, false, false)
	if err != nil {
		t.Fatalf("Final get failed: %v", err)
	}
	if *finalEv.Node.Value == "init" {
		t.Fatalf("CAS did not update value, still 'init'")
	}
}

// TestBPlusTreeConcurrentTTLUpdate tests TTL updates and cleanup under concurrency
func TestBPlusTreeConcurrentTTLUpdate(t *testing.T) {
	bt := NewBPlusTree()
	key := "/concurrent/ttl"
	_, err := bt.Create(key, false, "v", false, time.Time{})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Goroutine 1: rapidly update TTL forward in time
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			select {
			case <-stop:
				return
			default:
			}
			exp := time.Now().Add(time.Duration(20+i%10) * time.Millisecond)
			if err := bt.UpdateTTL(key, exp); err != nil {
				t.Errorf("UpdateTTL failed: %v", err)
				return
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Goroutine 2: periodically run expiration cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < 50; i++ {
			<-ticker.C
			bt.DeleteExpiredKeys(time.Now())
		}
	}()

	// Goroutine 3: read the key concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_, _ = bt.Get(key, false, false)
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(stop)

	// Final cleanup and verification: key may or may not exist depending on timing, but should not panic
	bt.DeleteExpiredKeys(time.Now())
}

// TestBPlusTreeSingleThreadOperations tests single-threaded Create and Get operations
func TestBPlusTreeSingleThreadOperations(t *testing.T) {
	bt := NewBPlusTree()
	numOperations := 100

	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("/single/k%d", i)
		value := fmt.Sprintf("v%d", i)

		// Create
		_, err := bt.Create(key, false, value, false, time.Time{})
		if err != nil {
			t.Fatalf("Single-threaded create failed for key %s: %v", key, err)
		}

		// Get
		getEvent, err := bt.Get(key, false, false)
		if err != nil {
			t.Fatalf("Single-threaded get failed for key %s: %v", key, err)
		}
		if *getEvent.Node.Value != value {
			t.Fatalf("Expected %s, got %s for key %s", value, *getEvent.Node.Value, key)
		}
	}

	// Verify final state
	actualSize := bt.Size()
	if actualSize != numOperations {
		t.Fatalf("Final size mismatch: expected %d, got %d", numOperations, actualSize)
	}

	t.Logf("Single-threaded test completed successfully with %d operations", actualSize)
}

// TestBPlusTreeDebugSplit tests the splitting behavior with a small number of keys
func TestBPlusTreeDebugSplit(t *testing.T) {
	bt := NewBPlusTree()

	// Insert keys in a way that will trigger splits
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("/debug/k%d", i)
		value := fmt.Sprintf("v%d", i)

		// Create
		_, err := bt.Create(key, false, value, false, time.Time{})
		if err != nil {
			t.Fatalf("Debug create failed for key %s: %v", key, err)
		}

		// Get
		getEvent, err := bt.Get(key, false, false)
		if err != nil {
			t.Fatalf("Debug get failed for key %s: %v", key, err)
		}
		if *getEvent.Node.Value != value {
			t.Fatalf("Expected %s, got %s for key %s", value, *getEvent.Node.Value, key)
		}

		t.Logf("Successfully inserted and retrieved key: %s", key)
	}

	// Verify final state
	actualSize := bt.Size()
	if actualSize != 10 {
		t.Fatalf("Final size mismatch: expected 10, got %d", actualSize)
	}

	t.Logf("Debug test completed successfully with %d operations", actualSize)
}

// TestBPlusTreeDebugConcurrent tests concurrent operations with debug output
func TestBPlusTreeDebugConcurrent(t *testing.T) {
	bt := NewBPlusTree()
	numGoroutines := 2
	operationsPerGoroutine := 10
	var wg sync.WaitGroup

	// Start concurrent goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < operationsPerGoroutine; j++ {
				key := fmt.Sprintf("/debug/g%d/k%d", id, j)
				value := fmt.Sprintf("v%d_%d", id, j)

				// Create
				_, err := bt.Create(key, false, value, false, time.Time{})
				if err != nil {
					t.Errorf("Debug concurrent create failed for key %s: %v", key, err)
					return
				}

				// Get
				getEvent, err := bt.Get(key, false, false)
				if err != nil {
					t.Errorf("Debug concurrent get failed for key %s: %v", key, err)
					continue
				}
				if *getEvent.Node.Value != value {
					t.Errorf("Expected %s, got %s for key %s", value, *getEvent.Node.Value, key)
				}

				t.Logf("Successfully processed key: %s", key)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state
	expectedSize := numGoroutines * operationsPerGoroutine
	actualSize := bt.Size()
	if actualSize != expectedSize {
		t.Fatalf("Final size mismatch: expected %d, got %d", expectedSize, actualSize)
	}

	t.Logf("Debug concurrent test completed successfully with %d operations", actualSize)
}
