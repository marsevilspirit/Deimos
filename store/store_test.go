package store

import (
	"testing"
	"time"

	Err "github.com/marsevilspirit/marstore/error"
	"github.com/stretchr/testify/assert"
)

// Ensure that the store can retrieve an existing value.
func TestStoreGetValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.Get("/foo", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "get", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.Equal(t, e.Node.Value, "bar", "")
}

// Ensure that the store can recrusively retrieve a directory listing.
// Note that hidden files should not be returned.
func TestStoreGetDirectory(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	s.Create("/foo/bar", false, "X", false, Permanent)
	s.Create("/foo/_hidden", false, "*", false, Permanent)
	s.Create("/foo/baz", true, "", false, Permanent)
	s.Create("/foo/baz/bat", false, "Y", false, Permanent)
	s.Create("/foo/baz/_hidden", false, "*", false, Permanent)
	s.Create("/foo/baz/ttl", false, "Y", false, time.Now().Add(time.Second*3))
	e, err := s.Get("/foo", true, true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "get", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.Equal(t, len(e.Node.Nodes), 2, "")
	assert.Equal(t, e.Node.Nodes[0].Key, "/foo/bar", "")
	assert.Equal(t, e.Node.Nodes[0].Value, "X", "")
	assert.Equal(t, e.Node.Nodes[0].Dir, false, "")
	assert.Equal(t, e.Node.Nodes[1].Key, "/foo/baz", "")
	assert.Equal(t, e.Node.Nodes[1].Dir, true, "")
	assert.Equal(t, len(e.Node.Nodes[1].Nodes), 2, "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[0].Key, "/foo/baz/bat", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[0].Value, "Y", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[0].Dir, false, "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[1].Key, "/foo/baz/ttl", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[1].Value, "Y", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[1].Dir, false, "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[1].TTL, int64(3), "")
}

// Ensure that the store can retrieve a directory in sorted order.
func TestStoreGetSorted(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	s.Create("/foo/x", false, "0", false, Permanent)
	s.Create("/foo/z", false, "0", false, Permanent)
	s.Create("/foo/y", true, "", false, Permanent)
	s.Create("/foo/y/a", false, "0", false, Permanent)
	s.Create("/foo/y/b", false, "0", false, Permanent)
	e, err := s.Get("/foo", true, true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Node.Nodes[0].Key, "/foo/x", "")
	assert.Equal(t, e.Node.Nodes[1].Key, "/foo/y", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[0].Key, "/foo/y/a", "")
	assert.Equal(t, e.Node.Nodes[1].Nodes[1].Key, "/foo/y/b", "")
	assert.Equal(t, e.Node.Nodes[2].Key, "/foo/z", "")
}

func TestSet(t *testing.T) {
	s := newStore()

	// Set /foo=""
	e, err := s.Set("/foo", false, "", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "set", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "", "")
	assert.Nil(t, e.Node.Nodes, "")
	assert.Nil(t, e.Node.Expiration, "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(1), "")

	// Set /foo="bar"
	e, err = s.Set("/foo", false, "bar", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "set", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "bar", "")
	assert.Nil(t, e.Node.Nodes, "")
	assert.Nil(t, e.Node.Expiration, "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(2), "")

	// check prevNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "", "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")

	// Set /dir as a directory
	e, err = s.Set("/dir", true, "", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "set", "")
	assert.Equal(t, e.Node.Key, "/dir", "")
	assert.True(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "", "")
	assert.Nil(t, e.Node.Nodes, "")
	assert.Nil(t, e.Node.Expiration, "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(3), "")
}

// Ensure that the store can create a new key if it doesn't already exist.
func TestStoreCreateValue(t *testing.T) {
	s := newStore()
	// Create /foo=bar
	e, err := s.Create("/foo", false, "bar", false, Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "bar", "")
	assert.Nil(t, e.Node.Nodes, "")
	assert.Nil(t, e.Node.Expiration, "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(1), "")

	// Create /empty=""
	e, err = s.Create("/empty", false, "", false, Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/empty", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "", "")
	assert.Nil(t, e.Node.Nodes, "")
	assert.Nil(t, e.Node.Expiration, "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(2), "")
}

// Ensure that the store can create a new directory if it doesn't already exist.
func TestStoreCreateDirectory(t *testing.T) {
	s := newStore()
	e, err := s.Create("/foo", true, "", false, Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.True(t, e.Node.Dir, "")
}

// Ensure that the store fails to create a key if it already exists.
func TestStoreCreateFailsIfExists(t *testing.T) {
	s := newStore()
	// create /foo as dir
	s.Create("/foo", true, "", false, Permanent)

	// create /foo as dir again
	e, _err := s.Create("/foo", true, "", false, Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNodeExist, "")
	assert.Equal(t, err.Message, "Key Already exists", "")
	assert.Equal(t, err.Cause, "/foo", "")
	assert.Equal(t, err.Index, uint64(1), "")
	assert.Nil(t, e, 0, "")
}

// Ensure that the store can update a key if it already exists.
func TestStoreUpdateValue(t *testing.T) {
	s := newStore()
	// create /foo=bar
	s.Create("/foo", false, "bar", false, Permanent)
	// update /foo="bzr"
	e, err := s.Update("/foo", "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "baz", "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(2), "")

	// check prevNode
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
	assert.Equal(t, e.PrevNode.TTL, int64(0), "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")

	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "baz", "")

	// update /foo=""
	e, err = s.Update("/foo", "", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.False(t, e.Node.Dir, "")
	assert.Equal(t, e.Node.Value, "", "")
	assert.Equal(t, e.Node.TTL, int64(0), "")
	assert.Equal(t, e.Node.ModifiedIndex, uint64(3), "")

	// check prevNode
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "baz", "")
	assert.Equal(t, e.PrevNode.TTL, int64(0), "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(2), "")

	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "", "")
}

// Ensure that the store cannot update a directory.
func TestStoreUpdateFailsIfDirectory(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	e, _err := s.Update("/foo", "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNotFile, "")
	assert.Equal(t, err.Message, "Not a file", "")
	assert.Equal(t, err.Cause, "/foo", "")
	assert.Nil(t, e, "")
}

// Ensure that the store can update the TTL on a value.
func TestStoreUpdateValueTTL(t *testing.T) {
	s := newStore()

	c := make(chan bool)
	defer func() {
		c <- true
	}()
	go mockSyncService(s.DeleteExpiredKeys, c)

	s.Create("/foo", false, "bar", false, Permanent)
	_, err := s.Update("/foo", "baz", time.Now().Add(500*time.Millisecond))
	e, _ := s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "baz", "")

	time.Sleep(600 * time.Millisecond)
	e, err = s.Get("/foo", false, false)
	assert.Nil(t, e, "")
	assert.Equal(t, err.(*Err.Error).ErrorCode, Err.EcodeKeyNotFound, "")
}

// Ensure that the store can update the TTL on a directory.
func TestStoreUpdateDirTTL(t *testing.T) {
	s := newStore()

	c := make(chan bool)
	defer func() {
		c <- true
	}()
	go mockSyncService(s.DeleteExpiredKeys, c)

	s.Create("/foo", true, "", false, Permanent)
	s.Create("/foo/bar", false, "baz", false, Permanent)
	_, err := s.Update("/foo", "", time.Now().Add(500*time.Millisecond))
	e, _ := s.Get("/foo/bar", false, false)
	assert.Equal(t, e.Node.Value, "baz", "")

	time.Sleep(600 * time.Millisecond)
	e, err = s.Get("/foo/bar", false, false)
	assert.Nil(t, e, "")
	assert.Equal(t, err.(*Err.Error).ErrorCode, Err.EcodeKeyNotFound, "")
}

// Ensure that the store can delete a value.
func TestStoreDeleteValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.Delete("/foo", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "delete", "")
	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
}

// Ensure that the store can delete a directory if recursive is specified.
func TestStoreDeleteDiretory(t *testing.T) {
	s := newStore()
	// create directory /foo
	s.Create("/foo", true, "", false, Permanent)
	// delete /foo with dir = true and recursive = false
	// this should success, since the directory is empty
	e, err := s.Delete("/foo", true, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "delete", "")

	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Dir, true, "")

	// create directory /foo and directory /foo/bar
	s.Create("/foo/bar", true, "", false, Permanent)
	// delete /foo with dir = true and recursive = false
	// this should fail, since the directory is not empty
	_, err = s.Delete("/foo", true, false)
	assert.NotNil(t, err, "")

	// delete /foo with dir=false and recursive = true
	// this should success, since recursive implies dir=true
	// and recursively delete should be able to delete all
	// items under the given directory
	e, err = s.Delete("/foo", false, true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "delete", "")
}

// Ensure that the store cannot delete a directory if both of recursive
// and dir are not specified.
func TestStoreDeleteDiretoryFailsIfNonRecursiveAndDir(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	e, _err := s.Delete("/foo", false, false)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNotFile, "")
	assert.Equal(t, err.Message, "Not a file", "")
	assert.Nil(t, e, "")
}

func TestRootRdOnly(t *testing.T) {
	s := newStore()

	_, err := s.Set("/", true, "", Permanent)
	assert.NotNil(t, err, "")

	_, err = s.Delete("/", true, true)
	assert.NotNil(t, err, "")

	_, err = s.Create("/", true, "", false, Permanent)
	assert.NotNil(t, err, "")

	_, err = s.Update("/", "", Permanent)
	assert.NotNil(t, err, "")

	_, err = s.CompareAndSwap("/", "", 0, "", Permanent)
	assert.NotNil(t, err, "")

}

func TestStoreCompareAndDeletePrevValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.CompareAndDelete("/foo", "bar", 0)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndDelete", "")
	assert.Equal(t, e.Node.Key, "/foo", "")

	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")
	assert.Equal(t, e.PrevNode.CreatedIndex, uint64(1), "")
}

func TestStoreCompareAndDeletePrevValueFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, _err := s.CompareAndDelete("/foo", "baz", 100)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Compare failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "bar", "")
}

func TestStoreCompareAndDeletePrevIndex(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.CompareAndDelete("/foo", "", 1)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndDelete", "")

	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")
	assert.Equal(t, e.PrevNode.CreatedIndex, uint64(1), "")
}

func TestStoreCompareAndDeletePrevIndexFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, _err := s.CompareAndDelete("/foo", "", 100)
	assert.NotNil(t, _err, "")
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Compare failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "bar", "")
}

// Ensure that the store cannot delete a directory.
func TestStoreCompareAndDeleteDiretoryFail(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	_, _err := s.CompareAndDelete("/foo", "", 0)
	assert.NotNil(t, _err, "")
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNotFile, "")
}

// Ensure that the store can conditionally update a key if it has a previous value.
func TestStoreCompareAndSwapPrevValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.CompareAndSwap("/foo", "bar", 0, "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Node.Value, "baz", "")

	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")
	assert.Equal(t, e.PrevNode.CreatedIndex, uint64(1), "")

	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "baz", "")
}

// Ensure that the store cannot conditionally update a key if it has the wrong previous value.
func TestStoreCompareAndSwapPrevValueFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, _err := s.CompareAndSwap("/foo", "wrong_value", 0, "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Compare failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "bar", "")
}

// Ensure that the store can conditionally update a key if it has a previous index.
func TestStoreCompareAndSwapPrevIndex(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, err := s.CompareAndSwap("/foo", "", 1, "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Node.Value, "baz", "")

	// check pervNode
	assert.NotNil(t, e.PrevNode, "")
	assert.Equal(t, e.PrevNode.Key, "/foo", "")
	assert.Equal(t, e.PrevNode.Value, "bar", "")
	assert.Equal(t, e.PrevNode.ModifiedIndex, uint64(1), "")
	assert.Equal(t, e.PrevNode.CreatedIndex, uint64(1), "")

	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "baz", "")
}

// Ensure that the store cannot conditionally update a key if it has the wrong previous index.
func TestStoreCompareAndSwapPrevIndexFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	e, _err := s.CompareAndSwap("/foo", "", 100, "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Compare failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Node.Value, "bar", "")
}

// Ensure that the store can watch for key creation.
func TestStoreWatchCreate(t *testing.T) {
	s := newStore()
	w, _ := s.Watch("/foo", false, false, 0)
	s.Create("/foo", false, "bar", false, Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	e = nbselect(w.EventChan)
	assert.Nil(t, e, "")
}

// Ensure that the store can watch for recursive key creation.
func TestStoreWatchRecursiveCreate(t *testing.T) {
	s := newStore()
	w, _ := s.Watch("/foo", true, false, 0)
	s.Create("/foo/bar", false, "baz", false, Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key updates.
func TestStoreWatchUpdate(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	w, _ := s.Watch("/foo", false, false, 0)
	s.Update("/foo", "baz", Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
}

// Ensure that the store can watch for recursive key updates.
func TestStoreWatchRecursiveUpdate(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", false, "baz", false, Permanent)
	w, _ := s.Watch("/foo", true, false, 0)
	s.Update("/foo/bar", "baz", Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Node.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key deletions.
func TestStoreWatchDelete(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	w, _ := s.Watch("/foo", false, false, 0)
	s.Delete("/foo", false, false)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "delete", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
}

// Ensure that the store can watch for recursive key deletions.
func TestStoreWatchRecursiveDelete(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", false, "baz", false, Permanent)
	w, _ := s.Watch("/foo", true, false, 0)
	s.Delete("/foo/bar", false, false)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "delete", "")
	assert.Equal(t, e.Node.Key, "/foo/bar", "")
}

// Ensure that the store can watch for CAS updates.
func TestStoreWatchCompareAndSwap(t *testing.T) {
	s := newStore()
	s.Create("/foo", false, "bar", false, Permanent)
	w, _ := s.Watch("/foo", false, false, 0)
	s.CompareAndSwap("/foo", "bar", 0, "baz", Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
}

// Ensure that the store can watch for recursive CAS updates.
func TestStoreWatchRecursiveCompareAndSwap(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", false, "baz", false, Permanent)
	w, _ := s.Watch("/foo", true, false, 0)
	s.CompareAndSwap("/foo/bar", "baz", 0, "bat", Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Node.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key expiration.
func TestStoreWatchExpire(t *testing.T) {
	s := newStore()

	stopChan := make(chan bool)
	defer func() {
		stopChan <- true
	}()

	go mockSyncService(s.DeleteExpiredKeys, stopChan)
	s.Create("/foo", false, "bar", false, time.Now().Add(500*time.Millisecond))
	s.Create("/foofoo", false, "barbarbar", false, time.Now().Add(500*time.Millisecond))

	w, _ := s.Watch("/", true, false, 0)
	c := w.EventChan
	e := nbselect(c)
	assert.Nil(t, e, "")
	time.Sleep(600 * time.Millisecond)
	e = nbselect(c)
	assert.Equal(t, e.Action, "expire", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	w, _ = s.Watch("/", true, false, 4)
	e = nbselect(w.EventChan)
	assert.Equal(t, e.Action, "expire", "")
	assert.Equal(t, e.Node.Key, "/foofoo", "")
}

// Ensure that the store can watch in streaming mode.
func TestStoreWatchStream(t *testing.T) {
	s := newStore()
	w, _ := s.Watch("/foo", false, true, 0)
	// first modification
	s.Create("/foo", false, "bar", false, Permanent)
	e := nbselect(w.EventChan)
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.Equal(t, e.Node.Value, "bar", "")
	e = nbselect(w.EventChan)
	assert.Nil(t, e, "")
	// second modification
	s.Update("/foo", "baz", Permanent)
	e = nbselect(w.EventChan)
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Node.Key, "/foo", "")
	assert.Equal(t, e.Node.Value, "baz", "")
	e = nbselect(w.EventChan)
	assert.Nil(t, e, "")
}

// Ensure that the store can recover from a previously saved state.
func TestStoreRecover(t *testing.T) {
	s := newStore()
	s.Create("/foo", true, "", false, Permanent)
	s.Create("/foo/x", false, "bar", false, Permanent)
	s.Create("/foo/y", false, "baz", false, Permanent)
	b, err := s.Save()

	s2 := newStore()
	s2.Recovery(b)

	e, err := s.Get("/foo/x", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Node.Value, "bar", "")

	e, err = s.Get("/foo/y", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Node.Value, "baz", "")
}

// Ensure that the store can recover from a previously saved state that includes an expiring key.
func TestStoreRecoverWithExpiration(t *testing.T) {
	s := newStore()

	c := make(chan bool)
	defer func() {
		c <- true
	}()

	go mockSyncService(s.DeleteExpiredKeys, c)
	s.Create("/foo", true, "", false, Permanent)
	s.Create("/foo/x", false, "bar", false, Permanent)
	s.Create("/foo/y", false, "baz", false, time.Now().Add(5*time.Millisecond))
	b, err := s.Save()

	time.Sleep(10 * time.Millisecond)

	c2 := make(chan bool)
	defer func() {
		c2 <- true
	}()

	s2 := newStore()
	go mockSyncService(s2.DeleteExpiredKeys, c2)
	s2.Recovery(b)

	time.Sleep(600 * time.Millisecond)

	e, err := s.Get("/foo/x", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Node.Value, "bar", "")

	e, err = s.Get("/foo/y", false, false)
	assert.NotNil(t, err, "")
	assert.Nil(t, e, "")
}

// Performs a non-blocking select on an event channel.
func nbselect(c <-chan *Event) *Event {
	select {
	case e := <-c:
		return e
	default:
		return nil
	}
}

func mockSyncService(f func(now time.Time), c chan bool) {
	// NOTE: 500ms is too long for test, sometimes failed.
	ticker := time.Tick(time.Millisecond * 50)
	for {
		select {
		case <-c:
			return
		case now := <-ticker:
			f(now)
		}
	}
}
