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
	s.Create("/foo", "bar", false, Permanent)
	e, err := s.Get("/foo", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "get", "")
	assert.Equal(t, e.Key, "/foo", "")
	assert.Equal(t, e.Value, "bar", "")
}

// Ensure that the store can recrusively retrieve a directory listing.
// Note that hidden files should not be returned.
func TestStoreGetDirectory(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	s.Create("/foo/bar", "X", false, Permanent)
	s.Create("/foo/_hidden", "*", false, Permanent)
	s.Create("/foo/baz", "", false, Permanent)
	s.Create("/foo/baz/bat", "Y", false, Permanent)
	s.Create("/foo/baz/_hidden", "*", false, Permanent)
	s.Create("/foo/baz/ttl", "Y", false, time.Now().Add(time.Second*3))
	e, err := s.Get("/foo", true, true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "get", "")
	assert.Equal(t, e.Key, "/foo", "")
	assert.Equal(t, len(e.KVPairs), 2, "")
	assert.Equal(t, e.KVPairs[0].Key, "/foo/bar", "")
	assert.Equal(t, e.KVPairs[0].Value, "X", "")
	assert.Equal(t, e.KVPairs[0].Dir, false, "")
	assert.Equal(t, e.KVPairs[1].Key, "/foo/baz", "")
	assert.Equal(t, e.KVPairs[1].Dir, true, "")
	assert.Equal(t, len(e.KVPairs[1].KVPairs), 2, "")
	assert.Equal(t, e.KVPairs[1].KVPairs[0].Key, "/foo/baz/bat", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[0].Value, "Y", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[0].Dir, false, "")
	assert.Equal(t, e.KVPairs[1].KVPairs[1].Key, "/foo/baz/ttl", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[1].Value, "Y", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[1].Dir, false, "")
	assert.Equal(t, e.KVPairs[1].KVPairs[1].TTL, int64(3), "")
}

// Ensure that the store can retrieve a directory in sorted order.
func TestStoreGetSorted(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	s.Create("/foo/x", "0", false, Permanent)
	s.Create("/foo/z", "0", false, Permanent)
	s.Create("/foo/y", "", false, Permanent)
	s.Create("/foo/y/a", "0", false, Permanent)
	s.Create("/foo/y/b", "0", false, Permanent)
	e, err := s.Get("/foo", true, true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.KVPairs[0].Key, "/foo/x", "")
	assert.Equal(t, e.KVPairs[1].Key, "/foo/y", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[0].Key, "/foo/y/a", "")
	assert.Equal(t, e.KVPairs[1].KVPairs[1].Key, "/foo/y/b", "")
	assert.Equal(t, e.KVPairs[2].Key, "/foo/z", "")
}

// Ensure that the store can create a new key if it doesn't already exist.
func TestStoreCreateValue(t *testing.T) {
	s := newStore()
	e, err := s.Create("/foo", "bar", false, Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Key, "/foo", "")
	assert.False(t, e.Dir, "")
	assert.Equal(t, e.PrevValue, "", "")
	assert.Equal(t, e.Value, "bar", "")
	assert.Nil(t, e.KVPairs, "")
	assert.Nil(t, e.Expiration, "")
	assert.Equal(t, e.TTL, int64(0), "")
	assert.Equal(t, e.ModifiedIndex, uint64(1), "")
}

// Ensure that the store can create a new directory if it doesn't already exist.
func TestStoreCreateDirectory(t *testing.T) {
	s := newStore()
	e, err := s.Create("/foo", "", false, Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Key, "/foo", "")
	assert.True(t, e.Dir, "")
}

// Ensure that the store fails to create a key if it already exists.
func TestStoreCreateFailsIfExists(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	e, _err := s.Create("/foo", "", false, Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNodeExist, "")
	assert.Equal(t, err.Message, "Already exists", "")
	assert.Equal(t, err.Cause, "/foo", "")
	assert.Equal(t, err.Index, uint64(1), "")
	assert.Nil(t, e, 0, "")
}

// Ensure that the store can update a key if it already exists.
func TestStoreUpdateValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, err := s.Update("/foo", "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Key, "/foo", "")
	assert.False(t, e.Dir, "")
	assert.Equal(t, e.PrevValue, "bar", "")
	assert.Equal(t, e.Value, "baz", "")
	assert.Equal(t, e.TTL, int64(0), "")
	assert.Equal(t, e.ModifiedIndex, uint64(2), "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "baz", "")
}

// Ensure that the store cannot update a directory.
func TestStoreUpdateFailsIfDirectory(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	e, _err := s.Update("/foo", "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNotFile, "")
	assert.Equal(t, err.Message, "Not A File", "")
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
	s.Create("/foo", "bar", false, Permanent)
	_, err := s.Update("/foo", "baz", time.Now().Add(500*time.Millisecond))
	e, _ := s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "baz", "")

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
	s.Create("/foo", "", false, Permanent)
	s.Create("/foo/bar", "baz", false, Permanent)
	_, err := s.Update("/foo", "", time.Now().Add(500*time.Millisecond))
	e, _ := s.Get("/foo/bar", false, false)
	assert.Equal(t, e.Value, "baz", "")

	time.Sleep(600 * time.Millisecond)
	e, err = s.Get("/foo/bar", false, false)
	assert.Nil(t, e, "")
	assert.Equal(t, err.(*Err.Error).ErrorCode, Err.EcodeKeyNotFound, "")
}

// Ensure that the store can delete a value.
func TestStoreDeleteValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, err := s.Delete("/foo", false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "delete", "")
}

// Ensure that the store can delete a directory if recursive is specified.
func TestStoreDeleteDiretory(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	e, err := s.Delete("/foo", true)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "delete", "")
}

// Ensure that the store cannot delete a directory if recursive is not specified.
func TestStoreDeleteDiretoryFailsIfNonRecursive(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	e, _err := s.Delete("/foo", false)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeNotFile, "")
	assert.Equal(t, err.Message, "Not A File", "")
	assert.Nil(t, e, "")
}

// Ensure that the store can conditionally update a key if it has a previous value.
func TestStoreCompareAndSwapPrevValue(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, err := s.CompareAndSwap("/foo", "bar", 0, "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.PrevValue, "bar", "")
	assert.Equal(t, e.Value, "baz", "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "baz", "")
}

// Ensure that the store cannot conditionally update a key if it has the wrong previous value.
func TestStoreCompareAndSwapPrevValueFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, _err := s.CompareAndSwap("/foo", "wrong_value", 0, "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Test Failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "bar", "")
}

// Ensure that the store can conditionally update a key if it has a previous index.
func TestStoreCompareAndSwapPrevIndex(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, err := s.CompareAndSwap("/foo", "", 1, "baz", Permanent)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.PrevValue, "bar", "")
	assert.Equal(t, e.Value, "baz", "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "baz", "")
}

// Ensure that the store cannot conditionally update a key if it has the wrong previous index.
func TestStoreCompareAndSwapPrevIndexFailsIfNotMatch(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	e, _err := s.CompareAndSwap("/foo", "", 100, "baz", Permanent)
	err := _err.(*Err.Error)
	assert.Equal(t, err.ErrorCode, Err.EcodeTestFailed, "")
	assert.Equal(t, err.Message, "Test Failed", "")
	assert.Nil(t, e, "")
	e, _ = s.Get("/foo", false, false)
	assert.Equal(t, e.Value, "bar", "")
}

// Ensure that the store can watch for key creation.
func TestStoreWatchCreate(t *testing.T) {
	s := newStore()
	c, _ := s.Watch("/foo", false, 0)
	s.Create("/foo", "bar", false, Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Key, "/foo", "")
	e = nbselect(c)
	assert.Nil(t, e, "")
}

// Ensure that the store can watch for recursive key creation.
func TestStoreWatchRecursiveCreate(t *testing.T) {
	s := newStore()
	c, _ := s.Watch("/foo", true, 0)
	s.Create("/foo/bar", "baz", false, Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "create", "")
	assert.Equal(t, e.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key updates.
func TestStoreWatchUpdate(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	c, _ := s.Watch("/foo", false, 0)
	s.Update("/foo", "baz", Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Key, "/foo", "")
}

// Ensure that the store can watch for recursive key updates.
func TestStoreWatchRecursiveUpdate(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", "baz", false, Permanent)
	c, _ := s.Watch("/foo", true, 0)
	s.Update("/foo/bar", "baz", Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "update", "")
	assert.Equal(t, e.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key deletions.
func TestStoreWatchDelete(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	c, _ := s.Watch("/foo", false, 0)
	s.Delete("/foo", false)
	e := nbselect(c)
	assert.Equal(t, e.Action, "delete", "")
	assert.Equal(t, e.Key, "/foo", "")
}

// Ensure that the store can watch for recursive key deletions.
func TestStoreWatchRecursiveDelete(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", "baz", false, Permanent)
	c, _ := s.Watch("/foo", true, 0)
	s.Delete("/foo/bar", false)
	e := nbselect(c)
	assert.Equal(t, e.Action, "delete", "")
	assert.Equal(t, e.Key, "/foo/bar", "")
}

// Ensure that the store can watch for CAS updates.
func TestStoreWatchCompareAndSwap(t *testing.T) {
	s := newStore()
	s.Create("/foo", "bar", false, Permanent)
	c, _ := s.Watch("/foo", false, 0)
	s.CompareAndSwap("/foo", "bar", 0, "baz", Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Key, "/foo", "")
}

// Ensure that the store can watch for recursive CAS updates.
func TestStoreWatchRecursiveCompareAndSwap(t *testing.T) {
	s := newStore()
	s.Create("/foo/bar", "baz", false, Permanent)
	c, _ := s.Watch("/foo", true, 0)
	s.CompareAndSwap("/foo/bar", "baz", 0, "bat", Permanent)
	e := nbselect(c)
	assert.Equal(t, e.Action, "compareAndSwap", "")
	assert.Equal(t, e.Key, "/foo/bar", "")
}

// Ensure that the store can watch for key expiration.
func TestStoreWatchExpire(t *testing.T) {
	s := newStore()

	stopChan := make(chan bool)
	defer func() {
		stopChan <- true
	}()

	go mockSyncService(s.DeleteExpiredKeys, stopChan)
	s.Create("/foo", "bar", false, time.Now().Add(500*time.Millisecond))
	s.Create("/foofoo", "barbarbar", false, time.Now().Add(500*time.Millisecond))

	c, _ := s.Watch("/", true, 0)
	e := nbselect(c)
	assert.Nil(t, e, "")
	time.Sleep(600 * time.Millisecond)
	e = nbselect(c)
	assert.Equal(t, e.Action, "expire", "")
	assert.Equal(t, e.Key, "/foo", "")
	c, _ = s.Watch("/", true, 4)
	e = nbselect(c)
	assert.Equal(t, e.Action, "expire", "")
	assert.Equal(t, e.Key, "/foofoo", "")
}

// Ensure that the store can recover from a previously saved state.
func TestStoreRecover(t *testing.T) {
	s := newStore()
	s.Create("/foo", "", false, Permanent)
	s.Create("/foo/x", "bar", false, Permanent)
	s.Create("/foo/y", "baz", false, Permanent)
	b, err := s.Save()

	s2 := newStore()
	s2.Recovery(b)

	e, err := s.Get("/foo/x", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Value, "bar", "")

	e, err = s.Get("/foo/y", false, false)
	assert.Nil(t, err, "")
	assert.Equal(t, e.Value, "baz", "")
}

// Ensure that the store can recover from a previously saved state that includes an expiring key.
func TestStoreRecoverWithExpiration(t *testing.T) {
	s := newStore()

	c := make(chan bool)
	defer func() {
		c <- true
	}()

	go mockSyncService(s.DeleteExpiredKeys, c)
	s.Create("/foo", "", false, Permanent)
	s.Create("/foo/x", "bar", false, Permanent)
	s.Create("/foo/y", "baz", false, time.Now().Add(5*time.Millisecond))
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
	assert.Equal(t, e.Value, "bar", "")

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
	ticker := time.Tick(time.Millisecond * 500)
	for {
		select {
		case <-c:
			return
		case now := <-ticker:
			f(now)
		}
	}
}
