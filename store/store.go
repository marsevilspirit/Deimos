package store

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	Err "github.com/marsevilspirit/marstore/error"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

type Store interface {
	Version() int
	// CommandFactory() CommandFactory
	Index() uint64
	Get(nodePath string, recursive, sorted bool) (*Event, error)
	Set(nodePath string, value string, expireTime time.Time) (*Event, error)
	Update(nodePath string, newValue string, expireTime time.Time) (*Event, error)
	Create(nodePath string, value string, incrementalSuffix bool,
		expireTime time.Time) (*Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireTime time.Time) (*Event, error)
	Delete(nodePath string, recursive bool) (*Event, error)
	Watch(prefix string, recursive bool, sinceIndex uint64) (<-chan *Event, error)
	Save() ([]byte, error)
	Recovery(state []byte) error
	TotalTransactions() uint64
	JsonStats() []byte
	DeleteExpiredKeys(cutoff time.Time)
}

type store struct {
	Root           *node
	WatcherHub     *watcherHub
	CurrentIndex   uint64
	Stats          *Stats
	CurrentVersion int
	ttlKeyHeap     *ttlKeyHeap  // need to recovery manually
	worldLock      sync.RWMutex // stop the world lock
}

func New() Store {
	return newStore()
}

func newStore() *store {
	s := new(store)
	s.CurrentVersion = defaultVersion
	s.Root = newDir(s, "/", s.CurrentIndex, nil, "", Permanent)
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)
	s.ttlKeyHeap = newTtlKeyHeap()

	return s
}

// Version retrieves current version of the store.
func (s *store) Version() int {
	return s.CurrentVersion
}

// Retireves current of the store
func (s *store) Index() uint64 {
	return s.CurrentIndex
}

// Get function returns a get event.
// If recursive is true, it will return all the content under the node path.
// If sorted is true, it will sort the content by keys.
func (s *store) Get(nodePath string, recursive, sorted bool) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)
	if err != nil {
		s.Stats.Inc(GetFail)
		return nil, err
	}

	e := newEvent(Get, nodePath, n.ModifiedIndex)

	if n.IsDir() { // node is dir
		e.Dir = true

		children, _ := n.List()
		e.KVPairs = make([]KeyValuePair, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0
		for _, child := range children {
			if child.IsHidden() { // get will not list hidden node
				continue
			}
			e.KVPairs[i] = child.Pair(recursive, sorted)
			i++
		}
		// eliminate hidden nodes
		e.KVPairs = e.KVPairs[:i]

		if sorted {
			sort.Sort(e.KVPairs)
		}
	} else { // node is file
		e.Value, _ = n.Read()
	}

	e.Expiration, e.TTL = n.ExpirationAndTTL()

	s.Stats.Inc(GetSuccess)

	return e, nil
}

// Create function creates the Node at nodePath.
// Create will help to create intermediate directories with no ttl.
// value is "", then create a dir.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
func (s *store) Create(nodePath string, value string, unique bool,
	expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	e, err := s.internalCreate(nodePath, value, unique, false, expireTime, Create)
	if err != nil {
		s.Stats.Inc(CreateFail)
	} else {
		s.Stats.Inc(CreateSuccess)
	}
	return e, err
}

// Set function creates or replace the Node at nodePath.
func (s *store) Set(nodePath string, value string, expireTime time.Time) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()
	e, err := s.internalCreate(nodePath, value, false, true, expireTime, Set)
	if err != nil {
		s.Stats.Inc(SetFail)
	} else {
		s.Stats.Inc(SetSuccess)
	}
	return e, err
}

func (s *store) CompareAndSwap(nodePath string, prevValue string,
	prevIndex uint64, value string, expireTime time.Time) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	n, err := s.internalGet(nodePath)
	if err != nil {
		s.Stats.Inc(CompareAndSwapFail)
		return nil, err
	}

	if n.IsDir() { // can only test and set file
		s.Stats.Inc(CompareAndSwapFail)
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if (prevValue == "" || n.Value == prevValue) && (prevIndex == 0 || n.ModifiedIndex == prevIndex) {
		// update marstore index
		s.CurrentIndex++

		e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex)
		e.PrevValue = n.Value

		// if test succeed, write the value
		n.Write(value, s.CurrentIndex)

		n.UpdateTTL(expireTime)

		e.Value = value
		e.Expiration, e.TTL = n.ExpirationAndTTL()

		s.WatcherHub.notify(e)
		s.Stats.Inc(CompareAndSwapSuccess)
		return e, nil
	}

	cause := fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	s.Stats.Inc(CompareAndSwapFail)
	return nil, Err.NewError(Err.EcodeTestFailed, cause, s.CurrentIndex)
}

// Delete function deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, recursive bool) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nextIndex := s.CurrentIndex + 1

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	e := newEvent(Delete, nodePath, nextIndex)

	if n.IsDir() {
		e.Dir = true
	} else {
		e.PrevValue = n.Value
	}

	callback := func(path string) { // notify fuction
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(recursive, callback)
	if err != nil {
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	// update marstore index
	s.CurrentIndex++

	s.WatcherHub.notify(e)
	s.Stats.Inc(DeleteSuccess)

	return e, nil
}

func (s *store) Watch(prefix string, recursive bool, sinceIndex uint64) (<-chan *Event, error) {
	prefix = path.Clean(path.Join("/", prefix))

	nextIndex := s.CurrentIndex + 1

	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	var c <-chan *Event
	var err *Err.Error

	if sinceIndex == 0 {
		c, err = s.WatcherHub.watch(prefix, recursive, nextIndex)
	} else {
		c, err = s.WatcherHub.watch(prefix, recursive, sinceIndex)
	}

	if err != nil {
		// watchhub do not know the current Index
		// we need to attach the currentIndex here
		err.Index = s.CurrentIndex
		return nil, err
	}

	return c, nil
}

// walk function walks all the nodePath and
// apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *node, component string) (*node, *Err.Error)) (*node, *Err.Error) {
	components := strings.Split(nodePath, "/")
	curr := s.Root
	var err *Err.Error
	for i := 1; i < len(components); i++ {
		if len(components[i]) == 0 { // ignore empty string
			return curr, nil
		}

		curr, err = walkFunc(curr, components[i])
		if err != nil {
			return nil, err
		}
	}
	return curr, nil
}

// Update function updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(UpdateFail)
		return nil, err
	}

	e := newEvent(Update, nodePath, nextIndex)

	if len(newValue) != 0 {
		if n.IsDir() {
			// if the node is a directory, we cannot update value
			s.Stats.Inc(UpdateFail)
			return nil, Err.NewError(Err.EcodeNotFile, nodePath, currIndex)
		}

		e.PrevValue = n.Value
		n.Write(newValue, nextIndex)
		e.Value = newValue
	} else {
		// do not update value
		e.Value = n.Value
	}

	// update ttl
	n.UpdateTTL(expireTime)

	e.Expiration, e.TTL = n.ExpirationAndTTL()

	s.WatcherHub.notify(e)

	s.Stats.Inc(UpdateSuccess)

	s.CurrentIndex = nextIndex

	return e, nil
}

func (s *store) internalCreate(nodePath string, value string, unique bool,
	replace bool, expireTime time.Time, action string) (*Event, error) {
	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique {
		// append unique item under the node path
		nodePath += "/" + strconv.FormatUint(nextIndex, 10)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	// Assume expire times that are way in the past are not valid.
	// This can occur when the time is serialized to JSON and read back in.
	if expireTime.Before(minExpireTime) {
		expireTime = Permanent
	}

	dir, newNodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dir, s.checkDir)
	if err != nil {
		s.Stats.Inc(SetFail)
		err.Index = currIndex
		return nil, err
	}

	e := newEvent(action, nodePath, nextIndex)

	n, _ := d.GetChild(newNodeName)
	// force will try to replace the existing file
	if n != nil {
		if replace {
			if n.IsDir() {
				return nil, Err.NewError(Err.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevValue, _ = n.Read()
			n.Remove(false, nil)
		} else {
			return nil, Err.NewError(Err.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if len(value) != 0 { // create file
		e.Value = value
		n = newKV(s, nodePath, value, nextIndex, d, "", expireTime)
	} else { // create directory
		e.Dir = true
		n = newDir(s, nodePath, nextIndex, d, "", expireTime)

	}

	// we are sure d is a directory and does not have the children with name n.Name
	d.Add(n)

	// Node with TTL
	if !n.IsPermanent() {
		s.ttlKeyHeap.push(n)

		e.Expiration, e.TTL = n.ExpirationAndTTL()
	}

	s.CurrentIndex = nextIndex

	s.WatcherHub.notify(e)
	return e, nil
}

// internalGet function get the node of the given nodePath.
func (s *store) internalGet(nodePath string) (*node, *Err.Error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	walkFunc := func(parent *node, name string) (*node, *Err.Error) {
		if !parent.IsDir() {
			err := Err.NewError(Err.EcodeNotDir, parent.Path, s.CurrentIndex)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, Err.NewError(Err.EcodeKeyNotFound, path.Join(parent.Path, name), s.CurrentIndex)
	}

	f, err := s.walk(nodePath, walkFunc)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// deleteExpiredKyes will delete all
func (s *store) DeleteExpiredKeys(cutoff time.Time) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	for {
		node := s.ttlKeyHeap.top()
		if node == nil || node.ExpireTime.After(cutoff) {
			break
		}

		s.ttlKeyHeap.pop()
		node.Remove(true, nil)

		s.CurrentIndex++

		s.Stats.Inc(ExpireCount)
		s.WatcherHub.notify(newEvent(Expire, node.Path, s.CurrentIndex))
	}
}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (s *store) checkDir(parent *node, dirName string) (*node, *Err.Error) {
	node, ok := parent.Children[dirName]
	if ok {
		if node.IsDir() {
			return node, nil
		}
		return nil, Err.NewError(Err.EcodeNotDir, parent.Path, s.CurrentIndex)
	}

	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, parent.ACL, Permanent)

	parent.Children[dirName] = n

	return n, nil
}

// Save function saves the static state of the store system.
// Save function will not be able to save the state of watchers.
// Save function will not save the parent field of the node.
// Or there will be cyclic dependencies issue for the json package.
func (s *store) Save() ([]byte, error) {
	s.worldLock.Lock()

	clonedStore := newStore()
	clonedStore.CurrentIndex = s.CurrentIndex
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()
	clonedStore.CurrentVersion = s.CurrentVersion

	s.worldLock.Unlock()

	b, err := json.Marshal(clonedStore)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (s *store) Recovery(state []byte) error {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	err := json.Unmarshal(state, s)
	if err != nil {
		return err
	}

	s.ttlKeyHeap = newTtlKeyHeap()

	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}

func (s *store) TotalTransactions() uint64 {
	return s.Stats.TotalTranscations()
}
