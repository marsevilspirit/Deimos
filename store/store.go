package store

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	Err "github.com/marsevilspirit/deimos/error"
)

// The default version to set when the store is first initialized.
const defaultVersion = 2

var minExpireTime time.Time

func init() {
	minExpireTime, _ = time.Parse(time.RFC3339, "2000-01-01T00:00:00Z")
}

type Store interface {
	Version() int
	Index() uint64

	Get(nodePath string, recursive, sorted bool) (*Event, error)
	Set(nodePath string, dir bool, value string, expireTime time.Time) (*Event, error)
	Update(nodePath string, newValue string, expireTime time.Time) (*Event, error)
	Create(nodePath string, dir bool, value string, unique bool,
		expireTime time.Time) (*Event, error)
	CompareAndSwap(nodePath string, prevValue string, prevIndex uint64,
		value string, expireTime time.Time) (*Event, error)
	Delete(nodePath string, dir, recursive bool) (*Event, error)
	CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error)
	Watch(prefix string, recursive, stream bool, sinceIndex uint64) (Watcher, error)

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
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()
	return s.CurrentIndex
}

// Get returns a get event.
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

	e := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
	e.DeimosIndex = s.CurrentIndex
	e.Node.loadInternalNode(n, recursive, sorted)

	s.Stats.Inc(GetSuccess)

	return e, nil
}

// Create creates the Node at nodePath.
// Create will help to create intermediate directories with no ttl.
// value is "", then create a dir.
// If the node has already existed, create will fail.
// If any node on the path is a file, create will fail.
func (s *store) Create(nodePath string, dir bool, value string, unique bool,
	expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	e, err := s.internalCreate(nodePath, dir, value, unique, false, expireTime, Create)
	if err == nil {
		e.DeimosIndex = s.CurrentIndex
		s.WatcherHub.notify(e)
		s.Stats.Inc(CreateSuccess)
	} else {
		s.Stats.Inc(CreateFail)
	}
	return e, err
}

// Set creates or replace the Node at nodePath.
func (s *store) Set(nodePath string, dir bool, value string, expireTime time.Time) (*Event, error) {
	var err error

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	defer func() {
		if err == nil {
			s.Stats.Inc(SetSuccess)
		} else {
			s.Stats.Inc(SetFail)
		}
	}()

	// Get prevNode value
	n, getErr := s.internalGet(nodePath)
	if getErr != nil && getErr.ErrorCode != Err.EcodeKeyNotFound {
		err = getErr
		return nil, err
	}

	// Set new value
	e, err := s.internalCreate(nodePath, dir, value, false, true, expireTime, Set)
	if err != nil {
		return nil, err
	}
	e.DeimosIndex = s.CurrentIndex

	// Put prevNode into event
	if getErr == nil {
		prev := newEvent(Get, nodePath, n.ModifiedIndex, n.CreatedIndex)
		prev.Node.loadInternalNode(n, false, false)
		e.PrevNode = prev.Node
	}

	s.WatcherHub.notify(e)

	return e, nil
}

// returns user-readable cause of failed comparison
func getCompareFailCause(n *node, which int, prevValue string, prevIndex uint64) string {
	switch which {
	case CompareIndexNotMatch:
		return fmt.Sprintf("[%v != %v]", prevIndex, n.ModifiedIndex)
	case CompareValueNotMatch:
		return fmt.Sprintf("[%v != %v]", prevValue, n.Value)
	default:
		return fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	}
}

func (s *store) CompareAndSwap(nodePath string, prevValue string,
	prevIndex uint64, value string, expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if nodePath == "/" {
		return nil, Err.NewError(Err.EcodeRootROnly, "/", s.CurrentIndex)
	}

	n, err := s.internalGet(nodePath)
	if err != nil {
		s.Stats.Inc(CompareAndSwapFail)
		return nil, err
	}

	if n.IsDir() { // can only compare and swap file
		s.Stats.Inc(CompareAndSwapFail)
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		s.Stats.Inc(CompareAndSwapFail)
		return nil, Err.NewError(Err.EcodeTestFailed, cause, s.CurrentIndex)

	}

	// update marstore index
	s.CurrentIndex++

	e := newEvent(CompareAndSwap, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.DeimosIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false)
	eNode := e.Node

	// if test succeed, write the value
	n.Write(value, s.CurrentIndex)

	n.UpdateTTL(expireTime)

	// copy the value for safety
	valueCopy := value
	eNode.Value = &valueCopy
	eNode.Expiration, eNode.TTL = n.ExpirationAndTTL()

	s.WatcherHub.notify(e)
	s.Stats.Inc(CompareAndSwapSuccess)

	return e, nil
}

// Delete deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, dir, recursive bool) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if nodePath == "/" {
		return nil, Err.NewError(Err.EcodeRootROnly, "/", s.CurrentIndex)
	}

	// recursive implies dir
	if recursive == true {
		dir = true
	}

	n, err := s.internalGet(nodePath)
	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	nextIndex := s.CurrentIndex + 1
	e := newEvent(Delete, nodePath, nextIndex, n.CreatedIndex)
	e.DeimosIndex = nextIndex
	e.PrevNode = n.Repr(false, false)
	eNode := e.Node

	if n.IsDir() {
		eNode.Dir = true
	}

	callback := func(path string) { // notify fuction
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	err = n.Remove(dir, recursive, callback)
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

func (s *store) CompareAndDelete(nodePath string, prevValue string, prevIndex uint64) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(CompareAndDeleteFail)
		return nil, err
	}

	if n.IsDir() { // can only compare and delete file
		s.Stats.Inc(CompareAndSwapFail)
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, s.CurrentIndex)
	}

	// If both of the prevValue and prevIndex are given, we will test both of them.
	// Command will be executed, only if both of the tests are successful.
	if ok, which := n.Compare(prevValue, prevIndex); !ok {
		cause := getCompareFailCause(n, which, prevValue, prevIndex)
		s.Stats.Inc(CompareAndDeleteFail)
		return nil, Err.NewError(Err.EcodeTestFailed, cause, s.CurrentIndex)
	}

	// update mars index
	s.CurrentIndex++

	e := newEvent(CompareAndDelete, nodePath, s.CurrentIndex, n.CreatedIndex)
	e.DeimosIndex = s.CurrentIndex
	e.PrevNode = n.Repr(false, false)

	callback := func(path string) { // notify function
		// notify the watchers with deleted set true
		s.WatcherHub.notifyWatchers(e, path, true)
	}

	// delete a key-value pair, no error should happen
	n.Remove(false, false, callback)

	s.WatcherHub.notify(e)
	s.Stats.Inc(CompareAndDeleteSuccess)

	return e, nil
}

func (s *store) Watch(key string, recursive, stream bool, sinceIndex uint64) (Watcher, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	key = path.Clean(path.Join("/", key))

	if sinceIndex == 0 {
		sinceIndex = s.CurrentIndex + 1
	}

	// WatcherHub does not know about the current index, so we need to pass it in
	w, err := s.WatcherHub.watch(key, recursive, stream, sinceIndex, s.CurrentIndex)
	if err != nil {
		return nil, err
	}

	return w, nil
}

// walk walks all the nodePath and
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

// Update updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireTime time.Time) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if nodePath == "/" {
		return nil, Err.NewError(Err.EcodeRootROnly, "/", s.CurrentIndex)
	}

	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	n, err := s.internalGet(nodePath)

	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(UpdateFail)
		return nil, err
	}

	e := newEvent(Update, nodePath, nextIndex, n.CreatedIndex)
	e.DeimosIndex = nextIndex
	e.PrevNode = n.Repr(false, false)
	eNode := e.Node

	if n.IsDir() && len(newValue) != 0 {
		// if the node is a directory, we cannot update to non-empty
		s.Stats.Inc(UpdateFail)
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, currIndex)
	}

	n.Write(newValue, nextIndex)

	if n.IsDir() {
		eNode.Dir = true
	} else {
		// copy the value for safety
		newValueCopy := newValue
		eNode.Value = &newValueCopy
	}

	// update ttl
	n.UpdateTTL(expireTime)

	eNode.Expiration, eNode.TTL = n.ExpirationAndTTL()

	s.WatcherHub.notify(e)

	s.Stats.Inc(UpdateSuccess)

	s.CurrentIndex = nextIndex

	return e, nil
}

func (s *store) internalCreate(nodePath string, dir bool, value string, unique,
	replace bool, expireTime time.Time, action string) (*Event, error) {
	currIndex, nextIndex := s.CurrentIndex, s.CurrentIndex+1

	if unique {
		// append unique item under the node path
		nodePath += "/" + strconv.FormatUint(nextIndex, 10)
	}

	nodePath = path.Clean(path.Join("/", nodePath))
	// we do not allow the user to change "/"
	if nodePath == "/" {
		return nil, Err.NewError(Err.EcodeRootROnly, "/", currIndex)
	}

	// Assume expire times that are way in the past are not valid.
	// This can occur when the time is serialized to JSON and read back in.
	if expireTime.Before(minExpireTime) {
		expireTime = Permanent
	}

	dirName, nodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dirName, s.checkDir)
	if err != nil {
		s.Stats.Inc(SetFail)
		err.Index = currIndex
		return nil, err
	}

	e := newEvent(action, nodePath, nextIndex, nextIndex)
	eNode := e.Node

	n, _ := d.GetChild(nodeName)
	// force will try to replace the existing file
	if n != nil {
		if replace {
			if n.IsDir() {
				return nil, Err.NewError(Err.EcodeNotFile, nodePath, currIndex)
			}
			e.PrevNode = n.Repr(false, false)

			n.Remove(false, false, nil)
		} else {
			return nil, Err.NewError(Err.EcodeNodeExist, nodePath, currIndex)
		}
	}

	if !dir { // create file
		// copy the value for safety
		valueCopy := value
		eNode.Value = &valueCopy
		n = newKV(s, nodePath, value, nextIndex, d, "", expireTime)
	} else { // create directory
		eNode.Dir = true
		n = newDir(s, nodePath, nextIndex, d, "", expireTime)

	}

	// we are sure d is a directory and does not have the children with name n.Name
	d.Add(n)

	// Node with TTL
	if !n.IsPermanent() {
		s.ttlKeyHeap.push(n)

		eNode.Expiration, eNode.TTL = n.ExpirationAndTTL()
	}

	s.CurrentIndex = nextIndex

	return e, nil
}

// internalGet get the node of the given nodePath.
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

		s.CurrentIndex++
		e := newEvent(Expire, node.Path, s.CurrentIndex, node.CreatedIndex)
		e.DeimosIndex = s.CurrentIndex
		e.PrevNode = node.Repr(false, false)

		callback := func(path string) { // notify function
			// notify the watchers with deleted set true
			s.WatcherHub.notifyWatchers(e, path, true)
		}

		s.ttlKeyHeap.pop()
		node.Remove(true, true, callback)

		s.Stats.Inc(ExpireCount)

		s.WatcherHub.notify(e)
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
		return nil, Err.NewError(Err.EcodeNotDir, node.Path, s.CurrentIndex)
	}

	n := newDir(s, path.Join(parent.Path, dirName), s.CurrentIndex+1, parent, parent.ACL, Permanent)

	parent.Children[dirName] = n

	return n, nil
}

// Save saves the static state of the store system.
// It will not be able to save the state of watchers.
// It will not save the parent field of the node.
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

// Recovery recovers the store system from a static state
// It needs to recover the parent field of the nodes.
// It needs to delete the expired nodes since the saved time and also
// needs to create monitoring go routines.
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
