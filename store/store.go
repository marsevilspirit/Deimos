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

type Store interface {
	Get(nodePath string, recursive, sorted bool, index uint64, term uint64) (*Event, error)
	Create(nodePath string, value string, incrementalSuffix bool, force bool,
		expireTime time.Time, index uint64, term uint64) (*Event, error)
	Update(nodePath string, newValue string, expireTime time.Time, index uint64, term uint64) (*Event, error)
	TestAndSet(nodePath string, prevValue string, prevIndex uint64,
		value string, expireTime time.Time, index uint64, term uint64) (*Event, error)
	Delete(nodePath string, recursive bool, index uint64, term uint64) (*Event, error)
	Watch(prefix string, recursive bool, sinceIndex uint64, index uint64, term uint64) (<-chan *Event, error)
	Save() ([]byte, error)
	Recovery(state []byte) error
	JsonStats() []byte
}

type store struct {
	Root       *Node
	WatcherHub *watcherHub
	Index      uint64
	Term       uint64
	Stats      *Stats
	worldLock  sync.RWMutex // stop the world lock
}

func New() Store {
	return newStore()
}

func newStore() *store {
	s := new(store)
	s.Root = newDir(s, "/", UndefIndex, UndefTerm, nil, "", Permanent)
	s.Stats = newStats()
	s.WatcherHub = newWatchHub(1000)

	return s
}

// Get function returns a get event.
// If recursive is true, it will return all the content under the node path.
// If sorted is true, it will sort the content by keys.
func (s *store) Get(nodePath string, recursive, sorted bool, index uint64, term uint64) (*Event, error) {
	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath, index, term)
	if err != nil {
		s.Stats.Inc(GetFail)
		return nil, err
	}

	e := newEvent(Get, nodePath, index, term)

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
func (s *store) Create(nodePath string, value string, incrementalSuffix bool, force bool, expireTime time.Time, index uint64, term uint64) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	return s.internalCreate(nodePath, value, incrementalSuffix, force, expireTime, index, term, Create)
}

// Update function updates the value/ttl of the node.
// If the node is a file, the value and the ttl can be updated.
// If the node is a directory, only the ttl can be updated.
func (s *store) Update(nodePath string, newValue string, expireTime time.Time, index uint64, term uint64) (*Event, error) {
	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	nodePath = path.Clean(path.Join("/", nodePath))

	n, err := s.internalGet(nodePath, index, term)
	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(UpdateFail)
		return nil, err
	}

	// create the event of update
	e := newEvent(Update, nodePath, s.Index, s.Term)

	if len(newValue) != 0 {
		if n.IsDir() {
			// if the node is a directory, we cannot update value
			s.Stats.Inc(UpdateFail)

			return nil, Err.NewError(Err.EcodeNotFile, nodePath, index, term)
		}

		e.PrevValue = n.Value
		n.Write(newValue, index, term)
	}

	// update ttl
	n.UpdateTTL(expireTime)

	e.Expiration, e.TTL = n.ExpirationAndTTL()

	s.WatcherHub.notify(e)

	s.Stats.Inc(UpdateSuccess)

	return e, nil
}

func (s *store) TestAndSet(nodePath string, prevValue string,
	prevIndex uint64, value string, expireTime time.Time, index uint64,
	term uint64) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	if prevValue == "" && prevIndex == 0 { // try create
		return s.internalCreate(nodePath, value, false, false, expireTime, index, term, TestAndSet)
	}

	n, err := s.internalGet(nodePath, index, term)
	if err != nil {
		s.Stats.Inc(TestAndSetFail)
		return nil, err
	}

	if n.IsDir() { // can only test and set file
		s.Stats.Inc(TestAndSetFail)
		return nil, Err.NewError(Err.EcodeNotFile, nodePath, index, term)
	}

	if n.Value == prevValue || n.ModifiedIndex == prevIndex {
		e := newEvent(TestAndSet, nodePath, index, term)
		e.PrevValue = n.Value

		// if test succeed, write the value
		n.Write(value, index, term)

		n.UpdateTTL(expireTime)

		e.Value = value
		e.Expiration, e.TTL = n.ExpirationAndTTL()

		s.WatcherHub.notify(e)
		s.Stats.Inc(TestAndSetSuccess)
		return e, nil
	}

	cause := fmt.Sprintf("[%v != %v] [%v != %v]", prevValue, n.Value, prevIndex, n.ModifiedIndex)
	s.Stats.Inc(TestAndSetFail)
	return nil, Err.NewError(Err.EcodeTestFailed, cause, index, term)
}

// Delete function deletes the node at the given path.
// If the node is a directory, recursive must be true to delete it.
func (s *store) Delete(nodePath string, recursive bool, index uint64, term uint64) (*Event, error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	s.worldLock.Lock()
	defer s.worldLock.Unlock()

	n, err := s.internalGet(nodePath, index, term)
	if err != nil { // if the node does not exist, return error
		s.Stats.Inc(DeleteFail)
		return nil, err
	}

	e := newEvent(Delete, nodePath, index, term)

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

	s.WatcherHub.notify(e)
	s.Stats.Inc(DeleteSuccess)
	return e, nil
}

func (s *store) Watch(prefix string, recursive bool, sinceIndex uint64, index uint64, term uint64) (<-chan *Event, error) {
	prefix = path.Clean(path.Join("/", prefix))

	s.worldLock.RLock()
	defer s.worldLock.RUnlock()

	s.Index, s.Term = index, term

	var c <-chan *Event
	var err *Err.Error

	if sinceIndex == 0 {
		c, err = s.WatcherHub.watch(prefix, recursive, index+1)
	} else {
		c, err = s.WatcherHub.watch(prefix, recursive, sinceIndex)
	}

	if err != nil {
		err.Index = index
		err.Term = term
		return nil, err
	}

	return c, nil
}

// walk function walks all the nodePath and
// apply the walkFunc on each directory
func (s *store) walk(nodePath string, walkFunc func(prev *Node, component string) (*Node, *Err.Error)) (*Node, *Err.Error) {
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

func (s *store) internalCreate(nodePath string, value string, incrementalSuffix bool, force bool, expireTime time.Time, index uint64, term uint64, action string) (*Event, error) {
	s.Index, s.Term = index, term

	// append unique incremental suffix to the node path
	if incrementalSuffix {
		nodePath += "_" + strconv.FormatUint(index, 10)
	}

	nodePath = path.Clean(path.Join("/", nodePath))

	dir, newNodeName := path.Split(nodePath)

	// walk through the nodePath, create dirs and get the last directory node
	d, err := s.walk(dir, s.checkDir)
	if err != nil {
		s.Stats.Inc(SetFail)
		return nil, err
	}

	e := newEvent(action, nodePath, s.Index, s.Term)

	n, _ := d.GetChild(newNodeName)
	if n != nil {
		if force {
			if n.IsDir() {
				return nil, Err.NewError(Err.EcodeNotFile, nodePath, index, term)
			}
			e.PrevValue, _ = n.Read()
			n.Remove(false, nil)
		} else {
			return nil, Err.NewError(Err.EcodeNodeExist, nodePath, index, term)
		}
	}

	if len(value) != 0 { // create file
		e.Value = value
		n = newKV(s, nodePath, value, index, term, d, "", expireTime)
	} else { // create directory
		e.Dir = true
		n = newDir(s, nodePath, index, term, d, "", expireTime)

	}

	err = d.Add(n)
	if err != nil {
		s.Stats.Inc(SetFail)
		return nil, err
	}

	// Node with TTL
	if expireTime.Sub(Permanent) != 0 {
		n.Expire()
		e.Expiration, e.TTL = n.ExpirationAndTTL()
	}

	s.WatcherHub.notify(e)
	s.Stats.Inc(SetSuccess)
	return e, nil
}

// internalGet function get the node of the given nodePath.
func (s *store) internalGet(nodePath string, index uint64, term uint64) (*Node, *Err.Error) {
	nodePath = path.Clean(path.Join("/", nodePath))

	// update file system known index and term
	if index > s.Index {
		s.Index, s.Term = index, term
	}

	walkFunc := func(parent *Node, name string) (*Node, *Err.Error) {
		if !parent.IsDir() {
			err := Err.NewError(Err.EcodeNotDir, parent.Path, index, term)
			return nil, err
		}

		child, ok := parent.Children[name]
		if ok {
			return child, nil
		}

		return nil, Err.NewError(Err.EcodeKeyNotFound, path.Join(parent.Path, name), index, term)
	}

	f, err := s.walk(nodePath, walkFunc)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// checkDir will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (s *store) checkDir(parent *Node, dirName string) (*Node, *Err.Error) {
	node, ok := parent.Children[dirName]
	if ok {
		if node.IsDir() {
			return node, nil
		}
		return nil, Err.NewError(Err.EcodeNotDir, parent.Path, UndefIndex, UndefTerm)
	}

	n := newDir(s, path.Join(parent.Path, dirName), s.Index, s.Term, parent, parent.ACL, Permanent)

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
	clonedStore.Index = s.Index
	clonedStore.Term = s.Term
	clonedStore.Root = s.Root.Clone()
	clonedStore.WatcherHub = s.WatcherHub.clone()
	clonedStore.Stats = s.Stats.clone()

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
	s.Root.recoverAndclean()
	return nil
}

func (s *store) JsonStats() []byte {
	s.Stats.Watchers = uint64(s.WatcherHub.count)
	return s.Stats.toJson()
}
