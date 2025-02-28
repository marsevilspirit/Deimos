package fileSystem

import (
	"fmt"
	"path"
	"strings"
	"time"

	Err "github.com/marsevilspirit/marstore/error"
)

type FileSystem struct {
	Root         *Node
	EventHistory *EventHistory
	WatcherHub   *watcherHub
	Index        uint64
	Term         uint64
}

func New() *FileSystem {
	return &FileSystem{
		Root:       newDir("/", 0, 0, nil, ""),
		WatcherHub: newWatchHub(1000),
	}

}

func (fs *FileSystem) Get(keyPath string, recusive bool, index uint64, term uint64) (*Event, error) {
	// TODO: add recursive get
	n, err := fs.InternalGet(keyPath, index, term)
	if err != nil {
		return nil, err
	}

	e := newEvent(Get, keyPath, index, term)

	if n.IsDir() { // node is dir
		children, _ := n.List()
		e.KVPairs = make([]KeyValuePair, len(children))

		// we do not use the index in the children slice directly
		// we need to skip the hidden one
		i := 0
		for _, child := range children {
			if child.IsHidden() { // get will not list hidden node
				continue
			}
			e.KVPairs[i] = child.Pair(recusive)
			i++
		}

		// eliminate hidden nodes
		e.KVPairs = e.KVPairs[:i]

	} else { // node is file
		e.Value = n.Value
	}
	return e, nil
}

func (fs *FileSystem) Create(keyPath string, value string, expireTime time.Time, index uint64, term uint64) (*Event, error) {
	keyPath = path.Clean("/" + keyPath)

	// make sure we can create the node
	_, err := fs.InternalGet(keyPath, index, term)
	if err == nil { // key already exists
		return nil, Err.NewError(105, keyPath)
	}

	error, _ := err.(Err.Error)

	if error.ErrorCode == 104 { // we cannot create the key due to meet a file while walking
		return nil, err
	}

	dir, _ := path.Split(keyPath)

	// walk through the keyPath, create dirs and get the last directory node
	d, err := fs.walk(dir, fs.checkDir)

	if err != nil {
		return nil, err
	}

	e := newEvent(Set, keyPath, fs.Index, fs.Term)
	e.Value = value

	f := newFile(keyPath, value, fs.Index, fs.Term, d, "", expireTime)

	err = d.Add(f)

	if err != nil {
		return nil, err
	}

	// Node with TTL
	if expireTime != Permanent {
		go f.Expire()
		e.Expiration = &f.ExpireTime
		e.TTL = int64(expireTime.Sub(time.Now()) / time.Second)
	}

	return e, nil
}

func (fs *FileSystem) Update(keyPath string, value string, expireTime time.Time, index uint64, term uint64) (*Event, error) {
	n, err := fs.InternalGet(keyPath, index, term)
	if err != nil { // if node does not exist, return error
		return nil, err
	}

	e := newEvent(Set, keyPath, fs.Index, fs.Term)

	if n.IsDir() { // can only update file
		if len(value) != 0 {
			return nil, Err.NewError(102, keyPath)
		}

		if n.ExpireTime != Permanent && expireTime == Permanent {
			n.stopExpire <- true
		}
	} else { // if the node is a file, we can update value and ttl
		e.PrevValue = n.Value

		if len(value) != 0 {
			e.Value = value
		}

		n.Write(value, index, term)

		if n.ExpireTime != Permanent && expireTime == Permanent {
			n.stopExpire <- true
		}

		// update ttl
		if expireTime != Permanent {
			go n.Expire()
			e.Expiration = &n.ExpireTime
			e.TTL = int64(expireTime.Sub(time.Now()) / time.Second)
		}
	}
	return e, nil
}

func (fs *FileSystem) TestAndSet(keyPath string, prevValue string, prevIndex uint64, value string, expireTime time.Time, index uint64, term uint64) (*Event, error) {
	f, err := fs.InternalGet(keyPath, index, term)
	if err != nil {
		return nil, err
	}

	if f.IsDir() { // can only test and set file
		return nil, Err.NewError(102, keyPath)
	}

	if f.Value == prevValue || f.ModifiedIndex == prevIndex {
		// if test succeed, write the value
		e := newEvent(TestAndSet, keyPath, index, term)
		e.PrevValue = f.Value
		e.Value = value
		f.Write(value, index, term)
		return e, nil
	}

	cause := fmt.Sprintf("[%v/%v] [%v/%v]", prevValue, f.Value, prevIndex, f.ModifiedIndex)
	return nil, Err.NewError(101, cause)
}

func (fs *FileSystem) Delete(keyPath string, recurisive bool, index uint64, term uint64) (*Event, error) {
	n, err := fs.InternalGet(keyPath, index, term)
	if err != nil {
		return nil, err
	}

	err = n.Remove(recurisive)
	if err != nil {
		return nil, err
	}

	e := newEvent(Delete, keyPath, index, term)

	if n.IsDir() {
		e.Dir = true
	} else {
		e.PrevValue = n.Value
	}

	return e, nil
}

// walk function walks all the keyPath and apply the walkFunc on each directory
func (fs *FileSystem) walk(keyPath string, walkFunc func(prev *Node, component string) (*Node, error)) (*Node, error) {
	components := strings.Split(keyPath, "/")
	curr := fs.Root
	var err error
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

// InternalGet function get the node of the given keyPath.
func (fs *FileSystem) InternalGet(keyPath string, index uint64, term uint64) (*Node, error) {
	keyPath = path.Clean("/" + keyPath)

	// update file system known index and term
	fs.Index, fs.Term = index, term

	walkFunc := func(parent *Node, dirName string) (*Node, error) {
		child, ok := parent.Children[dirName]
		if ok {
			return child, nil
		}

		return nil, Err.NewError(100, "get")
	}

	f, err := fs.walk(keyPath, walkFunc)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// checkDir function will check whether the component is a directory under parent node.
// If it is a directory, this function will return the pointer to that node.
// If it does not exist, this function will create a new directory and return the pointer to that node.
// If it is a file, this function will return error.
func (fs *FileSystem) checkDir(parent *Node, dirName string) (*Node, error) {
	subDir, ok := parent.Children[dirName]
	if ok {
		return subDir, nil
	}

	n := newDir(path.Join(parent.Path, dirName), fs.Index, fs.Term, parent, parent.ACL)

	parent.Children[dirName] = n

	return n, nil
}
