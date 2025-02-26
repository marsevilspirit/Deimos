package store

import (
	"path"
	"strings"
)

type WatcherHub struct {
	watchers map[string][]Watcher
}

type Watcher struct {
	c chan Response
}

// global watcherHub
var watcherHub *WatcherHub

func init() {
	watcherHub = createWatcherHub()
}

// create a new watcher
func createWatcherHub() *WatcherHub {
	return &WatcherHub{
		watchers: make(map[string][]Watcher),
	}
}

func GetWatcherHub() *WatcherHub {
	return watcherHub
}

// register a function with channel and prefix to the watcher
func AddWatcher(prefix string, c chan Response, sinceIndex uint64) error {
	prefix = "/" + path.Clean(prefix)
	if sinceIndex != 0 && sinceIndex >= store.ResponseStartIndex {
		for i := sinceIndex; i <= store.ResponseStartIndex; i++ {
			if check(prefix, i) {
				c <- store.Responses[i]
				return nil
			}
		}
	}
	_, ok := watcherHub.watchers[prefix]
	if !ok {
		watcherHub.watchers[prefix] = make([]Watcher, 0)
		watcher := Watcher{c}
		watcherHub.watchers[prefix] = append(watcherHub.watchers[prefix], watcher)
	} else {
		watcher := Watcher{c}
		watcherHub.watchers[prefix] = append(watcherHub.watchers[prefix], watcher)
	}
	return nil
}

// check if the response has what we are waching
func check(prefix string, index uint64) bool {
	index = index - store.ResponseStartIndex
	if index < 0 {
		return false
	}
	path := store.Responses[index].Key
	if strings.HasPrefix(path, prefix) {
		prefixLen := len(prefix)
		if len(path) == prefixLen || path[prefixLen] == '/' {
			return true
		}
	}
	return false
}

// notify the watcher a action happened
func notify(resp Response) error {
	resp.Key = path.Clean(resp.Key)
	segments := strings.Split(resp.Key, "/")
	currPath := ""

	// walk through all the pathes
	for _, segment := range segments {
		currPath = path.Join(currPath, segment)

		watchers, ok := watcherHub.watchers[currPath]
		if ok {
			newWatchers := make([]Watcher, 0)
			// notify all the watchers
			for _, watcher := range watchers {
				watcher.c <- resp
			}

			if len(newWatchers) == 0 {
				// we have notified all the watchers at this path
				// delete the map
				delete(watcherHub.watchers, currPath)
			} else {
				watcherHub.watchers[currPath] = newWatchers
			}
		}
	}
	return nil
}
