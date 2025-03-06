package store

type Watcher struct {
	EventChan  chan *Event
	stream     bool
	recursive  bool
	sinceIndex uint64
	hub        *watcherHub
	removed    bool
	remove     func()
}

// notify function notifies the watcher. If the watcher interests in the given path,
// the function will return true.
func (w *Watcher) notify(e *Event, originalPath bool, deleted bool) bool {
	// watcher is interested the path in three cases and under one condition
	// the condition is that the event happens after the watcher's sinceIndex

	// 1. the path at which the event happens is the path the watcher is watching at.
	// For example if the watcher is watching at "/foo" and the event happens at "/foo",
	// the watcher must be interested in that event.

	// 2. the watcher is a recursive watcher, it interests in the event happens after
	// its watching path. For example if watcher A watches at "/foo" and it is a recursive
	// one, it will interest in the event happens at "/foo/bar".

	// 3. when we delete a directory, we need to force notify all the watchers who watches
	// at the file we need to delete.
	// For example a watcher is watching at "/foo/bar". And we deletes "/foo". The watcher
	// should get notified even if "/foo" is not the path it is watching.

	if (w.recursive || originalPath || deleted) && e.Index() >= w.sinceIndex {
		// We cannot block here if the EventChan capacity is full, otherwise
		// marstore will hang. EventChan capacity is full when the rate of
		// notifications are higher than our send rate.
		// If this happens, we close the channel.
		select {
		case w.EventChan <- e:
		default:
			// We have missed a notification. Remove the watcher.
			// Removing the watcher also closes the EventChan.
			w.remove()
		}
		return true
	}
	return false
}

// Remove removes the watcher from watcherHub
// The actual remove function is guaranteed to only be executed once
func (w *Watcher) Remove() {
	w.hub.mutex.Lock()
	defer w.hub.mutex.Unlock()

	close(w.EventChan)
	if w.remove != nil {
		w.remove()
	}
}
