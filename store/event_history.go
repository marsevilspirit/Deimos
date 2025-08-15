package store

import (
	"fmt"
	"path"
	"strings"
	"sync"

	Err "github.com/marsevilspirit/deimos/error"
)

type EventHistory struct {
	Queue      eventQueue
	StartIndex uint64
	LastIndex  uint64
	rwl        sync.RWMutex
}

func newEventHistory(capacity int) *EventHistory {
	return &EventHistory{
		Queue: eventQueue{
			Capacity: capacity,
			Events:   make([]*Event, capacity),
		},
	}
}

// addEvent function adds event into the eventHistory
func (eh *EventHistory) addEvent(e *Event) *Event {
	eh.rwl.Lock()
	defer eh.rwl.Unlock()

	eh.Queue.insert(e)

	eh.LastIndex = e.Index()

	eh.StartIndex = eh.Queue.Events[eh.Queue.Front].Index()

	return e
}

// scan enumerates events from the index history and stops at the first point
// where the key matches.
func (eh *EventHistory) scan(key string, recursive bool, index uint64) (*Event, *Err.Error) {
	eh.rwl.RLock()
	defer eh.rwl.RUnlock()

	// index should be after the event history's StartIndex
	if index < eh.StartIndex {
		return nil,
			Err.NewError(Err.EcodeEventIndexCleared,
				fmt.Sprintf("the requested history has been cleared [%v/%v]", eh.StartIndex, index), 0)
	}

	// the index should come before the size of the queue minus the duplicate count
	if index > eh.LastIndex { // future index
		return nil, nil
	}

	offset := index - eh.StartIndex
	i := (eh.Queue.Front + int(offset)) % eh.Queue.Capacity

	for {
		e := eh.Queue.Events[i]

		ok := (e.Node.Key == key)

		if recursive {
			// add tailing slash
			cleanKey := path.Clean(key)
			if cleanKey[len(cleanKey)-1] != '/' {
				cleanKey = cleanKey + "/"
			}

			ok = ok || strings.HasPrefix(e.Node.Key, cleanKey)
		}

		if ok {
			return e, nil
		}

		i = (i + 1) % eh.Queue.Capacity

		if i == eh.Queue.Back {
			return nil, nil
		}
	}
}
