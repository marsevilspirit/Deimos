package fileSystem

import (
	"strings"
	"sync"
	"time"
)

const (
	Get        = "get"
	Set        = "set"
	Delete     = "delete"
	TestAndSet = "testAndSet"
)

type Event struct {
	Action     string         `json:"action"`
	Key        string         `json:"key"`
	Dir        bool           `json:"dir,omitempty"`
	PrevValue  string         `json:"prevValue,omitempty"`
	Value      string         `json:"value,omitempty"`
	KVPairs    []KeyValuePair `json:"kvs,omitempty"`
	Expiration *time.Time     `json:"expiration,omitempty"`
	TTL        int64          `json:"ttl,omitempty"` // Time to live in second
	Index      uint64         `json:"index"`
	Term       uint64         `json:"term"`
}

// When user list a directory, we add all the node into key-value pair slice
type KeyValuePair struct {
	Key     string         `json:"key,omitempty"`
	Value   string         `json:"value,omitempty"`
	Dir     bool           `json:"dir,omitempty"`
	KVPairs []KeyValuePair `json:"kvs,omitempty"`
}

func newEvent(action string, key string, index uint64, term uint64) *Event {
	return &Event{
		Action: action,
		Key:    key,
		Index:  index,
		Term:   term,
	}
}

type eventQueue struct {
	events   []*Event
	size     int
	front    int
	back     int
	capacity int
}

// return true if the queue is full
func (eq *eventQueue) insert(e *Event) {
	eq.back = (eq.back + 1) % eq.capacity
	eq.events[eq.back] = e

	// check if full
	if eq.size == eq.capacity {
		eq.front = (eq.front + 1) % eq.capacity
	} else {
		eq.size++
	}
}

type EventHistory struct {
	Queue      eventQueue
	StartIndex uint64
	rwl        sync.RWMutex
}

func newEventHistory(capacity int) *EventHistory {
	return &EventHistory{
		Queue: eventQueue{
			capacity: capacity,
			events:   make([]*Event, capacity),
			back:     -1,
		},
	}
}

// addEvent function adds event into the eventHistory
func (eh *EventHistory) addEvent(e *Event) {
	eh.rwl.Lock()
	defer eh.rwl.Unlock()

	eh.Queue.insert(e)
	eh.StartIndex = eh.Queue.events[eh.Queue.front].Index
}

func (eh *EventHistory) scan(prefix string, index uint64) (*Event, error) {
	eh.rwl.RLock()
	defer eh.rwl.RUnlock()

	start := index - eh.StartIndex

	if start < 0 {

		// TODO: Add error type
		return nil, nil
	}

	if start >= uint64(eh.Queue.size) {

		return nil, nil
	}

	i := int((start + uint64(eh.Queue.front)) % uint64(eh.Queue.capacity))

	for {
		e := eh.Queue.events[i]
		if strings.HasPrefix(e.Key, prefix) {
			return e, nil
		}

		i = (i + 1) % eh.Queue.capacity

		if i == eh.Queue.back {
			// TODO: Add error type
			return nil, nil
		}
	}
}
