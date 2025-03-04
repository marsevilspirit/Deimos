package store

type eventQueue struct {
	Events   []*Event
	Size     int
	Front    int
	Back     int
	Capacity int
}

// return true if the queue is full
func (eq *eventQueue) insert(e *Event) {
	eq.Events[eq.Back] = e
	eq.Back = (eq.Back + 1) % eq.Capacity

	// check if full
	if eq.Size == eq.Capacity {
		eq.Front = (eq.Front + 1) % eq.Capacity
	} else {
		eq.Size++
	}
}
