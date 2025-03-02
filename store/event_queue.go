package store

type eventQueue struct {
	Events   []*Event
	Size     int
	Front    int
	Capacity int
}

func (eq *eventQueue) back() int {
	return (eq.Front + eq.Size - 1 + eq.Capacity) % eq.Capacity
}

// return true if the queue is full
func (eq *eventQueue) insert(e *Event) {
	index := (eq.back() + 1) % eq.Capacity

	eq.Events[index] = e

	// check if full
	if eq.Size == eq.Capacity {
		eq.Front = (index + 1) % eq.Capacity
	} else {
		eq.Size++
	}
}
