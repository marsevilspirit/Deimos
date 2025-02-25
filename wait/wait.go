package wait

import (
	"sync"
)

type List struct {
	l sync.Mutex
	m map[int64]chan any
}

func New() *List {
	return &List{m: make(map[int64]chan any)}
}

func (w *List) Register(id int64) <-chan any {
	w.l.Lock()
	defer w.l.Unlock()
	if _, ok := w.m[id]; !ok {
		w.m[id] = make(chan any, 1)
	}
	return w.m[id]
}

func (w *List) Trigger(id int64, x any) {
	w.l.Lock()
	ch := w.m[id]
	delete(w.m, id)
	w.l.Unlock()
	if ch != nil {
		ch <- x
		close(ch)
	}
}
