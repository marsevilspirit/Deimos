package store

import (
	"container/heap"
	"time"
)

// bpTtlItem represents a TTL entry for a single key in B+ tree
type bpTtlItem struct {
	key        string
	expireTime time.Time
}

// bpTtlKeyHeap is a min-heap ordered by expireTime for B+ tree keys
type bpTtlKeyHeap struct {
	array  []*bpTtlItem
	keyMap map[string]int
}

func newBpTtlKeyHeap() *bpTtlKeyHeap {
	h := &bpTtlKeyHeap{keyMap: make(map[string]int)}
	heap.Init(h)
	return h
}

func (h *bpTtlKeyHeap) Len() int { return len(h.array) }

func (h *bpTtlKeyHeap) Less(i, j int) bool {
	return h.array[i].expireTime.Before(h.array[j].expireTime)
}

func (h *bpTtlKeyHeap) Swap(i, j int) {
	h.array[i], h.array[j] = h.array[j], h.array[i]
	h.keyMap[h.array[i].key] = i
	h.keyMap[h.array[j].key] = j
}

func (h *bpTtlKeyHeap) Push(x any) {
	it, _ := x.(*bpTtlItem)
	h.keyMap[it.key] = len(h.array)
	h.array = append(h.array, it)
}

func (h *bpTtlKeyHeap) Pop() any {
	x := h.array[len(h.array)-1]
	h.array = h.array[:len(h.array)-1]
	delete(h.keyMap, x.key)
	return x
}

func (h *bpTtlKeyHeap) top() *bpTtlItem {
	if h.Len() != 0 {
		return h.array[0]
	}
	return nil
}

func (h *bpTtlKeyHeap) pop() *bpTtlItem {
	x := heap.Pop(h)
	it, _ := x.(*bpTtlItem)
	return it
}

func (h *bpTtlKeyHeap) updateKey(key string, expireTime time.Time) {
	if idx, ok := h.keyMap[key]; ok {
		// replace and fix by removing then pushing again for simplicity
		heap.Remove(h, idx)
		heap.Push(h, &bpTtlItem{key: key, expireTime: expireTime})
		return
	}
	heap.Push(h, &bpTtlItem{key: key, expireTime: expireTime})
}

func (h *bpTtlKeyHeap) removeKey(key string) {
	if idx, ok := h.keyMap[key]; ok {
		heap.Remove(h, idx)
	}
}
