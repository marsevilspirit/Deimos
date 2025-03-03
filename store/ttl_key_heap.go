package store

import "container/heap"

type ttlKeyHeap struct {
	array  []*Node
	keyMap map[*Node]int
}

func newTtlKeyHeap() *ttlKeyHeap {
	h := &ttlKeyHeap{keyMap: make(map[*Node]int)}
	heap.Init(h)
	return h
}

func (h *ttlKeyHeap) Len() int {
	return len(h.array)
}

func (h *ttlKeyHeap) Less(i, j int) bool {
	return h.array[i].ExpireTime.Before(h.array[j].ExpireTime)
}

func (h *ttlKeyHeap) Swap(i, j int) {
	// swap node
	h.array[i], h.array[j] = h.array[j], h.array[i]

	// update map
	h.keyMap[h.array[i]] = i
	h.keyMap[h.array[j]] = j
}

func (h *ttlKeyHeap) Push(x any) {
	n, _ := x.(*Node)
	h.keyMap[n] = len(h.array)
	h.array = append(h.array, n)
}

func (h *ttlKeyHeap) Pop() any {
	x := h.array[len(h.array)-1]
	h.array = h.array[:len(h.array)-1]
	delete(h.keyMap, x)
	return x
}

func (h *ttlKeyHeap) top() *Node {
	if h.Len() != 0 {
		return h.array[0]
	}
	return nil
}

func (h *ttlKeyHeap) pop() *Node {
	x := heap.Pop(h)
	n, _ := x.(*Node)
	return n
}

func (h *ttlKeyHeap) push(x any) {
	heap.Push(h, x)
}

func (h *ttlKeyHeap) update(n *Node) {
	index, ok := h.keyMap[n]
	if ok {
		heap.Remove(h, index)
		heap.Push(h, n)
	}
}

func (h *ttlKeyHeap) remove(n *Node) {
	index, ok := h.keyMap[n]
	if ok {
		heap.Remove(h, index)
	}
}
