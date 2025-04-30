package fifocache

// ghost represents the `ghost` structure in the S3FIFO algorithm.
// At the time of writing, this implementation is not carefully designed.
// There can be a better way to implement `ghost`.
type ghost[K comparable] struct {
	size  int
	queue Queue[K]
	items map[K]struct{}
}

func newGhost[K comparable](size int) *ghost[K] {
	return &ghost[K]{
		size:  size,
		queue: *NewQueue[K](),
		items: make(map[K]struct{}),
	}
}

func (b *ghost[K]) add(key K) {
	if _, ok := b.items[key]; ok {
		return
	}

	for len(b.items) >= b.size {
		key, ok := b.queue.dequeue()
		if !ok {
			// empty
			break
		}
		delete(b.items, key)
	}

	b.queue.enqueue(key)
	b.items[key] = struct{}{}
}

func (b *ghost[K]) remove(key K) {
	delete(b.items, key)
	// keys in queue will be removed in add
}

func (b *ghost[K]) contains(key K) bool {
	_, ok := b.items[key]
	return ok
}

func (b *ghost[K]) clear() {
	b.queue = Queue[K]{}
	clear(b.items)
}
