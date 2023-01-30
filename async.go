package batcher

import (
	"sync"
	"time"
)

// Async is similar to the Batcher type, but provides no mechanism for
// determining when another caller has completed processing a batch. In
// exchange, it allocates less memory and can be faster.
type Async[T any] struct {
	interval time.Duration
	maxItems int

	// All further fields are protected by mu
	mu    sync.Mutex
	items []T
}

// NewAsyn creates a new Async which will wait up to interval for additional
// calls to Batch, and will batch up to maxItems in a single batch.
func NewAsync[T any](interval time.Duration, maxItems int) *Async[T] {
	ret := &Async[T]{
		interval: interval,
		maxItems: maxItems,
	}
	return ret
}

// Batch will collect all items passed to concurrent callers (up to the maximum
// items limit), and return the batched set of items and a boolean indicating
// whether this caller is responsible for processing the returned batch.
//
// For example, if a call to Batch is already in progress, other callers will
// observe nil, false to indicate that their items have been collected by
// another caller, and the first caller will observe []T{...}, true.
func (b *Async[T]) Batch(items []T) ([]T, bool) {
	b.mu.Lock()
	if b.items != nil {
		// Something else has started waiting; check to see if adding
		// this set of items would be over our maximum item size.
		if len(b.items)+len(items) <= b.maxItems {
			// Space remaining; append our items to the stored slice and return.
			b.items = append(b.items, items...)
			b.mu.Unlock()
			return nil, false
		}

		// Adding our items to the existing list would pass the maximum
		// size; return the set of existing stored items (since they
		// were enqueued previously and we attempt to be roughly FIFO)
		// and swap with our current items. The previous waiter will
		// get these items when it re-awakens.
		items, b.items = b.items, items
		b.mu.Unlock()
		return items, true
	}

	// Nothing else is waiting; store our items as a sentinel that
	// something is waiting and then wait.
	b.items = items
	b.mu.Unlock()

	time.Sleep(b.interval)

	// Get all accumulated items from while we were waiting.
	b.mu.Lock()
	items = b.items
	b.items = nil
	b.mu.Unlock()

	return items, true
}
