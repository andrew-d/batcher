package batcher

import (
	"sync"
	"time"
)

// Batcher allows collecting and batching multiple calls that occur in a given
// timespan into a single batch of returned items.
type Batcher[T any] struct {
	interval time.Duration
	maxItems int

	// All further fields are protected by mu
	mu       sync.Mutex
	batchErr *BatchError
	items    []T
}

// New creates a new Batcher which will wait up to interval for additional
// calls to Batch, and will batch up to maxItems in a single batch.
func New[T any](interval time.Duration, maxItems int) *Batcher[T] {
	ret := &Batcher[T]{
		interval: interval,
		maxItems: maxItems,
	}
	return ret
}

// Batch will collect all items passed to concurrent callers (up to the maximum
// items limit), and return the batched set of items. If a call to Batch is
// already in progress, other callers will return an empty batch to indicate
// that their items have been collected by another caller.
//
// The BatchError return value has two meanings: if the returned batch size is
// greater than zero, then the caller is expected to process those items and
// call Set when it is finished doing so, to signal to other callers that items
// have been successfully processed or have errored.
//
// If the batch size is zero, the returned BatchError can be waited on to
// determine when the batched messages were processed and if there was an
// error. This can be helpful to ensure that callers aren't incorrectly
// reporting that no error occurred to a client when one occurs.
func (b *Batcher[T]) Batch(items []T) ([]T, *BatchError) {
	if len(items) == 0 {
		// No error; return a BatchError that has already completed.
		return nil, completedBatchError
	}

	b.mu.Lock()
	if b.items != nil {
		// Something else has started waiting; check to see if adding
		// this set of items would be over our maximum item size.
		if len(b.items)+len(items) <= b.maxItems {
			// Space remaining; append our items to the stored slice and return.
			b.items = append(b.items, items...)
			be := b.batchErr
			b.mu.Unlock()
			return nil, be
		}

		// Adding our items to the existing list would pass the maximum
		// size; return the set of existing stored items (since they
		// were enqueued previously and we attempt to be roughly FIFO)
		// and swap with our current items. The previous waiter will
		// get these items when it re-awakens.
		items, b.items = b.items, items
		b.mu.Unlock()
		return items, newBatchError()
	}

	// Nothing else is waiting; store our items as a sentinel that
	// something is waiting and then wait.
	b.items = items
	b.batchErr = newBatchError()
	b.mu.Unlock()

	time.Sleep(b.interval)

	// Get all accumulated items from while we were waiting.
	b.mu.Lock()
	defer b.mu.Unlock()
	items = b.items
	b.items = nil

	return items, b.batchErr
}

// BatchError is a type returned from Batch so that callers can either signal
// or track the success/failure of a batch.
type BatchError struct {
	done chan struct{}
	err  error
}

func newBatchError() *BatchError {
	return &BatchError{done: make(chan struct{})}
}

// Wait will wait for the Set call to be called, and then return the error that
// was passed to Set.
func (b *BatchError) Wait() error {
	<-b.done
	return b.err
}

// Set will unblock all waiters and cause them to return the provided error.
func (b *BatchError) Set(err error) {
	b.err = err
	close(b.done)
}

var completedBatchError *BatchError = func() *BatchError {
	be := newBatchError()
	be.Set(nil)
	return be
}()
