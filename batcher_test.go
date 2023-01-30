package batcher

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

type Range struct {
	// [Lower, Upper)
	Lower, Upper int
}

func doBatcherTest(t *testing.T, cb func(ranges []Range, maxItems int, flush func([]int))) {
	seed := time.Now().UnixMilli()
	t.Logf("random seed: %d", seed)
	rand.Seed(seed)

	const upperBound = 10_000
	const maxItems = 100

	var (
		batchMu sync.Mutex
		allNums = make(map[int]bool, upperBound)
	)
	flush := func(batch []int) {
		if len(batch) > maxItems {
			t.Errorf("got batch of size %d which is > max size %d", len(batch), maxItems)
		}

		batchMu.Lock()
		defer batchMu.Unlock()
		for _, num := range batch {
			if allNums[num] {
				t.Errorf("duplicate number: %d", num)
				continue
			}
			allNums[num] = true
		}
	}

	half := 500 + rand.Intn(9000)
	lowerQuarter := 100 + rand.Intn(half-100)
	upperQuarter := (half + 100) + rand.Intn(10_000-(half+100))

	ranges := []Range{
		{0, lowerQuarter},
		{lowerQuarter, half},
		{half, upperQuarter},
		{upperQuarter, upperBound},
	}
	t.Logf("ranges: [0, %d), [%d, %d), [%d, %d), [%d, %d)",
		lowerQuarter,
		lowerQuarter, half,
		half, upperQuarter,
		upperQuarter, upperBound,
	)

	// Batch and insert.
	cb(ranges, maxItems, flush)

	// Verify that we got all numbers.
	batchMu.Lock()
	defer batchMu.Unlock()

	for i := 0; i < upperBound; i++ {
		if !allNums[i] {
			t.Errorf("missing number: %d", i)
		}
	}

	if len(allNums) != upperBound {
		t.Logf("excess items in map; got %d, want %d", len(allNums), upperBound)
	}
}

func TestBatcher(t *testing.T) {
	doBatcherTest(t, func(ranges []Range, maxItems int, flush func([]int)) {
		batcher := New[int](1*time.Millisecond, maxItems)

		// Insert numbers using a variety of sizes.
		var wg sync.WaitGroup
		insertNums := func(start, end int) {
			defer wg.Done()

			var batch []int
			for i := start; i < end; {
				// Make a slice of items to batch with.
				remaining := end - i
				if remaining > 10 {
					remaining = 10
				}
				batch = batch[:0]
				for j := i; j < i+remaining; j++ {
					batch = append(batch, j)
				}
				i += remaining

				// Flush our batch
				var batchErr *BatchError
				batch, batchErr = batcher.Batch(batch)
				if len(batch) > 0 {
					flush(batch)
					batchErr.Set(nil)
				} else {
					batchErr.Wait()
				}
			}
		}

		wg.Add(len(ranges))
		for _, rr := range ranges {
			go insertNums(rr.Lower, rr.Upper)
		}
		wg.Wait()
	})
}

func TestBatcherZeroSize(t *testing.T) {
	const maxItems = 100
	batcher := New[int](1*time.Millisecond, maxItems)
	batch, batchErr := batcher.Batch(nil)
	if len(batch) != 0 {
		t.Errorf("expected no items in batch, got: %d", len(batch))
	}
	if err := batchErr.Wait(); err != nil {
		t.Error(err)
	}
}

func TestBatcherOversize(t *testing.T) {
	const maxItems = 100
	batcher := New[int](1*time.Millisecond, maxItems)

	batch := make([]int, 0, maxItems*2)
	for i := 0; i < maxItems*2; i++ {
		batch = append(batch, i)
	}

	batch, batchErr := batcher.Batch(batch)
	if len(batch) != maxItems*2 {
		t.Errorf("got len=%d; want %d", len(batch), maxItems*2)
	}
	batchErr.Set(nil)
}

func TestAsync(t *testing.T) {
	doBatcherTest(t, func(ranges []Range, maxItems int, flush func([]int)) {
		batcher := NewAsync[int](1*time.Millisecond, maxItems)

		// Insert numbers using a variety of sizes.
		var wg sync.WaitGroup
		insertNums := func(start, end int) {
			defer wg.Done()

			var batch []int
			for i := start; i < end; {
				// Make a slice of items to batch with.
				remaining := end - i
				if remaining > 10 {
					remaining = 10
				}
				batch = batch[:0]
				for j := i; j < i+remaining; j++ {
					batch = append(batch, j)
				}
				i += remaining

				// Flush our batch
				var got bool
				batch, got = batcher.Batch(batch)
				if got {
					flush(batch)
				}
			}
		}

		wg.Add(len(ranges))
		for _, rr := range ranges {
			go insertNums(rr.Lower, rr.Upper)
		}
		wg.Wait()
	})
}
