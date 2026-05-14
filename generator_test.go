package snowflake

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestGeneratorUnique(t *testing.T) {
	gen := NewGenerator(1)
	ids := make(map[int64]struct{}, 10000)

	for range 10000 {
		id := gen.Next()
		if _, exists := ids[id]; exists {
			t.Fatalf("duplicate id: %d", id)
		}

		ids[id] = struct{}{}
	}
}

func TestGeneratorUniqueParallel(t *testing.T) {
	gen := NewGenerator(1)

	const workers = 12
	const idsPerWorker = 10000

	ids := make(map[int64]struct{}, workers*idsPerWorker)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			localIDs := make([]int64, idsPerWorker)
			for i := range idsPerWorker {
				localIDs[i] = gen.Next()
			}

			mu.Lock()
			for _, id := range localIDs {
				if _, exists := ids[id]; exists {
					t.Errorf("duplicate id: %d", id)
				}

				ids[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
}

func TestGeneratorMonotonicSingleShard(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prevProcs)

	gen := NewGenerator(1)
	var prevID int64

	for range 100000 {
		id := gen.Next()
		if id <= prevID {
			t.Fatalf("non-monotonic: %d <= %d", id, prevID)
		}

		prevID = id
	}
}

func TestGeneratorBatch(t *testing.T) {
	gen := NewGenerator(1)
	batch := make([]int64, 1000)

	gen.NextBatch(batch)

	ids := make(map[int64]struct{}, len(batch))
	for _, id := range batch {
		if _, exists := ids[id]; exists {
			t.Fatalf("duplicate id in batch: %d", id)
		}

		ids[id] = struct{}{}
	}

	for i := 1; i < len(batch); i++ {
		if batch[i] <= batch[i-1] {
			t.Fatalf("batch not monotonic at %d: %d <= %d", i, batch[i], batch[i-1])
		}
	}
}

func TestGeneratorBatchCrossesSequenceBoundary(t *testing.T) {
	gen := NewGenerator(1)
	batch := make([]int64, maxSeq+2)

	gen.NextBatch(batch)

	for i := 1; i < len(batch); i++ {
		if batch[i] <= batch[i-1] {
			t.Fatalf("batch not monotonic at %d: %d <= %d", i, batch[i], batch[i-1])
		}
	}
}

func TestGeneratorBatchStartsFromCurrentTime(t *testing.T) {
	gen := NewGenerator(1)
	batch := make([]int64, 4)

	gen.NextBatch(batch)

	if got := batch[0] & maxSeq; got != 0 {
		t.Fatalf("unexpected first sequence: got %d want 0", got)
	}

	if got := batch[0] >> timeShift; got == 0 {
		t.Fatalf("unexpected zero timestamp in first batch id: %d", batch[0])
	}
}

func TestGeneratorNodeIDsStayWithinField(t *testing.T) {
	prevProcs := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(prevProcs)

	gen := NewGenerator(maxNode)
	for i := range gen.shards {
		if gen.shards[i].nodeID > maxNode {
			t.Fatalf("shard %d node id overflowed field: %d", i, gen.shards[i].nodeID)
		}
	}
}

func TestGeneratorRace(t *testing.T) {
	gen := NewGenerator(1)
	var wg sync.WaitGroup

	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10000 {
				_ = gen.Next()
			}
		}()
	}

	wg.Wait()
}

func BenchmarkGeneratorSequential(b *testing.B) {
	gen := NewGenerator(1)
	for b.Loop() {
		_ = gen.Next()
	}
}

func BenchmarkGeneratorParallel(b *testing.B) {
	gen := NewGenerator(1)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = gen.Next()
		}
	})
}

func BenchmarkGeneratorBatch(b *testing.B) {
	gen := NewGenerator(1)
	batch := make([]int64, 1024)

	for b.Loop() {
		gen.NextBatch(batch)
	}
}

func BenchmarkClockZigCGo(b *testing.B) {
	for b.Loop() {
		_ = clockNowMilliseconds()
	}
}

func BenchmarkClockGoTimeNow(b *testing.B) {
	for b.Loop() {
		_ = time.Now().UnixMilli()
	}
}
