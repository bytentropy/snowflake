# Snowflake

- Sharded Snowflake ID generator for Go.
- A small C helper provides the wall-clock function used by the Go generator.
- The public import path is `github.com/bytentropy/snowflake`.

## API

- `type Generator` - Holds one shard per `GOMAXPROCS` processor slot.
- `func NewGenerator(nodeID int64) *Generator` - Creates a generator for one logical node.
- `func (generator *Generator) Next() int64` - Returns one Snowflake ID.
- `func (generator *Generator) NextBatch(destination []int64)` - Fills a slice with sequential IDs.

## ID Layout

- `41 bits` - Millisecond timestamp offset from the custom epoch.
- `10 bits` - Node identifier.
- `12 bits` - Per-millisecond sequence.

## Behavior

- The generator pins a goroutine to one processor slot while it updates shard-local state.
- `NewGenerator` masks `nodeID` to the bits left after shard indexing and panics when `GOMAXPROCS` does not fit into the 10-bit node field.
- Each shard stores timestamp and sequence in one atomic value.
- `NextBatch` writes every ID into the provided slice and stores the final shard state once.
- The C helper exposes `fast_now_ms()` through cgo.

## Performance

- Current benchmark snapshot (`darwin/arm64`, Apple M1, Go `1.26.2`):

| Benchmark | Result | Notes |
| --- | ---: | --- |
| `BenchmarkGeneratorSequential-8` | `8.238 ns/op` | Single-call steady-state path |
| `BenchmarkGeneratorParallel-8` | `2.261 ns/op` | Parallel throughput with shard-local state |
| `BenchmarkGeneratorBatch-8` | `537.5 ns/op` | `1024` IDs per batch, about `0.52 ns/id` |
| `BenchmarkClockCGo-8` | `38.83 ns/op` | C clock helper via cgo |
| `BenchmarkClockGoTimeNow-8` | `35.33 ns/op` | Baseline `time.Now().UnixMilli()` |

- All current benchmarks report `0 B/op` and `0 allocs/op`.
- Re-run locally with `go test -run '^$' -bench . -benchmem`.

## Example

```go
package main

import (
	"fmt"

	"github.com/bytentropy/snowflake"
)

func main() {
	generator := snowflake.NewGenerator(1)

	fmt.Println(generator.Next())

	batch := make([]int64, 4)
	generator.NextBatch(batch)
	fmt.Println(batch)
}
```

## Build

- Build the project with `go build ./...`.
- Run tests with `GOCACHE=/tmp/gocache go test ./...` when the default Go cache is not writable.
- Rebuild after changes in `clock.c`.
