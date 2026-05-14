// Package snowflake provides a sharded Snowflake ID generator.
package snowflake

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"time"
	_ "unsafe"
)

// pinCurrentProcessor keeps a goroutine on one processor slot while it uses shard-local state.
//
//go:linkname pinCurrentProcessor runtime.procPin
func pinCurrentProcessor() int

// unpinCurrentProcessor releases the pin created by pinCurrentProcessor.
//
//go:linkname unpinCurrentProcessor runtime.procUnpin
func unpinCurrentProcessor()

const (
	epochMs = int64(1767225600000)

	nodeBits = 10
	seqBits  = 12

	maxNode = (1 << nodeBits) - 1
	maxSeq  = (1 << seqBits) - 1

	nodeShift = seqBits
	timeShift = nodeBits + seqBits
)

type shard struct {
	state  atomic.Int64
	nodeID int64
	pad    [48]byte
}

// Generator creates Snowflake IDs with one shard per logical processor.
type Generator struct {
	shards []shard
}

// NewGenerator creates a generator for the given node ID.
func NewGenerator(nodeID int64) *Generator {
	shardCount := runtime.GOMAXPROCS(0)
	shardBits := bits.Len64(uint64(shardCount - 1))
	if shardBits > nodeBits {
		panic("snowflake: GOMAXPROCS exceeds node ID space")
	}

	nodeMask := int64((1 << (nodeBits - shardBits)) - 1)
	baseNode := (nodeID & nodeMask) << shardBits

	gen := &Generator{
		shards: make([]shard, shardCount),
	}

	for idx := range gen.shards {
		gen.shards[idx].nodeID = baseNode | int64(idx)
	}

	return gen
}

// Next returns one Snowflake ID.
func (gen *Generator) Next() int64 {
	shard, nodeField := gen.pinShard()

	for {
		state := shard.state.Load()
		ts, seq := unpackState(state)

		nextTs := ts
		nextSeq := seq + 1

		if state == 0 || seq == maxSeq {
			unpinCurrentProcessor()
			clockTs := loadClockTs()

			shard, nodeField = gen.pinShard()
			state = shard.state.Load()
			ts, _ = unpackState(state)

			nextTs = nextTsAfterClock(clockTs, state, ts)
			nextSeq = 0
		}

		nextState := packState(nextTs, nextSeq)
		if shard.state.CompareAndSwap(state, nextState) {
			unpinCurrentProcessor()
			return composeID(nextTs, nodeField, nextSeq)
		}
	}
}

// NextBatch fills destination with sequential Snowflake IDs.
func (gen *Generator) NextBatch(dst []int64) {
	if len(dst) == 0 {
		return
	}

	shard, nodeField := gen.pinShard()
	state := shard.state.Load()
	ts, seq := unpackState(state)

	if state == 0 {
		// Fresh shards should emit sequence 0 in the current millisecond.
		ts = loadPinnedTs()
		seq = -1
	}

	for idx := range dst {
		seq++
		if seq > maxSeq {
			clockTs := loadPinnedTs()
			if clockTs > ts {
				ts = clockTs
			} else {
				ts++
			}

			seq = 0
		}

		dst[idx] = composeID(ts, nodeField, seq)
	}

	shard.state.Store(packState(ts, seq))
	unpinCurrentProcessor()
}

func (gen *Generator) pinShard() (*shard, int64) {
	idx := pinCurrentProcessor()
	shard := &gen.shards[idx]
	return shard, shard.nodeID << nodeShift
}

func loadClockTs() int64 {
	return clockNowMilliseconds() - epochMs
}

func loadPinnedTs() int64 {
	return time.Now().UnixMilli() - epochMs
}

func nextTsAfterClock(clockTs, state, ts int64) int64 {
	if state != 0 && clockTs <= ts {
		return ts + 1
	}

	return clockTs
}

func packState(ts, seq int64) int64 {
	return (ts << seqBits) | seq
}

func unpackState(state int64) (ts int64, seq int64) {
	return state >> seqBits, state & maxSeq
}

func composeID(ts, nodeField, seq int64) int64 {
	return (ts << timeShift) | nodeField | seq
}
