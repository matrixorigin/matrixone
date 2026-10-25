// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

package hashbuild

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (hashBuild *HashBuild) initMemoryBudget(proc *process.Process) {
	if !hashBuild.ctr.spillAllowed || !hashBuild.NeedHashMap {
		return
	}
	if value, ok := runtime.ServiceRuntime(proc.GetService()).GetGlobalVariables(runtime.CNMemoryThrottler); ok {
		hashBuild.ctr.memoryThrottler, _ = value.(rscthrottler.RSCThrottler)
	}
}

func (ctr *container) acquireMemory(bytes int64) bool {
	if bytes <= 0 {
		return true
	}
	if bytes == math.MaxInt64 {
		return false
	}
	if ctr.memoryThrottler != nil {
		if _, ok := ctr.memoryThrottler.Acquire(bytes); !ok {
			return false
		}
	}
	if ctr.memoryReserved > math.MaxInt64-bytes {
		if ctr.memoryThrottler != nil {
			ctr.memoryThrottler.Release(bytes)
		}
		return false
	}
	ctr.memoryReserved += bytes
	return true
}

func (ctr *container) releaseMemory(bytes int64) {
	bytes = min(bytes, ctr.memoryReserved)
	if bytes <= 0 {
		return
	}
	ctr.memoryReserved -= bytes
	if ctr.memoryThrottler != nil {
		ctr.memoryThrottler.Release(bytes)
	}
}

func (ctr *container) reconcileBatches() {
	if ctr.memoryReserved > 0 {
		ctr.releaseMemory(ctr.memoryReserved - ctr.hashmapBuilder.Batches.RetainedBytes())
	}
}

func (hashBuild *HashBuild) shouldSpillBeforeCopy(bat *batch.Batch, growth int64) (bool, string) {
	if !hashBuild.ctr.spillAllowed || !hashBuild.NeedHashMap {
		return false, ""
	}
	ctr := &hashBuild.ctr
	if ctr.spillThreshold > 0 {
		if ctr.spillThreshold <= 100000 {
			if int64(ctr.hashmapBuilder.Batches.RowCount()+bat.RowCount()) >= ctr.spillThreshold {
				return true, "row-threshold"
			}
		} else if growth > ctr.spillThreshold-ctr.hashmapBuilder.Batches.RetainedBytes() {
			return true, "byte-threshold"
		}
	}
	if !ctr.acquireMemory(growth) {
		return true, "reservation-denied"
	}
	return false, ""
}

// BuildMemoryBound obtains encoded-key bounds from actual column values. An
// expression without a precomputed capacity contract returns false. Controlled
// imports must spill in that case; ordinary joins may retain their legacy
// expression path. A known bound is reserved before either path allocates.
func (hb *HashmapBuilder) BuildMemoryBound(conditions []*plan.Expr, needSels, needUnique bool) (int64, bool) {
	keyMax, keyTotal := int64(0), int64(0)
	for _, condition := range conditions {
		if condition == nil || condition.GetCol() == nil {
			return hb.expressionBuildMemoryBound(conditions, needSels, needUnique)
		}
	}
	for _, bat := range hb.Batches.Buf {
		for start := 0; start < bat.RowCount(); start += hashmap.UnitLimit {
			end := min(start+hashmap.UnitLimit, bat.RowCount())
			window := int64(0)
			for _, condition := range conditions {
				position := condition.GetCol().ColPos
				if position < 0 || int(position) >= len(bat.Vecs) {
					return 0, false
				}
				vec := bat.Vecs[position]
				if !vec.GetType().IsVarlen() {
					bytes := int64(vec.GetType().TypeSize() + 1)
					rows := int64(end - start)
					if bytes > 0 && rows > (math.MaxInt64-window)/bytes {
						return 0, false
					}
					window += rows * bytes
				} else {
					constant, constantNull := vec.IsConst(), vec.IsConstNull()
					for row := start; row < end; row++ {
						index := row
						if constant {
							index = 0
						}
						bytes := int64(3)
						if !constantNull && !vec.GetNulls().Contains(uint64(index)) {
							bytes += int64(len(vec.GetBytesAt(index)))
						}
						if window > math.MaxInt64-bytes {
							return 0, false
						}
						window += bytes
					}
				}
				if needUnique {
					payload := vec.WindowSize(start, end)
					if keyTotal > math.MaxInt64-payload {
						return 0, false
					}
					keyTotal += payload
				}
			}
			keyMax = max(keyMax, window)
		}
	}
	bound, ok := hashmap.BuildMemoryBound(uint64(hb.InputBatchRowCount), hb.keyWidth > 8, keyMax, needSels)
	if !ok {
		return 0, false
	}
	if needUnique {
		// Appending unique keys retains old storage while full replacement grows.
		// Pre-extension in buildHashmap makes this a single complete allocation.
		overhead := int64(len(conditions)) * 16384
		if overhead > math.MaxInt64-bound || keyTotal > (math.MaxInt64-bound-overhead)/2 {
			return 0, false
		}
		bound += keyTotal*2 + overhead
	}
	return bound, true
}

func spillBatchBytes(proc *process.Process) int64 {
	if proc.Base.ImportMemory != nil {
		return proc.Base.ImportMemory.UnitBytes
	}
	return 64 << 20
}

// EncodedKeyRowBytes returns the actual referenced key payload for one row.
// Projected column keys have no expression allocation or dependency on unrelated
// wide payload columns. It includes null and variable-length prefixes.
func EncodedKeyRowBytes(conditions []*plan.Expr, bat *batch.Batch, row int) (int64, bool) {
	var total int64
	for _, condition := range conditions {
		if condition == nil || condition.GetCol() == nil {
			return 0, false
		}
		position := condition.GetCol().ColPos
		if position < 0 || int(position) >= len(bat.Vecs) {
			return 0, false
		}
		vec := bat.Vecs[position]
		index := row
		if vec.IsConst() {
			index = 0
		}
		bytes := int64(vec.GetType().TypeSize() + 1)
		if vec.GetType().IsVarlen() {
			bytes = 3
			if !vec.IsConstNull() && !vec.GetNulls().Contains(uint64(index)) {
				bytes += int64(len(vec.GetBytesAt(index)))
			}
		}
		if bytes > math.MaxInt64-total {
			return 0, false
		}
		total += bytes
	}
	return total, true
}

// UsesStringKeys mirrors Prepare's physical hash table selection.
func (hb *HashmapBuilder) UsesStringKeys() bool { return hb.keyWidth > 8 }

// Expression contracts include all intermediate executor results and the
// duplicate root vectors retained by evalBatch, before any expression is run.
func (hb *HashmapBuilder) expressionBuildMemoryBound(conditions []*plan.Expr, needSels, needUnique bool) (int64, bool) {
	input := int64(0)
	for _, bat := range hb.Batches.Buf {
		for _, vec := range bat.Vecs {
			var ok bool
			input, ok = keySum(input, vec.WindowSize(0, bat.RowCount()))
			if !ok {
				return 0, false
			}
		}
	}
	// Physical shared area and repeated const references are separate bounds.
	input = max(input, hb.Batches.Allocated())
	rows := min(hb.InputBatchRowCount, 8192)
	keys, ok := SpillKeyMemoryBound(conditions, input, rows)
	if !ok {
		return 0, false
	}
	bound, ok := hashmap.BuildMemoryBound(uint64(hb.InputBatchRowCount), keys.StringKeys, keys.EncodedUnit, needSels)
	if !ok {
		return 0, false
	}
	bound, ok = keySum(bound, keys.ExpressionBytes)
	if !ok {
		return 0, false
	}
	if needUnique {
		unique, ok := SpillKeyMemoryBound(conditions, input, hb.InputBatchRowCount)
		if !ok {
			return 0, false
		}
		bound, ok = keySum(bound, unique.ExpressionBytes)
		if !ok {
			return 0, false
		}
	}
	return bound, true
}
