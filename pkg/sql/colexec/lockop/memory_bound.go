// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"math"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// MemoryTarget is the immutable allocation-relevant part of a lock target. It
// is evaluated locally before Prepare.
type MemoryTarget struct {
	PrimaryColumn  int32
	RefreshColumn  int32
	Type           types.Type
	LockTable      bool
	LockTableAtEnd bool
}

func boundedLockType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_bool, types.T_bit, types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64, types.T_float32, types.T_float64,
		types.T_date, types.T_year, types.T_time, types.T_datetime, types.T_timestamp,
		types.T_decimal64, types.T_decimal128, types.T_uuid, types.T_enum,
		types.T_char, types.T_varchar, types.T_binary, types.T_varbinary:
		return typ.TypeSize() > 0
	default:
		return false
	}
}

// MemoryTargets conservatively includes refresh storage when no execution
// client is available. MemoryTargetsForRefresh accepts the actual immutable
// client feature used by performLock.
func (lockOp *LockOp) MemoryTargets() ([]MemoryTarget, bool) {
	return lockOp.MemoryTargetsForRefresh(true)
}

func (lockOp *LockOp) MemoryTargetsForRefresh(refreshEnabled bool) ([]MemoryTarget, bool) {
	targets := make([]MemoryTarget, 0, len(lockOp.targets))
	for _, target := range lockOp.targets {
		// Arbitrary lock-row expressions or filters need their own allocation
		// contract. Prepare installs only GetFetchRowsFunc for these types.
		if target.lockRows != nil || target.filter != nil || target.primaryColumnIndexInBatch < 0 ||
			target.refreshTimestampIndexInBatch < -1 || !boundedLockType(target.primaryColumnType) {
			return nil, false
		}
		refreshColumn := target.refreshTimestampIndexInBatch
		if !refreshEnabled {
			// Disabled clients never read or append the timestamp vector.
			// Some insert plans leave this unused protobuf field at zero.
			refreshColumn = -1
		}
		targets = append(targets, MemoryTarget{PrimaryColumn: target.primaryColumnIndexInBatch,
			RefreshColumn: refreshColumn, Type: target.primaryColumnType, LockTable: target.lockTable,
			LockTableAtEnd: target.lockTableAtTheEnd})
	}
	return targets, true
}

func lockMemoryAdd(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func lockMemoryMul(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a != 0 && b > math.MaxInt64/a {
		return 0, false
	}
	return a * b, true
}

// lockVectorPeak covers the live vector plus complete replacement data/area
// during repeated append. Grow cannot exceed rounded twice-required capacity;
// null/grouping words have two simultaneous generations as well.
func lockVectorPeak(typ types.Type, payload int64, rows int) (int64, bool) {
	data, ok := lockMemoryMul(int64(typ.TypeSize()), int64(rows))
	if !ok || data > int64(mpool.CapLimit) || typ.IsVarlen() && payload > int64(mpool.CapLimit) {
		return 0, false
	}
	area := int64(0)
	if typ.IsVarlen() {
		area = payload
	}
	data = mpool.GrowCapacity(0, min(2*data, int64(mpool.CapLimit)))
	area = mpool.GrowCapacity(0, min(2*area, int64(mpool.CapLimit)))
	size, ok := lockMemoryAdd(data, area)
	if !ok {
		return 0, false
	}
	size, ok = lockMemoryMul(size, 2)
	if !ok {
		return 0, false
	}
	bitmaps := int64((uint64(rows)+64)/64) * 8 * 4
	return lockMemoryAdd(size, bitmaps+int64(unsafe.Sizeof(vector.Vector{})))
}

// LockTable creates a temporary analyzer in the empty-input terminal path.
// Its struct holds one time.Time, scalar timing/index fields and two pointers;
// eight machine words cover those fields plus alignment. The unit test checks
// this layout allowance against the actual private analyzer type. The metric
// map holds one OpWaitLockTime entry: header plus a padded eight-slot bucket.
const lockTempAnalyzerStructBytes = int64(unsafe.Sizeof(time.Time{}) + 8*unsafe.Sizeof(uintptr(0)))
const lockTempMetricMapBytes = int64(64 + 8*(unsafe.Sizeof(process.MetricType(0))+unsafe.Sizeof(int64(0))) + 2*unsafe.Sizeof(uintptr(0)))
const lockTempAnalyzerBytes = 2 * (lockTempAnalyzerStructBytes + lockTempMetricMapBytes +
									int64(unsafe.Sizeof(perfcounter.CounterSet{})+unsafe.Sizeof(process.OperatorStats{})))
const lockPackerObjectBytes = 2 * int64(unsafe.Sizeof(types.Packer{})) // Go object class rounding.
const lockInitialPackerBytes = 4096 + lockPackerObjectBytes

func lockEmptyTableBytes(typ types.Type) int64 {
	// Empty-table fetches encode fixed min/max values (or the one-byte varlen
	// lower/upper sentinel), never source data. Keep a second pair for the
	// same row-to-table upgrade path as doLock. Every scalar key is bounded
	// by twice its fixed width plus the type/terminator bytes.
	keyBytes := 2*int64(typ.TypeSize()) + 2
	keys := 4 * (2*keyBytes + 8 + int64(unsafe.Sizeof([]byte{})))
	return lockInitialPackerBytes + keys + lockTempAnalyzerBytes
}

// MemoryBound bounds operator-owned storage, independently of the input and
// the lock service's transaction/lock-table ownership. inputBatchBytes is a
// logical referenced-payload bound: shared varlen offsets count once per row,
// since each lock key is encoded and cloned independently. The producer proof
// must provide this bound, rather than physical vector area capacity.
//
// One class-backed Packer persists until Free. Each doLock owns a nullable-key clone,
// Go row-key copies and their slice during the lock wait; targets run in order.
// A row-to-table upgrade creates two range keys while the original row keys
// remain referenced by AddWaitLock. RC timestamp columns persist in the output.
func MemoryBound(targets []MemoryTarget, inputBatchBytes, inputRowBytes int64, rows int) (int64, bool) {
	return memoryBound(targets, inputBatchBytes, inputRowBytes, rows, nil, false)
}

// ColumnMemoryBound contains referenced logical bytes for one input column.
// Unrelated payload cannot grow a primary-key packer or key clone.
type ColumnMemoryBound struct {
	RowBytes, BatchBytes int64
}

// MemoryBoundForColumns uses each target's actual primary-key column while
// retaining the same sequential-target, shared-packer and refresh lifetimes.
func MemoryBoundForColumns(targets []MemoryTarget, columns []ColumnMemoryBound, rows int) (int64, bool) {
	for _, col := range columns {
		if col.RowBytes < 0 || col.BatchBytes < 0 {
			return 0, false
		}
	}
	return memoryBound(targets, 0, 0, rows, columns, true)
}

func memoryBound(targets []MemoryTarget, inputBatchBytes, inputRowBytes int64, rows int, columns []ColumnMemoryBound, perColumn bool) (int64, bool) {
	if inputBatchBytes < 0 || inputRowBytes < 0 || rows < 0 {
		return 0, false
	}
	packerPeak := lockInitialPackerBytes // Prepare allocates even if Call is optimistic/empty.
	var targetPeak, timestamps, emptyTablePeak int64
	for _, target := range targets {
		if target.PrimaryColumn < 0 || target.RefreshColumn < -1 || !boundedLockType(target.Type) {
			return 0, false
		}
		batchBytes, rowBytes := inputBatchBytes, inputRowBytes
		if perColumn {
			if int(target.PrimaryColumn) >= len(columns) {
				return 0, false
			}
			column := columns[target.PrimaryColumn]
			batchBytes, rowBytes = column.BatchBytes, column.RowBytes
		}
		if target.LockTableAtEnd {
			emptyTablePeak = max(emptyTablePeak, lockEmptyTableBytes(target.Type))
		}
		if rows == 0 {
			continue
		}
		width := int64(target.Type.TypeSize())
		fixed, ok := lockMemoryMul(width, int64(rows))
		if !ok || rowBytes < width || batchBytes < fixed {
			return 0, false
		}
		keyRow := width
		if target.Type.IsVarlen() {
			keyRow = rowBytes
		}
		keyRow, ok = lockMemoryMul(keyRow, 2) // string NUL escaping also covers scalar encodings.
		if !ok {
			return 0, false
		}
		keyRow, ok = lockMemoryAdd(keyRow, 2) // string type and terminator / integer framing.
		if !ok {
			return 0, false
		}
		// ensureSizeSlow requests oldCapacity+nextAppend. Before growth,
		// oldCapacity and nextAppend are each below the final encoded length.
		request, ok := lockMemoryMul(keyRow, 2)
		if !ok {
			return 0, false
		}
		class, ok := malloc.ClassAllocationSize(uint64(max(int64(4096), request)))
		if !ok || class > math.MaxInt64/2 {
			return 0, false
		}
		packerPeak = max(packerPeak, int64(class)*2+lockPackerObjectBytes) // old+new Packer backing.

		keyBytes, ok := lockMemoryMul(batchBytes, 2)
		if !ok {
			return 0, false
		}
		framing, ok := lockMemoryMul(int64(rows), 2)
		if !ok {
			return 0, false
		}
		keyBytes, ok = lockMemoryAdd(keyBytes, framing)
		if !ok {
			return 0, false
		}
		// Bool/table-range fetchers can emit two keys for a one-row input.
		keyBytes = max(keyBytes, 2*keyRow)
		keyBytes, ok = lockMemoryAdd(keyBytes, 2*keyRow) // simultaneous upgrade range.
		if !ok {
			return 0, false
		}
		count, ok := lockMemoryAdd(int64(max(rows, 2)), 2)
		if !ok {
			return 0, false
		}
		// slices.Clone rounds individual []byte stores. Twice encoded bytes
		// plus one eight-byte quantum per key covers small/large Go classes.
		keyBytes, ok = lockMemoryMul(keyBytes, 2)
		if !ok {
			return 0, false
		}
		headers, ok := lockMemoryMul(count, int64(unsafe.Sizeof([]byte{}))+8)
		if !ok {
			return 0, false
		}
		keyBytes, ok = lockMemoryAdd(keyBytes, headers)
		if !ok {
			return 0, false
		}
		clone, ok := lockVectorPeak(target.Type, batchBytes, rows)
		if !ok {
			return 0, false
		}
		peak, ok := lockMemoryAdd(clone, keyBytes)
		if !ok {
			return 0, false
		}
		targetPeak = max(targetPeak, peak)
		if target.RefreshColumn >= 0 {
			peak, ok := lockVectorPeak(types.T_TS.ToType(), 0, rows)
			if !ok {
				return 0, false
			}
			timestamps, ok = lockMemoryAdd(timestamps, peak)
			if !ok {
				return 0, false
			}
		}
	}
	size, ok := lockMemoryAdd(packerPeak, targetPeak)
	if !ok {
		return 0, false
	}
	size, ok = lockMemoryAdd(size, timestamps)
	if !ok {
		return 0, false
	}
	// With any input row lockCount is nonzero, so the terminal helper cannot
	// coexist with normal row-lock/refresh storage. At empty EOF only the
	// operator's original 4096-byte Packer coexists with the helper's owners.
	if emptyTablePeak > 0 {
		size = max(size, lockInitialPackerBytes+emptyTablePeak)
	}
	// Prepare appends fetchers from an empty slice. growslice requests at
	// most twice the required slots; Go allocation-class rounding (including
	// its scanned-object header) can enlarge that request again. Four N
	// slots bound the new backing; the old (<N) backing coexists during the
	// copy. Relations use one fixed N-interface allocation without growth.
	metadata, ok := lockMemoryMul(int64(len(targets)), int64(5*unsafe.Sizeof(FetchLockRowsFunc(nil))+2*unsafe.Sizeof(uintptr(0))))
	if !ok {
		return 0, false
	}
	return lockMemoryAdd(size, metadata)
}

func (lockOp *LockOp) MemoryBound(inputBatchBytes, inputRowBytes int64, rows int) (int64, bool) {
	targets, ok := lockOp.MemoryTargets()
	if !ok {
		return 0, false
	}
	return MemoryBound(targets, inputBatchBytes, inputRowBytes, rows)
}
