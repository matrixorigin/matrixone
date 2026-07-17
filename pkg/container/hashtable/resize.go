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

package hashtable

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ResizePlan is a side-effect-free description of a hash table growth.
//
// The byte fields describe the cell storage only (the Go slice headers are not
// part of the mpool allocation). AdditionalBytes is the complete replacement
// allocation, including every 256 MiB block when a table is block-backed.
// ProjectedPeakBytes is the old allocation plus AdditionalBytes; this is the
// amount an admission policy must be able to accommodate while the old table
// is retained for rehashing.
type ResizePlan struct {
	CurrentBytes       uint64
	AdditionalBytes    uint64
	ProjectedPeakBytes uint64

	CurrentCellCount      uint64
	TargetCellCount       uint64
	CurrentBlockCount     uint64
	TargetBlockCount      uint64
	CurrentBlockCellCount uint64
	TargetBlockCellCount  uint64
	TargetMaxElemCount    uint64

	// TableVersion and ShapeID reject a plan made against an older table
	// shape. ShapeID is deliberately redundant with the shape fields so a
	// caller can persist either identity in an accounting token.
	TableVersion   uint64
	ShapeID        uint64 // identity of the current table shape
	TargetShapeID  uint64
	CurrentShapeID uint64

	// Compatibility aliases for callers that use old/new terminology.
	OldBytes, NewBytes           uint64
	OldCellCount, NewCellCount   uint64
	OldBlockCount, NewBlockCount uint64
	Version                      uint64

	Noop    bool
	Invalid bool
}

// ResizeReservation owns the temporary-peak admission for one resize.
// Commit is called after the replacement is published and the old cells are
// freed. Rollback is called on every failure before publication. Implementors
// must make both operations idempotent because they commonly wrap an
// exactly-once memory reservation token.
type ResizeReservation interface {
	Commit(ResizePlan)
	Rollback()
}

// ResizeAdmission is called exactly once, before a growth allocates or
// mutates the hash table. Returning an error rejects the resize unchanged.
// A non-nil reservation is resolved exactly once by ResizeWithPlan.
type ResizeAdmission func(ResizePlan) (ResizeReservation, error)

// ErrStaleResizePlan indicates that a plan no longer describes the table.
var ErrStaleResizePlan = moerr.NewInternalErrorNoCtx("stale hash table resize plan")

var ErrInvalidResizePlan = moerr.NewInternalErrorNoCtx("invalid hash table resize plan")

func resizeShapeID(version, cells, blockCells, blocks uint64) uint64 {
	// A cheap deterministic identity; collisions are harmless because the
	// individual shape fields are checked as well.
	x := version + 0x9e3779b97f4a7c15
	x ^= cells + (x << 6) + (x >> 2)
	x ^= blockCells + (x << 6) + (x >> 2)
	x ^= blocks + (x << 6) + (x >> 2)
	return x
}

func newResizePlan(elemCnt, additional, cellCnt, blockCellCnt, blockMaxElemCnt, blockCount, cellSize, maxCellsPerBlock, version uint64) ResizePlan {
	plan := ResizePlan{
		CurrentCellCount:      cellCnt,
		CurrentBlockCount:     blockCount,
		CurrentBlockCellCount: blockCellCnt,
		TableVersion:          version,
		Version:               version,
	}
	currentCells, ok := checkedMultiply(blockCount, blockCellCnt)
	if !ok {
		plan.Invalid = true
		return plan
	}
	plan.CurrentBytes, ok = checkedMultiply(currentCells, cellSize)
	if !ok {
		plan.Invalid = true
		return plan
	}
	plan.OldBytes = plan.CurrentBytes
	plan.TargetCellCount = cellCnt
	plan.TargetBlockCount = blockCount
	plan.TargetBlockCellCount = blockCellCnt
	plan.TargetMaxElemCount = blockMaxElemCnt
	plan.ShapeID = resizeShapeID(version, cellCnt, blockCellCnt, blockCount)
	plan.CurrentShapeID = plan.ShapeID
	plan.TargetShapeID = plan.ShapeID
	plan.Noop = true

	if additional > math.MaxUint64-elemCnt {
		plan.Invalid = true
		return plan
	}
	target := elemCnt + additional
	currentMaxElemCnt, ok := checkedMultiply(blockCount, blockMaxElemCnt)
	if !ok {
		plan.Invalid = true
		return plan
	}
	if additional == 0 || target <= currentMaxElemCnt {
		plan.OldCellCount, plan.NewCellCount = cellCnt, cellCnt
		plan.OldBlockCount, plan.NewBlockCount = blockCount, blockCount
		return plan
	}

	newCellCnt := cellCnt << 1
	if newCellCnt < cellCnt {
		plan.Invalid = true
		return plan
	}
	newMaxElemCnt := maxElemCnt(newCellCnt, cellSize)
	for newMaxElemCnt < target {
		if newCellCnt > math.MaxUint64/2 {
			plan.Invalid = true
			return plan
		}
		newCellCnt <<= 1
		newMaxElemCnt = maxElemCnt(newCellCnt, cellSize)
	}
	newAlloc, ok := checkedMultiply(newCellCnt, cellSize)
	if !ok {
		plan.Invalid = true
		return plan
	}
	newBlockCellCnt := newCellCnt
	newBlockCount := uint64(1)
	if newAlloc > maxBlockSize {
		newBlockCellCnt = maxCellsPerBlock
		newBlockCount = newAlloc / maxBlockSize
		if newAlloc%maxBlockSize != 0 {
			newBlockCount++
		}
		newCellCnt, ok = checkedMultiply(newBlockCellCnt, newBlockCount)
		if !ok {
			plan.Invalid = true
			return plan
		}
		newMaxElemCnt = maxElemCnt(newBlockCellCnt, cellSize)
	}
	if newBlockCount > uint64(math.MaxInt) || newBlockCellCnt > uint64(math.MaxInt) {
		plan.Invalid = true
		return plan
	}

	plan.TargetCellCount = newCellCnt
	plan.TargetBlockCount = newBlockCount
	plan.TargetBlockCellCount = newBlockCellCnt
	plan.TargetMaxElemCount = newMaxElemCnt
	plan.AdditionalBytes = newCellCnt * cellSize
	plan.NewBytes = plan.AdditionalBytes
	if plan.AdditionalBytes > math.MaxUint64-plan.CurrentBytes {
		plan.Invalid = true
		return plan
	}
	plan.ProjectedPeakBytes = plan.CurrentBytes + plan.AdditionalBytes
	plan.OldCellCount, plan.NewCellCount = cellCnt, newCellCnt
	plan.OldBlockCount, plan.NewBlockCount = blockCount, newBlockCount
	plan.TargetShapeID = resizeShapeID(version, newCellCnt, newBlockCellCnt, newBlockCount)
	plan.Noop = false
	return plan
}

func checkedMultiply(a, b uint64) (uint64, bool) {
	if a != 0 && b > math.MaxUint64/a {
		return 0, false
	}
	return a * b, true
}

func (p ResizePlan) matches(version, cellCnt, blockCellCnt, blockCount uint64) bool {
	return p.TableVersion == version &&
		p.CurrentCellCount == cellCnt &&
		p.CurrentBlockCellCount == blockCellCnt &&
		p.CurrentBlockCount == blockCount &&
		p.ShapeID == resizeShapeID(version, cellCnt, blockCellCnt, blockCount)
}
