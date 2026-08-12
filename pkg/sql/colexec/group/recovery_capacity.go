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

package group

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	groupSpillHashBytes      = uint64(8)
	groupSpillSelectionBytes = uint64(1)
	groupSpillRowIDBytes     = uint64(4)
)

func groupRecoveryAdd(left, right uint64) (uint64, error) {
	if left > math.MaxUint64-right {
		return 0, process.ErrExecutionResourceInvalid
	}
	return left + right, nil
}

func groupRecoveryMul(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, process.ErrExecutionResourceInvalid
	}
	return left * right, nil
}

// ensureRecoveryCapacity reserves the minimum reusable headroom needed to
// partition all state that can exist after the next input chunk. Spill needs
// exactly one uint64 hash per resident group plus one byte of selection state
// and one int32 row id for the largest aggregate chunk. The row ids implement
// an O(rows) counting partition; keeping them in the floor avoids an
// unaccounted 32*8K stack matrix precisely when spill runs under pressure.
// Serialization is streaming and its I/O buffers are optional, so no guessed
// per-column payload belongs in this hard reservation. Physical scratch later
// borrows this floor through the operator's recovery capacity class, avoiding
// a duplicate budget charge.
func (ctr *container) ensureRecoveryCapacity(
	incomingRows int,
	analyzer process.Analyzer,
) error {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return nil
	}
	if incomingRows < 0 {
		return process.ErrExecutionResourceInvalid
	}
	target, err := ctr.recoveryCapacityTarget(incomingRows)
	if err != nil {
		return err
	}
	if err = ctr.recoveryCapacity.EnsureCapacity(target); err != nil {
		if analyzer != nil {
			analyzer.GetOpStats().AddExtraStat(
				"GroupSpillRecoveryReserveRejects", 1)
		}
		return err
	}
	ctr.recoveryCapacityFloor = target
	return nil
}

// recoveryCapacityCovers avoids taking the controller lock when the existing
// floor already admits the next bounded chunk. Group is single-threaded; only
// recovery-class borrowers synchronize with it after spill begins.
func (ctr *container) recoveryCapacityCovers(incomingRows int) bool {
	if ctr == nil || ctr.recoveryCapacity == nil {
		return true
	}
	if ctr.recoveryCapacityFloor == 0 {
		return false
	}
	if incomingRows < 0 {
		return false
	}
	groups := uint64(incomingRows)
	if !ctr.hr.IsEmpty() {
		current := ctr.hr.Hash.GroupCount()
		if current > math.MaxUint64-groups {
			return false
		}
		groups += current
	}
	chunkRows := min(groups, uint64(aggBatchSize))
	if groups != 0 && groupSpillHashBytes > ctr.recoveryCapacityFloor/groups {
		return false
	}
	target := groups * groupSpillHashBytes
	selectionAndRows := chunkRows *
		(groupSpillSelectionBytes + groupSpillRowIDBytes)
	if selectionAndRows > math.MaxUint64-target {
		return false
	}
	target += selectionAndRows
	return target <= ctr.recoveryCapacityFloor
}

func (ctr *container) recoveryCapacityTarget(incomingRows int) (uint64, error) {
	if incomingRows < 0 {
		return 0, process.ErrExecutionResourceInvalid
	}
	groups := uint64(incomingRows)
	if !ctr.hr.IsEmpty() {
		current := ctr.hr.Hash.GroupCount()
		if current > math.MaxUint64-groups {
			return 0, process.ErrExecutionResourceInvalid
		}
		groups += current
	}
	hashBytes, err := groupRecoveryMul(groups, groupSpillHashBytes)
	if err != nil {
		return 0, err
	}
	chunkRows := min(groups, uint64(aggBatchSize))
	selectionBytes, err := groupRecoveryMul(chunkRows, groupSpillSelectionBytes)
	if err != nil {
		return 0, err
	}
	rowIDBytes, err := groupRecoveryMul(chunkRows, groupSpillRowIDBytes)
	if err != nil {
		return 0, err
	}
	target, err := groupRecoveryAdd(hashBytes, selectionBytes)
	if err != nil {
		return 0, err
	}
	target, err = groupRecoveryAdd(target, rowIDBytes)
	return target, err
}
