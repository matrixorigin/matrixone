// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// hodler to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTabel bool,
	// used to filter rows
	filter RowsFilter,
	// used by filter rows func
	filterCols []int) ([][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxBytesPerLock int
	mode            lock.LockMode
	lockTable       bool
	parker          *types.Packer
	fetchFunc       FetchLockRowsFunc
	filter          RowsFilter
	filterCols      []int
}

// Argument lock op argument.
type Argument struct {
	parker  *types.Packer
	targets []lockTarget
}

type lockTarget struct {
	tableID                      uint64
	primaryColumnIndexInBatch    int32
	refreshTimestampIndexInBatch int32
	primaryColumnType            types.Type
	fetcher                      FetchLockRowsFunc
	filter                       RowsFilter
	filterColIndexInBatch        int32
}

// RowsFilter used to filter row from primary vector. The row will not lock if filter return false.
type RowsFilter func(row int, fliterCols []int) bool
