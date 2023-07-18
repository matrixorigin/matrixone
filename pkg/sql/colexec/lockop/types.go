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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// FetchLockRowsFunc fetch lock rows from vector.
type FetchLockRowsFunc func(
	// primary data vector
	vec *vector.Vector,
	// holder to encode primary key to lock row
	parker *types.Packer,
	// primary key type
	tp types.Type,
	// global config: max lock rows bytes per lock
	max int,
	// is lock table lock
	lockTable bool,
	// used to filter rows
	filter RowsFilter,
	// used by filter rows func
	filterCols []int32) (bool, [][]byte, lock.Granularity)

// LockOptions lock operation options
type LockOptions struct {
	maxCountPerLock          int
	mode                     lock.LockMode
	lockTable                bool
	parker                   *types.Packer
	fetchFunc                FetchLockRowsFunc
	filter                   RowsFilter
	filterCols               []int32
	hasNewVersionInRangeFunc hasNewVersionInRangeFunc
}

// Argument lock op argument.
type Argument struct {
	engine  engine.Engine
	targets []lockTarget
	block   bool

	// state used for save lock op temp state.
	rt *state
}

type lockTarget struct {
	tableID                      uint64
	primaryColumnIndexInBatch    int32
	refreshTimestampIndexInBatch int32
	primaryColumnType            types.Type
	filter                       RowsFilter
	filterColIndexInBatch        int32
	lockTable                    bool
}

// RowsFilter used to filter row from primary vector. The row will not lock if filter return false.
type RowsFilter func(row int, filterCols []int32) bool

type hasNewVersionInRangeFunc func(
	proc *process.Process,
	tableID uint64,
	eng engine.Engine,
	vec *vector.Vector,
	from, to timestamp.Timestamp) (bool, error)

type state struct {
	colexec.ReceiverOperator

	parker               *types.Packer
	retryError           error
	step                 int
	fetchers             []FetchLockRowsFunc
	cachedBatches        []*batch.Batch
	batchFetchFunc       func(process.Analyze) (*batch.Batch, bool, error)
	hasNewVersionInRange hasNewVersionInRangeFunc
}

const (
	stepLock = iota
	stepDownstream
	stepEnd
)
