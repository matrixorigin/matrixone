// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

type AggFuncExec interface {
	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	// Fill BulkFill and BatchFill add the value to the aggregation.
	Fill(groupIndex int, row int, vectors []*vector.Vector) error
	BulkFill(groupIndex int, vectors []*vector.Vector) error
	BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error

	// SetPreparedResult add a partial result to speed up.
	SetPreparedResult(partialResult any, groupIndex int)

	// Flush return the aggregation result.
	Flush() (*vector.Vector, error)

	// Copy returns a copy of the aggregation.
	// This copy will be allocated in the inputting memory pool.
	// todo: there is no place use this method now.
	//  please refer to the old codes' Dup(pool) method.
	//Copy(mp *mpool.MPool) AggFuncExec

	// Free free the aggregation.
	// This method will do the reset and reuse the aggregation if possible.
	Free()
}

// indicate who implements the AggFuncExec interface.
var (
	_ AggFuncExec = (*singleAggFuncExec1[int8, int64])(nil)
)

// AggFuncPrivate1 and AggFuncPrivate2 are the private struct for aggregation.
//
//	1 for single column aggregation.
//	2 for multi column aggregation.
//
// all the aggregation function should implement this interface.
type AggFuncPrivate1 interface {
}
type AggFuncPrivate2 interface {
	getAddFunc(idx int) any
}
