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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

const (
	// GroupNotMatched is a constant for the BatchFill method.
	// if the group is GroupNotMatched, the BatchFill method will ignore the row.
	GroupNotMatched = 0
)

// AggFuncExec is an interface to do execution for aggregation.
// todo: use vector... to replace the []*vector.Vector may be better.
type AggFuncExec interface {
	// Init initialize the aggregation.
	// Init()

	AggID() int64

	// TypesInfo return the argument types and return type of the function.
	TypesInfo() ([]types.Type, types.Type)

	// GrowGroup(more int) error

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
	_ AggFuncExec = (*singleAggFuncExec2[int64])(nil)
	_ AggFuncExec = (*singleAggFuncExec3[int64])(nil)
	_ AggFuncExec = &singleAggFuncExec4{}
	_ AggFuncExec = (*multiAggFuncExec1[int8])(nil)
	_ AggFuncExec = (*multiAggFuncExec2)(nil)
)

// MakeAgg supports to create an aggregation function executor for single column.
func MakeAgg(
	aggID int64, param types.Type, result types.Type, private any) AggFuncExec {

	// switch the param and result type
	// or let the user deliver the correct implementation with [from, to].

	panic(fmt.Sprintf("invalid implementation for single-column aggregation, agg id: %d, param is %s, result is %s", aggID, param.String(), result.String()))
}

// MakeMultiAgg supports to create an aggregation function executor for multiple columns.
func MakeMultiAgg(
	aggID int64, param []types.Type, result types.Type, private any) AggFuncExec {

	// same as the above comment of MakeAgg.

	panic(fmt.Sprintf("invalid implementation for multi-column aggregation, agg id: %d, param is %s, result is %s", aggID, param, result.String()))
}

// MakeGroupConcat is one special case of MakeMultiAgg.
// it supports creating an aggregation function executor for special aggregation `group_concat()`.
func MakeGroupConcat() AggFuncExec {
	// todo : no implementation now.
	return nil
}
