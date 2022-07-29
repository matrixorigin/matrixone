// Copyright 2021 Matrix Origin
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

package aggregate

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

const (
	Sum = iota
	Avg
	Max
	Min
	Count
	StarCount
	ApproxCountDistinct
	Variance
	BitAnd
	BitXor
	BitOr
	StdDevPop
	AnyValue
)

var Names = [...]string{
	Sum:                 "sum",
	Avg:                 "avg",
	Max:                 "max",
	Min:                 "min",
	Count:               "count",
	StarCount:           "starcount",
	ApproxCountDistinct: "approx_count_distinct",
	Variance:            "var",
	BitAnd:              "bit_and",
	BitXor:              "bit_xor",
	BitOr:               "bit_or",
	StdDevPop:           "stddev_pop",
	AnyValue:            "any",
}

// Agg agg interface
type Agg[T any] interface {
	// Dup will duplicate a new agg with the same type.
	Dup() Agg[any]

	// Type return the type of the agg's result.
	OutputType() types.Type

	// InputType return the type of the agg's input.
	InputTypes() []types.Type

	// String return related information of the agg.
	// used to show query plans.
	String() string

	// Free the agg.
	Free(*mheap.Mheap)

	// Grows allocates n groups for the agg.
	Grows(n int, _ *mheap.Mheap) error

	// Eval method calculates and returns the final result of the aggregate function.
	Eval(_ *mheap.Mheap) (*vector.Vector, error)

	// Fill use the rowIndex-rows of vector to update the data of groupIndex-group.
	// rowCount indicates the number of times the rowIndex-row is repeated.
	Fill(groupIndex int64, rowIndex int64, rowCount int64, vecs []*vector.Vector)

	// BulkFill use a whole vector to update the data of agg's group
	// groupIndex is the index number of the group
	// rowCounts is the count number of each row.
	BulkFill(groupIndex int64, rowCounts []int64, vecs []*vector.Vector)

	// BatchFill use part of the vector to update the data of agg's group
	//      os(origin-s) records information about which groups need to be updated
	//      if length of os is N, we use first N of vps to do update work.
	//      And if os[i] > 0, it means the agg's (vps[i]-1)th group is a new one (never been assigned a value),
	//      Maybe this feature can help us to do some optimization work.
	//      So we use the os as a parameter but not len(os).
	//
	//      agg's (vps[i]-1)th group is related to vector's (offset+i)th row.
	//      rowCounts[i] is count number of the row[i]
	// For a more detailed introduction of rowCounts, please refer to comments of Function Fill.
	BatchFill(offset int64, os []uint8, vps []uint64, rowCounts []int64, vecs []*vector.Vector)

	// Merge will merge a couple of group between 2 aggregate function structures.
	// It merges the groupIndex1-group of agg1 and
	// groupIndex2-group of agg2
	Merge(agg2 Agg[any], groupIndex1 int64, groupIndex2 int64)

	// BatchAdd merges multi groups of agg1 and agg2
	//  agg1's (vps[i]-1)th group is related to agg2's (start+i)th group
	// For more introduction of os, please refer to comments of Function BatchFill.
	BatchMerge(agg2 Agg[any], start int64, os []uint8, vps []uint64)
}

type Aggregate struct {
	Op   int
	Dist bool
	E    *plan.Expr
}
