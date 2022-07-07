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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type Agg[T any] interface {
	// Dup will duplicate a new agg with the same type.
	Dup() Agg[any]

	// Type return the type of the agg's result.
	Type() types.Type

	// InputType return the type of the agg's input.
	InputType() types.Type

	// String return related information of the agg.
	// used to show query plans.
	String() string

	// Free the agg.
	Free(*mheap.Mheap)

	// Grows allocates n groups for the agg.
	Grows(n int, _ *mheap.Mheap) error

	// Eval returns the final result of the agg.
	// the argument zs records the size of data source of a group item.
	// e.g.
	// If zs is [1, 2, 1], and the agg hold a group Sums, Sums is the sum of some data.
	// zs[0] = 1 means group 0 has 1 data.
	// zs[1] = 2 means group 1 has 2 date.

	// Eval returns the final result of the agg.
	// the argument zs records the size of data source of a group item.
	// e.g.
	// If zs is [1, 2, 1], and the agg hold a group Sums, Sums is the sum of some data.
	// zs[0] = 1 means group 0 has 1 data.
	// zs[1] = 2 means group 1 has 2 date.
	// and data source may contain the NULL value and not always all numbers.
	Eval(zs []int64, _ *mheap.Mheap) vector.AnyVector

	// Fill use a row of the vector to update the data of agg's group
	// i is the index number of the group
	// j is the index number of the row
	// z is the count number of the row, if N, there are other N-1 rows are same to this.
	// and we should update the aggs N times.
	Fill(i int64, j int64, z int64, v vector.AnyVector)

	// BulkFill use the whole vector to update the data of agg's group
	// i is the index number of the group
	// zs is the count number of each row.
	BulkFill(i int64, zs []int64, v vector.AnyVector)

	// BatchFill use part of the vector to update the data of agg's group
	//      os(origin-s) records information about which groups need to be updated
	//      if length of os is N, we use first N of vps to do update work.
	//      And if os[i] > 0, it means the agg's (vps[i]-1)th group is a new one (never been assigned a value),
	//      Maybe this feature can help us to do some optimization work.
	//      So we use the os as an argument but not len(os).
	//
	//      agg's (vps[i]-1)th group is related to vector's (offset+i)th row.
	//      zs[i] is count number of the row[i]
	// For a more detailed introduction of zs, please refer to comments of Function Fill.
	BatchFill(offset int64, os []uint8, vps []uint64, zs []int64, v vector.AnyVector)

	// Add may be called merge in other databases.
	// It merges group1 and group2.
	// group1 is the i-th of agg1. group is the j-th of agg2.
	Add(r2 Agg[any], i int64, j int64)

	// BatchAdd merges multi groups of agg1 and agg2
	//  agg1's (vps[i]-1)th group is related to agg2's (start+i)th group
	// For more introduction of os, please refer to comments of Function BatchFill.
	BatchAdd(r2 Agg[any], start int64, os []uint8, vps []uint64)
}
