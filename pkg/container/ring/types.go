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

package ring

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

// Ring is an important structure in factorization.
// It allows us to compute the aggregate functions for each part before do table-join-work.
type Ring interface {
	// Count return the group number of a ring.
	// e.g.
	// the query sql is `select area, avg(age) of persons group by area;` and the area has 3 different values.
	// the Count will return 3.
	Count() int

	// Size return the memory size of a ring.
	Size() int

	// Dup will duplicate a new ring with the same type.
	Dup() Ring

	// Type return the type of the ring's result.
	Type() types.Type

	// String return related information of the ring.
	// used to show query plans.
	String() string

	// Free the ring.
	Free(*mheap.Mheap)

	// Grow allocates a new group for the ring.
	Grow(*mheap.Mheap) error

	// Grows allocates N groups for the ring.
	Grows(N int, _ *mheap.Mheap) error

	// SetLength set the group length to reduce the ring's size, the first N group wii be kept
	// and others will be free
	SetLength(N int)

	// Shrink used an index group to reduce the ring's size
	// indexes records the index number of the group which we want to keep
	// e.g. If indexes is [0, 2], the first group and 3rd group will be kept.
	Shrink(indexes []int64)

	// Shuffle not used now.
	Shuffle([]int64, *mheap.Mheap) error

	// Eval returns the final result of the ring.
	// the argument zs records the size of data source of a group item.
	// e.g.
	// If zs is [1, 2, 1], and the ring hold a group Sums, Sums is the sum of some data.
	// zs[0] = 1 means group 0 has 1 data.
	// zs[1] = 2 means group 1 has 2 date.
	// and data source may contain the NULL value and not always all numbers.
	Eval(zs []int64) *vector.Vector

	// Fill use a row of the vector to update the data of ring's group
	// i is the index number of the group
	// j is the index number of the row
	// z is the count number of the row, if N, there are other N-1 rows are same to this.
	// and we should update the rings N times.
	Fill(i int64, j int64, z int64, v *vector.Vector)

	// BulkFill use the whole vector to update the data of ring's group
	// i is the index number of the group
	// zs is the count number of each row.
	BulkFill(i int64, zs []int64, v *vector.Vector)

	// BatchFill use part of the vector to update the data of ring's group
	// 		os(origin-s) records information about which groups need to be updated
	//		if length of os is N, we use first N of vps to do update work.
	//		And if os[i] > 0, it means the ring's (vps[i]-1)th group is a new one (never been assigned a value),
	//		Maybe this feature can help us to do some optimization work.
	//		So we use the os as an argument but not len(os).
	//
	// 		ring's (vps[i]-1)th group is related to vector's (offset+i)th row.
	// 		zs[i] is count number of the row[i]
	// For a more detailed introduction of zs, please refer to comments of Function Fill.
	BatchFill(offset int64, os []uint8, vps []uint64, zs []int64, v *vector.Vector)

	// Add may be called merge in other databases.
	// It merges group1 and group2.
	// group1 is the i-th of ring1. group is the j-th of ring2.
	Add(ring2 interface{}, i int64, j int64)

	// BatchAdd merges multi groups of ring1 and ring2
	// 	ring1's (vps[i]-1)th group is related to ring2's (start+i)th group
	// For more introduction of os, please refer to comments of Function BatchFill.
	BatchAdd(ring2 interface{}, start int64, os []uint8, vps []uint64)

	// Mul is the function to merge 2 rings when join
	Mul(interface{}, int64, int64, int64)
}
