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

package bitxor

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestBitXor(t *testing.T) {
	// verify that if we can calculate
	// the variance of {1, 2, null, 0, 3, 4} and {2, 3, null, null, 4, 5} correctly

	// 1. make the test case
	v1 := NewBitXor(types.Type{Oid: types.T_float64})
	v2 := v1.Dup().(*BitXorRing)
	{
		// first 3 rows.
		// column1: {1, 2, null}, column2: {2, 3, null}
		v1.Values = []uint64{1 ^ 2, 2 ^ 3}
		v1.NullCounts = []int64{1, 1}
	}
	{
		// last 3 rows.
		// column1: {0, 3, 4}, column2: {null, 4, 5}
		v2.Values = []uint64{0 ^ 3 ^ 4, 4 ^ 5}
		v2.NullCounts = []int64{0, 1}
	}
	v1.Add(v2, 0, 0)
	v1.Add(v2, 1, 1)

	result := v1.Eval([]int64{6, 6})

	expected := []uint64{4, 0}
	if !reflect.DeepEqual(result.Col, expected) {
		t.Errorf(fmt.Sprintf("TestVariance wrong, expected %v, but got %v", expected, result.Col))
	}
}
