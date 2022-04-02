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
package bitand

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func TestBitAnd(t *testing.T) {
	testCases := []struct {
		Name string
		Args []*NumericRing
		Want []uint64
	}{
		{
			// given that {6, 4, 2, null}, {null, 1, 4,6}
			Name: "uint64",
			Args: []*NumericRing{
				{
					Typ:          types.Type{Oid: types.T_uint64},
					BitAndResult: []uint64{6 & 4, 1},
					NullCnt:      []int64{0, 1},
				}, {
					Typ:          types.Type{Oid: types.T_uint64},
					BitAndResult: []uint64{2, 4 & 6},
					NullCnt:      []int64{1, 0},
				},
			},
			Want: []uint64{0, 0},
		},
		{
			// given that {6.1, 4.2, 2.3, null}, {null, 1.3, 4.2, 6.6}
			Name: "float32",
			Args: []*NumericRing{
				{
					Typ:          types.Type{Oid: types.T_float32},
					BitAndResult: []uint64{6 & 4, 1},
					NullCnt:      []int64{0, 1},
				}, {
					Typ:          types.Type{Oid: types.T_float32},
					BitAndResult: []uint64{2, 4 & 6},
					NullCnt:      []int64{1, 0},
				},
			},
			Want: []uint64{0, 0},
		},
		{
			// given that {^uint32(0), 5, 3, null}, {null, 1, -4, 10}
			Name: "int32",
			Args: []*NumericRing{
				{
					Typ:          types.Type{Oid: types.T_uint64},
					BitAndResult: []uint64{uint64(^uint32(0)) & 5, 1},
					NullCnt:      []int64{0, 1},
				}, {
					Typ:          types.Type{Oid: types.T_uint64},
					BitAndResult: []uint64{3, -4 & 10},
					NullCnt:      []int64{1, 0},
				},
			},
			Want: []uint64{1, 0},
		},
	}

	for _, c := range testCases {
		c.Args[0].Add(c.Args[1], 0, 0)
		c.Args[0].Add(c.Args[1], 1, 1)
		result := c.Args[0].Eval([]int64{4, 4})
		if !reflect.DeepEqual(result.Col, c.Want) {
			t.Errorf(fmt.Sprintf("TestBitAnd wrong, in case %v expected %v, but got %v", c.Name, c.Want, result.Col))
		}
	}
}
