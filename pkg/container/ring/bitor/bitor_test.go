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

package bitor

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"reflect"
	"testing"
)

// Testbit_or just for verify bit_orRing related process
func TestBitOr(t *testing.T) {
	// verify that if we can calculates

	// 1. make the test case
	v1 := NewBitOr(types.Type{Oid: types.T_int32})
	v2 := v1.Dup().(*BitOrRing)
	{
		v1.Values = []uint64{1, 2, 8}
		v1.NullCounts = []int64{1, 1, 0}
	}
	{
		v2.Values = []uint64{2, 3}
		v2.NullCounts = []int64{0, 1}
	}
	v1.Add(v2, 0, 0)
	v1.Add(v2, 1, 1)

	result := v1.Eval([]int64{2, 2, 4})

	expected := []uint64{3, 3, 8}
	if !reflect.DeepEqual(result.Col, expected) {
		t.Errorf(fmt.Sprintf("TestBit_or wrong, expected %v, but got %v", expected, result.Col))
	}
}
