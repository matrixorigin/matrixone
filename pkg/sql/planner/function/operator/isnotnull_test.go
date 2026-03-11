// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestIsNotNullNormal(t *testing.T) {
	proc := testutil.NewProc()
	vecs1 := []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 14, 42, 7656, 4324234, 543534523}, nil)}
	expected1 := []bool{true, true, true, true, true, true}
	vecs2 := []*vector.Vector{testutil.MakeVarcharVector([]string{"1", "2", "3", "4"}, nil)}
	expected2 := []bool{true, true, true, true}
	vecs3 := []*vector.Vector{testutil.MakeTextVector([]string{"1", "2", "3", "4"}, nil)}
	expected3 := []bool{true, true, true, true}

	t.Run("isnotnull test - normal", func(t *testing.T) {
		result, err := IsNotNull(vecs1, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected1, false)
		result, err = IsNotNull(vecs2, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, false)
		result, err = IsNotNull(vecs3, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected3, false)
	})
}

func TestIsNotNullNormalWithNull(t *testing.T) {
	proc := testutil.NewProc()
	vecs1 := []*vector.Vector{testutil.MakeFloat64Vector([]float64{1, 14, 42, 7656, 4324234, 543534523}, []uint64{0, 1, 4})}
	expected1 := []bool{false, false, true, true, false, true}
	vecs2 := []*vector.Vector{testutil.MakeVarcharVector([]string{"1", "2", "3", "4"}, []uint64{0, 1, 3})}
	expected2 := []bool{false, false, true, false}
	vecs3 := []*vector.Vector{testutil.MakeTextVector([]string{"1", "2", "3", "4"}, []uint64{2, 3})}
	expected3 := []bool{true, true, false, false}

	t.Run("isnotnull test - normal with null", func(t *testing.T) {
		result, err := IsNotNull(vecs1, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected1, false)
		result, err = IsNotNull(vecs2, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, false)
		result, err = IsNotNull(vecs3, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected3, false)
	})
}

func TestIsNotNullScalar(t *testing.T) {
	proc := testutil.NewProc()
	vecs1 := []*vector.Vector{testutil.MakeScalarInt32(543534523, 1)}
	expected1 := []bool{true}
	vecs2 := []*vector.Vector{testutil.MakeScalarVarchar("hao", 1)}
	expected2 := []bool{true}

	t.Run("isnotnull test - not null scalar", func(t *testing.T) {
		result, err := IsNotNull(vecs1, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected1, true)
		result, err = IsNotNull(vecs2, proc)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, true)
	})
}

func TestIsNotNullScalarNull(t *testing.T) {
	procs := testutil.NewProc()
	vecs1 := []*vector.Vector{testutil.MakeScalarNull(types.T_bool, 1)}
	expected1 := []bool{false}
	vecs2 := []*vector.Vector{testutil.MakeScalarNull(types.T_bool, 1)}
	expected2 := []bool{false}

	t.Run("isnotnull test - scalar null", func(t *testing.T) {
		result, err := IsNotNull(vecs1, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected1, true)
		result, err = IsNotNull(vecs2, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, true)
	})
}
