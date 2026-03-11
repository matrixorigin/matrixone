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
	"github.com/stretchr/testify/require"
)

func TestIsNullNormal(t *testing.T) {
	procs := testutil.NewProc()
	vecs1 := []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 14, 42, 7656, 4324234, 543534523}, nil)}
	expected1 := []bool{false, false, false, false, false, false}
	vecs2 := []*vector.Vector{testutil.MakeVarcharVector([]string{"1", "2", "3", "4"}, nil)}
	expected2 := []bool{false, false, false, false}
	vecs3 := []*vector.Vector{testutil.MakeTextVector([]string{"1", "2", "3", "4"}, nil)}
	expected3 := []bool{false, false, false, false}

	t.Run("test null normal", func(t *testing.T) {
		result, err := IsNull(vecs1, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected1, false)
		result, err = IsNull(vecs2, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, false)
		result, err = IsNull(vecs3, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected3, false)
	})
}

func TestIsNullNormalWithNull(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 14, 42, 7656, 4324234, 543534523}, []uint64{0, 1, 4})}
	expected := []bool{true, true, false, false, true, false}
	vecs2 := []*vector.Vector{testutil.MakeVarcharVector([]string{"1", "2", "3", "4"}, []uint64{1, 2})}
	expected2 := []bool{false, true, true, false}
	vecs3 := []*vector.Vector{testutil.MakeTextVector([]string{"1", "2", "3", "4"}, []uint64{1, 3})}
	expected3 := []bool{false, true, false, true}

	t.Run("test null normal with null", func(t *testing.T) {
		result, err := IsNull(vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected, false)
		result, err = IsNull(vecs2, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, false)
		result, err = IsNull(vecs3, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected3, false)
	})
}

func TestIsNullScalar(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeScalarInt32(543534523, 1)}
	expected := []bool{false}
	vecs2 := []*vector.Vector{testutil.MakeScalarVarchar("cms", 1)}
	expected2 := []bool{false}

	t.Run("test null scalar", func(t *testing.T) {
		result, err := IsNull(vecs, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected, true)
		result, err = IsNull(vecs2, procs)
		if err != nil {
			t.Fatal(err)
		}
		checkIsNullResult(t, result, expected2, true)
	})
}

func TestIsNullScalarNull(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeScalarNull(types.T_any, 1)}
	expected := []bool{true}

	t.Run("test null scalar null", func(t *testing.T) {
		result, err := IsNull(vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkIsNullResult(t, result, expected, true)
	})
}

func checkIsNullResult(t *testing.T, result *vector.Vector, expected []bool, isScalar bool) {
	col := vector.MustFixedCol[bool](result)

	require.Equal(t, expected, col)
	require.Equal(t, isScalar, result.IsConst())
}
