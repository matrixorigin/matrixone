// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either acosress or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/testutil"
)

func TestIsNotNullNormal(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 14, 42, 7656, 4324234, 543534523}, nil)}

	expected := []bool{true, true, true, true, true, true}

	t.Run("isnotnull test - normal", func(t *testing.T) {
		result, err := IsNotNull[int32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkIsNullResult(t, result, expected, false)
	})
}

func TestIsNotNullNormalWithNull(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt32Vector([]int32{1, 14, 42, 7656, 4324234, 543534523}, []uint64{0, 1, 4})}

	expected := []bool{false, false, true, true, false, true}

	t.Run("isnotnull test - normal with null", func(t *testing.T) {
		result, err := IsNotNull[int32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkIsNullResult(t, result, expected, false)
	})
}

func TestIsNotNullScalar(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeInt32Vector([]int32{543534523}, nil)}
	vecs[0].IsConst = true

	expected := []bool{true}

	t.Run("isnotnull test - not null scalar", func(t *testing.T) {
		result, err := IsNotNull[int32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkIsNullResult(t, result, expected, true)
	})
}

func TestIsNotNullScalarNull(t *testing.T) {
	procs := testutil.NewProc()
	vecs := []*vector.Vector{testutil.MakeScalarNull(0)}
	expected := []bool{false}

	t.Run("isnotnull test - scalar null", func(t *testing.T) {
		result, err := IsNotNull[int32](vecs, procs)
		if err != nil {
			t.Fatal(err)
		}

		checkIsNullResult(t, result, expected, true)
	})
}
