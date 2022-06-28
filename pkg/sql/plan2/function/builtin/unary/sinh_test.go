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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either sinhress or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package unary

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

//I get the testcase results from PG12.3
func TestSinh(t *testing.T) {
	SinhIntAndFloat[int8](t, types.T_int8, 0, 0)
	SinhIntAndFloat[int16](t, types.T_int16, -2, -3.626860407847019)
	SinhIntAndFloat[int32](t, types.T_int32, 2, 3.626860407847019)
	SinhIntAndFloat[int64](t, types.T_int64, 12, 81377.39570642984)

	SinhIntAndFloat[uint8](t, types.T_uint8, 2, 3.626860407847019)
	SinhIntAndFloat[uint16](t, types.T_uint16, 12, 81377.39570642984)
	SinhIntAndFloat[uint32](t, types.T_uint32, 2, 3.626860407847019)
	SinhIntAndFloat[uint64](t, types.T_uint64, 12, 81377.39570642984)

	SinhIntAndFloat[float32](t, types.T_float32, 1.0001, math.Sinh(float64(float32(1.0001))))

	SinhIntAndFloat(t, types.T_float64, 0.2, 0.201336002541094)
}

func SinhIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
	procs := makeProcess()
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makesinhVectors(src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Sinh[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makesinhVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 1)
	vectors[0] = &vector.Vector{
		Col:     []T{src},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: srcScalar,
		Length:  1,
	}
	return vectors
}

// NULL return not a value
// func MakeScalarNullSlice(length int) []*vector.Vector {
// 	vectors := make([]*vector.Vector, 1)
// 	vectors[0] = testutil.MakeScalarNull(4)
// 	return vectors
// }
