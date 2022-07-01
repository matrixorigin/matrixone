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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either cotress or implied.
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

func myCot(x float64) float64 {
	// Yes, div 0 golang will return Inf
	return math.Cos(x) / math.Sin(x)
}

func TestCot(t *testing.T) {
	cotIntAndFloat[int8](t, types.T_int8, 1, myCot(1))
	cotIntAndFloat[int16](t, types.T_int16, 1, myCot(1))
	cotIntAndFloat[int32](t, types.T_int32, 1, myCot(1))
	cotIntAndFloat[int64](t, types.T_int64, 1, myCot(1))

	cotIntAndFloat[int64](t, types.T_int64, -2, myCot(-2))

	cotIntAndFloat[uint8](t, types.T_uint8, 1, myCot(1))
	cotIntAndFloat[uint16](t, types.T_uint16, 1, myCot(1))
	cotIntAndFloat[uint32](t, types.T_uint32, 1, myCot(1))
	cotIntAndFloat[uint64](t, types.T_uint64, 1, myCot(1))

	cotIntAndFloat[float32](t, types.T_float32, 7.5, myCot(7.5))
	cotIntAndFloat[float32](t, types.T_float32, -123425, myCot(-123425))

	cotIntAndFloat(t, types.T_float64, 0.1234, myCot(0.1234))
	cotIntAndFloat(t, types.T_float64, -1243, myCot(-1243))

	var small float64 = 0.000000000001
	cotIntAndFloat(t, types.T_float64, small, myCot(small))
	cotIntAndFloat(t, types.T_float64, -small, myCot(-small))
}

func cotIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
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
			vecs:       makecotVectors(src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Cot[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.InEpsilonSlice(t, c.wantBytes, plus.Col, 0.001)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makecotVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
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
