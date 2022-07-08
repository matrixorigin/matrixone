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

func TestSin(t *testing.T) {
	sinIntAndFloat[int8](t, types.T_int8, -1, -0.8414709848078965)
	sinIntAndFloat[int8](t, types.T_int8, 1, 0.8414709848078965)
	sinIntAndFloat[int16](t, types.T_int16, 2, 0.9092974268256816)
	sinIntAndFloat[int32](t, types.T_int32, 3, 0.1411200080598672)
	sinIntAndFloat[int64](t, types.T_int64, 4, -0.7568024953079282)

	sinIntAndFloat[uint8](t, types.T_uint8, 5, -0.9589242746631385)
	sinIntAndFloat[uint16](t, types.T_uint16, 6, -0.27941549819892586)
	sinIntAndFloat[uint32](t, types.T_uint32, 7, math.Sin(7))
	sinIntAndFloat[uint64](t, types.T_uint64, 8, 0.9893582466233817)

	sinIntAndFloat[float32](t, types.T_float32, -0.5, -0.479425538604203)
	sinIntAndFloat[float32](t, types.T_float32, 0.5, 0.479425538604203)
	sinIntAndFloat[float32](t, types.T_float32, 1.5, 0.9974949866040544)

	sinIntAndFloat(t, types.T_float64, 2.5, 0.5984721441039564)
	sinIntAndFloat(t, types.T_float64, 3.5, -0.35078322768961984)
	sinIntAndFloat(t, types.T_float64, 4.5, -0.977530117665097)
	sinIntAndFloat(t, types.T_float64, 5.5, -0.7055403255703919)
}

func sinIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
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
			vecs:       makeSinVectors(src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Sin[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makeSinVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
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
