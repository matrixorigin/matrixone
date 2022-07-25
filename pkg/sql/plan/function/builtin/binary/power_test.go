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

package binary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestPower(t *testing.T) {
	powerFloat64[float64](t, types.T_float64, 1, 2, 1)
	powerFloat64[float64](t, types.T_float64, 2, 2, 4)
	powerFloat64[float64](t, types.T_float64, 3, 2, 9)
	powerFloat64[float64](t, types.T_float64, 3, 3, 27)
	powerFloat64[float64](t, types.T_float64, 4, 2, 16)
	powerFloat64[float64](t, types.T_float64, 4, 3, 64)
	powerFloat64[float64](t, types.T_float64, 4, 0.5, 2)
	powerFloat64[float64](t, types.T_float64, 5, 2, 25)
	powerFloat64[float64](t, types.T_float64, 6, 2, 36)
	powerFloat64[float64](t, types.T_float64, 7, 2, 49)
	powerFloat64[float64](t, types.T_float64, 8, 2, 64)
	powerFloat64[float64](t, types.T_float64, 0.5, 2, 0.25)
	powerFloat64[float64](t, types.T_float64, 1.5, 2, 2.25)
	powerFloat64[float64](t, types.T_float64, 2.5, 2, 6.25)
	powerFloat64[float64](t, types.T_float64, 3.5, 2, 12.25)
	powerFloat64[float64](t, types.T_float64, 4.5, 2, 20.25)
	powerFloat64[float64](t, types.T_float64, 5.5, 2, 30.25)
}

func powerFloat64[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, src2 T, res float64) {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	procs := process.New(mheap.New(gm))
	cases := []struct {
		name       string
		vecs       []*vector.Vector
		proc       *process.Process
		wantBytes  interface{}
		wantScalar bool
	}{
		{
			name:       "TEST01",
			vecs:       makePowerVectors(src, src2, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Power(c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makePowerVectors[T constraints.Integer | constraints.Float](src T, src2 T, srcScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 2)
	vectors[0] = &vector.Vector{
		Col:     []T{src},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: srcScalar,
		Length:  1,
	}
	vectors[1] = &vector.Vector{
		Col:     []T{src2},
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
