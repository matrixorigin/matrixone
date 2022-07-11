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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either cosress or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func makeProcess() *process.Process {
	hm := host.New(1 << 40)
	gm := guest.New(1<<40, hm)
	return process.New(mheap.New(gm))
}

func TestCos(t *testing.T) {
	cosIntAndFloat[int8](t, types.T_int8, 1, 0.5403023058681398)
	cosIntAndFloat[int16](t, types.T_int16, 1, 0.5403023058681398)
	cosIntAndFloat[int32](t, types.T_int32, 1, 0.5403023058681398)
	cosIntAndFloat[int64](t, types.T_int64, 1, 0.5403023058681398)

	cosIntAndFloat[int64](t, types.T_int64, -2, -0.4161468365471424)

	cosIntAndFloat[uint8](t, types.T_uint8, 1, 0.5403023058681398)
	cosIntAndFloat[uint16](t, types.T_uint16, 1, 0.5403023058681398)
	cosIntAndFloat[uint32](t, types.T_uint32, 1, 0.5403023058681398)
	cosIntAndFloat[uint64](t, types.T_uint64, 1, 0.5403023058681398)

	cosIntAndFloat[float32](t, types.T_float32, 7.5, 0.3466353178350258)
	cosIntAndFloat[float32](t, types.T_float32, -123425, -0.31587422160647155)

	cosIntAndFloat(t, types.T_float64, 0.141241241241313, 0.9900420267854725)
	cosIntAndFloat(t, types.T_float64, -124314124124.12412341, -0.9992386393564632)
}

func cosIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
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
			vecs:       makecosVectors(src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Cos[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makecosVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
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
