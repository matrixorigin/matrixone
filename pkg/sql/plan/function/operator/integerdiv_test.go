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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestIntegerDiv(t *testing.T) {
	integerDivFloat[float32](t, types.T_float32, 235, 7.5, 31)
	integerDivFloat(t, types.T_float64, 21.45, 40.55, 0)
}

// Unit test input of float type parameter of integerdiv operator
func integerDivFloat[T constraints.Float](t *testing.T, typ types.T, left T, right T, res int64) {
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
			vecs:       makeIntegerDivVectors(left, true, right, true, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeIntegerDivVectors(left, false, right, true, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeIntegerDivVectors(left, true, right, false, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeIntegerDivVectors(left, false, right, false, typ),
			proc:       procs,
			wantBytes:  []int64{res},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := IntegerDiv[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameters of the integerdiv operator
func makeIntegerDivVectors[T constraints.Float](left T, leftScalar bool, right T, rightScalar bool, t types.T) []*vector.Vector {
	vectors := make([]*vector.Vector, 2)
	vectors[0] = &vector.Vector{
		Col:     []T{left},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: leftScalar,
		Length:  1,
	}
	vectors[1] = &vector.Vector{
		Col:     []T{right},
		Nsp:     &nulls.Nulls{},
		Typ:     types.Type{Oid: t},
		IsConst: rightScalar,
		Length:  1,
	}
	return vectors
}
