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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestMod(t *testing.T) {
	modInteger[int8](t, types.T_int8, 28, -5, 3)
	modInteger[int16](t, types.T_int16, 28, -5, 3)
	modInteger[int32](t, types.T_int32, 28, -5, 3)
	modInteger[int64](t, types.T_int64, 28, -5, 3)

	modInteger[uint8](t, types.T_uint8, 28, 5, 3)
	modInteger[uint16](t, types.T_uint16, 28, 5, 3)
	modInteger[uint32](t, types.T_uint32, 28, 5, 3)
	modInteger[uint64](t, types.T_uint64, 28, 5, 3)

	modFloater(t, types.T_float32, 24.45, 12.4, float32(math.Mod(float64(float32(24.45)), float64(float32(12.4)))))
	modFloater(t, types.T_float64, 24.45, 12.4, math.Mod(24.45, 12.4))
}

// Integer unit test entry for mod operator
func modInteger[T constraints.Integer](t *testing.T, typ types.T, left T, right T, res T) {
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
			vecs:       makeModVectors(left, true, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeModVectors(left, false, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeModVectors(left, true, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeModVectors(left, false, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := ModInt[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Float unit test entry for mod operator
func modFloater[T constraints.Float](t *testing.T, typ types.T, left T, right T, res T) {
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
			vecs:       makeModVectors(left, true, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: true,
		},
		{
			name:       "TEST02",
			vecs:       makeModVectors(left, false, right, true, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST03",
			vecs:       makeModVectors(left, true, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
		{
			name:       "TEST04",
			vecs:       makeModVectors(left, false, right, false, typ),
			proc:       procs,
			wantBytes:  []T{res},
			wantScalar: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := ModFloat[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct vector parameters of mod operator
func makeModVectors[T constraints.Integer | constraints.Float](left T, leftScalar bool, right T, rightScalar bool, t types.T) []*vector.Vector {
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
