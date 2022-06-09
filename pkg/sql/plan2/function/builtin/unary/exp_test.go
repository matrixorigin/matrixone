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

package unary

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
	"testing"
)

func TestExp(t *testing.T) {
	expIntAndFloat[int8](t, types.T_int8, 23, 9744803446.248903)
	expIntAndFloat[int16](t, types.T_int16, 23, 9744803446.248903)
	expIntAndFloat[int32](t, types.T_int32, 23, 9744803446.248903)
	expIntAndFloat[int64](t, types.T_int64, 23, 9744803446.248903)

	expIntAndFloat[int64](t, types.T_int64, -12, 0.00000614421235332821)

	expIntAndFloat[uint8](t, types.T_uint8, 23, 9744803446.248903)
	expIntAndFloat[uint16](t, types.T_uint16, 23, 9744803446.248903)
	expIntAndFloat[uint32](t, types.T_uint32, 23, 9744803446.248903)
	expIntAndFloat[uint64](t, types.T_uint64, 23, 9744803446.248903)

	expIntAndFloat[float32](t, types.T_float32, 7.5, 1808.0424144560632)
	expIntAndFloat[float32](t, types.T_float32, -12342534564, 0)

	expIntAndFloat[float64](t, types.T_float64, 0.141241241241313, 1.1517024526037205)
	expIntAndFloat[float64](t, types.T_float64, -124314124124.12412341, 0)
}

// Unit test input for int and float type parameters of the plus operator
func expIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
	makeProcess := func() *process.Process {
		hm := host.New(1 << 40)
		gm := guest.New(1<<40, hm)
		return process.New(mheap.New(gm))
	}

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
			vecs:       makeExpVectors[T](src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Exp[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makeExpVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
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
