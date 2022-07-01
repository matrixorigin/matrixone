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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func TestLn(t *testing.T) {
	lnIntAndFloat[int8](t, types.T_int8, 1, 0)
	lnIntAndFloat[int16](t, types.T_int16, 2, 0.6931471805599453)
	lnIntAndFloat[int32](t, types.T_int32, 3, 1.0986122886681096)
	lnIntAndFloat[int64](t, types.T_int64, 4, 1.3862943611198906)

	lnIntAndFloat[uint8](t, types.T_uint8, 5, 1.6094379124341003)
	lnIntAndFloat[uint16](t, types.T_uint16, 6, 1.791759469228055)
	lnIntAndFloat[uint32](t, types.T_uint32, 7, 1.9459101490553132)
	lnIntAndFloat[uint64](t, types.T_uint64, 8, 2.0794415416798357)

	lnIntAndFloat[float32](t, types.T_float32, 0.5, -0.6931471805599453)
	lnIntAndFloat[float32](t, types.T_float32, 1.5, 0.4054651081081644)

	lnIntAndFloat(t, types.T_float64, 2.5, 0.9162907318741551)
	lnIntAndFloat(t, types.T_float64, 3.5, 1.252762968495368)
	lnIntAndFloat(t, types.T_float64, 4.5, 1.5040773967762742)
	lnIntAndFloat(t, types.T_float64, 5.5, 1.7047480922384253)
}

func lnIntAndFloat[T constraints.Integer | constraints.Float](t *testing.T, typ types.T, src T, res float64) {
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
			vecs:       makelnVectors(src, true, typ),
			proc:       procs,
			wantBytes:  []float64{res},
			wantScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plus, err := Ln[T](c.vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, c.wantBytes, plus.Col)
			require.Equal(t, c.wantScalar, plus.IsScalar())
		})
	}
}

// Construct the vector parameter of the plus operator
func makelnVectors[T constraints.Integer | constraints.Float](src T, srcScalar bool, t types.T) []*vector.Vector {
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
