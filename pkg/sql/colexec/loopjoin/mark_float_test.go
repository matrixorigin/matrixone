// Copyright 2026 Matrix Origin
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

package loopjoin

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestMarkHashFloatEquality(t *testing.T) {
	negzero := math.Copysign(0, -1)
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		for _, tc := range []struct {
			name    string
			values  []float64
			hasNull bool
		}{
			{"empty", nil, false}, {"positive zero", []float64{0}, false},
			{"negative zero", []float64{negzero}, false}, {"nan", []float64{math.NaN()}, false},
			{"finite and infinite", []float64{negzero, math.NaN(), 1, math.Inf(1)}, false},
			{"null and nan", []float64{1, math.NaN(), math.Inf(1), 0}, true},
		} {
			for _, mode := range []string{"regular", "zero", "nan", "null"} {
				t.Run(typ.String()+"/"+tc.name+"/"+mode, func(t *testing.T) {
					proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
					join := &LoopJoin{JoinType: plan.Node_MARK, LeftTypes: []types.Type{typ.ToType()},
						ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)}}
					probe := batch.NewWithSize(1)
					defer func() {
						join.Free(proc, false, nil)
						probe.Clean(proc.Mp())
						proc.Free()
						require.Zero(t, proc.Mp().CurrNB())
					}()
					m, err := newMarkHash(proc, markEquality(t, typ, "="))
					require.NoError(t, err)
					require.NotNil(t, m)
					join.ctr.markHash = m
					build := batch.NewWithSize(1)
					build.Vecs[0] = vector.NewVec(typ.ToType())
					for i, v := range tc.values {
						if typ == types.T_float32 {
							err = vector.AppendFixed(build.Vecs[0], float32(v), tc.hasNull && i == len(tc.values)-1, proc.Mp())
						} else {
							err = vector.AppendFixed(build.Vecs[0], v, tc.hasNull && i == len(tc.values)-1, proc.Mp())
						}
						require.NoError(t, err)
					}
					build.SetRowCount(len(tc.values))
					join.ctr.mp = markTestMap(t, proc, []*batch.Batch{build})
					require.NoError(t, m.build(proc, join.ctr.mp))
					values := []float64{0, negzero, math.NaN(), math.Inf(1), math.Inf(-1), 1, 2, 0}
					if mode == "regular" {
						probe.Vecs[0] = vector.NewVec(typ.ToType())
						for _, v := range values {
							if typ == types.T_float32 {
								err = vector.AppendFixed(probe.Vecs[0], float32(v), false, proc.Mp())
							} else {
								err = vector.AppendFixed(probe.Vecs[0], v, false, proc.Mp())
							}
							require.NoError(t, err)
						}
						nulls.Add(probe.Vecs[0].GetNulls(), uint64(len(values)-1))
					} else {
						v := negzero
						if mode == "nan" {
							v = math.NaN()
						}
						for i := range values {
							values[i] = v
						}
						if mode == "null" {
							probe.Vecs[0] = vector.NewConstNull(typ.ToType(), len(values), proc.Mp())
						} else if typ == types.T_float32 {
							probe.Vecs[0], err = vector.NewConstFixed(typ.ToType(), float32(v), len(values), proc.Mp())
						} else {
							probe.Vecs[0], err = vector.NewConstFixed(typ.ToType(), v, len(values), proc.Mp())
						}
						require.NoError(t, err)
					}
					probe.SetRowCount(len(values))
					join.ctr.inBat = probe
					join.resetResultBat()
					result := vm.NewCallResult()
					require.NoError(t, join.ctr.probeMarkHash(join, proc, &result))
					got := vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[1])
					for i, p := range values {
						probeNull := mode == "null" || mode == "regular" && i == len(values)-1
						matched := false
						for j, b := range tc.values {
							// Independent oracle: equalFn compares floats with Go ==.
							if !probeNull && !(tc.hasNull && j == len(tc.values)-1) && p == b {
								matched = true
							}
						}
						unknown := !matched && len(tc.values) > 0 && (probeNull || tc.hasNull)
						require.Equal(t, matched, got[i], "row %d", i)
						require.Equal(t, unknown, result.Batch.Vecs[1].GetNulls().Contains(uint64(i)), "row %d", i)
					}
				})
			}
		}
	}
}
