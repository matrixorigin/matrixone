// Copyright 2026 Matrix Origin
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

package table_scan

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// The reader's Select path returns the kept rows as a mask composed over the
// predicate chain (a later predicate sees only the survivors). It keeps the
// same rows as the Apply path, which shrinks the batch and returns a list,
// for partial, all-true and all-false predicates over NULLs.
func TestSelectReaderFilterMaskMatchesApply(t *testing.T) {
	intType := types.T_int32.ToType()
	chains := map[string][][2]any{
		"one-partial":        {{">", int32(300)}},
		"three-partial":      {{">", int32(100)}, {"<", int32(900)}, {">", int32(300)}},
		"all-true-in-middle": {{">", int32(100)}, {"<", int32(5000)}, {"<", int32(700)}},
		"all-true":           {{">", int32(-1)}},
		"none-after-partial": {{">", int32(500)}, {"<", int32(200)}},
		"const-true":         {{"const", true}, {">", int32(300)}},
		"const-false-later":  {{">", int32(300)}, {"const", false}},
		"const-null-first":   {{"null", nil}, {">", int32(300)}},
	}
	for name, chain := range chains {
		t.Run(name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(len(name))))
			const rows = 3000
			values := make([]int32, rows)
			nulls := make([]bool, rows)
			for i := range values {
				values[i] = int32(rng.Intn(1000))
				nulls[i] = rng.Intn(11) == 0
			}
			run := func(useSelect bool) ([]int64, bool) {
				proc := testutil.NewProc(t)
				var exprs []*pbplan.Expr
				boolType := types.T_bool.ToType()
				for _, p := range chain {
					switch p[0] {
					case "const", "null":
						v, _ := p[1].(bool)
						exprs = append(exprs, &pbplan.Expr{
							Typ:  plan.MakePlan2Type(&boolType),
							Expr: &pbplan.Expr_Lit{Lit: &pbplan.Literal{Isnull: p[0] == "null", Value: &pbplan.Literal_Bval{Bval: v}}},
						})
					default:
						exprs = append(exprs, lateTestCompare(t, p[0].(string), 0, p[1].(int32)))
					}
				}
				scan := &TableScan{
					Attrs:       []string{"a"},
					Types:       []pbplan.Type{plan.MakePlan2Type(&intType)},
					FilterExprs: exprs,
				}
				require.NoError(t, scan.Prepare(proc))
				bat := batch.NewWithSize(1)
				bat.Vecs[0] = vector.NewVec(intType, scan.mp(proc))
				for i, v := range values {
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], v, nulls[i], scan.mp(proc)))
				}
				bat.SetRowCount(rows)
				var kept []int64
				var all bool
				if useSelect {
					res, err := scan.selectReaderFilter(proc, bat, []int{0})
					require.NoError(t, err)
					require.Equal(t, rows, bat.RowCount(), "Select does not shrink its input")
					all = res.All
					if !all {
						require.NotNil(t, res.Mask)
						require.Nil(t, res.Sels)
						require.Equal(t, int64(rows), res.Mask.Len())
						kept = res.RowList(nil)
					}
				} else {
					res, err := scan.applyReaderFilter(proc, bat, []int{0})
					require.NoError(t, err)
					all = res.All
					kept = res.RowList(nil)
				}
				bat.Clean(scan.mp(proc))
				scan.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.GetMPool().CurrNB())
				return kept, all
			}
			gotRows, gotAll := run(true)
			wantRows, wantAll := run(false)
			require.Equal(t, wantAll, gotAll)
			if len(wantRows) == 0 {
				require.Empty(t, gotRows)
			} else {
				require.Equal(t, wantRows, gotRows)
			}
		})
	}
}
