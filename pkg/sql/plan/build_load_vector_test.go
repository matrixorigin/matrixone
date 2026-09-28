// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestMakeCastExprKeepsDirectParallelLoadVector(t *testing.T) {
	vectorType := plan.Type{Id: int32(types.T_array_float32), Width: 3}
	original := &plan.Expr{
		Typ:  vectorType,
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "v"}},
	}
	node := &plan.Node{ProjectList: []*plan.Expr{original}}
	tableDef := &plan.TableDef{Cols: []*plan.ColDef{{Name: "v", Typ: vectorType}}}

	result := makeCastExpr(&tree.Load{}, "vectors.csv", tableDef, node, map[string]int32{"v": 0})
	require.Len(t, result, 1)
	require.Same(t, original, result[0])
}

func TestParallelLoadBlobTextAssignmentUsesOriginalPayload(t *testing.T) {
	for _, oid := range []types.T{types.T_text, types.T_blob} {
		t.Run(oid.String(), func(t *testing.T) {
			builder := NewQueryBuilder(plan.Query_INSERT, NewMockCompilerContext(true), false, false)
			proc := builder.compCtx.GetProcess()
			sink := &loadAssignmentWarningSink{}
			proc.WarningSink = sink
			typ := plan.Type{Id: int32(oid), Width: types.MaxTinyTextLen}
			table := &plan.TableDef{Cols: []*plan.ColDef{{Name: "v", Typ: typ}}}
			node := &plan.Node{ProjectList: []*plan.Expr{{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}}}
			load := &tree.Load{Param: &tree.ExternParam{}}
			staged := makeCastExpr(load, "rows.csv", table, node, map[string]int32{"v": 0})[0]
			expr, err := builder.forceAssignmentCastExpr(staged, typ, false)
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			defer input.Clean(proc.Mp())
			require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(strings.Repeat("x", 256)), false, proc.Mp()))
			input.SetRowCount(1)
			_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
			require.Error(t, err, "strict assignment must see the original 256 bytes")
			require.Contains(t, err.Error(), "Src length 256")
			ignored, err := builder.forceAssignmentCastExpr(staged, typ, true)
			require.NoError(t, err)
			ignoreExecutor, err := colexec.NewExpressionExecutor(proc, ignored)
			require.NoError(t, err)
			defer ignoreExecutor.Free()
			result, err := ignoreExecutor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Len(t, result.GetBytesAt(0), 255)
			require.Equal(t, []uint16{moerr.WARN_DATA_TRUNCATED}, sink.codes)
		})
	}
}

type loadAssignmentWarningSink struct{ codes []uint16 }

func (s *loadAssignmentWarningSink) AppendWarningDiagnostic(code uint16, _ string) {
	s.codes = append(s.codes, code)
}
