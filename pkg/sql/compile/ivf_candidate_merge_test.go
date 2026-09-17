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

package compile

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_function"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func ivfCandidateCompileFixture(t *testing.T, cnCount int, cap *plan.Expr) (*Compile, *plan.Node) {
	t.Helper()
	c := NewMockCompile(t)
	t.Cleanup(func() {
		c.proc.Free()
	})
	c.addr = "cn0:6001"
	c.execType = plan2.ExecTypeAP_MULTICN
	c.anal = &AnalyzeModule{isFirst: true}
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	for i := 0; i < cnCount; i++ {
		c.cnList = append(c.cnList, engine.Node{Id: fmt.Sprintf("cn%d", i), Addr: fmt.Sprintf("cn%d:6001", i), Mcpu: 1})
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		TableDef: &plan.TableDef{TableType: "func_table", TblFunc: &plan.TableFunction{Name: "ivf_search"}, Cols: []*plan.ColDef{
			{Name: "pkid", Typ: plan.Type{Id: int32(types.T_int64)}},
			{Name: "score", Typ: plan.Type{Id: int32(types.T_float64)}},
			{Name: "payload", Typ: plan.Type{Id: int32(types.T_varchar)}},
		}},
		Stats:            &plan.Stats{BlockNum: 1, Dop: 1},
		Limit:            cap,
		IndexReaderParam: &plan.IndexReaderParam{OrigFuncName: "l2_distance", Limit: plan2.DeepCopyExpr(cap)},
		// Projection is deliberately unrelated to the raw score position.
		ProjectList: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 2}}},
			{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		},
	}
	return c, node
}

func TestCompileIvfCandidateMergeBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cns    int
		capped bool
		k      uint64
	}{
		{"single_cn", 1, true, 3},
		{"uncapped_multi_cn", 2, false, 0},
		{"zero_cap", 2, true, 0},
		{"one_candidate", 2, true, 1},
		{"three_cn", 3, true, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cap *plan.Expr
			if tc.capped {
				cap = plan2.MakePlan2Uint64ConstExprWithType(tc.k)
			}
			c, node := ivfCandidateCompileFixture(t, tc.cns, cap)
			before := node.String()
			var scopes []*Scope
			var err error
			t.Cleanup(func() {
				for _, scope := range scopes {
					scope.FreeOperator(c)
				}
				ReleaseScopes(scopes)
				require.Zero(t, c.proc.Mp().CurrNB())
			})
			scopes, err = c.compileIvfSearchParallel(node)
			require.NoError(t, err)
			require.Equal(t, before, node.String(), "compilation must not mutate the shared plan")
			if tc.cns == 1 || !tc.capped {
				require.Len(t, scopes, tc.cns)
				for _, s := range scopes {
					require.IsType(t, &table_function.TableFunction{}, s.RootOp)
				}
				return
			}
			require.Len(t, scopes, 1, "partition candidates need one ordered global selection")
			merged, ok := scopes[0].RootOp.(*mergetop.MergeTop)
			require.True(t, ok, "candidate merge must compare distances before the ordinary cap")
			require.Len(t, merged.Fs, 1)
			require.Equal(t, int32(1), merged.Fs[0].Expr.GetCol().ColPos)
			require.Zero(t, merged.Fs[0].Flag&plan.OrderBySpec_DESC)
			require.Equal(t, tc.k, merged.Limit.GetLit().GetU64Val())
			require.Len(t, scopes[0].PreScopes, tc.cns)
			for i, s := range scopes[0].PreScopes {
				local, ok := s.RootOp.GetOperatorBase().GetChildren(0).(*top.Top)
				require.True(t, ok)
				require.Equal(t, tc.k, local.Limit.GetLit().GetU64Val())
				tf, ok := local.GetChildren(0).(*table_function.TableFunction)
				require.True(t, ok)
				require.Equal(t, int32(tc.cns), tf.IndexReaderParam.PartitionCnCnt)
				require.Equal(t, int32(i), tf.IndexReaderParam.PartitionCnIdx)
				require.Equal(t, tc.k, tf.IndexReaderParam.Limit.GetLit().GetU64Val())
			}
		})
	}
}

type ivfCandidateRow struct {
	id      int64
	score   float64
	payload string
	isNull  bool
}

func ivfCandidateBatch(t *testing.T, proc *process.Process, rows []ivfCandidateRow) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(3)
	b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	b.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	b.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	for _, row := range rows {
		require.NoError(t, vector.AppendFixed(b.Vecs[0], row.id, false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], row.score, false, proc.Mp()))
		require.NoError(t, vector.AppendBytes(b.Vecs[2], []byte(row.payload), row.isNull, proc.Mp()))
	}
	b.SetRowCount(len(rows))
	return b
}

// Execute the operators produced by the compiler, replacing only storage/network
// inputs with deterministic batches. This checks results rather than plan text.
func ivfRunCandidateOperator(t *testing.T, proc *process.Process, op vm.Operator, input []*batch.Batch) []ivfCandidateRow {
	t.Helper()
	oldChildren := append([]vm.Operator(nil), op.GetOperatorBase().Children...)
	source := colexec.NewMockOperator().WithBatchs(input)
	op.GetOperatorBase().SetChildren([]vm.Operator{source})
	defer func() {
		op.GetOperatorBase().SetChildren(oldChildren)
		source.Free(proc, true, nil)
	}()
	require.NoError(t, op.Prepare(proc))
	rows := make([]ivfCandidateRow, 0)
	for calls := 0; ; calls++ {
		require.Less(t, calls, 20, "candidate operator did not terminate")
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if result.Batch != nil {
			b := result.Batch
			for i := 0; i < b.RowCount(); i++ {
				isNull := b.Vecs[2].IsNull(uint64(i))
				payload := ""
				if !isNull {
					payload = b.Vecs[2].GetStringAt(i)
				}
				rows = append(rows, ivfCandidateRow{
					id:      vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], i),
					score:   vector.GetFixedAtNoTypeCheck[float64](b.Vecs[1], i),
					payload: payload, isNull: isNull,
				})
			}
		}
		if result.Status == vm.ExecStop {
			break
		}
	}
	op.Reset(proc, false, nil)
	return rows
}

func TestCompileIvfCandidateMergeResults(t *testing.T) {
	// IDs oppose distance ordering. Each partition can emit a later, better
	// batch, as happens when INCLUDE drains independently sorted search rounds.
	partitions := [][]ivfCandidateRow{
		{{11, 9, "far", false}, {81, 2, "", true}, {41, 6, "middle", false}, {91, 1, "best", false}},
		{{22, 8, "farther", false}, {62, 4, "four", false}, {72, 3, "three", false}},
		{{33, 7, "seven", false}, {53, 5, "five", false}},
	}
	for _, tc := range []struct {
		name  string
		parts [][]ivfCandidateRow
		k     uint64
	}{
		{"two_partitions_k1", partitions[:2], 1},
		{"three_partitions", partitions, 3},
		{"one_empty_partition", [][]ivfCandidateRow{nil, partitions[0]}, 3},
		{"all_empty", [][]ivfCandidateRow{nil, nil}, 3},
		{"fewer_than_k", [][]ivfCandidateRow{partitions[1][:1], partitions[2][:1]}, 3},
		{"exactly_k", [][]ivfCandidateRow{partitions[1][:2], partitions[2][:1]}, 3},
		{"zero", partitions[:2], 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, node := ivfCandidateCompileFixture(t, len(tc.parts), plan2.MakePlan2Uint64ConstExprWithType(tc.k))
			scopes, err := c.compileIvfSearchParallel(node)
			require.NoError(t, err)
			t.Cleanup(func() {
				for _, scope := range scopes {
					scope.FreeOperator(c)
				}
				ReleaseScopes(scopes)
				require.Zero(t, c.proc.Mp().CurrNB())
			})
			require.Len(t, scopes, 1)
			global, ok := scopes[0].RootOp.(*mergetop.MergeTop)
			require.True(t, ok)
			locals := make([]*top.Top, len(tc.parts))
			for i, s := range scopes[0].PreScopes {
				locals[i], ok = s.RootOp.GetOperatorBase().GetChildren(0).(*top.Top)
				require.True(t, ok)
			}
			expected := make([]ivfCandidateRow, 0)
			for _, p := range tc.parts {
				expected = append(expected, p...)
			}
			sort.Slice(expected, func(i, j int) bool { return expected[i].score < expected[j].score })
			if uint64(len(expected)) > tc.k {
				expected = expected[:tc.k]
			}
			// Reuse the same operators after Reset, reversing arrival order.
			for execution := 0; execution < 2; execution++ {
				localRows := make([][]ivfCandidateRow, len(locals))
				for i, local := range locals {
					input := []*batch.Batch{batch.EmptyBatch}
					for _, row := range tc.parts[i] {
						input = append(input, ivfCandidateBatch(t, c.proc, []ivfCandidateRow{row}))
					}
					localRows[i] = ivfRunCandidateOperator(t, c.proc, local, input)
				}
				input := make([]*batch.Batch, 0, len(locals))
				for i := range locals {
					idx := i
					if execution == 1 {
						idx = len(locals) - 1 - i
					}
					input = append(input, ivfCandidateBatch(t, c.proc, localRows[idx]))
				}
				actual := ivfRunCandidateOperator(t, c.proc, global, input)
				require.Equal(t, expected, actual, "candidate identities, distances and INCLUDE payload must survive global selection")
			}
		})
	}
}
