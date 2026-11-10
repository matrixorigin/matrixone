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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	groupop "github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

type dictionaryPipelineReader struct{ done, closed bool }

func (r *dictionaryPipelineReader) Read(_ context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.done {
		return true, nil
	}
	r.done = true
	for i, key := range []string{"A", "A", "B", "B"} {
		if err := vector.AppendBytes(out.Vecs[0], []byte(key), false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendFixed(out.Vecs[1], int32(i), false, mp); err != nil {
			return false, err
		}
		if err := vector.AppendBytes(out.Vecs[2], []byte("unprojected retained payload"), false, mp); err != nil {
			return false, err
		}
	}
	out.SetRowCount(4)
	return false, nil
}
func (r *dictionaryPipelineReader) Close() error                       { r.closed = true; return nil }
func (*dictionaryPipelineReader) SetFilterZM(objectio.ZoneMap)         {}
func (*dictionaryPipelineReader) SetOrderBy([]*plan.OrderBySpec)       {}
func (*dictionaryPipelineReader) GetOrderBy() []*plan.OrderBySpec      { return nil }
func (*dictionaryPipelineReader) SetIndexParam(*plan.IndexReaderParam) {}

func TestDictionaryPipelineAccountLifetime(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := budget.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(64<<20, budget)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	})
	keyType := types.New(types.T_char, 1, 0)
	valueType := types.T_int32.ToType()
	payloadType := types.New(types.T_varchar, 32, 0)
	column := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	predicate := func(bound int32) *plan.Expr {
		literal := &plan.Expr{Typ: valueType.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: bound}}}}
		expression, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*plan.Expr{column(1, valueType), literal})
		require.NoError(t, err)
		return expression
	}
	reader := &dictionaryPipelineReader{}
	scan := table_scan.NewArgument().WithTypes([]plan.Type{keyType.PlanType(), valueType.PlanType(), payloadType.PlanType()})
	scan.Reader, scan.Attrs, scan.FilterExprs = reader, []string{"k", "v", "payload"}, []*plan.Expr{predicate(0)}
	restrict := filter.NewArgument()
	restrict.FilterExprs = []*plan.Expr{predicate(1)}
	restrict.AppendChild(scan)
	project := projection.NewArgument()
	project.ProjectList = []*plan.Expr{column(0, keyType), column(1, valueType)}
	project.AppendChild(restrict)
	aggregate := groupop.NewArgument()
	aggregate.NeedEval = true
	aggregate.GroupBy = []*plan.Expr{column(0, keyType)}
	aggregate.Aggs = []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, []*plan.Expr{column(1, valueType)}, nil)}
	aggregate.AppendChild(project)
	ops := []vm.Operator{aggregate, project, restrict, scan}
	var configured []executionAllocationAccountOwner
	t.Cleanup(func() {
		for _, op := range ops {
			vm.Reset(op, proc, false, nil)
		}
		for _, op := range ops {
			vm.Free(op, proc, false, nil)
		}
		for i := len(configured) - 1; i >= 0; i-- {
			require.NoError(t, configured[i].ClearAllocationAccount(account))
		}
		require.True(t, reader.closed)
	})
	owners, err := collectAllocationAccountOwners([]*Scope{{RootOp: aggregate}})
	require.NoError(t, err)
	configured, err = configureAllocationAccountOwners(owners, account)
	require.NoError(t, err)
	require.NoError(t, vm.Prepare(aggregate, proc))
	require.True(t, scan.DictionaryOutputState().AllowsDictionaryOutput())
	require.True(t, restrict.DictionaryOutputState().AllowsDictionaryOutput())
	require.True(t, project.DictionaryOutputState().AllowsDictionaryOutput())
	rows := 0
	for {
		result, err := vm.Exec(aggregate, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		require.False(t, result.Batch.HasDictionary())
		for row := 0; row < result.Batch.RowCount(); row++ {
			require.Equal(t, "B", string(result.Batch.Vecs[0].GetBytesAt(row)))
			require.Equal(t, int64(5), vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[1], row))
			rows++
		}
	}
	require.Equal(t, 1, rows)
	require.Positive(t, scan.OpAnalyzer.GetOpStats().ExtraStats["DictionaryScanSelectedRows"])
	require.Positive(t, restrict.OpAnalyzer.GetOpStats().ExtraStats["DictionaryFilterSelectedRows"])
}
