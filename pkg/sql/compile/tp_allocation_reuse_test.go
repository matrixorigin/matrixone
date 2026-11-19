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
	"errors"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	groupop "github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTPReusableOwnerTreeKeepsBlockingOwners(t *testing.T) {
	scan := table_scan.NewArgument()
	restrict := filter.NewArgument()
	project := projection.NewArgument()
	aggregate := groupop.NewArgument()
	sortOp := order.NewArgument()
	topOp := top.NewArgument()
	build := hashbuild.NewArgument()
	t.Cleanup(func() {
		scan.Release()
		restrict.Release()
		project.Release()
		aggregate.Release()
		sortOp.Release()
		topOp.Release()
		build.Release()
	})
	restrict.AppendChild(scan)
	project.AppendChild(restrict)
	aggregate.AppendChild(project)
	sortOp.AppendChild(aggregate)
	topOp.AppendChild(project)
	build.AppendChild(scan)
	// Exercise nested scopes and shared nodes as well as the flat input chain.
	scopes := []*Scope{{RootOp: sortOp, PreScopes: []*Scope{{RootOp: build}, {RootOp: topOp}}}}
	owners, err := collectExecutionAllocationAccountOwners(scopes, true)
	require.NoError(t, err)
	require.ElementsMatch(t, []executionAllocationAccountOwner{aggregate, sortOp, build, topOp}, owners)
	owners, err = collectExecutionAllocationAccountOwners(scopes, false)
	require.NoError(t, err)
	require.ElementsMatch(t, []executionAllocationAccountOwner{scan, restrict, project, aggregate, sortOp, build, topOp}, owners)
}

func TestTPReusableOperatorsAllocationScope(t *testing.T) {
	for _, execType := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN, plan2.ExecTypeAP_MULTICN} {
		t.Run(fmt.Sprint(execType), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(proc.Free)
			col := &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			scan := table_scan.NewArgument()
			scan.FilterExprs = []*plan.Expr{col}
			restrict := filter.NewArgument()
			restrict.FilterExprs = []*plan.Expr{col}
			restrict.AppendChild(scan)
			project := projection.NewArgument()
			project.ProjectList = []*plan.Expr{col}
			project.AppendChild(restrict)
			c := &Compile{proc: proc, execType: execType, isPrepare: true, MessageBoard: message.NewMessageBoard(), scopes: []*Scope{{RootOp: project}}}
			t.Cleanup(func() {
				for _, op := range []vm.Operator{project, restrict, scan} {
					vm.Free(op, proc, false, nil)
				}
				require.NoError(t, c.finishAllocationAccountAttempt())
				project.Release()
				restrict.Release()
				scan.Release()
			})
			export := func(s mpool.AllocationAccountTerminalSnapshot) { require.Zero(t, s.Used) }
			c.isPrepare = false
			require.NoError(t, c.ensureAllocationAccountLifecycle(export))
			require.Len(t, c.allocationAccountOwners, 3, "ordinary TP and AP keep accounting")
			c.isPrepare = true
			require.NoError(t, c.ensureAllocationAccountLifecycle(export))
			if execType == plan2.ExecTypeTP {
				require.Nil(t, c.allocationAccountRegistry)
				require.NoError(t, c.attachRuntimeAllocationOwners(c.scopes))
			} else {
				require.Len(t, c.allocationAccountOwners, 3)
				attempt, err := c.beginAllocationAccountAttempt()
				require.NoError(t, err)
				require.NotNil(t, attempt)
				require.NotNil(t, scan.OutputAllocation)
				require.NoError(t, c.finishAllocationAccountAttempt())
			}

			// An aggregate must retain its budget even when its TP input is flat.
			aggregate := groupop.NewArgument()
			t.Cleanup(func() {
				vm.Free(aggregate, proc, false, nil)
				require.NoError(t, c.finishAllocationAccountAttempt())
				aggregate.Release()
			})
			aggregate.AppendChild(project)
			c.scopes[0].RootOp = aggregate
			c.MessageBoard = message.NewMessageBoard()
			require.NoError(t, c.ensureAllocationAccountLifecycle(export))
			wantOwners := 4
			if execType == plan2.ExecTypeTP {
				wantOwners = 1
			}
			require.Len(t, c.allocationAccountOwners, wantOwners)
			attempt, err := c.beginAllocationAccountAttempt()
			require.NoError(t, err)
			require.NotNil(t, attempt)
			clone := table_scan.NewArgument()
			clone.FilterExprs = []*plan.Expr{col}
			require.NoError(t, c.attachRuntimeAllocationOwners([]*Scope{{RootOp: clone}}))
			if execType == plan2.ExecTypeTP {
				require.Nil(t, scan.OutputAllocation)
				require.Nil(t, clone.OutputAllocation)
			} else {
				require.True(t, clone.OutputAllocation.UsesAccount(attempt.account))
			}
			require.NoError(t, c.finishAllocationAccountAttempt())
			clone.Release()
		})
	}
}

type tpReuseReader struct {
	dictionaryPipelineReader
	err error
}

func (r *tpReuseReader) Read(ctx context.Context, attrs []string, expr *plan.Expr, mp *mpool.MPool, out *batch.Batch) (bool, error) {
	if r.err != nil {
		return false, r.err
	}
	return r.dictionaryPipelineReader.Read(ctx, attrs, expr, mp, out)
}

type tpReusePipeline struct {
	c      *Compile
	scan   *table_scan.TableScan
	root   vm.Operator
	ops    []vm.Operator
	params *vector.Vector
	proc   *process.Process
}

func newTPReusePipeline(t testing.TB, execType plan2.ExecType, aggregate bool) *tpReusePipeline {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.SetBaseProcessRunningStatus(true)
	keyType, valueType, payloadType := types.T_varchar.ToType(), types.T_int32.ToType(), types.T_varchar.ToType()
	col := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	lit := func(n int32) *plan.Expr {
		return &plan.Expr{Typ: valueType.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: n}}}}
	}
	scan := table_scan.NewArgument().WithTypes([]plan.Type{keyType.PlanType(), valueType.PlanType(), payloadType.PlanType()})
	scan.Attrs = []string{"k", "v", "payload"}
	scan.FilterExprs = []*plan.Expr{bind(">", col(1, valueType), lit(0))}
	restrict := filter.NewArgument()
	restrict.FilterExprs = []*plan.Expr{bind(">", col(0, keyType), &plan.Expr{Typ: keyType.PlanType(), Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}})}
	restrict.AppendChild(scan)
	project := projection.NewArgument()
	project.ProjectList = []*plan.Expr{col(0, keyType), bind("+", col(1, valueType), lit(1))}
	project.AppendChild(restrict)
	f := &tpReusePipeline{scan: scan, root: project, ops: []vm.Operator{project, restrict, scan}, params: vector.NewVec(keyType), proc: proc}
	var group *groupop.Group
	if aggregate {
		group = groupop.NewArgument()
		group.NeedEval = true
		group.Aggs = []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountColumn, false, []*plan.Expr{col(0, keyType)}, nil)}
		group.AppendChild(project)
		f.root = group
		f.ops = append([]vm.Operator{group}, f.ops...)
	}
	f.c = &Compile{proc: proc, execType: execType, isPrepare: true, scopes: []*Scope{{RootOp: f.root}}}
	proc.SetPrepareParams(f.params)
	t.Cleanup(func() {
		for _, op := range f.ops {
			vm.Free(op, proc, false, nil)
		}
		require.NoError(t, f.c.finishAllocationAccountAttempt())
		proc.DetachPrepareParams()
		f.params.Free(proc.Mp())
		require.Zero(t, proc.Mp().CurrNB())
		if group != nil {
			group.Release()
		}
		project.Release()
		restrict.Release()
		scan.Release()
	})
	return f
}

func (f *tpReusePipeline) execute(param string, null bool, readErr error, consume func(*batch.Batch)) (err error) {
	f.params.CleanOnlyData()
	if err = vector.AppendBytes(f.params, []byte(param), null, f.proc.Mp()); err != nil {
		return err
	}
	f.c.MessageBoard = message.NewMessageBoard()
	if err = f.c.ensureAllocationAccountLifecycle(func(mpool.AllocationAccountTerminalSnapshot) {}); err != nil {
		return err
	}
	if _, err = f.c.beginAllocationAccountAttempt(); err != nil {
		return err
	}
	reader := &tpReuseReader{err: readErr}
	f.scan.Reader = reader
	defer func() {
		for _, op := range f.ops {
			vm.Reset(op, f.proc, err != nil, err)
		}
		err = errors.Join(err, f.c.finishAllocationAccountAttempt())
		if !reader.closed {
			err = errors.Join(err, errors.New("reader not closed"))
		}
	}()
	if err = vm.Prepare(f.root, f.proc); err != nil {
		return err
	}
	for {
		result, callErr := vm.Exec(f.root, f.proc)
		if callErr != nil {
			return callErr
		}
		if result.Batch == nil {
			return nil
		}
		consume(result.Batch)
	}
}

func TestTPReusablePipelineQueries(t *testing.T) {
	for _, execType := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN} {
		for _, aggregate := range []bool{false, true} {
			t.Run(fmt.Sprintf("exec=%d/count=%t", execType, aggregate), func(t *testing.T) {
				f := newTPReusePipeline(t, execType, aggregate)
				cases := []struct {
					param string
					null  bool
					err   error
					want  []string
				}{
					{param: "A", want: []string{"B:3", "B:4"}},
					{null: true},
					{param: "B"},
					{param: "A", err: context.Canceled},
					{param: "A", err: errors.New("injected reader error")},
					{param: "", want: []string{"A:2", "B:3", "B:4"}},
				}
				var warmBytes int64
				for cycle := range 12 {
					for _, tc := range cases {
						var got []string
						var count int64
						err := f.execute(tc.param, tc.null, tc.err, func(b *batch.Batch) {
							require.False(t, b.HasDictionary(), "root must publish flat results")
							if aggregate {
								count += vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0)
								return
							}
							for row := range b.RowCount() {
								got = append(got, fmt.Sprintf("%s:%d", b.Vecs[0].GetStringAt(row), vector.GetFixedAtNoTypeCheck[int64](b.Vecs[1], row)))
							}
						})
						if tc.err != nil {
							require.ErrorIs(t, err, tc.err)
							continue
						}
						require.NoError(t, err)
						if aggregate {
							require.Equal(t, int64(len(tc.want)), count)
						} else {
							require.Equal(t, tc.want, got)
						}
					}
					if cycle == 0 {
						warmBytes = f.proc.Mp().CurrNB()
					} else {
						require.Equal(t, warmBytes, f.proc.Mp().CurrNB(), "retained capacity grew in cycle %d", cycle)
					}
				}
			})
		}
	}
}

// TestTPReusePowerProjectionDomainErrorRecovery binds a real Power projection
// consumer (power(v-2, 0.5)) onto the reused prepared-TP pipeline: the same
// compiled f.c/f.root must serve repeated executions, surface the typed
// out-of-range rejection for domain rows WITHOUT publishing the failed batch
// to the consumer, and recover valid results on the next execution.
func TestTPReusePowerProjectionDomainErrorRecovery(t *testing.T) {
	f := newTPReusePipeline(t, plan2.ExecTypeTP, false)
	keyType := types.T_varchar.ToType()
	valueType := types.T_int32.ToType()
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(f.proc.Ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	col := func(pos int32, typ types.Type) *plan.Expr {
		return &plan.Expr{Typ: typ.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	project := f.root.(*projection.Projection)
	// One-shot real consumer binding: keep the key column and project
	// power(v-2, 0.5). The reader rows are k=A/B/B with v=1/2/3, so the bases
	// are -1/0/1 and only the v=1 row is inside the rejected real domain.
	two := &plan.Expr{Typ: valueType.PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 2}}}}
	half := &plan.Expr{Typ: types.T_float64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 0.5}}}}
	project.ProjectList = []*plan.Expr{col(0, keyType), bind("power", bind("-", col(1, valueType), two), half)}

	runSelected := func() ([]string, error) {
		var got []string
		err := f.execute("A", false, nil, func(b *batch.Batch) {
			require.False(t, b.HasDictionary(), "root must publish flat results")
			for row := range b.RowCount() {
				got = append(got, fmt.Sprintf("%s:%v", b.Vecs[0].GetStringAt(row), vector.GetFixedAtNoTypeCheck[float64](b.Vecs[1], row)))
			}
		})
		return got, err
	}
	var warmBytes int64
	for cycle := range 2 {
		got, err := runSelected()
		require.NoError(t, err)
		require.Equal(t, []string{"B:0", "B:1"}, got)
		var failedBatchRows []string
		err = f.execute("", false, nil, func(b *batch.Batch) {
			for row := range b.RowCount() {
				failedBatchRows = append(failedBatchRows, b.Vecs[0].GetStringAt(row))
			}
		})
		var moErr *moerr.Error
		require.True(t, errors.As(err, &moErr) && moerr.IsMoErrCode(moErr, moerr.ErrOutOfRange),
			"empty-param selection must surface the typed domain rejection, got %v", err)
		require.Empty(t, failedBatchRows, "the failed batch must not be published to the consumer")
		got, err = runSelected()
		require.NoError(t, err)
		require.Equal(t, []string{"B:0", "B:1"}, got)
		if cycle == 0 {
			warmBytes = f.proc.Mp().CurrNB()
		} else {
			require.Equal(t, warmBytes, f.proc.Mp().CurrNB(),
				"prepared TP executor reuse must not retain capacity")
		}
	}
}

func BenchmarkTPReusablePipelineQueries(b *testing.B) {
	for _, execType := range []plan2.ExecType{plan2.ExecTypeTP, plan2.ExecTypeAP_ONECN} {
		b.Run(fmt.Sprint(execType), func(b *testing.B) {
			f := newTPReusePipeline(b, execType, true)
			consume := func(out *batch.Batch) {
				if out.RowCount() != 1 || vector.GetFixedAtNoTypeCheck[int64](out.Vecs[0], 0) != 2 {
					b.Fatal("wrong count")
				}
			}
			require.NoError(b, f.execute("A", false, nil, consume))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := f.execute("A", false, nil, consume); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

func TestTPReusablePipelinePreservesAggregateAdmission(t *testing.T) {
	f := newTPReusePipeline(t, plan2.ExecTypeTP, true)
	require.NoError(t, vector.AppendBytes(f.params, []byte("A"), false, f.proc.Mp()))
	f.c.MessageBoard = message.NewMessageBoard()
	require.NoError(t, f.c.ensureAllocationAccountLifecycle(func(mpool.AllocationAccountTerminalSnapshot) {}))
	f.c.allocationAccountLimit = 1
	attempt, err := f.c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotNil(t, attempt)
	f.scan.Reader = &tpReuseReader{}
	err = vm.Prepare(f.root, f.proc)
	if err == nil {
		_, err = vm.Exec(f.root, f.proc)
	}
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	for _, op := range f.ops {
		vm.Reset(op, f.proc, true, err)
	}
	require.NoError(t, f.c.finishAllocationAccountAttempt())
	require.Zero(t, attempt.account.Snapshot().Used)
	rows := 0
	require.NoError(t, f.execute("A", false, nil, func(b *batch.Batch) {
		rows += b.RowCount()
		require.Equal(t, int64(2), vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0))
	}))
	require.Equal(t, 1, rows)
}
