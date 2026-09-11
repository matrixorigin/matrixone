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

package compile

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestInternalSQLWarningAttemptOutcomes(t *testing.T) {
	for _, outcome := range []string{"success", "parent failure", "child failure", "retry"} {
		t.Run(outcome, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			session := &remoteWarningSession{}
			proc.Session = session
			ctx := attachInternalExecutorSession(defines.AttachAccountId(context.Background(), catalog.System_Account), session)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				switch name {
				case "group_concat_max_len":
					return int64(5), nil
				case "lower_case_table_names":
					return int64(1), nil
				case plan2.SQLSelectLimitVariable:
					return ^uint64(0), nil
				default:
					return "STRICT_TRANS_TABLES", nil
				}
			})
			ctrl := gomock.NewController(t)
			proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
			buf := buffer.New()
			t.Cleanup(buf.Free)
			internal := &sqlExecutor{addr: proc.GetService(), eng: newStubEngine(), mp: proc.Mp(), txnClient: proc.Base.TxnClient, buf: buf}
			rt := moruntime.ServiceRuntime(proc.GetService())
			previous, existed := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, internal)
			t.Cleanup(func() {
				if existed {
					rt.SetGlobalVariables(moruntime.InternalSQLExecutor, previous)
				} else {
					rt.CompareAndDeleteGlobalVariables(moruntime.InternalSQLExecutor, internal)
				}
			})
			parent := newWarningAttempt(proc)
			defer func() { parent.finish(false, session) }()
			c := &Compile{proc: proc}
			sql := "select group_concat(s order by s separator '') from (select 'abc' s union all select 'def') t"
			if outcome == "child failure" {
				parent.collector.AppendWarningDiagnostic(1260, "parent")
				sql = "select cast('not-an-integer' as bigint)"
			}
			res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
			if outcome == "child failure" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, res.Batches, 1)
				require.Equal(t, "abcde", string(res.Batches[0].Vecs[0].GetBytesAt(0)))
				res.Close()
			}
			require.Zero(t, session.totalWarnings, "internal SQL must not publish directly to Session")
			if outcome == "retry" {
				parent.finish(false, session)
				parent = newWarningAttempt(proc)
				res, err = c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
				require.NoError(t, err)
				res.Close()
			}
			parent.finish(outcome != "parent failure", session)
			want := uint64(1)
			if outcome == "parent failure" {
				want = 0
			}
			require.Equal(t, want, session.totalWarnings)
			require.Same(t, session, proc.Session)
			require.Nil(t, proc.WarningSink)
		})
	}
}

func TestGroupConcatWarningAttemptOutcomes(t *testing.T) {
	for _, outcome := range []string{"success", "retry", "failure", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
			proc := testutil.NewProcess(t)
			session := &remoteWarningSession{}
			session.AppendWarningDiagnostic(1, "before execution")
			proc.Session = session
			proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "group_concat_max_len" {
					return int64(5), nil
				}
				if name == plan2.SQLSelectLimitVariable {
					return ^uint64(0), nil
				}
				return "STRICT_TRANS_TABLES", nil
			})
			compilerCtx := plan2.NewEmptyCompilerContext()
			compilerCtx.SetContext(ctx)
			sql := "select group_concat(s order by s separator '') from (select 'abc' s union all select 'def') t"
			stmts, err := mysql.Parse(ctx, sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
			require.NoError(t, err)
			pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
			ctrl := gomock.NewController(t)
			proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)
			evaluations := 0
			var senders []*messageSenderOnClient
			latePayload, err := json.Marshal(remoteTerminalEnvelope{WarningCount: 1, WarningDiagnostics: []remoteWarningDiagnostic{{Code: 1260, Message: "late terminal RPC"}}})
			require.NoError(t, err)
			c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
			defer c.Release()
			require.NoError(t, c.Compile(ctx, pn, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
				if bat == nil {
					return nil
				}
				evaluations++
				require.Equal(t, "abcde", string(bat.Vecs[0].GetBytesAt(0)))
				require.Equal(t, uint64(1), session.totalWarnings, "attempt diagnostics must not be published yet")
				if len(senders) > 0 {
					require.NoError(t, senders[0].dealRemoteTerminal(latePayload), "old generation callback during the new attempt")
				}
				senders = append(senders, &messageSenderOnClient{warningSink: proc.GetWarningSink().(warningDiagnosticSink)})
				switch outcome {
				case "retry":
					if evaluations == 1 {
						return moerr.NewTxnNeedRetryNoCtx()
					}
				case "failure":
					return moerr.NewInternalErrorNoCtx("downstream failed after aggregate finalization")
				case "panic":
					panic("downstream panic after aggregate finalization")
				}
				return nil
			}))
			if outcome == "panic" {
				// Pipeline panic recovery may convert the panic to an execution error.
				func() { defer func() { _ = recover() }(); _, err = c.Run(0); require.Error(t, err) }()
			} else {
				_, err = c.Run(0)
				if outcome == "failure" {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
			want := uint64(1)
			if outcome == "success" || outcome == "retry" {
				want++
			}
			require.Equal(t, want, session.totalWarnings)
			require.NotEmpty(t, senders)
			require.NoError(t, senders[len(senders)-1].dealRemoteTerminal(latePayload))
			require.Equal(t, want, session.totalWarnings)
			require.Nil(t, proc.WarningSink)
			if outcome == "retry" {
				require.Equal(t, 2, evaluations)
				require.Equal(t, 1, c.retryTimes)
			}
		})
	}
}

func TestWarningAttemptNestedAndBounded(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	parent := newWarningAttempt(proc)
	childProc := proc.NewNoContextChildProc(0)
	child := newWarningAttempt(childProc)
	child.collector.AppendWarningBatch(1000, []uint16{1260}, []string{"child"})
	child.finish(true, parent.collector)
	require.Zero(t, session.totalWarnings)
	require.Same(t, parent.collector, childProc.GetWarningSink())
	parent.finish(false, session)
	require.Zero(t, session.totalWarnings)
	childProc.GetWarningSink().(warningDiagnosticSink).AppendWarningDiagnostic(1260, "late child")
	require.Zero(t, session.totalWarnings)

	attempt := newWarningAttempt(proc)
	for i := 0; i < 1000; i++ {
		attempt.collector.AppendWarningDiagnostic(1260, "bounded")
	}
	attempt.finish(true, session)
	require.Equal(t, uint64(1000), session.totalWarnings)
	require.Len(t, session.warnings, remoteWarningRetentionLimit)
	attempt.finish(true, session)
	require.Equal(t, uint64(1000), session.totalWarnings, "one-shot publish")
}

func TestStringAssignmentWarningAttemptDiscard(t *testing.T) {
	proc := testutil.NewProcess(t)
	session := &remoteWarningSession{}
	proc.Session = session
	attempt := newWarningAttempt(proc)
	require.NotNil(t, attempt)

	source := vector.NewVec(types.T_varchar.ToType())
	defer source.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(source, []byte("abcd"), false, proc.Mp()))
	target := types.New(types.T_varchar, 3, 0)
	destination := vector.NewVec(target)
	defer destination.Free(proc.Mp())
	result := vector.NewFunctionResultWrapper(target, proc.Mp())
	defer result.Free()
	require.NoError(t, result.PreExtendAndReset(1))

	require.NoError(t, planfunction.NewAssignIgnoreCast(
		[]*vector.Vector{source, destination}, result, proc, 1, nil))
	total, warnings := attempt.collector.SnapshotWarnings()
	require.Equal(t, uint64(1), total)
	require.Len(t, warnings, 1)
	require.Zero(t, session.totalWarnings, "attempt diagnostics must not publish early")

	attempt.discard()
	attempt.restore()
	require.Zero(t, session.totalWarnings, "discarded attempt diagnostics must not leak")
	require.Nil(t, proc.WarningSink)
}

func TestPreparedGroupConcatFloorFreshAndRetryCompile(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc := testutil.NewProcess(t)
	sessionMaxLen := int64(5)
	proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		if name == "group_concat_max_len" {
			return sessionMaxLen, nil
		}
		if name == plan2.SQLSelectLimitVariable {
			return ^uint64(0), nil
		}
		return "STRICT_TRANS_TABLES", nil
	})
	compilerCtx := plan2.NewEmptyCompilerContext()
	compilerCtx.SetContext(ctx)
	sql := "select group_concat(s order by s separator '') from (select 'abc' s union all select 'def') t"
	stmts, err := mysql.Parse(ctx, sql, 1)
	require.NoError(t, err)
	defer stmts[0].Free()
	query, err := plan2.NewPrepareOptimizer(compilerCtx).Optimize(stmts[0], false)
	require.NoError(t, err)
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
	ctrl := gomock.NewController(t)
	proc.Base.TxnClient, proc.Base.TxnOperator = newTestTxnClientAndOpWithIsolation(ctrl, txn.TxnIsolation_RC)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)
	for _, tc := range []struct {
		floor   uint64
		current int64
		want    string
	}{
		{1024, 5, "abcdef"}, {5, 1024, "abcdef"}, {5, 4, "abcde"}, {0, 4, "abcd"},
	} {
		sessionMaxLen = tc.current
		c := NewCompile("test", "test", sql, "", "", newStubEngine(), proc, stmts[0], false, nil, time.Now())
		c.SetGroupConcatMaxLenFloor(tc.floor)
		calls := 0
		require.NoError(t, c.Compile(ctx, plan2.DeepCopyPlan(pn), func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
			if bat != nil {
				calls++
				require.Equal(t, tc.want, string(bat.Vecs[0].GetBytesAt(0)))
			}
			return nil
		}))
		// Both fresh AP-style compilation and the two retry rebuild paths must
		// retain the same statement-owned floor, independently of physical config.
		for _, rebuild := range []bool{false, true} {
			c.SetBuildPlanFunc(func(context.Context) (*plan.Plan, error) { return plan2.DeepCopyPlan(pn), nil })
			retry, err := c.buildRetryCompile(rebuild)
			require.NoError(t, err)
			require.Equal(t, tc.floor, retry.groupConcatMaxLenFloor)
			_, err = retry.Run(0)
			require.NoError(t, err)
			retry.Release()
		}
		require.Equal(t, 2, calls)
		c.clear()
		require.Zero(t, c.groupConcatMaxLenFloor)
		doCompileRelease(c)
	}
}

func TestWarningAttemptConcurrentSeal(t *testing.T) {
	collector := &remoteWarningCollector{}
	var writers sync.WaitGroup
	ready := make(chan struct{})
	for range 2 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			<-ready
			for range 100 {
				collector.AppendWarningDiagnostic(1260, "concurrent")
			}
		}()
	}
	close(ready)
	collector.closeWarnings(false)
	writers.Wait()
	total, records := collector.SnapshotWarnings()
	require.Zero(t, total)
	require.Empty(t, records)
	// No Session means no diagnostics work or traversal on background queries.
	proc := testutil.NewProcess(t)
	proc.Session = nil
	require.Zero(t, testing.AllocsPerRun(100, func() { require.Nil(t, newWarningAttempt(proc)) }))
}
