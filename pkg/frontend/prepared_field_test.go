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

package frontend

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestPreparedFieldHistoryCacheAndMigration(t *testing.T) {
	ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25301,
		"select field(case when ? then ? else null end, 'Z', 'A', 'a')")
	prepared.proc = cw.proc
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
	original := prepared.PreparePlan.String()
	setParams := func(typ defines.MysqlType, value string, null bool) {
		cw.discardRuntimeCacheCandidate()
		cw.proc.SetPrepareParams(nil)
		if prepared.params != nil {
			prepared.params.Free(cw.proc.Mp())
		}
		prepared.params = vector.NewVec(types.T_text.ToType())
		require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
		require.NoError(t, vector.AppendBytes(prepared.params, []byte(value), null, cw.proc.Mp()))
		prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(typ), 0}
	}
	// Failure after binding must not freeze the first text return type.
	setParams(defines.MYSQL_TYPE_VAR_STRING, "a", false)
	writer := ec.resper.MysqlRrWr().(*testMysqlWriter)
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		return nil, errors.New("field metadata failure")
	}
	_, _, _, _, _, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	require.ErrorContains(t, err, "field metadata failure")
	require.Nil(t, prepared.fieldTypes)
	writer.makeColumnDefDataFunc = nil

	cacheHits := 0
	for _, run := range []struct {
		typ   defines.MysqlType
		value string
		null  bool
		want  uint64
	}{
		{defines.MYSQL_TYPE_NULL, "", true, 0},
		{defines.MYSQL_TYPE_VAR_STRING, "a", false, 3},
		{defines.MYSQL_TYPE_VAR_STRING, "a", false, 3},
		{defines.MYSQL_TYPE_BLOB, "a", false, 3},
		{defines.MYSQL_TYPE_LONGLONG, "123", false, 0},
		{defines.MYSQL_TYPE_VAR_STRING, "a", false, 1},
		{defines.MYSQL_TYPE_NEWDECIMAL, "123.45", false, 0},
		{defines.MYSQL_TYPE_BLOB, "a", false, 1},
	} {
		setParams(run.typ, run.value, run.null)
		if run.typ == defines.MYSQL_TYPE_LONGLONG {
			prior := prepared.fieldTypes
			writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
				return nil, errors.New("field transition failure")
			}
			_, _, _, _, _, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
			require.ErrorContains(t, err, "field transition failure")
			require.Same(t, prior, prepared.fieldTypes)
			writer.makeColumnDefDataFunc = nil
		}
		_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
		if owned && stmt != nil {
			stmt.Free()
		}
		require.NoError(t, err)
		require.NotNil(t, prepared.fieldTypes)
		q := p.GetQuery()
		expr := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
		func() {
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, run.want, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
		}()
		if cw.runtimeCachePlan != nil {
			cacheHits++
			prepared.installRuntimeSpecializationCache(cw.runtimeCacheKey, cw.runtimeCachePlan, nil, cw.runtimeCacheDiagnostics, cw.runtimeCacheColDefData)
			cw.discardRuntimeCacheCandidate()
			_, cached, clone, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
			if owned && clone != nil {
				clone.Free()
			}
			require.NoError(t, err)
			require.Same(t, p, cached, "must exercise the real logical cache hit")
			require.Nil(t, cw.runtimeCachePlan)
		}
		require.Equal(t, original, prepared.PreparePlan.String())
	}
	require.Positive(t, cacheHits, "the lifecycle test must reach specialization cache hits")
	prior := prepared.fieldTypes
	priorPlan := prepared.PreparePlan
	prepared.NativeMode = !prepared.NativeMode
	setParams(defines.MYSQL_TYPE_VAR_STRING, "a", false)
	metadataCalls := 0
	writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
		metadataCalls++
		if metadataCalls == 2 {
			return nil, errors.New("field generation metadata failure")
		}
		return nil, nil
	}
	t.Cleanup(func() { writer.makeColumnDefDataFunc = nil })
	_, _, _, _, _, err = initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	require.ErrorContains(t, err, "field generation metadata failure")
	require.NotSame(t, priorPlan, prepared.PreparePlan, "the new generation must already be committed")
	require.Same(t, prior, prepared.fieldTypes, "failed metadata must not publish a replacement snapshot")
	require.True(t, prepared.fieldTypesResetPending)
	writer.makeColumnDefDataFunc = nil
	setParams(defines.MYSQL_TYPE_VAR_STRING, "a", false)
	_, p, clone, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	if owned && clone != nil {
		clone.Free()
	}
	require.NoError(t, err)
	require.NotSame(t, prior, prepared.fieldTypes, "new generation must not inherit numeric history")
	require.False(t, prepared.fieldTypesResetPending)
	q := p.GetQuery()
	func() {
		result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
		require.NoError(t, err)
		defer free()
		require.Equal(t, uint64(2), vector.GetFixedAtNoTypeCheck[uint64](result, 0))
	}()
	rt := &Routine{mc: newMigrateController()}
	rt.setSession(ses)
	response := &query.MigrateConnFromResponse{}
	require.True(t, rt.mc.tryBeginRequest())
	held := true
	defer func() {
		if held {
			rt.mc.endRequest()
		}
	}()
	waiting := make(chan struct{})
	rt.mc.operationWaitHook = func() { close(waiting) }
	migrationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed := make(chan error, 1)
	migrationDone := make(chan struct{})
	go func() {
		defer close(migrationDone)
		completed <- rt.migrateConnectionFromWithContext(migrationCtx, response)
	}()
	defer func() {
		cancel()
		if held {
			rt.mc.endRequest()
			held = false
		}
		<-migrationDone
	}()
	select {
	case <-waiting:
	case <-migrationCtx.Done():
		t.Fatal("migration did not reach request admission")
	}
	// Publish a new snapshot while migration is waiting on the request owner.
	setParams(defines.MYSQL_TYPE_LONGLONG, "123", false)
	_, _, pending, _, pendingOwned, publishErr := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	if pendingOwned && pending != nil {
		pending.Free()
	}
	require.NoError(t, publishErr)
	rt.mc.endRequest()
	held = false
	select {
	case err = <-completed:
	case <-migrationCtx.Done():
		t.Fatal("migration did not leave admission")
	}
	require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedNotSafeToStartTransfer))
	require.Empty(t, response.PrepareStmts)
	require.True(t, rt.mc.tryBeginRequest())
	rt.mc.endRequest()
	_, _, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
	if owned && stmt != nil {
		stmt.Free()
	}
	require.NoError(t, err, "rejected migration must leave source usable")
	require.True(t, ses.RemovePrepareStmt(prepared.Name))
	require.Nil(t, prepared.fieldTypes)
	require.NoError(t, rt.migrateConnectionFrom(&query.MigrateConnFromResponse{}))
}

func TestPreparedFieldProtocolHistory(t *testing.T) {
	const sql = "select field(case when ? then ? else null end, 'Z','A','a')"
	ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25302, sql)
	prepared.proc = cw.proc
	t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
	proto, _, scratch := newBinaryPrepareProtocolTestCase(t, sql)
	t.Cleanup(scratch.Close)
	for _, run := range []struct {
		typ         defines.MysqlType
		value       string
		null, reuse bool
		want        uint64
	}{
		{defines.MYSQL_TYPE_BLOB, "a", false, false, 2},
		{defines.MYSQL_TYPE_BLOB, "", true, true, 0},
		{defines.MYSQL_TYPE_VAR_STRING, "a", false, false, 2},
		{defines.MYSQL_TYPE_LONGLONG, "1", false, false, 0},
		{defines.MYSQL_TYPE_VAR_STRING, "1.9tail", false, false, 0},
		{defines.MYSQL_TYPE_VAR_STRING, "a", false, true, 1},
	} {
		cw.discardRuntimeCacheCandidate()
		packet := []byte{0, 1, 0, 0, 0, 0, 1}
		if run.null {
			packet[5] = 2
		}
		if run.reuse {
			packet[6] = 0
		} else {
			packet = append(packet, byte(defines.MYSQL_TYPE_TINY), 0, byte(run.typ), 0)
		}
		packet = append(packet, 1)
		if !run.null {
			if run.typ == defines.MYSQL_TYPE_LONGLONG {
				packet = binary.LittleEndian.AppendUint64(packet, 1)
			} else {
				start := len(packet)
				packet = append(packet, make([]byte, 9+len(run.value))...)
				packet = packet[:proto.writeStringLenEnc(packet, start, run.value)]
			}
		}
		require.NoError(t, proto.ParseExecuteData(ec.reqCtx, cw.proc, prepared, packet, 0))
		_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
		if owned && stmt != nil {
			stmt.Free()
		}
		require.NoError(t, err)
		q := p.GetQuery()
		func() {
			result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, run.want, vector.GetFixedAtNoTypeCheck[uint64](result, 0), run.value)
		}()
		prepared.clearBinaryParamState(cw.proc)
	}
}

func TestPreparedFieldCandidateHistory(t *testing.T) {
	type run struct {
		typ   defines.MysqlType
		value string
		want  uint64
	}
	for _, tc := range []struct {
		sql  string
		runs []run
	}{
		{"select field(case when ? then ? else null end,'1.9')", []run{{defines.MYSQL_TYPE_LONGLONG, "123", 0}, {defines.MYSQL_TYPE_VAR_STRING, "1.9tail", 1}}},
		{"select field('1.9',case when ? then ? else null end)", []run{{defines.MYSQL_TYPE_LONGLONG, "123", 0}, {defines.MYSQL_TYPE_VAR_STRING, "1.9tail", 1}, {defines.MYSQL_TYPE_BLOB, "1.9tail", 1}, {defines.MYSQL_TYPE_NULL, "", 0}}},
		{"select field('Z',case when ? then ? else null end)", []run{{defines.MYSQL_TYPE_NULL, "", 0}, {defines.MYSQL_TYPE_VAR_STRING, "a", 0}, {defines.MYSQL_TYPE_LONGLONG, "123", 0}, {defines.MYSQL_TYPE_VAR_STRING, "a", 1}}},
		{"select field(case when ? then ? else null end,cast('9007199254740992' as decimal(30,0)),cast('9007199254740993' as decimal(30,0)))", []run{{defines.MYSQL_TYPE_LONGLONG, "9007199254740993", 2}, {defines.MYSQL_TYPE_VAR_STRING, "9007199254740993", 2}, {defines.MYSQL_TYPE_BLOB, "9007199254740993", 2}}},
		{"select field(cast('9007199254740992' as decimal(30,0)),case when ? then ? else null end,cast('9007199254740992' as decimal(30,0)))", []run{{defines.MYSQL_TYPE_LONGLONG, "9007199254740993", 2}, {defines.MYSQL_TYPE_VAR_STRING, "9007199254740993", 2}, {defines.MYSQL_TYPE_BLOB, "9007199254740993", 2}}},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25303, tc.sql)
			prepared.proc = cw.proc
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			for _, r := range tc.runs {
				prepared.params = vector.NewVec(types.T_text.ToType())
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
				require.NoError(t, vector.AppendBytes(prepared.params, []byte(r.value), r.typ == defines.MYSQL_TYPE_NULL, cw.proc.Mp()))
				prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(r.typ), 0}
				_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					stmt.Free()
				}
				require.NoError(t, err)
				q := p.GetQuery()
				func() {
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					require.Equal(t, r.want, vector.GetFixedAtNoTypeCheck[uint64](result, 0), r.value)
				}()
				retry := cw.preparedExecutionRetry()
				require.NotNil(t, retry)
				require.Same(t, prepared.fieldTypes, retry.fieldTypes)
				retryPlan, err := buildPlanForCompileRetry(ec.reqCtx, ses, ses.GetTxnCompileCtx(), prepared.PrepareStmt, false, retry)
				require.NoError(t, err)
				retryQuery := retryPlan.GetQuery()
				func() {
					result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, retryQuery.Nodes[retryQuery.Steps[len(retryQuery.Steps)-1]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
					require.NoError(t, err)
					defer free()
					require.Equal(t, r.want, vector.GetFixedAtNoTypeCheck[uint64](result, 0), "compile retry must preserve execution history")
				}()
				prepared.clearBinaryParamState(cw.proc)
			}
		})
	}
}

func TestPreparedFieldPlanInvalidationKeepsHistory(t *testing.T) {
	for _, tc := range []struct {
		name, setting string
		before, after any
	}{
		{"lc_time_names", "lc_time_names", "en_US", "fr_FR"},
		{"enable_vector_prefilter_by_default", "enable_vector_prefilter_by_default", int64(0), int64(1)},
		{"enable_vector_auto_mode_by_default", "enable_vector_auto_mode_by_default", int64(0), int64(1)},
		{"ONLY_FULL_GROUP_BY without grouping", "sql_mode", "", "ONLY_FULL_GROUP_BY"},
		{"NO_UNSIGNED_SUBTRACTION without subtraction", "sql_mode", "", "NO_UNSIGNED_SUBTRACTION"},
		{"ENABLE_BOOL_SUMAVG without aggregation", "sql_mode", "", "ENABLE_BOOL_SUMAVG"},
		{"div_precision_increment without division", "div_precision_increment", int64(4), int64(8)},
		{"unrelated DDL", "", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses, prepared, cw, ec := newPreparedExecuteEnvForSQL(t, 25304, "select field(case when ? then ? else null end,'A','a')")
			prepared.proc = cw.proc
			t.Cleanup(func() { cw.proc.SetPrepareParams(nil); ses.Close() })
			if tc.before != nil {
				original, err := ses.GetSessionSysVar(tc.setting)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, original)) })
				require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.before))
			}
			execute := func(null bool, want uint64, fail bool) {
				cw.discardRuntimeCacheCandidate()
				prepared.clearBinaryParamState(cw.proc)
				prepared.params = vector.NewVec(types.T_text.ToType())
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("1"), false, cw.proc.Mp()))
				require.NoError(t, vector.AppendBytes(prepared.params, []byte("a"), null, cw.proc.Mp()))
				typ := defines.MYSQL_TYPE_VAR_STRING
				if null {
					typ = defines.MYSQL_TYPE_NULL
				}
				prepared.ParamTypes = []byte{byte(defines.MYSQL_TYPE_LONGLONG), 0, byte(typ), 0}
				_, p, stmt, _, owned, err := initExecuteStmtParam(ec, ses, cw, nil, prepared.Name)
				if owned && stmt != nil {
					stmt.Free()
				}
				if fail {
					require.ErrorContains(t, err, "field plan invalidation metadata failure")
					return
				}
				require.NoError(t, err)
				q := p.GetQuery()
				result, free, err := colexec.GetReadonlyResultFromExpression(cw.proc, q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[uint64](result, 0))
			}
			execute(true, 0, false)
			execute(false, 2, false)
			snapshot := prepared.fieldTypes
			oldPlan := prepared.PreparePlan
			require.NotNil(t, snapshot)
			if tc.after == nil {
				require.Empty(t, prepared.PreparePlan.GetDcl().GetPrepare().GetSchemas())
				ses.advanceDDLVersion()
				require.NotEqual(t, prepared.ddlVersion, ses.getDDLVersion())
			} else {
				require.NoError(t, ses.SetSessionSysVar(ec.reqCtx, tc.setting, tc.after))
				if tc.setting == "div_precision_increment" {
					require.NotEqual(t, prepared.divPrecisionIncrement, ses.currentDivPrecisionIncrement(), "the mode comparison must invalidate the plan")
				} else {
					require.True(t, prepared.needsRebuild, "the public setter must actually invalidate the prepared plan")
				}
			}
			writer := ec.resper.MysqlRrWr().(*testMysqlWriter)
			metadataCalls := 0
			writer.makeColumnDefDataFunc = func(context.Context, []*plan.ColDef) ([][]byte, error) {
				metadataCalls++
				if metadataCalls == 2 {
					return nil, errors.New("field plan invalidation metadata failure")
				}
				return nil, nil
			}
			t.Cleanup(func() { writer.makeColumnDefDataFunc = nil })
			execute(false, 2, true)
			require.Same(t, snapshot, prepared.fieldTypes)
			require.NotSame(t, oldPlan, prepared.PreparePlan)
			require.False(t, prepared.fieldTypesResetPending, "plan-only failure must not reset types on retry")
			writer.makeColumnDefDataFunc = nil
			execute(false, 2, false)
			require.NotSame(t, oldPlan, prepared.PreparePlan, "the stale plan must still be rebuilt")
			require.Same(t, snapshot, prepared.fieldTypes, "plan invalidation must preserve immutable SQL type history")
		})
	}
}
