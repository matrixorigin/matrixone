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

package frontend

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	plan0 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

// The tracer is process-global; each case borrows it only for the lifetime of
// the real statement and its memory epoch.
func TestRecordStatementTraceOwnership(t *testing.T) {
	provider := motrace.GetTracerProvider()
	// The frontend test provider is initialized with the no-op exporter. It
	// leaves these statements with this test, allowing deterministic release.
	require.IsType(t, motrace.NoopBatchProcessor{}, motrace.GetGlobalBatchProcessor())
	wasEnabled := provider.IsEnable()
	t.Cleanup(func() { provider.SetEnable(wasEnabled) })
	provider.SetEnable(true)

	newSession := func(t *testing.T) *Session {
		t.Helper()
		service := t.Name()
		InitServerLevelVars(service)
		t.Cleanup(func() { serverVarsMap.Delete(service) })
		pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
		pu.SV.SetDefaultValues()
		setPu(service, pu)
		proto := &internalProtocol{result: &internalExecResult{}}
		require.Equal(t, "0.0.0.0:0", proto.Peer())
		require.Equal(t, "0.0.0.0:0", proto.GetStr(PEER))
		ses := NewSession(context.Background(), service, proto, nil)
		ses.SetTenantInfo(&TenantInfo{Tenant: "trace_account", User: "trace_user"})
		t.Cleanup(ses.Close)
		t.Cleanup(func() {
			h := ses.GetTxnHandler()
			h.mu.Lock()
			h.txnOp, h.txnCtx = nil, nil
			h.mu.Unlock()
		})
		return ses
	}
	installTxn := func(ses *Session, id uuid.UUID) {
		op := newTestTxnOp()
		op.meta.ID = append([]byte(nil), id[:]...)
		h := ses.GetTxnHandler()
		h.mu.Lock()
		h.txnOp, h.txnCtx = op, context.Background()
		h.mu.Unlock()
	}
	start := func(t *testing.T, ses *Session) (context.Context, *motrace.StatementInfo, *resource.Root) {
		t.Helper()
		ctx, err := RecordStatement(context.Background(), ses, ses.GetProc(), nil, time.Now(), "select 17", constant.ExternSql, true)
		require.NoError(t, err)
		stmt := ses.GetStmtInfo()
		require.NotNil(t, stmt)
		row := motrace.SingleStatementTable.GetRow(ctx)
		t.Cleanup(func() {
			stmt.EndStatement(ctx, nil, 0, 0, 0)
			stmt.FillRow(ctx, row)
			row.Free()
			stmt.Free()
			ses.SetTStmt(nil)
		})
		root := resource.RootFromContext(ctx)
		require.NotNil(t, root)
		return ctx, stmt, root
	}

	t.Run("active transaction and available epoch", func(t *testing.T) {
		ses := newSession(t)
		id := uuid.MustParse("01020304-0506-0708-090a-0b0c0d0e0f10")
		installTxn(ses, id)
		ctx, stmt, root := start(t, ses)
		require.Equal(t, id[:], stmt.TransactionID[:])
		require.Equal(t, "trace_account", stmt.Account)
		require.Equal(t, "trace_user", stmt.User)
		require.Equal(t, "0.0.0.0:0", stmt.Host)
		require.Equal(t, constant.ExternSql, stmt.SqlSourceType)
		require.Equal(t, "select 17", string(stmt.Statement))
		pool := ses.GetProc().Mp()
		allocation, err := pool.Alloc(64, true)
		require.NoError(t, err)
		pool.Free(allocation)
		require.GreaterOrEqual(t, root.PreResponseSummary().Memory.MaxDomainPeakLiveBytes, uint64(64))
		stmt.EndStatement(ctx, nil, 0, 0, 0)
		token := pool.StartResourcePeakEpoch()
		require.NotNil(t, token, "statement completion must release its pool epoch")
		_, ok := pool.EndResourcePeakEpoch(token)
		require.True(t, ok)
	})

	t.Run("transaction acquired after statement starts", func(t *testing.T) {
		ses := newSession(t)
		ctx, stmt, _ := start(t, ses)
		require.Equal(t, motrace.NilTxnID, stmt.TransactionID)
		require.NoError(t, RecordStatementTxnID(ctx, ses))
		require.Equal(t, motrace.NilTxnID, stmt.TransactionID)
		id := uuid.MustParse("11121314-1516-1718-191a-1b1c1d1e1f20")
		installTxn(ses, id)
		require.NoError(t, RecordStatementTxnID(ctx, ses))
		require.Equal(t, id[:], stmt.TransactionID[:])
		got := ses.GetTxnId()
		require.Equal(t, id[:], got[:])
		installTxn(ses, uuid.MustParse("21222324-2526-2728-292a-2b2c2d2e2f30"))
		require.NoError(t, RecordStatementTxnID(ctx, ses))
		require.Equal(t, id[:], stmt.TransactionID[:], "an existing statement transaction ID must not be replaced")
	})

	t.Run("nested epoch remains owned by outer scope", func(t *testing.T) {
		ses := newSession(t)
		pool := ses.GetProc().Mp()
		outer := pool.StartResourcePeakEpoch()
		require.NotNil(t, outer)
		t.Cleanup(func() { pool.EndResourcePeakEpoch(outer) })
		ctx, stmt, root := start(t, ses)
		preview := root.PreResponseSummary()
		require.Equal(t, uint64(1), preview.MissingMemoryDomainCount)
		require.NotZero(t, preview.Quality&resource.QualityPartial)
		require.NotZero(t, preview.Quality&resource.QualityMissingMemoryDomain)
		stmt.EndStatement(ctx, nil, 0, 0, 0)
		terminal := root.PreResponseSummary()
		require.Equal(t, uint64(1), terminal.MissingMemoryDomainCount)
		require.NotZero(t, terminal.Quality&resource.QualityPartial)
		_, ok := pool.EndResourcePeakEpoch(outer)
		require.True(t, ok, "the nested statement must not close the outer epoch")
	})

	t.Run("terminal summary reaches the plan", func(t *testing.T) {
		ses := newSession(t)
		ctx, stmt, root := start(t, ses)
		stmt.RequestAt = time.Now().Add(-motrace.GetLongQueryTime() - time.Second)
		plan := &plan0.Plan{Plan: &plan0.Plan_Query{Query: &plan0.Query{
			Nodes: []*plan0.Node{{NodeId: 0, NodeType: plan0.Node_VALUE_SCAN}},
			Steps: []int32{0},
		}}}
		handler := NewJsonPlanHandler(ctx, stmt, nil, plan, nil)
		stmt.SetSerializableExecPlan(handler)
		stmt.EndStatement(ctx, nil, 0, 17, 1)
		var payload struct {
			PhyPlan struct {
				Resource *resource.StatementResourceSummary `json:"resource"`
			}
		}
		require.NoError(t, json.Unmarshal(handler.Marshal(ctx), &payload))
		want := root.PreResponseSummary()
		require.Equal(t, uint64(17), want.Usage.ClientEgressBytes)
		require.Equal(t, &want, payload.PhyPlan.Resource)
	})

	t.Run("failed response logs recorded statement once", func(t *testing.T) {
		ses := newSession(t)
		ctx, stmt, root := start(t, ses)
		observed, logs := newObservedProtocolSession()
		ses.logger = observed.logger
		ses.loggerOnce.Do(func() {})
		writer := &accountingMysqlWriter{}
		ses.feSessionImpl.respr = NewMysqlResp(writer)
		ses.SetSqlOfStmt("stale session text")
		ses.beginResponseAccounting()
		execErr := moerr.NewInternalErrorNoCtx("statement failed")
		logStatementStringStatusWithLog(ctx, ses, "", fail, execErr, true)
		entries := logs.FilterMessage("query trace status").All()
		require.Len(t, entries, 1)
		require.Equal(t, "select 17", entries[0].ContextMap()["statement"])
		require.True(t, ses.pendingStatementFailed)
		require.Same(t, stmt, ses.GetStmtInfo())
		writer.bytes, writer.packets = 17, 1
		ses.finishResponseAccounting(ctx, execErr, true)
		require.Nil(t, ses.GetStmtInfo())
		require.Equal(t, motrace.StatementStatusFailed, stmt.Status)
		require.Equal(t, uint64(17), root.PreResponseSummary().Usage.ClientEgressBytes)
		require.Equal(t, 2, writer.calls)
		ses.finishResponseAccounting(ctx, execErr, true)
		require.Equal(t, 2, writer.calls, "terminal accounting must run only once")
	})
}

func TestRecordStatementTraceOwnershipRestoresProvider(t *testing.T) {
	provider := motrace.GetTracerProvider()
	wasEnabled := provider.IsEnable()
	t.Cleanup(func() { provider.SetEnable(wasEnabled) })
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			provider.SetEnable(enabled)
			require.True(t, t.Run("statement ownership", TestRecordStatementTraceOwnership))
			require.Same(t, provider, motrace.GetTracerProvider())
			require.Equal(t, enabled, provider.IsEnable())
		})
	}
}
