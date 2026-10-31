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
	"errors"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
)

// This small manufactured slot exercises the pending ownership protocol only.
// It deliberately does not stand in for the planner's publication certificate.
func temporalPendingSlotFixture(t *testing.T) (*Session, *ExecCtx, *TxnComputationWrapper, *pb.Plan) {
	t.Helper()
	ses := newTestSession(t, gomock.NewController(t))
	t.Cleanup(ses.Close)
	ses.SetCmd(COM_QUERY)
	ctx := defines.AttachAccountId(t.Context(), 0)
	op, closeClient := client.NewTestTxnOperator(ctx)
	ses.GetTxnHandler().txnOp = op
	ses.GetProc().Base.TxnOperator = op
	t.Cleanup(func() {
		ses.GetProc().Base.TxnOperator = nil
		ses.GetTxnHandler().txnOp = nil
		require.NoError(t, op.Rollback(context.Background()))
		closeClient()
	})
	input := &UserInput{sql: "select 1"}
	input.genHash()
	stmts, err := mysql.Parse(ctx, input.sql, 1)
	require.NoError(t, err)
	dop, err := ses.GetSessionSysVar("max_dop")
	require.NoError(t, err)
	raw := &pb.Plan{Plan: &pb.Plan_Query{Query: &pb.Query{StmtType: pb.Query_SELECT, TemporalIndexNeedsReplan: true, MaxDop: dop.(int64), Nodes: []*pb.Node{{NodeType: pb.Node_PROJECT}}}}}
	snap := timestamp.Timestamp{PhysicalTime: 27, LogicalTime: 3, NodeID: 2}
	seal := &temporalCacheSeal{raw: raw, terminal: true}
	ses.mu.Lock()
	ses.planCache.cacheWithPlanSnapshotsAndStatsVersionsOwned(input.getHash(), stmts, []*pb.Plan{raw}, []timestamp.Timestamp{snap}, []map[optimizerStatsTableKey]uint64{nil}, seal)
	ses.mu.Unlock()
	ec := &ExecCtx{reqCtx: ctx, ses: ses, proc: ses.GetProc(), input: input, singleStatementQuery: true}
	cws, err := GetComputationWrapper(ec, "", "root", nil, ec.proc, ses)
	require.NoError(t, err)
	require.Len(t, cws, 1)
	cw := cws[0].(*TxnComputationWrapper)
	t.Cleanup(cw.Free)
	ec.cw, ec.cws, ec.stmt = cw, cws, cw.GetAst()
	ses.GetTxnCompileCtx().SetExecCtx(ec)
	return ses, ec, cw, raw
}

func TestTemporalCachePendingPlanDoesNotEscapeAndCloneIsDetached(t *testing.T) {
	ses, ec, cw, raw := temporalPendingSlotFixture(t)
	require.Nil(t, cw.Plan(), "a pre-dispatch callback sees an ordinary missing plan")
	_, ok := cw.PlanSnapshotTS()
	require.False(t, ok)
	require.False(t, cw.planGenerationReused)
	require.True(t, cw.pendingTemporalGenerationCurrent(ses))
	before := proto.Clone(raw).(*pb.Plan)
	require.NoError(t, checkCachedStatementPlan(ses, ec))
	require.NotNil(t, cw.Plan())
	require.NotSame(t, raw, cw.Plan())
	snap, ok := cw.PlanSnapshotTS()
	require.True(t, ok)
	require.Equal(t, int64(27), snap.PhysicalTime)
	require.True(t, cw.planGenerationReused)
	cw.Plan().GetQuery().Nodes[0].NodeType = pb.Node_DELETE
	require.True(t, proto.Equal(before, raw))
	next, err := clonePrivateTemporalGeneration(ec.reqCtx, raw)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, next))
	require.NotSame(t, raw.GetQuery().Nodes[0], next.GetQuery().Nodes[0])
}

func TestTemporalCachePendingOwnerAndCancellation(t *testing.T) {
	_, ec, cw, raw := temporalPendingSlotFixture(t)
	ctx, cancel := context.WithCancelCause(ec.reqCtx)
	cause := errors.New("stop pending generation")
	cancel(cause)
	ec.reqCtx = ctx
	require.Same(t, cause, checkCachedStatementPlan(cw.ses, ec))
	require.Nil(t, cw.Plan())
	require.Same(t, raw, cw.cachedPlanGeneration)
	ec.reqCtx = t.Context()
	ec.cw = &TxnComputationWrapper{}
	_, err := cw.admitTemporalCachedExecution(ec)
	require.ErrorContains(t, err, "execution owner mismatch")
	require.Nil(t, cw.Plan())
}

func TestTemporalCachePendingMembershipDoesNotEvictOrAuthorizeReplacement(t *testing.T) {
	ses, _, cw, raw := temporalPendingSlotFixture(t)
	require.True(t, cw.pendingTemporalGenerationCurrent(ses))
	cw.planSnapshotTS.NodeID++
	require.False(t, cw.pendingTemporalGenerationCurrent(ses))
	cw.planSnapshotTS.NodeID--
	ses.invalidateCachedPlanGeneration(cw.cachedPlanSQL, 0, raw)
	require.False(t, cw.pendingTemporalGenerationCurrent(ses))
	require.NotNil(t, cw.GetAst(), "a membership check must not free the borrowed AST")
	ses.mu.Lock()
	slot := ses.planCache.cachePool[cw.cachedPlanSQL].Value.(*cachedPlan)
	replacement := proto.Clone(raw).(*pb.Plan)
	slot.invalid = false
	slot.plans[0] = replacement
	ses.mu.Unlock()
	require.False(t, cw.pendingTemporalGenerationCurrent(ses))
	ses.invalidateCachedPlanGeneration(cw.cachedPlanSQL, 0, raw)
	require.False(t, slot.invalid, "old borrower cannot invalidate a replacement generation")
}

func TestTemporalCachePendingDeclinesStagedMultiStatement(t *testing.T) {
	ses, ec, cw, raw := temporalPendingSlotFixture(t)
	ec.singleStatementQuery = false
	admitted, err := cw.admitTemporalCachedExecution(ec)
	require.NoError(t, err)
	require.False(t, admitted)
	require.Nil(t, cw.Plan())
	require.False(t, cw.hasPlanSnapshotTS)
	require.False(t, cw.planGenerationReused)
	require.True(t, cw.pendingTemporalGenerationCurrent(ses))
	require.Same(t, raw, cw.cachedPlanGeneration)
	// The real stale path obtains a fresh owned AST and revokes only this slot.
	require.NoError(t, checkCachedStatementPlan(ses, ec))
	require.Nil(t, cw.plan)
	require.False(t, cw.temporalCacheHit)
	require.False(t, cw.stmtBorrowed)
	require.NotNil(t, cw.GetAst())
	require.False(t, ses.isCached(ec.input.getHash()))
}

func TestTemporalCacheDirectCompileCancelledOwnerMismatch(t *testing.T) {
	_, ec, cw, _ := temporalPendingSlotFixture(t)
	cause := errors.New("stop before direct Compile")
	ctx, cancel := context.WithCancelCause(ec.reqCtx)
	cancel(cause)
	ec.reqCtx = ctx
	ec.cw = &TxnComputationWrapper{}
	_, err := cw.Compile(ec, nil)
	require.Same(t, cause, err)
	require.Nil(t, cw.Plan())
}

func TestTemporalCachePendingRejectsDifferentTransaction(t *testing.T) {
	_, ec, cw, _ := temporalPendingSlotFixture(t)
	original := cw.proc.GetTxnOperator()
	cw.proc.Base.TxnOperator = nil
	defer func() { cw.proc.Base.TxnOperator = original }()
	admitted, err := cw.admitTemporalCachedExecution(ec)
	require.False(t, admitted)
	require.ErrorContains(t, err, "transaction owner mismatch")
	require.Nil(t, cw.Plan())
}
