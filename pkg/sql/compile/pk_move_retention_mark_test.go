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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// D206(B.1) setter sites (5) and (6) on the compile funnels (D211(B.2) /
// D212(B.3)): the KEEP funnel marks the drop statement's own transaction at
// entry with a nil-observation fail-closed no-op shape, and the retire
// funnel marks the retiring statement's own transaction BEFORE the fencing
// CS with a nil-observation hard error. The mock-proc harness degrades the
// fencing substrate (no lock service), so the retire funnel's paired
// deletes run as the D212(C.2) skip path exercised by these SQL-shape
// fixtures.

// markSpyOperator implements the D206(B.1) optional interface by hand: the
// embedded nil TxnOperator supplies the base surface (never consulted on
// the harness's SQL-shape paths), and MarkExpectedConflict counts the
// setter invocations.
type markSpyOperator struct {
	client.TxnOperator
	marks int32
}

func (s *markSpyOperator) MarkExpectedConflict() { atomic.AddInt32(&s.marks, 1) }
func (s *markSpyOperator) marked() int32         { return atomic.LoadInt32(&s.marks) }

// Txn answers a deliberately NON-pessimistic meta: the fencing CS guard
// (RunDataBranchFencingRetirementCS) takes done=false for it, which is
// exactly the skip path these SQL-shape fixtures exercise; the embedded
// nil operator's other methods are never consulted on these paths.
func (s *markSpyOperator) Txn() txn.TxnMeta {
	return txn.TxnMeta{Mode: txn.TxnMode_Optimistic}
}

// GetWorkspace answers a nil workspace: pkMoveGenesisTxnWorkspace then
// degrades the genesis-journal transition to its uniform no-op, exactly
// the mock-proc degrade class the fixtures own.
func (s *markSpyOperator) GetWorkspace() client.Workspace { return nil }

func TestRetentionKeepFunnelMarksStatementTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
	spy := &markSpyOperator{}
	c.proc.Base.TxnOperator = spy

	insertPkMoveLinkRetention(c, 7, 800, 900, "db1", "t800")
	require.Equal(t, int32(1), spy.marked(),
		"SITE (5) marks the KEEP funnel's own transaction at entry")
	require.NotZero(t, countSqlWithPrefix(*got, pkMoveRetentionInsertSQL),
		"the funnel then stages its insert write in the marked transaction")

	insertPkMoveLinkRetention(c, 7, 801, 901, "db2", "t802")
	require.Equal(t, int32(2), spy.marked(),
		"the entry placement re-marks idempotently for every KEEP write")
}

func TestRetentionKeepFunnelNoTxnNoWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
	c.proc.Base.TxnOperator = nil

	insertPkMoveLinkRetention(c, 7, 800, 900, "db1", "t800")
	require.Zero(t, len(*got),
		"SITE (5) fails closed on a nil observation: no mark and NO WRITE")
}

func TestRetireFunnelMarksTxnAndRunsSkipPathDeletes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
	spy := &markSpyOperator{}
	c.proc.Base.TxnOperator = spy

	require.NoError(t, deletePkMoveLinkAndRetention(c, 7, 800))
	require.Equal(t, int32(1), spy.marked(),
		"SITE (6) marks the retiring statement's own transaction BEFORE the fencing CS")
	links := collectSqlWithPrefix(*got, "delete from mo_catalog.mo_data_branch_update_link")
	require.Len(t, links, 1, "the skip path runs the paired link DELETE plainly")
	retDeletes := collectSqlWithPrefix(*got, pkMoveRetentionDeleteSQL)
	require.Len(t, retDeletes, 1, "the skip path runs the retention DELETE plainly")
	require.True(t, strings.Contains(retDeletes[0], "row_kind = 0"),
		"the primary retirement DELETE pins row_kind = 0 (D212(C.4) partition pin)")

	require.NoError(t, deletePkMoveLinkAndRetention(c, 7, 801))
	require.Equal(t, int32(2), spy.marked(),
		"the multi-id loop re-marks idempotently per funnel invocation")
}

func TestRetireFunnelNoTxnFailsClosed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c, got := newPkMoveOwnerMockCompile(t, ctrl, nil)
	c.proc.Base.TxnOperator = nil

	err := deletePkMoveLinkAndRetention(c, 7, 800)
	require.Error(t, err)
	require.Contains(t, err.Error(), "pk-move retirement mark: txn operator absent",
		"SITE (6) fails closed per D210(B) on a nil observation")
	require.Zero(t, len(*got), "no write escapes the failed mark resolution")
}

// D212(C.3) legacy arm: on a legacy catalog (no row_kind/logical_id
// columns) the retirement DELETE degrades to the names-only shape while the
// funnel's own mark discipline is unchanged.
func TestRetireFunnelLegacyDeleteDegrade(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	missingCol := pkMoveMissingLedgerColumnErr()
	c, got := newPkMoveOwnerMockCompile(t, ctrl, []pkMoveMockHandler{
		{pkMoveRetentionDeleteSQL, func(sql string) (executor.Result, error) {
			return executor.Result{}, missingCol
		}},
	})
	spy := &markSpyOperator{}
	c.proc.Base.TxnOperator = spy

	require.NoError(t, deletePkMoveLinkAndRetention(c, 7, 800))
	require.Equal(t, int32(1), spy.marked())
	require.Equal(t, 1, countSqlWithPrefix(*got, pkMoveRetentionDeleteSQL),
		"the wide shape ran first")
	legacies := collectSqlWithPrefix(*got, pkMoveRetentionLegacyDeleteSQL)
	require.Len(t, legacies, 1, "the legacy names-only DELETE arm ran")
	require.True(t, strings.Contains(legacies[0], "table_id = 800"))
}

// D206(B.2) fatalLog consult twin: with the checker armed, a marked
// transaction's covered conflict RETURNS (the byte-identical checker-
// disabled disposition) at the statement twin; every non-fatal and
// non-covered shrug keeps its pre-existing shape. The unmarked +
// checker-armed arm is a process Fatal by construction and is owned by the
// cell-owned live RED twins (D206(C.2)-(C.4), D212(C.2)).
type markStateOperator struct {
	client.TxnOperator
	state bool
}

func (s *markStateOperator) ExpectedConflictMarked() bool { return s.state }

func newFatalLogCompile(t *testing.T, ctrl *gomock.Controller, op client.TxnOperator, checker bool) *Compile {
	exec := &dropDDLExecutor{}
	exec.exec = nil
	c, _ := newDropDDLCompile(t, ctrl, exec)
	c.proc.Base.TxnOperator = op
	moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(
		moruntime.EnableCheckInvalidRCErrors, checker)
	return c
}

func TestFatalLogMarkConsultDispositions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ww := moerr.NewTxnWWConflict(context.Background(), 42, "covered conflict")

	t.Run("nil error returns", func(t *testing.T) {
		c := newFatalLogCompile(t, ctrl, &markStateOperator{state: false}, true)
		c.fatalLog(1, nil)
	})
	t.Run("non-covered class returns", func(t *testing.T) {
		c := newFatalLogCompile(t, ctrl, &markStateOperator{state: false}, true)
		c.fatalLog(1, moerr.NewInternalErrorNoCtx("plain failure"))
	})
	t.Run("need-retry at zero retry returns", func(t *testing.T) {
		c := newFatalLogCompile(t, ctrl, &markStateOperator{state: false}, true)
		c.fatalLog(0, moerr.NewTxnNeedRetryNoCtx())
	})
	t.Run("covered class with checker disabled returns", func(t *testing.T) {
		c := newFatalLogCompile(t, ctrl, &markStateOperator{state: false}, false)
		c.fatalLog(1, ww)
	})
	t.Run("marked consult returns with checker armed", func(t *testing.T) {
		c := newFatalLogCompile(t, ctrl, &markStateOperator{state: true}, true)
		c.fatalLog(1, ww)
	})
}
