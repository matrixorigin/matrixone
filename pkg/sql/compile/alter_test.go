// Copyright 2024 Matrix Origin
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

package compile

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	mock_lock "github.com/matrixorigin/matrixone/pkg/frontend/test/mock_lock"
	"github.com/matrixorigin/matrixone/pkg/incrservice"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestShouldEnableAlterCopyPipelineFlush(t *testing.T) {
	assert.False(t, shouldEnableAlterCopyPipelineFlush(nil))
	assert.False(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: false}))
	assert.True(t, shouldEnableAlterCopyPipelineFlush(&plan2.AlterCopyOpt{SkipPkDedup: true}))
}

func TestLineageLifecycleWriterRejectsOptimisticTransaction(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{
		Mode: txn.TxnMode_Optimistic, Isolation: txn.TxnIsolation_SI,
	})
	proc.Base.TxnOperator = txnOp

	err := (&Compile{proc: proc}).lockDataBranchLineageOwnerLifecycle()
	require.ErrorContains(t, err, "requires a pessimistic transaction")
}

func TestShouldUseFixedAlterCopySnapshot(t *testing.T) {
	require.True(t, isExplicitAlterTxn(true, true))
	require.True(t, isExplicitAlterTxn(false, false))
	require.False(t, isExplicitAlterTxn(false, true))

	require.True(t, shouldUseFixedAlterCopySnapshot(true, false))
	require.False(t, shouldUseFixedAlterCopySnapshot(true, true))
	require.False(t, shouldUseFixedAlterCopySnapshot(false, false))
	require.False(t, shouldUseFixedAlterCopySnapshot(false, true))
}

func TestAlterCopySQLAtLineageSnapshot(t *testing.T) {
	const sql = "insert into copy select * from source"
	require.Equal(t, sql, alterCopySQLAtLineageSnapshot(sql, alterDataBranchLineagePlan{}))
	require.Equal(t, sql, alterCopySQLAtLineageSnapshot(sql, alterDataBranchLineagePlan{
		enabled: true,
		cloneTS: 123,
	}))
	require.Equal(t, sql+" {MO_TS = 123}", alterCopySQLAtLineageSnapshot(sql, alterDataBranchLineagePlan{
		enabled:     true,
		fixedCopyTS: true,
		cloneTS:     123,
	}))
}

func TestAlterCopySameStatementColumnReplacement(t *testing.T) {
	tableDef := &plan2.TableDef{Cols: []*plan2.ColDef{
		{Name: "a", ColId: 1, Seqnum: 0},
		{Name: "b", ColId: 2, Seqnum: 1},
	}}
	replacement := &plan2.AlterTable{
		TableDef: tableDef,
		ChangeTblColIdMap: map[uint64]*plan2.ColDef{
			1: {Name: "a"},
		},
		CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
			{Name: "a", ColId: 1, Seqnum: 0},
			{Name: "B", ColId: ^uint64(0), Seqnum: 0},
		}},
	}
	name, ok := alterCopySameStatementColumnReplacement(replacement)
	require.True(t, ok)
	require.Equal(t, "B", name)

	t.Run("same identity survives rename and reorder", func(t *testing.T) {
		unchanged := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
				2: {Name: "B"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "B", ColId: 2, Seqnum: 1},
				{Name: "a", ColId: 1, Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(unchanged)
		require.False(t, replaced)
	})

	t.Run("different-name drop and add is rejected", func(t *testing.T) {
		dropped := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
				{Name: "c", ColId: ^uint64(0), Seqnum: 0},
			}},
		}
		name, replaced := alterCopySameStatementColumnReplacement(dropped)
		require.True(t, replaced)
		require.Equal(t, "c", name)
	})

	t.Run("target-only add without a drop remains supported", func(t *testing.T) {
		added := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
				2: {Name: "b"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
				{Name: "b", ColId: 2, Seqnum: 1},
				{Name: "c", ColId: ^uint64(0), Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(added)
		require.False(t, replaced)
	})

	t.Run("drop without an add remains supported", func(t *testing.T) {
		dropped := &plan2.AlterTable{
			TableDef: tableDef,
			ChangeTblColIdMap: map[uint64]*plan2.ColDef{
				1: {Name: "a"},
			},
			CopyTableDef: &plan2.TableDef{Cols: []*plan2.ColDef{
				{Name: "a", ColId: 1, Seqnum: 0},
			}},
		}
		_, replaced := alterCopySameStatementColumnReplacement(dropped)
		require.False(t, replaced)
	})
}

func TestBuildAlterDataBranchLineageSQL(t *testing.T) {
	metadataSQL, snapshotSQL := buildAlterDataBranchLineageSQL(
		11, 22, 123456, 7,
		"alter:table", "tenant'o", "db'x", "tbl'y", "snapshot-id",
	)

	require.Equal(t,
		"insert into mo_catalog.mo_branch_metadata values(22, 123456, 11, 7, 'alter:table', false)",
		metadataSQL,
	)
	require.Contains(t, snapshotSQL, "insert into mo_catalog.mo_snapshots")
	require.Contains(t, snapshotSQL, "'snapshot-id', '__mo_branch_22', 123456")
	require.Contains(t, snapshotSQL, "'tenant''o', 'db''x', 'tbl''y', 11, 'branch'")
}

func TestAlterDataBranchHistoricalSourceSQL(t *testing.T) {
	for _, sql := range []string{
		alterDataBranchHistoricalSnapshotSourceSQL("tenant'o", "db'x", "tbl'y", 42),
		alterDataBranchHistoricalPitrSourceSQL("tenant'o", "db'x", "tbl'y", 42),
	} {
		require.Contains(t, sql, "account_name = 'tenant''o'")
		require.Contains(t, sql, "database_name = 'db''x'")
		require.Contains(t, sql, "table_name = 'tbl''y'")
		require.Contains(t, sql, "obj_id = 42")
		require.Contains(t, sql, "limit 1 for update")
	}
}

func TestAlterTableHasLatestHistoricalBranchSourceUsesFreshUnlockedProbe(t *testing.T) {
	const (
		oldTableID = uint64(42)
		database   = "test"
		table      = "dept"
	)
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	snapshotSQL := alterDataBranchHistoricalSnapshotSourceProbeSQL(
		"", database, table, oldTableID, false, 0,
	)
	spyExec.results[snapshotSQL] = newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)

	hasHistory, err := c.alterTableHasLatestHistoricalBranchSource(oldTableID, database, table)
	require.NoError(t, err)
	require.True(t, hasHistory)
	require.NotContains(t, snapshotSQL, "for update")
	require.Equal(t, []string{snapshotSQL}, spyExec.executedSQLs)
}

func TestAlterDataBranchLineageMetadata(t *testing.T) {
	dag := databranchutils.NewBranchReclaimDag([]databranchutils.DataBranchMetadata{
		{TableID: 2, PTableID: 1, Creator: 9, Level: "table", TableDeleted: false},
	})

	creator, level := alterDataBranchLineageMetadata(dag, 2)
	require.Equal(t, uint32(9), creator)
	require.Equal(t, "alter:table", level)

	creator, level = alterDataBranchLineageMetadata(dag, 1)
	require.Equal(t, uint32(catalog.System_Account), creator)
	require.Equal(t, "alter", level)
}

func TestValidateAlterDataBranchLineageTxn(t *testing.T) {
	require.NoError(t, validateAlterDataBranchLineageTxn("ALTER", false, true, true))
	require.NoError(t, validateAlterDataBranchLineageTxn("ALTER", false, true, false))

	for _, tc := range []struct {
		name        string
		statement   string
		byBegin     bool
		autocommit  bool
		pessimistic bool
		want        string
	}{
		{
			name:        "explicit begin",
			statement:   "ALTER",
			byBegin:     true,
			autocommit:  true,
			pessimistic: true,
			want:        "not supported inside an explicit transaction",
		},
		{
			name:        "autocommit disabled",
			statement:   "ALTER",
			autocommit:  false,
			pessimistic: true,
			want:        "not supported inside an explicit transaction",
		},
		{
			name:        "truncate explicit begin identifies statement",
			statement:   "TRUNCATE",
			byBegin:     true,
			autocommit:  true,
			pessimistic: true,
			want:        "TRUNCATE on a data-branch lineage is not supported inside an explicit transaction",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateAlterDataBranchLineageTxn(tc.statement, tc.byBegin, tc.autocommit, tc.pessimistic)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestPrepareAlterDataBranchLineageRejectsLiveBranchTxnWithStatement(t *testing.T) {
	const (
		oldTableID    = uint64(42)
		parentTableID = uint64(41)
		database      = "test"
		table         = "dept"
	)
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{ByBegin: true, Autocommit: true})
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{})
	c.proc.Base.TxnOperator = txnOp

	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	metadataSQL := "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata"
	spyExec.results[participationSQL] = newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)
	spyExec.results[metadataSQL] = newAlterLineageMetadataResult(
		t, c.proc.Mp(), []uint64{oldTableID}, []uint64{parentTableID}, []int64{100},
		[]uint64{uint64(catalog.System_Account)}, []string{"table"}, []bool{false},
	)

	lineagePlan, err := c.prepareAlterDataBranchLineage(oldTableID, database, table, "TRUNCATE")
	require.ErrorContains(t, err, "TRUNCATE on a data-branch lineage is not supported inside an explicit transaction")
	require.False(t, lineagePlan.enabled)
	require.Equal(t, []string{participationSQL, metadataSQL}, spyExec.executedSQLs)
}

func TestPrepareAlterDataBranchLineageRejectsImplicitCommitOrigin(t *testing.T) {
	const (
		oldTableID    = uint64(42)
		parentTableID = uint64(41)
		database      = "test"
		table         = "dept"
	)
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true})
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{})
	c.proc.Base.TxnOperator = txnOp
	c.proc.ReplaceTopCtx(context.WithValue(
		c.proc.GetTopContext(),
		defines.ImplicitCommitFromExplicitTxn{},
		true,
	))

	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	metadataSQL := "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata"
	spyExec.results[participationSQL] = newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)
	spyExec.results[metadataSQL] = newAlterLineageMetadataResult(
		t, c.proc.Mp(), []uint64{oldTableID}, []uint64{parentTableID}, []int64{100},
		[]uint64{uint64(catalog.System_Account)}, []string{"table"}, []bool{false},
	)

	lineagePlan, err := c.prepareAlterDataBranchLineage(oldTableID, database, table, "TRUNCATE")
	require.ErrorContains(t, err, "TRUNCATE on a data-branch lineage is not supported inside an explicit transaction")
	require.False(t, lineagePlan.enabled)
	require.Equal(t, []string{participationSQL, metadataSQL}, spyExec.executedSQLs)
}

func TestPrepareAlterDataBranchLineageAllowsHistoricalSourceTxn(t *testing.T) {
	const (
		oldTableID = uint64(42)
		database   = "test"
		table      = "dept"
	)
	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	snapshotSQL := alterDataBranchHistoricalSnapshotSourceSQL("", database, table, oldTableID)
	pitrSQL := alterDataBranchHistoricalPitrSourceSQL("", database, table, oldTableID)

	for _, tc := range []struct {
		name     string
		history  string
		wantSQLs []string
	}{
		{
			name:     "snapshot",
			history:  snapshotSQL,
			wantSQLs: []string{participationSQL, snapshotSQL},
		},
		{
			name:     "pitr",
			history:  pitrSQL,
			wantSQLs: []string{participationSQL, snapshotSQL, pitrSQL},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			spyExec.results[tc.history] = newAlterCopyFixedResult(
				t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
			)

			lineagePlan, err := c.prepareAlterDataBranchLineage(oldTableID, database, table, "ALTER")
			require.NoError(t, err)
			require.True(t, lineagePlan.enabled)
			require.True(t, lineagePlan.preserveHistoricalSource)
			require.Equal(t, tc.wantSQLs, spyExec.executedSQLs)
		})
	}
}

func TestInspectAlterDataBranchLineageSeesBranchCreatedAfterPreflight(t *testing.T) {
	const (
		oldTableID    = uint64(42)
		parentTableID = uint64(41)
		database      = "test"
		table         = "dept"
	)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	spyExec := &alterCopyInsertSpyExecutor{
		resultSequences: make(map[string][]executor.Result),
		results:         make(map[string]executor.Result),
	}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{Autocommit: true}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	c.proc.Base.TxnOperator = txnOperator
	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	metadataSQL := "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata"
	emptyParticipation := newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{},
	)
	liveParticipation := newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)
	spyExec.resultSequences[participationSQL] = []executor.Result{
		emptyParticipation, liveParticipation,
	}
	spyExec.results[metadataSQL] = newAlterLineageMetadataResult(
		t, c.proc.Mp(), []uint64{oldTableID}, []uint64{parentTableID}, []int64{100},
		[]uint64{uint64(catalog.System_Account)}, []string{"table"}, []bool{false},
	)

	preflight, err := c.inspectAlterDataBranchLineage(
		oldTableID, database, table, "ALTER", false,
	)
	require.NoError(t, err)
	require.False(t, preflight.participates)
	require.False(t, preflight.plan.enabled)

	afterGate, err := c.inspectAlterDataBranchLineage(
		oldTableID, database, table, "ALTER", true,
	)
	require.NoError(t, err)
	require.True(t, afterGate.participates)
	require.True(t, afterGate.plan.enabled)
	require.Equal(t, []string{
		participationSQL,
		alterDataBranchHistoricalSnapshotSourceProbeSQL("", database, table, oldTableID, false, 0),
		alterDataBranchHistoricalPitrSourceProbeSQL("", database, table, oldTableID, false, 0),
		participationSQL,
		metadataSQL,
	}, spyExec.executedSQLs)
}

func TestPrepareAlterDataBranchLineageAllowsHistoricalOnlyGenerationInExplicitTxn(t *testing.T) {
	const (
		oldTableID    = uint64(42)
		parentTableID = uint64(41)
		database      = "test"
		table         = "dept"
		cloneTS       = int64(100)
	)
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{
		results:         make(map[string]executor.Result),
		resultSequences: make(map[string][]executor.Result),
	}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{ByBegin: true, Autocommit: true}).AnyTimes()
	txnOp.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: cloneTS + 1}).AnyTimes()
	c.proc.Base.TxnOperator = txnOp

	participationSQL := alterDataBranchParticipationSQL(oldTableID)
	metadataSQL := "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata"
	lockedMetadataSQL := metadataSQL + " for update"
	edgeSQL := alterDataBranchLineageEdgeSQL()
	snapshotSourceSQL := alterDataBranchSnapshotSourceSQL()
	pitrSourceSQL := alterDataBranchPitrSourceSQL()
	spyExec.results[participationSQL] = newAlterCopyFixedResult(
		t, c.proc.Mp(), types.T_int32.ToType(), []int32{1},
	)
	newMetadataResult := func() executor.Result {
		return newAlterLineageMetadataResult(
			t, c.proc.Mp(), []uint64{oldTableID}, []uint64{parentTableID}, []int64{cloneTS},
			[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
		)
	}
	spyExec.resultSequences[metadataSQL] = []executor.Result{newMetadataResult(), newMetadataResult()}
	spyExec.results[lockedMetadataSQL] = newMetadataResult()
	spyExec.results[edgeSQL] = newAlterLineageEdgeResult(
		t, c.proc.Mp(), []string{databranchutils.BranchSnapshotName(oldTableID)}, []int64{cloneTS},
		[]string{""}, []string{database}, []string{table}, []uint64{parentTableID},
	)
	spyExec.results[snapshotSourceSQL] = newAlterLineageSnapshotSourceResult(
		t, c.proc.Mp(), []int64{cloneTS - 1}, []string{"table"}, []string{""},
		[]string{database}, []string{table}, []uint64{parentTableID},
	)
	spyExec.results[pitrSourceSQL] = newAlterLineagePitrSourceResult(
		t, c.proc.Mp(), nil, nil, nil, nil, nil, nil, nil,
	)

	lineagePlan, err := c.prepareAlterDataBranchLineage(oldTableID, database, table, "ALTER")
	require.NoError(t, err)
	require.True(t, lineagePlan.enabled)
	require.False(t, lineagePlan.preserveHistoricalSource)
	require.Equal(t, []string{
		participationSQL,
		metadataSQL,
		lockedMetadataSQL,
		edgeSQL,
		snapshotSourceSQL,
		pitrSourceSQL,
		metadataSQL,
	}, spyExec.executedSQLs)
}

func TestShouldAdvanceAlterDataBranchLineageSnapshot(t *testing.T) {
	require.True(t, shouldAdvanceAlterDataBranchLineageSnapshot(true, true))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(true, false))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(false, true))
	require.False(t, shouldAdvanceAlterDataBranchLineageSnapshot(false, false))
}

func TestAdvanceAlterDataBranchLineageSnapshotUsesWorkspace(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	ws := mock_frontend.NewMockWorkspace(ctrl)
	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = op
	gomock.InOrder(
		op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 1000}),
		op.EXPECT().GetWorkspace().Return(ws),
		ws.EXPECT().AdvanceSnapshot(proc.Ctx, timestamp.Timestamp{PhysicalTime: 2000}).Return(nil),
		op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 2001}),
	)
	cloneTS, err := (&Compile{proc: proc}).advanceAlterDataBranchLineageSnapshot()
	require.NoError(t, err)
	require.Equal(t, int64(2000), cloneTS)
}

func TestIsAlterAffectedPluginIndexMatchesIndexNamePartsAndIncludedColumns(t *testing.T) {
	indexDef := &plan2.IndexDef{
		IndexName:       "idx_vec",
		Parts:           []string{"embedding"},
		IncludedColumns: []string{"doc_id", catalog.CreateAlias("category")},
	}

	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"idx_vec"}))
	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"embedding"}))
	require.True(t, isAlterAffectedPluginIndex(indexDef, []string{"category"}))
	require.False(t, isAlterAffectedPluginIndex(indexDef, []string{"other"}))
	require.False(t, isAlterAffectedPluginIndex(indexDef, nil))
	require.False(t, isAlterAffectedPluginIndex(nil, []string{"idx_vec"}))
}

func TestIsAlterRebuiltPluginIndexKeepsIdentitySeparateFromColumns(t *testing.T) {
	existing := &plan2.IndexDef{
		IndexName: "ft_existing",
		Parts:     []string{"body"},
	}
	newIndex := &plan2.IndexDef{
		IndexName: "body",
		Parts:     []string{"content"},
	}
	newPluginIndexes := map[string]bool{"body": true}

	require.False(t, isAlterRebuiltPluginIndex(existing, nil, newPluginIndexes),
		"a new index name equal to an existing index column must not rebuild the existing index")
	require.True(t, isAlterRebuiltPluginIndex(newIndex, nil, newPluginIndexes))
	require.True(t, isAlterRebuiltPluginIndex(existing, []string{"body"}, newPluginIndexes))
	require.False(t, isAlterRebuiltPluginIndex(nil, []string{"body"}, newPluginIndexes))
}

func TestCloneAlterCopyOptClonesNewPluginIndexes(t *testing.T) {
	source := &plan2.AlterCopyOpt{
		SkipUniqueIdxDedup: map[string]bool{"uk": true},
		SkipIndexesCopy:    map[string]bool{"idx": true},
		NewPluginIndexes:   map[string]bool{"ft": true},
	}

	cloned := cloneAlterCopyOpt(source)
	require.Equal(t, source, cloned)
	cloned.NewPluginIndexes["ft"] = false
	require.True(t, source.NewPluginIndexes["ft"])
}

func TestReplaceRefChildTableID(t *testing.T) {
	t.Run("replace altered child and preserve siblings", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Equal(t, []uint64{10, 21, 30}, canonicalRefChildTableIDs(constraintDef))
	})

	t.Run("do not invent a missing child reference", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 30}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Equal(t, []uint64{10, 30}, canonicalRefChildTableIDs(constraintDef))
	})

	t.Run("canonicalize duplicate definitions and table ids", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 21}},
			&engine.RefChildTableDef{Tables: []uint64{20, 30, 0}},
			&engine.RefChildTableDef{Tables: []uint64{0}},
		}}
		replaceRefChildTableID(constraintDef, 20, 21)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{10, 21, 30, 0},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})

	t.Run("keep an empty reference list empty", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{}
		replaceRefChildTableID(constraintDef, 20, 21)
		require.Len(t, constraintDef.Cts, 1)
		require.Empty(t, canonicalRefChildTableIDs(constraintDef))
	})
}

func TestTruncateRefChildTableIDReplacementCanonicalizesLegacyState(t *testing.T) {
	constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.RefChildTableDef{Tables: []uint64{0, 10, 20}},
		&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		&engine.RefChildTableDef{Tables: []uint64{0}},
	}}

	replaceRefChildTableID(constraintDef, 20, 21)

	require.Len(t, constraintDef.Cts, 1)
	require.Equal(
		t,
		[]uint64{0, 10, 21, 30},
		constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
	)
}

func TestCanonicalRefChildTableIDMutations(t *testing.T) {
	t.Run("add merges definitions and deduplicates sentinel", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{0, 10}},
			&engine.RefChildTableDef{Tables: []uint64{10, 20}},
		}}

		addRefChildTableIDs(constraintDef, []uint64{0, 20, 30})

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{0, 10, 20, 30},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})

	t.Run("remove deletes every duplicate and keeps other ids", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{0, 10, 20}},
			&engine.RefChildTableDef{Tables: []uint64{10, 30}},
		}}

		removeRefChildTableID(constraintDef, 10)

		require.Len(t, constraintDef.Cts, 1)
		require.Equal(
			t,
			[]uint64{0, 20, 30},
			constraintDef.Cts[0].(*engine.RefChildTableDef).Tables,
		)
	})
}

func TestRewriteForeignKeyReferencesForAlterCopy(t *testing.T) {
	constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{
			{ForeignTbl: 10, ForeignCols: []uint64{1, 2}},
			{ForeignTbl: 10, ForeignCols: []uint64{3}},
			{ForeignTbl: 20, ForeignCols: []uint64{1}},
		}},
	}}

	changed, err := rewriteForeignKeyReferencesForAlterCopy(
		context.Background(),
		constraintDef,
		map[uint64]*plan2.ColDef{1: {ColId: 101}, 3: {ColId: 103}},
		10,
		11,
	)
	require.NoError(t, err)
	require.True(t, changed)

	fkeys := constraintDef.Cts[0].(*engine.ForeignKeyDef).Fkeys
	require.Equal(t, uint64(11), fkeys[0].ForeignTbl)
	require.Equal(t, []uint64{101, 2}, fkeys[0].ForeignCols)
	require.Equal(t, uint64(11), fkeys[1].ForeignTbl)
	require.Equal(t, []uint64{103}, fkeys[1].ForeignCols)
	require.Equal(t, uint64(20), fkeys[2].ForeignTbl)
	require.Equal(t, []uint64{1}, fkeys[2].ForeignCols)

	changed, err = rewriteForeignKeyReferencesForAlterCopy(context.Background(), constraintDef, nil, 10, 11)
	require.NoError(t, err)
	require.False(t, changed)

	_, err = rewriteForeignKeyReferencesForAlterCopy(context.Background(), &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{nil}},
	}}, nil, 10, 11)
	require.ErrorContains(t, err, "nil foreign key definition")
}

func TestRemapAlterCopyForeignKeyState(t *testing.T) {
	source := []*plan2.ForeignKeyDef{
		{Name: "fk_parent", Cols: []uint64{1}, ForeignTbl: 20, ForeignCols: []uint64{7}},
		{Name: "fk_self", Cols: []uint64{2}, ForeignTbl: 0, ForeignCols: []uint64{1}},
		{Name: "fk_legacy_self", Cols: []uint64{1}, ForeignTbl: 10, ForeignCols: []uint64{2}},
	}
	remapped, refChildTbls, err := remapAlterCopyForeignKeyState(
		context.Background(),
		source,
		[]uint64{30, 10, 0, 30},
		map[uint64]*plan2.ColDef{1: {ColId: 101}, 2: {ColId: 102}},
		10,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{101}, remapped[0].Cols)
	require.Equal(t, uint64(20), remapped[0].ForeignTbl)
	require.Equal(t, []uint64{7}, remapped[0].ForeignCols)
	require.Equal(t, []uint64{102}, remapped[1].Cols)
	require.Equal(t, uint64(0), remapped[1].ForeignTbl)
	require.Equal(t, []uint64{101}, remapped[1].ForeignCols)
	require.Equal(t, uint64(0), remapped[2].ForeignTbl)
	require.Equal(t, []uint64{102}, remapped[2].ForeignCols)
	require.Equal(t, []uint64{30, 0}, refChildTbls)

	// The source relation constraint is still needed until the replacement is
	// published, so remapping must not mutate it in place.
	require.Equal(t, []uint64{1}, source[0].Cols)
	require.Equal(t, []uint64{1}, source[1].ForeignCols)
	require.Equal(t, uint64(10), source[2].ForeignTbl)

	_, _, err = remapAlterCopyForeignKeyState(
		context.Background(), source[:1], nil, map[uint64]*plan2.ColDef{}, 10,
	)
	require.ErrorContains(t, err, "was not retained")
}

func TestSnapshotAlterCopyForeignKeyState(t *testing.T) {
	ctrl := gomock.NewController(t)
	relation := mock_frontend.NewMockRelation(ctrl)
	sourceForeignKey1 := &plan2.ForeignKeyDef{
		Name: "fk_parent", Cols: []uint64{1}, ForeignTbl: 20, ForeignCols: []uint64{7},
	}
	sourceForeignKey2 := &plan2.ForeignKeyDef{
		Name: "fk_other_parent", Cols: []uint64{2}, ForeignTbl: 21, ForeignCols: []uint64{8},
	}
	relation.EXPECT().TableDefs(gomock.Any()).Return([]engine.TableDef{
		&engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{sourceForeignKey1}},
			&engine.RefChildTableDef{Tables: []uint64{30, 31}},
			&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{sourceForeignKey2}},
			&engine.RefChildTableDef{Tables: []uint64{31, 32}},
		}},
	}, nil)

	foreignKeys, refChildTbls, err := snapshotAlterCopyForeignKeyState(context.Background(), relation)
	require.NoError(t, err)
	require.Equal(t, []*plan2.ForeignKeyDef{sourceForeignKey1, sourceForeignKey2}, foreignKeys)
	require.Equal(t, []uint64{30, 31, 32}, refChildTbls)

	foreignKeys[0].Cols[0] = 101
	refChildTbls[0] = 130
	require.Equal(t, []uint64{1}, sourceForeignKey1.Cols)
}

func TestRestoreAlterCopyForeignKeyStateUsesExactLiveSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	relation := mock_frontend.NewMockRelation(ctrl)
	constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.IndexDef{},
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{{
			Name: "stale_fk", ForeignTbl: 20,
		}}},
		&engine.RefChildTableDef{Tables: []uint64{0, 30}},
	}}

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(
		_ context.Context, got engine.Relation,
	) (*engine.ConstraintDef, error) {
		require.Same(t, relation, got)
		return constraintDef, nil
	})
	defer getConstraintDef.Reset()
	relation.EXPECT().UpdateConstraint(gomock.Any(), constraintDef).Return(nil).Times(1)

	// The live source has no foreign keys. Restoring that exact empty set must
	// remove a stale planned FK installed by the temporary CREATE.
	require.NoError(t, restoreAlterCopyForeignKeyState(proc.Ctx, relation, nil, nil))
	require.Len(t, constraintDef.Cts, 3)
	var foreignKeyDef *engine.ForeignKeyDef
	hasIndexDef := false
	for _, constraint := range constraintDef.Cts {
		switch definition := constraint.(type) {
		case *engine.ForeignKeyDef:
			foreignKeyDef = definition
		case *engine.IndexDef:
			hasIndexDef = true
		}
	}
	require.True(t, hasIndexDef)
	require.NotNil(t, foreignKeyDef)
	require.Empty(t, foreignKeyDef.Fkeys)

	require.Empty(t, canonicalRefChildTableIDs(constraintDef))
}

func TestApplyAlterCopyForeignKeyStateCanonicalizesLegacySelfReference(t *testing.T) {
	for _, reverseMarker := range []uint64{0, 10} {
		t.Run(fmt.Sprintf("reverse marker %d", reverseMarker), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			proc := testutil.NewProcess(t)
			replacement := mock_frontend.NewMockRelation(ctrl)
			constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
				&engine.ForeignKeyDef{},
				&engine.RefChildTableDef{},
			}}
			sourceForeignKey := &plan2.ForeignKeyDef{
				Name: "fk_self", Cols: []uint64{1}, ForeignTbl: 10, ForeignCols: []uint64{1},
			}

			getConstraintDef := gostub.Stub(&GetConstraintDef, func(
				_ context.Context, got engine.Relation,
			) (*engine.ConstraintDef, error) {
				require.Same(t, replacement, got)
				return constraintDef, nil
			})
			defer getConstraintDef.Reset()
			replacement.EXPECT().UpdateConstraint(gomock.Any(), constraintDef).DoAndReturn(
				func(_ context.Context, _ *engine.ConstraintDef) error {
					var restored []*plan2.ForeignKeyDef
					for _, constraint := range constraintDef.Cts {
						if definition, ok := constraint.(*engine.ForeignKeyDef); ok {
							restored = definition.Fkeys
						}
					}
					require.Len(t, restored, 1)
					require.Equal(t, uint64(0), restored[0].ForeignTbl)
					require.Equal(t, []uint64{101}, restored[0].Cols)
					require.Equal(t, []uint64{101}, restored[0].ForeignCols)
					require.Equal(t, []uint64{0}, canonicalRefChildTableIDs(constraintDef))
					return nil
				},
			)

			// A self-only state must not resolve either the dropped old generation
			// or the replacement generation as an external relation.
			eng := mock_frontend.NewMockEngine(ctrl)
			c := NewCompile("test", "test", "alter table self_ref add column v int", "", "", eng, proc, nil, false, nil, time.Now())
			require.NoError(t, applyAlterCopyForeignKeyState(
				c,
				replacement,
				[]*plan2.ForeignKeyDef{sourceForeignKey},
				nil,
				[]uint64{reverseMarker},
				map[uint64]*plan2.ColDef{1: {ColId: 101}},
				10,
				11,
			))
			require.Equal(t, uint64(10), sourceForeignKey.ForeignTbl)
		})
	}
}

func TestCollectAlterCopyAddedForeignKeys(t *testing.T) {
	qry := &plan2.AlterTable{
		Database: "db",
		TableDef: &plan2.TableDef{Name: "child"},
		Actions: []*plan2.AlterTable_Action{
			{Action: &plan2.AlterTable_Action_AddFk{AddFk: &plan2.AlterTableAddFk{
				DbName: "db", TableName: "child", Cols: []string{"parent_id"},
				Fkey: &plan2.ForeignKeyDef{Name: "fk_self"},
			}}},
			{Action: &plan2.AlterTable_Action_AddFk{AddFk: &plan2.AlterTableAddFk{
				DbName: "db", TableName: "parent", Cols: []string{"parent_id"},
				Fkey: &plan2.ForeignKeyDef{Name: "fk_parent"},
			}}},
		},
	}
	replacement := &plan2.TableDef{Fkeys: []*plan2.ForeignKeyDef{
		{Name: "fk_existing", Cols: []uint64{101}, ForeignTbl: 50, ForeignCols: []uint64{51}},
		{Name: "FK_SELF", Cols: []uint64{102}, ForeignTbl: 0, ForeignCols: []uint64{101}},
		{Name: "fk_parent", Cols: []uint64{102}, ForeignTbl: 50, ForeignCols: []uint64{51}},
	}}

	foreignKeys, err := collectAlterCopyAddedForeignKeys(
		context.Background(), qry, replacement,
	)
	require.NoError(t, err)
	require.Len(t, foreignKeys, 2)
	require.Equal(t, []uint64{102}, foreignKeys[0].Cols)
	require.Equal(t, []uint64{101}, foreignKeys[0].ForeignCols)
	require.Equal(t, uint64(0), foreignKeys[0].ForeignTbl)
	require.Equal(t, []uint64{102}, foreignKeys[1].Cols)
	require.Equal(t, []uint64{51}, foreignKeys[1].ForeignCols)
	require.Equal(t, uint64(50), foreignKeys[1].ForeignTbl)
	foreignKeys[0].Cols[0] = 999
	require.Equal(t, []uint64{102}, replacement.Fkeys[1].Cols)

	merged, refChildren, err := mergeAlterCopyAddedForeignKeys(
		context.Background(),
		[]*plan2.ForeignKeyDef{{Name: "fk_existing"}},
		[]uint64{7},
		foreignKeys,
	)
	require.NoError(t, err)
	require.Len(t, merged, 3)
	require.Equal(t, []uint64{7, 0}, refChildren)
}

func TestCollectAlterCopyAddedForeignKeysPreservesActionOrigins(t *testing.T) {
	actionForeignKeys := []*plan2.ForeignKeyDef{
		{
			Name:           "fk_default",
			Cols:           []uint64{1},
			ForeignTbl:     2,
			ForeignCols:    []uint64{3},
			OnDelete:       plan2.ForeignKeyDef_NO_ACTION,
			OnUpdate:       plan2.ForeignKeyDef_NO_ACTION,
			OnDeleteOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_DEFAULT,
			OnUpdateOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_DEFAULT,
		},
		{
			Name:           "fk_restrict",
			Cols:           []uint64{4},
			ForeignTbl:     5,
			ForeignCols:    []uint64{6},
			OnDelete:       plan2.ForeignKeyDef_RESTRICT,
			OnUpdate:       plan2.ForeignKeyDef_RESTRICT,
			OnDeleteOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
			OnUpdateOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
		},
		{
			Name:           "fk_no_action",
			Cols:           []uint64{7},
			ForeignTbl:     8,
			ForeignCols:    []uint64{9},
			OnDelete:       plan2.ForeignKeyDef_NO_ACTION,
			OnUpdate:       plan2.ForeignKeyDef_NO_ACTION,
			OnDeleteOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
			OnUpdateOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
		},
	}
	qry := &plan2.AlterTable{}
	replacement := &plan2.TableDef{}
	for i, actionForeignKey := range actionForeignKeys {
		qry.Actions = append(qry.Actions, &plan2.AlterTable_Action{
			Action: &plan2.AlterTable_Action_AddFk{AddFk: &plan2.AlterTableAddFk{
				Fkey: actionForeignKey,
			}},
		})
		replacement.Fkeys = append(replacement.Fkeys, &plan2.ForeignKeyDef{
			Name:           actionForeignKey.Name,
			Cols:           []uint64{uint64(101 + i)},
			ForeignTbl:     uint64(201 + i),
			ForeignCols:    []uint64{uint64(301 + i)},
			OnDelete:       actionForeignKey.OnDelete,
			OnUpdate:       actionForeignKey.OnUpdate,
			OnDeleteOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
			OnUpdateOrigin: plan2.ForeignKeyDef_ACTION_ORIGIN_EXPLICIT,
		})
	}

	foreignKeys, err := collectAlterCopyAddedForeignKeys(
		context.Background(), qry, replacement,
	)
	require.NoError(t, err)
	require.Len(t, foreignKeys, len(actionForeignKeys))
	for i, foreignKey := range foreignKeys {
		require.Equal(t, []uint64{uint64(101 + i)}, foreignKey.Cols)
		require.Equal(t, uint64(201+i), foreignKey.ForeignTbl)
		require.Equal(t, []uint64{uint64(301 + i)}, foreignKey.ForeignCols)
		require.Equal(t, actionForeignKeys[i].OnDelete, foreignKey.OnDelete)
		require.Equal(t, actionForeignKeys[i].OnUpdate, foreignKey.OnUpdate)
		require.Equal(t, actionForeignKeys[i].OnDeleteOrigin, foreignKey.OnDeleteOrigin)
		require.Equal(t, actionForeignKeys[i].OnUpdateOrigin, foreignKey.OnUpdateOrigin)
	}

	foreignKeys[0].Cols[0] = 999
	require.Equal(t, uint64(101), replacement.Fkeys[0].Cols[0],
		"the collected definition must not alias recreated table metadata")
}

func TestCollectAlterCopyAddedForeignKeysRejectsInconsistentPlan(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name        string
		qry         *plan2.AlterTable
		replacement *plan2.TableDef
		wantError   string
	}{
		{
			name:        "nil replacement foreign key",
			qry:         &plan2.AlterTable{},
			replacement: &plan2.TableDef{Fkeys: []*plan2.ForeignKeyDef{nil}},
			wantError:   "nil foreign key definition in ALTER COPY replacement",
		},
		{
			name: "nil action foreign key",
			qry: &plan2.AlterTable{Actions: []*plan2.AlterTable_Action{{
				Action: &plan2.AlterTable_Action_AddFk{AddFk: &plan2.AlterTableAddFk{}},
			}}},
			replacement: &plan2.TableDef{},
			wantError:   "nil foreign key definition in ALTER COPY action",
		},
		{
			name: "action foreign key missing from replacement",
			qry: &plan2.AlterTable{Actions: []*plan2.AlterTable_Action{{
				Action: &plan2.AlterTable_Action_AddFk{AddFk: &plan2.AlterTableAddFk{
					Fkey: &plan2.ForeignKeyDef{Name: "fk_missing"},
				}},
			}}},
			replacement: &plan2.TableDef{},
			wantError:   "foreign key fk_missing was not created by ALTER COPY",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreignKeys, err := collectAlterCopyAddedForeignKeys(ctx, tc.qry, tc.replacement)
			require.Nil(t, foreignKeys)
			require.ErrorContains(t, err, tc.wantError)
		})
	}

	for _, tc := range []struct {
		name        string
		qry         *plan2.AlterTable
		replacement *plan2.TableDef
	}{
		{name: "nil query", replacement: &plan2.TableDef{}},
		{name: "nil replacement", qry: &plan2.AlterTable{}},
		{
			name: "non foreign key actions are ignored",
			qry: &plan2.AlterTable{Actions: []*plan2.AlterTable_Action{
				nil,
				{},
			}},
			replacement: &plan2.TableDef{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreignKeys, err := collectAlterCopyAddedForeignKeys(ctx, tc.qry, tc.replacement)
			require.NoError(t, err)
			require.Empty(t, foreignKeys)
		})
	}
}

func TestMergeAlterCopyAddedForeignKeysRejectsInvalidState(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name              string
		sourceForeignKeys []*plan2.ForeignKeyDef
		addedForeignKeys  []*plan2.ForeignKeyDef
		wantError         string
	}{
		{
			name:              "nil source foreign key",
			sourceForeignKeys: []*plan2.ForeignKeyDef{nil},
			wantError:         "nil foreign key definition in ALTER COPY",
		},
		{
			name:             "nil added foreign key",
			addedForeignKeys: []*plan2.ForeignKeyDef{nil},
			wantError:        "nil added foreign key definition in ALTER COPY",
		},
		{
			name:              "duplicate name is case insensitive",
			sourceForeignKeys: []*plan2.ForeignKeyDef{{Name: "fk_parent"}},
			addedForeignKeys:  []*plan2.ForeignKeyDef{{Name: "FK_PARENT"}},
			wantError:         "duplicate foreign key FK_PARENT in ALTER COPY",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			foreignKeys, refChildren, err := mergeAlterCopyAddedForeignKeys(
				ctx, tc.sourceForeignKeys, nil, tc.addedForeignKeys,
			)
			require.Nil(t, foreignKeys)
			require.Nil(t, refChildren)
			require.ErrorContains(t, err, tc.wantError)
		})
	}

	t.Run("existing self marker is not duplicated", func(t *testing.T) {
		foreignKeys, refChildren, err := mergeAlterCopyAddedForeignKeys(
			ctx,
			nil,
			[]uint64{0},
			[]*plan2.ForeignKeyDef{{Name: "fk_self", ForeignTbl: 0}},
		)
		require.NoError(t, err)
		require.Len(t, foreignKeys, 1)
		require.Equal(t, []uint64{0}, refChildren)
	})
}

func TestReconcileRefChildTableIDForAlterCopy(t *testing.T) {
	t.Run("replace child in existing reverse reference", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{Cts: []engine.Constraint{
			&engine.RefChildTableDef{Tables: []uint64{10, 20, 30}},
		}}
		reconcileRefChildTableID(constraintDef, 20, 21)

		require.Equal(t, []uint64{10, 21, 30}, canonicalRefChildTableIDs(constraintDef))
	})

	t.Run("restore reverse reference removed while dropping old child", func(t *testing.T) {
		constraintDef := &engine.ConstraintDef{}
		reconcileRefChildTableID(constraintDef, 20, 21)

		require.Equal(t, []uint64{21}, canonicalRefChildTableIDs(constraintDef))
	})
}

func TestReconcileAlterCopyForeignKeyReferencesOncePerRelation(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)

	childUpdated := mock_frontend.NewMockRelation(ctrl)
	childUnchanged := mock_frontend.NewMockRelation(ctrl)
	parentOne := mock_frontend.NewMockRelation(ctrl)
	parentTwo := mock_frontend.NewMockRelation(ctrl)

	childUpdatedConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{{ForeignTbl: 1}}},
	}}
	childUnchangedConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{{ForeignTbl: 99}}},
	}}
	parentOneConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.RefChildTableDef{Tables: []uint64{1}},
	}}
	parentTwoConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.RefChildTableDef{Tables: []uint64{1}},
	}}

	childUpdated.EXPECT().UpdateConstraint(gomock.Any(), childUpdatedConstraint).Return(nil).Times(1)
	parentOne.EXPECT().UpdateConstraint(gomock.Any(), parentOneConstraint).Return(nil).Times(1)
	parentTwo.EXPECT().UpdateConstraint(gomock.Any(), parentTwoConstraint).Return(nil).Times(1)

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(10)).Return("", "", childUpdated, nil).Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(20)).Return("", "", childUnchanged, nil).Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(30)).Return("", "", parentOne, nil).Times(1)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(40)).Return("", "", parentTwo, nil).Times(1)

	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		switch rel {
		case childUpdated:
			return childUpdatedConstraint, nil
		case childUnchanged:
			return childUnchangedConstraint, nil
		case parentOne:
			return parentOneConstraint, nil
		case parentTwo:
			return parentTwoConstraint, nil
		default:
			t.Fatalf("unexpected relation passed to GetConstraintDef")
			return nil, nil
		}
	})
	defer getConstraintDef.Reset()

	c := NewCompile("test", "test", "alter table child", "", "", eng, proc, nil, false, nil, time.Now())
	require.NoError(t, reconcileAlterCopyChildForeignKeyReferences(c, nil, []uint64{10, 10, 0, 20}, 1, 2))
	require.NoError(t, reconcileAlterCopyParentForeignKeyReferences(c, []*plan2.ForeignKeyDef{
		{ForeignTbl: 30},
		{ForeignTbl: 30},
		{ForeignTbl: 0},
		{ForeignTbl: 40},
	}, 1, 2))

	require.Equal(t, uint64(2), childUpdatedConstraint.Cts[0].(*engine.ForeignKeyDef).Fkeys[0].ForeignTbl)
	require.Equal(t, uint64(99), childUnchangedConstraint.Cts[0].(*engine.ForeignKeyDef).Fkeys[0].ForeignTbl)
	require.Equal(t, []uint64{2}, canonicalRefChildTableIDs(parentOneConstraint))
	require.Equal(t, []uint64{2}, canonicalRefChildTableIDs(parentTwoConstraint))
	require.ErrorContains(t, reconcileAlterCopyParentForeignKeyReferences(c, []*plan2.ForeignKeyDef{nil}, 1, 2), "nil foreign key definition")
}

func TestCheckAlterCopyForeignKeyColumnsForKeys(t *testing.T) {
	ctx := context.Background()
	affected := map[uint64]string{42: "generated_key"}

	t.Run("incoming scans every FK on a child table", func(t *testing.T) {
		foreignKeys := []*plan2.ForeignKeyDef{
			{Name: "fk_unrelated", Cols: []uint64{1}, ForeignCols: []uint64{7}},
			{Name: "fk_generated", Cols: []uint64{2}, ForeignCols: []uint64{42}},
		}
		err := checkAlterCopyForeignKeyColumnsForKeys(ctx, foreignKeys, affected, true, "db.child")
		require.ErrorContains(t, err, "fk_generated")
		require.ErrorContains(t, err, "db.child")
	})

	t.Run("outgoing child key", func(t *testing.T) {
		err := checkAlterCopyForeignKeyColumnsForKeys(ctx, []*plan2.ForeignKeyDef{{
			Name: "fk_child_generated", Cols: []uint64{42}, ForeignCols: []uint64{1},
		}}, affected, false, "")
		require.ErrorContains(t, err, "fk_child_generated")
		require.NotContains(t, err.Error(), "of table")
	})

	t.Run("self referenced parent key", func(t *testing.T) {
		err := checkAlterCopyForeignKeyColumnsForKeys(ctx, []*plan2.ForeignKeyDef{{
			Name: "fk_self_parent", Cols: []uint64{2}, ForeignCols: []uint64{42},
		}}, affected, true, "db.self_ref")
		require.ErrorContains(t, err, "fk_self_parent")
		require.ErrorContains(t, err, "db.self_ref")
	})

	t.Run("unrelated key remains allowed", func(t *testing.T) {
		err := checkAlterCopyForeignKeyColumnsForKeys(ctx, []*plan2.ForeignKeyDef{{
			Name: "fk_other", Cols: []uint64{2}, ForeignCols: []uint64{43},
		}}, affected, true, "db.child")
		require.NoError(t, err)
	})

	t.Run("malformed FK metadata fails closed", func(t *testing.T) {
		err := checkAlterCopyForeignKeyColumnsForKeys(ctx, []*plan2.ForeignKeyDef{{
			Name: "fk_malformed", Cols: []uint64{1},
		}}, affected, true, "db.child")
		require.ErrorContains(t, err, "mismatched child and parent columns")
	})
}

func TestCheckAlterCopyForeignKeyUsesLiveChildMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = context.Background()

	childRel := mock_frontend.NewMockRelation(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(200)).
		Return("db", "child", childRel, nil).Times(1)

	childConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{
			{Name: "fk_unrelated", Cols: []uint64{10}, ForeignTbl: 100, ForeignCols: []uint64{3}},
			{Name: "fk_live_generated", Cols: []uint64{11}, ForeignTbl: 100, ForeignCols: []uint64{2}},
		}},
	}}
	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		require.Equal(t, childRel, rel)
		return childConstraint, nil
	})
	defer getConstraintDef.Reset()

	typDecimal := plan2.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 1}
	typInt := plan2.Type{Id: int32(types.T_int32)}
	oldTable := &plan2.TableDef{
		TblId:         100,
		Name:          "parent",
		Name2ColIndex: map[string]int32{"source": 0, "generated_key": 1, "unrelated": 2},
		Cols: []*plan2.ColDef{
			{ColId: 1, Name: "source", Typ: typDecimal},
			{
				ColId: 2, Name: "generated_key", Typ: typInt,
				GeneratedCol: &plan2.GeneratedCol{IsStored: true, Expr: &plan2.Expr{
					Typ: typInt,
					Expr: &plan2.Expr_Col{Col: &plan2.ColRef{
						ColPos: 0, Name: "source",
					}},
				}},
			},
			{ColId: 3, Name: "unrelated", Typ: typInt},
		},
	}
	copyTable := &plan2.TableDef{
		Cols: []*plan2.ColDef{
			{ColId: 1, Name: "source", Typ: typInt},
			oldTable.Cols[1],
			oldTable.Cols[2],
		},
	}
	changeColDefMap := map[uint64]*plan2.ColDef{
		1: {Name: "source"},
		2: {Name: "generated_key"},
		3: {Name: "unrelated"},
	}
	qry := &plan2.AlterTable{
		TableDef: oldTable, CopyTableDef: copyTable, ChangeTblColIdMap: changeColDefMap,
	}
	c := NewCompile("db", "db", "alter table db.parent", "", "", eng, proc, nil, false, nil, time.Now())

	// The planner-facing TableDef deliberately has no FK metadata. Only the
	// lock-held child relation snapshot contains this newly committed FK.
	err := checkAlterCopyForeignKeyColumns(
		c, qry, oldTable, nil, []uint64{200}, oldTable.TblId, "db", oldTable.Name,
	)
	require.ErrorContains(t, err, "fk_live_generated")
	require.ErrorContains(t, err, "db.child")
}

func TestCheckAlterCopyForeignKeyUsesLiveChildMetadataWithoutGeneratedColumns(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = context.Background()

	childRel := mock_frontend.NewMockRelation(ctrl)
	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().GetRelationById(gomock.Any(), gomock.Any(), uint64(200)).
		Return("db", "child", childRel, nil).Times(1)

	childConstraint := &engine.ConstraintDef{Cts: []engine.Constraint{
		&engine.ForeignKeyDef{Fkeys: []*plan2.ForeignKeyDef{{
			Name: "fk_live_source", Cols: []uint64{11}, ForeignTbl: 100, ForeignCols: []uint64{1},
		}}},
	}}
	getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, rel engine.Relation) (*engine.ConstraintDef, error) {
		require.Equal(t, childRel, rel)
		return childConstraint, nil
	})
	defer getConstraintDef.Reset()

	typDecimalScale1 := plan2.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 1}
	typDecimalScale0 := plan2.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 0}
	oldTable := &plan2.TableDef{
		TblId: 100, Name: "parent",
		// No generated columns and no Name2ColIndex: direct FK validation must
		// not depend on generated-dependency metadata being present.
		Cols: []*plan2.ColDef{{ColId: 1, Name: "source", Typ: typDecimalScale1}},
	}
	copyTable := &plan2.TableDef{
		Cols: []*plan2.ColDef{{ColId: 1, Name: "source", Typ: typDecimalScale0}},
	}
	qry := &plan2.AlterTable{
		TableDef: oldTable, CopyTableDef: copyTable,
		ChangeTblColIdMap: map[uint64]*plan2.ColDef{1: {Name: "source"}},
	}
	c := NewCompile("db", "db", "alter table db.parent", "", "", eng, proc, nil, false, nil, time.Now())

	err := checkAlterCopyForeignKeyColumns(
		c, qry, oldTable, nil, []uint64{200}, oldTable.TblId, "db", oldTable.Name,
	)
	require.ErrorContains(t, err, "fk_live_source")
	require.ErrorContains(t, err, "db.child")
}

func TestAlterCopyAutoIncrementCleanupDiscardsTrackedReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	proc := testutil.NewProcess(t)
	proc.Ctx = context.Background()
	_, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnOperator = txnOp

	cleanupErr := errors.New("discard failed")
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(11), txnOp).Return(cleanupErr)
	autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(12), txnOp).Return(nil)
	incrservice.SetAutoIncrementServiceByID(proc.GetService(), autoSvc)

	cleanup := newAlterAutoIncrementResetCleanup(&Compile{proc: proc})
	cleanup.track(11)
	cleanup.track(11)
	cleanup.track(12)
	originalErr := errors.New("statement failed")
	statementErr := originalErr
	cleanup.finish(&statementErr)

	require.ErrorIs(t, statementErr, originalErr)
	require.ErrorIs(t, statementErr, cleanupErr)
}

type partitionAlterTestExecutor struct {
	executedSQLs []string
	failAt       int
	failErr      error
	cancel       context.CancelFunc
}

func (e *partitionAlterTestExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	index := len(e.executedSQLs)
	e.executedSQLs = append(e.executedSQLs, sql)
	if index > 0 {
		if err := ctx.Err(); err != nil {
			return executor.Result{}, err
		}
	}
	if index == e.failAt && e.failErr != nil {
		return executor.Result{}, e.failErr
	}
	if index == 0 && e.cancel != nil {
		e.cancel()
	}
	return executor.Result{}, nil
}

func (e *partitionAlterTestExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

func TestAlterPartitionTablesKeepsAutoIncrementCleanupAtStatementBoundary(t *testing.T) {
	partitionFailure := errors.New("partition alter failed")
	for _, tc := range []struct {
		name      string
		configure func(context.CancelFunc) *partitionAlterTestExecutor
		wantErr   error
	}{
		{
			name: "later partition fails",
			configure: func(context.CancelFunc) *partitionAlterTestExecutor {
				return &partitionAlterTestExecutor{failAt: 1, failErr: partitionFailure}
			},
			wantErr: partitionFailure,
		},
		{
			name: "cancel after earlier partition succeeds",
			configure: func(cancel context.CancelFunc) *partitionAlterTestExecutor {
				return &partitionAlterTestExecutor{failAt: -1, cancel: cancel}
			},
			wantErr: context.Canceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			baseCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exec := tc.configure(cancel)
			c := newAlterCopyPrecheckCompile(t, ctrl, exec)
			c.proc.Ctx = defines.AttachAccountId(baseCtx, catalog.System_Account)
			c.proc.ReplaceTopCtx(c.proc.Ctx)

			autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
			gomock.InOrder(
				autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(10), c.proc.GetTxnOperator()).Return(nil),
				autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), uint64(11), c.proc.GetTxnOperator()).Return(nil),
			)
			incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

			st, err := parsers.ParseOne(
				c.proc.Ctx,
				dialect.MYSQL,
				"alter table test.t auto_increment = 100",
				1,
			)
			require.NoError(t, err)
			cleanup := newAlterAutoIncrementResetCleanup(c)
			cleanup.track(10)
			statementErr := c.alterPartitionTables(
				st.(*tree.AlterTable),
				[]partition.Partition{
					{PartitionID: 11, PartitionTableName: "t_p0"},
					{PartitionID: 12, PartitionTableName: "t_p1"},
				},
				true,
				cleanup,
			)
			require.ErrorIs(t, statementErr, tc.wantErr)
			cleanup.finish(&statementErr)
			require.ErrorIs(t, statementErr, tc.wantErr)
			require.Len(t, exec.executedSQLs, 2)
			require.Contains(t, exec.executedSQLs[0], "`t_p0`")
			require.Contains(t, exec.executedSQLs[1], "`t_p1`")
		})
	}
}

type alterCopyInsertSpyExecutor struct {
	insertSQL       string
	insertErr       error
	insertCtx       context.Context
	insertOption    executor.StatementOption
	results         map[string]executor.Result
	resultSequences map[string][]executor.Result
	errs            map[string]error
	executedSQLs    []string
}

type alterCopyAutoIncrEpochWorkspace struct {
	client.Workspace
	supported bool
}

func (w alterCopyAutoIncrEpochWorkspace) SupportsAutoIncrEpochFence() bool {
	return w.supported
}

func TestReconcileAlterCopyAutoIncrementUsesStableIdentityAndSafeBounds(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
	renamedMaxSQL := "select cast(coalesce(max(case when `renamed_id` > 0 then `renamed_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	reusedMaxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		sourceOffsetSQL: newTableCloneOffsetResult(t, resultMP, 0, 500),
		renamedMaxSQL:   newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
		reusedMaxSQL:    newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	srcDef := &plan.TableDef{
		TblId: 1,
		Cols: []*plan.ColDef{
			{ColId: 10, Name: "id", Typ: autoType},
			{ColId: 11, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	copyDef := &plan.TableDef{
		TblId:          2,
		Name:           "dept_copy",
		AutoIncrOffset: 99,
		Cols: []*plan.ColDef{
			{ColId: 12, Name: "id", Typ: autoType},
			{ColId: 10, Name: "renamed_id", Typ: autoType},
			{ColId: 11, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Len(t, reqs, 2)
			require.Equal(t, api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 99, 0), reqs[0])
			require.Equal(t, api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 500, 0), reqs[1])
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	gomock.InOrder(
		autoSvc.EXPECT().SetOffset(incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator()),
		autoSvc.EXPECT().SetOffset(incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 1, "renamed_id", uint64(500), c.proc.GetTxnOperator()),
		autoSvc.EXPECT().DiscardOffsetReset(gomock.Any(), copyDef.TblId, c.proc.GetTxnOperator()).Return(nil),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	cleanup := newAlterAutoIncrementResetCleanup(c)
	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, false, cleanup,
	))
	require.Equal(t, []string{sourceOffsetSQL, reusedMaxSQL, renamedMaxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB(), "all internal SQL results must be closed")
	laterErr := errors.New("later ALTER COPY step failed")
	cleanup.finish(&laterErr)
	require.ErrorContains(t, laterErr, "later ALTER COPY step failed")
}

func TestReconcileAlterCopyAutoIncrementPreservesFreshColumnInitialization(t *testing.T) {
	for _, sessionOffset := range []int64{1, 10} {
		t.Run(fmt.Sprintf("session offset %d", sessionOffset), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			resultMP := mpool.MustNewZero()
			maxSQL := "select cast(coalesce(max(case when `new_id` > 0 then `new_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
			spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
				maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
			}}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			autoOffsetRequested := false
			c.proc.SetResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
				switch name {
				case "auto_increment_offset":
					autoOffsetRequested = true
					require.True(t, isSystemVar)
					require.False(t, isGlobalVar)
					return sessionOffset, nil
				case "lower_case_table_names":
					return int64(1), nil
				default:
					return nil, fmt.Errorf("unexpected variable %q", name)
				}
			})
			srcDef := &plan.TableDef{
				TblId: 1,
				Cols: []*plan.ColDef{{
					ColId: 10, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)},
				}},
			}
			copyDef := &plan.TableDef{
				TblId: 2,
				Name:  "dept_copy",
				Cols: []*plan.ColDef{
					{ColId: 10, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
					{ColId: 11, Name: catalog.Row_ID, Hidden: true, Typ: plan.Type{Id: int32(types.T_Rowid)}},
					{ColId: 12, Name: catalog.FakePrimaryKeyColName, Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
					{ColId: 20, Name: "new_id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
				},
			}
			createdDef := &plan.TableDef{
				TblId: 2,
				Name:  "dept_copy",
				Cols: []*plan.ColDef{
					{ColId: 30, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
					{ColId: 31, Name: "new_id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
					{ColId: 32, Name: catalog.FakePrimaryKeyColName, Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
				},
			}
			copyRel := mock_frontend.NewMockRelation(ctrl)
			copyRel.EXPECT().GetTableDef(gomock.Any()).Return(createdDef)
			copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId)
			// A fresh empty allocator needs no SetOffset, epoch publication, or
			// cleanup ownership. Unexpected mock calls make those boundaries
			// explicit and keep this test independent of session variables.
			autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
			incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

			cleanup := newAlterAutoIncrementResetCleanup(c)
			require.NoError(t, c.reconcileAlterCopyAutoIncrement(
				"test", srcDef, copyDef, copyRel, false, cleanup,
			))
			require.False(t, autoOffsetRequested)
			require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
			require.Zero(t, resultMP.CurrNB())
			statementErr := errors.New("later ALTER COPY step failed")
			cleanup.finish(&statementErr)
			require.ErrorContains(t, statementErr, "later ALTER COPY step failed")
		})
	}
}

func TestReconcileAlterCopyAutoIncrementAdvancesFreshColumnFromCopiedRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `new_id` > 0 then `new_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{7}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	srcDef := &plan.TableDef{
		TblId: 1,
		Cols: []*plan.ColDef{{
			ColId: 10, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)},
		}},
	}
	copyDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
		Cols: []*plan.ColDef{
			{ColId: 10, Name: "payload", Typ: plan.Type{Id: int32(types.T_int64)}},
			{ColId: 20, Name: "new_id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId)
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Equal(t, []*api.AlterTableReq{
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 7, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 1, "new_id", uint64(7), c.proc.GetTxnOperator(),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementPreservesFreshColumnAlongsideRetainedColumn(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
	retainedMaxSQL := "select cast(coalesce(max(case when `old_id` > 0 then `old_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	freshMaxSQL := "select cast(coalesce(max(case when `new_id` > 0 then `new_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		sourceOffsetSQL: newTableCloneOffsetResult(t, resultMP, 0, 50),
		retainedMaxSQL:  newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
		freshMaxSQL:     newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	autoOffsetRequested := false
	c.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		switch name {
		case "lower_case_table_names":
			return int64(1), nil
		case "auto_increment_offset":
			autoOffsetRequested = true
			return int64(10), nil
		default:
			return nil, fmt.Errorf("unexpected variable %q", name)
		}
	})
	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	srcDef := &plan.TableDef{
		TblId: 1,
		Cols: []*plan.ColDef{{
			ColId: 10, Name: "old_id", Typ: autoType,
		}},
	}
	copyDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
		Cols: []*plan.ColDef{
			{ColId: 10, Name: "old_id", Typ: autoType},
			{ColId: 20, Name: "new_id", Typ: autoType},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId)
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Equal(t, []*api.AlterTableReq{
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 50, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	gomock.InOrder(
		autoSvc.EXPECT().SetOffset(
			incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 0, "old_id", uint64(50), c.proc.GetTxnOperator(),
		),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(
		t,
		[]string{sourceOffsetSQL, retainedMaxSQL, freshMaxSQL},
		spyExec.executedSQLs,
	)
	require.False(t, autoOffsetRequested)
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementExplicitResetIgnoresReservedSourceRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
	maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{500}),
	}, errs: map[string]error{sourceOffsetSQL: errors.New("source offset must not be read")}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	srcDef := &plan.TableDef{TblId: 1, Cols: []*plan.ColDef{{
		ColId: 10, Name: "id", Typ: autoType,
	}}}
	copyDef := &plan.TableDef{
		TblId: 2, Name: "dept_copy", AutoIncrOffset: 99,
		Cols: []*plan.ColDef{{ColId: 10, Name: "id", Typ: autoType}},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Equal(t, []*api.AlterTableReq{
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 500, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 0, "id", uint64(500), c.proc.GetTxnOperator(),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, true, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs,
		"an explicit epoch-fenced reset must not inherit the source allocator's reserved high-water mark")
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementAdvancesReplacementEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	autoType := plan.Type{Id: int32(types.T_uint64), AutoIncr: true}
	copyDef := &plan.TableDef{
		TblId: 2, Name: "dept_copy", AutoIncrOffset: 99,
		Cols: []*plan.ColDef{{ColId: 10, Name: "id", Typ: autoType}},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Equal(t, []*api.AlterTableReq{
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 99, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(c.proc.Ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator(),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", &plan.TableDef{}, copyDef, copyRel, true, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementRejectsLegacyTN(t *testing.T) {
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(
		t,
		ctrl,
		spyExec,
	)
	legacyTxn := mock_frontend.NewMockTxnOperator(ctrl)
	legacyTxn.EXPECT().GetWorkspace().Return(alterCopyAutoIncrEpochWorkspace{})
	c.proc.Base.TxnOperator = legacyTxn
	copyDef := &plan.TableDef{Cols: []*plan.ColDef{{
		Name: "id",
		Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
	}}}

	err := c.reconcileAlterCopyAutoIncrement(
		"test",
		&plan.TableDef{},
		copyDef,
		mock_frontend.NewMockRelation(ctrl),
		false,
		newAlterAutoIncrementResetCleanup(c),
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
	require.Empty(t, spyExec.executedSQLs)
}

func TestAutoIDCacheResetCarriesReplacementPolicy(t *testing.T) {
	for _, size := range []uint64{0, 1, 8} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			db.EXPECT().Relation(c.proc.Ctx, "t", nil).Return(rel, nil)
			rel.EXPECT().GetTableDef(c.proc.Ctx).Return(&plan.TableDef{TblId: 42, AutoIdCache: size, Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{AutoIncr: true}}}})
			svc := mock_frontend.NewMockAutoIncrementService(ctrl)
			svc.EXPECT().Reset(incrservice.WithAutoIDCachePolicy(c.proc.Ctx, 42, size), uint64(41), uint64(42), false, c.proc.GetTxnOperator()).Return(nil)
			incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), svc)
			require.NoError(t, maybeResetAutoIncrement(c.proc.Ctx, c.proc.GetService(), db, "t", 41, 42, false, c.proc.GetTxnOperator()))
		})
	}
}

func TestAppendAlterAutoIncrementReqsUsesStableColumnIndexAfterRename(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `renamed_id` > 0 then `renamed_id` else 0 end), 0) as unsigned) from `resolved_db`.`dept`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{140}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	tableDef := &plan.TableDef{
		TblId:       7,
		Name:        "dept",
		AutoIdCache: 8,
		Cols: []*plan.ColDef{{
			Name: "renamed_id",
			Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(c.proc.Ctx, tableDef.TblId, tableDef.AutoIdCache),
		tableDef.TblId,
		0,
		"renamed_id",
		uint64(140),
		c.proc.GetTxnOperator(),
	).Return(nil)
	autoSvc.EXPECT().DiscardOffsetReset(
		gomock.Any(),
		tableDef.TblId,
		c.proc.GetTxnOperator(),
	).Return(nil)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	cleanup := newAlterAutoIncrementResetCleanup(c)
	var reqs []*api.AlterTableReq
	require.NoError(t, c.appendAlterAutoIncrementReqs(
		"resolved_db", tableDef, tableDef, 6, tableDef.TblId, 99, cleanup, &reqs,
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
	require.Len(t, reqs, 1)
	require.Equal(t, uint64(6), reqs[0].GetDbId())
	require.Equal(t, tableDef.TblId, reqs[0].GetTableId())
	require.Equal(t, uint64(140), reqs[0].GetUpdateAutoIncrement().GetOffset())
	require.Zero(t, reqs[0].GetUpdateAutoIncrement().GetEpoch(),
		"disttae must assign the actual next catalog epoch when applying the request")
	require.Zero(t, resultMP.CurrNB(), "the internal MAX result must be closed")

	statementErr := errors.New("later ALTER step failed")
	cleanup.finish(&statementErr)
	require.ErrorContains(t, statementErr, "later ALTER step failed")
}

func TestAppendAlterAutoIncrementReqsUsesFinalColumnNameInCombinedRename(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `resolved_db`.`dept`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{140}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	tableDef := &plan.TableDef{TblId: 7, Name: "dept", Cols: []*plan.ColDef{{
		ColId: 11, Name: "id",
		Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
	}}}
	targetTableDef := plan.DeepCopyTableDef(tableDef, true)
	targetTableDef.Cols[0].Name = "new_id"
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(c.proc.Ctx, tableDef.TblId, tableDef.AutoIdCache), tableDef.TblId, 0, "new_id", uint64(140), c.proc.GetTxnOperator(),
	).Return(nil)
	autoSvc.EXPECT().DiscardOffsetReset(
		gomock.Any(), tableDef.TblId, c.proc.GetTxnOperator(),
	).Return(nil)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	cleanup := newAlterAutoIncrementResetCleanup(c)
	var reqs []*api.AlterTableReq
	require.NoError(t, c.appendAlterAutoIncrementReqs(
		"resolved_db", tableDef, targetTableDef, 6, tableDef.TblId, 99, cleanup, &reqs,
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs,
		"MAX must use the source column that exists before the ALTER is applied")
	require.Len(t, reqs, 1)
	require.Zero(t, resultMP.CurrNB())

	statementErr := errors.New("later ALTER step failed")
	cleanup.finish(&statementErr)
	require.ErrorContains(t, statementErr, "later ALTER step failed")
}

func TestAppendAlterAutoIncrementReqsRejectsLegacyTNBeforeQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	legacyTxn := mock_frontend.NewMockTxnOperator(ctrl)
	legacyTxn.EXPECT().GetWorkspace().Return(alterCopyAutoIncrEpochWorkspace{})
	c.proc.Base.TxnOperator = legacyTxn
	tableDef := &plan.TableDef{Name: "dept", Cols: []*plan.ColDef{{
		Name: "id",
		Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
	}}}

	var reqs []*api.AlterTableReq
	err := c.appendAlterAutoIncrementReqs(
		"test", tableDef, tableDef, 6, 7, 99, newAlterAutoIncrementResetCleanup(c), &reqs,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), err)
	require.Empty(t, spyExec.executedSQLs)
	require.Empty(t, reqs)
}

func TestAppendAlterAutoIncrementReqsDiscardsResetAfterCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)
	tableDef := &plan.TableDef{TblId: 7, Name: "dept", Cols: []*plan.ColDef{{
		Name: "id",
		Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
	}}}
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		incrservice.WithAutoIDCachePolicy(ctx, tableDef.TblId, tableDef.AutoIdCache), tableDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator(),
	).DoAndReturn(func(context.Context, uint64, int, string, uint64, client.TxnOperator) error {
		cancel()
		return nil
	})
	autoSvc.EXPECT().DiscardOffsetReset(
		gomock.Any(), tableDef.TblId, c.proc.GetTxnOperator(),
	).Return(nil)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	cleanup := newAlterAutoIncrementResetCleanup(c)
	var reqs []*api.AlterTableReq
	err := c.appendAlterAutoIncrementReqs(
		"test", tableDef, tableDef, 6, tableDef.TblId, 99, cleanup, &reqs,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, reqs)
	cleanup.finish(&err)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, resultMP.CurrNB())
}

func TestAppendAlterAutoIncrementReqsRejectsNarrowedOverflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	tableDef := &plan.TableDef{TblId: 7, Name: "dept", Cols: []*plan.ColDef{{
		Name: "id",
		Typ:  plan.Type{Id: int32(types.T_uint8), AutoIncr: true},
	}}}
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Times(0)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	var reqs []*api.AlterTableReq
	err := c.appendAlterAutoIncrementReqs(
		"test", tableDef, tableDef, 6, tableDef.TblId, 300,
		newAlterAutoIncrementResetCleanup(c), &reqs,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
	require.Empty(t, reqs)
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementSkipsHiddenAndRejectsNarrowedOverflow(t *testing.T) {
	t.Run("hidden only", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		spyExec := &alterCopyInsertSpyExecutor{}
		c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
		copyDef := &plan.TableDef{
			TblId: 2,
			Name:  "dept_copy",
			Cols: []*plan.ColDef{{
				ColId: 1, Name: catalog.FakePrimaryKeyColName, Hidden: true,
				Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
			}},
		}
		copyRel := mock_frontend.NewMockRelation(ctrl)
		autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
		autoSvc.EXPECT().SetOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
		incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

		require.NoError(t, c.reconcileAlterCopyAutoIncrement(
			"test", &plan.TableDef{}, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
		))
		require.Empty(t, spyExec.executedSQLs)
	})

	t.Run("source offset exceeds narrowed type", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		resultMP := mpool.MustNewZero()
		sourceOffsetSQL := "select col_index, offset from mo_catalog.mo_increment_columns where table_id = 1"
		maxSQL := "select cast(coalesce(max(case when `id` > 0 then `id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
		spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
			sourceOffsetSQL: newTableCloneOffsetResult(t, resultMP, 0, 300),
			maxSQL:          newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
		}}
		c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
		srcDef := &plan.TableDef{TblId: 1, Cols: []*plan.ColDef{{
			ColId: 10, Name: "id", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}}}
		copyDef := &plan.TableDef{TblId: 2, Name: "dept_copy", Cols: []*plan.ColDef{{
			ColId: 10, Name: "id", Typ: plan.Type{Id: int32(types.T_uint8), AutoIncr: true},
		}}}
		copyRel := mock_frontend.NewMockRelation(ctrl)
		copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
		copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
		autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
		autoSvc.EXPECT().SetOffset(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
		incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

		err := c.reconcileAlterCopyAutoIncrement(
			"test", srcDef, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
		)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), err)
		require.Zero(t, resultMP.CurrNB(), "all internal SQL results must be closed")
	})
}

func TestReconcileAlterCopyAutoIncrementStopsAfterCancellation(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	firstMaxSQL := "select cast(coalesce(max(case when `first` > 0 then `first` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		firstMaxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{40}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)

	copyDef := &plan.TableDef{
		TblId:          2,
		Name:           "dept_copy",
		AutoIncrOffset: 99,
		Cols: []*plan.ColDef{
			{ColId: 20, Name: "first", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
			{ColId: 21, Name: "second", Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
		},
	}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(copyDef.TblId).AnyTimes()
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(incrservice.WithAutoIDCachePolicy(ctx, copyDef.TblId, copyDef.AutoIdCache), copyDef.TblId, 0, "first", uint64(99), c.proc.GetTxnOperator()).DoAndReturn(
		func(context.Context, uint64, int, string, uint64, client.TxnOperator) error {
			cancel()
			return nil
		},
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	err := c.reconcileAlterCopyAutoIncrement(
		"test", &plan.TableDef{}, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []string{firstMaxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
}

const (
	alterCopyTestPkNullCheckSQL      = "SELECT `col4` FROM `test`.`dept` WHERE `col4` IS NULL LIMIT 1"
	alterCopyTestPkDuplicateCheckSQL = "SELECT `col4` FROM `test`.`dept` GROUP BY `col4` HAVING count(*) > 1 LIMIT 1"
)

func (e *alterCopyInsertSpyExecutor) Exec(
	ctx context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	e.executedSQLs = append(e.executedSQLs, sql)
	if sql == e.insertSQL {
		e.insertCtx = ctx
		e.insertOption = opts.StatementOption()
		return executor.Result{}, e.insertErr
	}
	if e.errs != nil {
		if err, ok := e.errs[sql]; ok {
			return executor.Result{}, err
		}
	}
	if results := e.resultSequences[sql]; len(results) > 0 {
		e.resultSequences[sql] = results[1:]
		return results[0], nil
	}
	if e.results != nil {
		if res, ok := e.results[sql]; ok {
			return res, nil
		}
	}
	return executor.Result{}, nil
}

func (e *alterCopyInsertSpyExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

type alterCopyGateConcurrencyCoordinator struct {
	insertStarted     chan struct{}
	allowInsertReturn chan struct{}
	gateMu            sync.Mutex
	releaseGate       chan struct{}
}

type alterCopyGateConcurrencyExecutor struct {
	insertSQL   string
	insertErr   error
	coordinator *alterCopyGateConcurrencyCoordinator
}

type alterCopyNoChangesHandle struct{}

func (alterCopyNoChangesHandle) Next(
	context.Context, *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (alterCopyNoChangesHandle) Close() error {
	return nil
}

type alterCopyOneRowChangesHandle struct {
	data     *batch.Batch
	closed   bool
	nextUsed bool
}

func (h *alterCopyOneRowChangesHandle) Next(
	context.Context, *mpool.MPool,
) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	if h.nextUsed {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	h.nextUsed = true
	return h.data, nil, engine.ChangesHandle_Tail_done, nil
}

func (h *alterCopyOneRowChangesHandle) Close() error {
	h.closed = true
	return nil
}

type alterCopySourceChangesProvider interface {
	alterCopySourceChanges() engine.ChangesHandle
}

type alterCopySourceDefVersionProvider interface {
	alterCopySourceDefVersion() uint32
}

type alterCopyCreatedInCurrentTxnRelation struct {
	engine.Relation
	collectCalled bool
}

func (rel *alterCopyCreatedInCurrentTxnRelation) CollectChanges(
	_ context.Context,
	_, _ types.TS,
	_ bool,
	_ *mpool.MPool,
) (engine.ChangesHandle, error) {
	rel.collectCalled = true
	return nil, errors.New("created-in-current-txn relation has no committed CDC history")
}

func (rel *alterCopyCreatedInCurrentTxnRelation) CreatedInCurrentTxn(
	_ context.Context,
) (bool, error) {
	return true, nil
}

func (e *alterCopyGateConcurrencyExecutor) Exec(
	_ context.Context,
	sql string,
	opts executor.Options,
) (executor.Result, error) {
	if sql == e.insertSQL {
		e.coordinator.insertStarted <- struct{}{}
		<-e.coordinator.allowInsertReturn
		return executor.Result{}, e.insertErr
	}
	if sql == databranchutils.LineageOwnerLifecycleLockSQL() {
		e.coordinator.gateMu.Lock()
		<-e.coordinator.releaseGate
		e.coordinator.gateMu.Unlock()
	}
	return executor.Result{}, nil
}

func (e *alterCopyGateConcurrencyExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

func newAlterCopyGateConcurrencyFixture(
	t *testing.T,
	ctrl *gomock.Controller,
	exec executor.SQLExecutor,
	serviceSuffix string,
	snapshotTS int64,
	sourceIDProviders ...func() uint64,
) (*Scope, *Compile) {
	c := newAlterCopyPrecheckCompile(t, ctrl, exec, serviceSuffix)
	c.proc.GetTxnOperator().(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().
		Return(timestamp.Timestamp{PhysicalTime: snapshotTS}).AnyTimes()
	tableDef := &plan.TableDef{
		TblId: 1,
		Name:  "dept",
	}
	sourceID := func() uint64 { return 1 }
	if len(sourceIDProviders) > 0 {
		sourceID = sourceIDProviders[0]
	}
	sourceDefVersion := func() uint32 { return 7 }
	if provider, ok := exec.(alterCopySourceDefVersionProvider); ok {
		sourceDefVersion = provider.alterCopySourceDefVersion
	}
	copyTableDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
	}
	alterTable := &plan2.AlterTable{
		Database:          "test",
		TableDef:          tableDef,
		CopyTableDef:      copyTableDef,
		CreateTmpTableSql: "create table dept_copy",
		InsertTmpDataSql:  "insert into dept_copy select * from dept",
		Options:           &plan2.AlterCopyOpt{SkipPkDedup: true},
	}
	scope := &Scope{
		Magic: AlterTable,
		Plan: &plan.Plan{
			Plan: &plan2.Plan_Ddl{
				Ddl: &plan2.DataDefinition{
					DdlType: plan2.DataDefinition_ALTER_TABLE,
					Definition: &plan2.DataDefinition_AlterTable{
						AlterTable: alterTable,
					},
				},
			},
		},
	}
	originRel := mock_frontend.NewMockRelation(ctrl)
	originRel.EXPECT().GetTableID(gomock.Any()).DoAndReturn(
		func(context.Context) uint64 { return sourceID() },
	).AnyTimes()
	originRel.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(
		func(context.Context) *plan2.TableDef {
			return &plan2.TableDef{
				TblId:   sourceID(),
				Name:    "dept",
				Version: sourceDefVersion(),
			}
		},
	).AnyTimes()
	originRel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
	originRel.EXPECT().CopyTableDef(gomock.Any()).
		Return(plan.DeepCopyTableDef(tableDef, true)).AnyTimes()
	originRel.EXPECT().CollectChanges(
		gomock.Any(), gomock.Any(), gomock.Any(), false, gomock.Any(),
	).DoAndReturn(func(
		_ context.Context,
		_, _ types.TS,
		_ bool,
		_ *mpool.MPool,
	) (engine.ChangesHandle, error) {
		if provider, ok := exec.(alterCopySourceChangesProvider); ok {
			if handle := provider.alterCopySourceChanges(); handle != nil {
				return handle, nil
			}
		}
		return alterCopyNoChangesHandle{}, nil
	}).AnyTimes()
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(copyTableDef).AnyTimes()
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyTableDef).AnyTimes()
	copyRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(2)).AnyTimes()
	copyRel.EXPECT().TableRenameInTxn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, [][]byte) error {
			if renameRecorder, ok := exec.(interface{ markSourceRenamed() }); ok {
				renameRecorder.markSourceRenamed()
			}
			return nil
		},
	).AnyTimes()
	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
	mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()
	eng := c.e.(*mock_frontend.MockEngine)
	eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()
	c.pn = scope.Plan
	return scope, c
}

func TestAlterCopyPhysicalWorkDoesNotWaitForLineageOwnerGate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	coordinator := &alterCopyGateConcurrencyCoordinator{
		insertStarted:     make(chan struct{}, 2),
		allowInsertReturn: make(chan struct{}),
		releaseGate:       make(chan struct{}),
	}
	insertErr := errors.New("stop after physical ALTER COPY")
	type alterCopyFixture struct {
		scope   *Scope
		compile *Compile
	}
	fixtures := make([]alterCopyFixture, 2)
	for index := range fixtures {
		exec := &alterCopyGateConcurrencyExecutor{
			insertSQL:   "insert into dept_copy select * from dept",
			insertErr:   insertErr,
			coordinator: coordinator,
		}
		scope, compile := newAlterCopyGateConcurrencyFixture(
			t, ctrl, exec, fmt.Sprint(index), 0,
		)
		fixtures[index] = alterCopyFixture{scope: scope, compile: compile}
	}

	done := make(chan error, len(fixtures))
	for _, fixture := range fixtures {
		scope := fixture.scope
		compile := fixture.compile
		go func() {
			done <- scope.AlterTableCopy(compile)
		}()
	}

	started := 0
	deadline := time.After(2 * time.Second)
	for started < len(fixtures) {
		select {
		case <-coordinator.insertStarted:
			started++
		case <-deadline:
			started = len(fixtures) + 1
		}
	}
	if started != len(fixtures) {
		t.Errorf("independent ALTER COPY physical work waited for the global lineage gate")
	}
	close(coordinator.allowInsertReturn)
	close(coordinator.releaseGate)
	for range fixtures {
		require.ErrorIs(t, <-done, insertErr)
	}
}

func newAlterCopyPessimisticGateConcurrencyFixture(
	t *testing.T,
	ctrl *gomock.Controller,
	exec executor.SQLExecutor,
	serviceSuffix string,
	snapshotTS int64,
) (*Scope, *Compile) {
	scope, c := newAlterCopyGateConcurrencyFixture(
		t, ctrl, exec, serviceSuffix, snapshotTS,
	)
	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().GetWorkspace().Return(&Ws{}).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
		Mode:      txn.TxnMode_Pessimistic,
		Isolation: txn.TxnIsolation_SI,
	}).AnyTimes()
	txnOperator.EXPECT().TxnOptions().Return(txn.TxnOptions{}).AnyTimes()
	txnOperator.EXPECT().TryEnterRunSqlWithTokenAndSQL(gomock.Any(), gomock.Any()).
		Return(uint64(1), nil).AnyTimes()
	txnOperator.EXPECT().ExitRunSqlWithToken(gomock.Any()).Return().AnyTimes()
	txnOperator.EXPECT().CheckLockTableBinds(gomock.Any()).Return(nil).AnyTimes()
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Status().Return(txn.TxnStatus_Active).AnyTimes()
	txnOperator.EXPECT().SnapshotTS().
		Return(timestamp.Timestamp{PhysicalTime: snapshotTS}).AnyTimes()
	c.proc.Base.TxnOperator = txnOperator
	return scope, c
}

type alterCopyLockOrderRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *alterCopyLockOrderRecorder) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func (r *alterCopyLockOrderRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.events)
}

type alterCopyLockOrderCoordinator struct {
	gateMu                sync.Mutex
	recorder              *alterCopyLockOrderRecorder
	dropGateEntered       chan struct{}
	releaseDropGate       chan struct{}
	truncateGateEntered   chan struct{}
	releaseTruncateGate   chan struct{}
	alterGateEntered      chan struct{}
	releaseAlterGate      chan struct{}
	dropSourceEntered     chan struct{}
	releaseDropSource     chan struct{}
	truncateSourceEntered chan struct{}
	releaseTruncateSource chan struct{}
	physicalStarted       chan struct{}
}

type alterCopyLockOrderExecutor struct {
	role        string
	insertSQL   string
	coordinator *alterCopyLockOrderCoordinator
}

func (e *alterCopyLockOrderExecutor) Exec(
	_ context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	if sql == e.insertSQL {
		e.coordinator.recorder.record("alter:physical")
		e.coordinator.physicalStarted <- struct{}{}
		return executor.Result{}, nil
	}
	if sql == databranchutils.LineageOwnerLifecycleLockSQL() ||
		sql == databranchutils.LineageOwnerLifecyclePessimisticLockSQL() {
		e.coordinator.gateMu.Lock()
		e.coordinator.recorder.record(e.role + ":gate")
		if e.role == "drop" {
			e.coordinator.dropGateEntered <- struct{}{}
			<-e.coordinator.releaseDropGate
		} else if e.role == "truncate" {
			e.coordinator.truncateGateEntered <- struct{}{}
			<-e.coordinator.releaseTruncateGate
		} else {
			e.coordinator.alterGateEntered <- struct{}{}
			<-e.coordinator.releaseAlterGate
		}
		e.coordinator.gateMu.Unlock()
		return executor.Result{}, nil
	}
	return executor.Result{}, nil
}

func (e *alterCopyLockOrderExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

func TestAlterCopyAndDropUseGateBeforeSourceLocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	coordinator := &alterCopyLockOrderCoordinator{
		recorder:          &alterCopyLockOrderRecorder{},
		dropGateEntered:   make(chan struct{}, 1),
		releaseDropGate:   make(chan struct{}),
		alterGateEntered:  make(chan struct{}, 1),
		releaseAlterGate:  make(chan struct{}),
		dropSourceEntered: make(chan struct{}, 1),
		releaseDropSource: make(chan struct{}),
		physicalStarted:   make(chan struct{}, 1),
	}
	dropExec := &alterCopyLockOrderExecutor{
		role:        "drop",
		coordinator: coordinator,
	}
	alterExec := &alterCopyLockOrderExecutor{
		role:        "alter",
		insertSQL:   "insert into dept_copy select * from dept",
		coordinator: coordinator,
	}
	dropScope, dropCompile := newAlterCopyPessimisticGateConcurrencyFixture(
		t, ctrl, dropExec, "drop", 0,
	)
	dropCompile.originSQL = "drop table dept"
	alterScope, alterCompile := newAlterCopyPessimisticGateConcurrencyFixture(
		t, ctrl, alterExec, "alter", 0,
	)

	dropSourceErr := errors.New("stop drop after source lock")
	alterSourceErr := errors.New("stop alter after source lock")
	stubs := gostub.New()
	stubs.Stub(&lockMoDatabase, func(c *Compile, _ string, _ lock.LockMode) error {
		if c == dropCompile {
			coordinator.recorder.record("drop:source-db")
			coordinator.dropSourceEntered <- struct{}{}
			<-coordinator.releaseDropSource
			return dropSourceErr
		}
		coordinator.recorder.record("alter:source-db")
		return alterSourceErr
	})
	stubs.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error {
		return errors.New("unexpected catalog table lock")
	})
	stubs.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return errors.New("unexpected physical table lock")
	})
	defer stubs.Reset()

	dropDone := make(chan error, 1)
	alterDone := make(chan error, 1)
	dropScope.Plan = &plan.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
		DdlType: plan2.DataDefinition_DROP_TABLE,
		Definition: &plan2.DataDefinition_DropTable{DropTable: &plan2.DropTable{
			Database: "test",
			Table:    "dept",
			TableDef: &plan2.TableDef{},
		}},
	}}}
	go func() {
		dropDone <- dropScope.DropTable(dropCompile)
	}()
	requireRecv(t, coordinator.dropGateEntered, "drop gate")
	go func() { alterDone <- alterScope.AlterTableCopy(alterCompile) }()
	requireRecv(t, coordinator.physicalStarted, "alter physical work")
	requireNoEvent(t, coordinator.recorder.snapshot(), "alter:source")

	close(coordinator.releaseDropGate)
	requireRecv(t, coordinator.dropSourceEntered, "drop source lock")
	close(coordinator.releaseAlterGate)
	requireRecv(t, coordinator.alterGateEntered, "alter gate")
	coordinator.releaseDropSource <- struct{}{}

	require.ErrorIs(t, <-dropDone, dropSourceErr)
	require.ErrorIs(t, <-alterDone, alterSourceErr)
	require.Eventually(t, func() bool {
		return slices.Contains(coordinator.recorder.snapshot(), "alter:source-db")
	}, time.Second, 10*time.Millisecond)
	events := coordinator.recorder.snapshot()
	gateIndex := slices.Index(events, "alter:gate")
	sourceIndex := slices.Index(events, "alter:source-db")
	require.GreaterOrEqual(t, gateIndex, 0)
	require.GreaterOrEqual(t, sourceIndex, 0)
	require.Less(t, gateIndex, sourceIndex)
}

func TestAlterCopyAndTruncateUseGateBeforeSourceLocks(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	coordinator := &alterCopyLockOrderCoordinator{
		recorder:              &alterCopyLockOrderRecorder{},
		truncateGateEntered:   make(chan struct{}, 1),
		releaseTruncateGate:   make(chan struct{}),
		alterGateEntered:      make(chan struct{}, 1),
		releaseAlterGate:      make(chan struct{}),
		truncateSourceEntered: make(chan struct{}, 1),
		releaseTruncateSource: make(chan struct{}),
		physicalStarted:       make(chan struct{}, 1),
	}
	truncateExec := &alterCopyLockOrderExecutor{
		role:        "truncate",
		coordinator: coordinator,
	}
	alterExec := &alterCopyLockOrderExecutor{
		role:        "alter",
		insertSQL:   "insert into dept_copy select * from dept",
		coordinator: coordinator,
	}
	truncateScope, truncateCompile := newAlterCopyPessimisticGateConcurrencyFixture(
		t, ctrl, truncateExec, "truncate", 0,
	)
	truncateCompile.originSQL = "truncate table dept"
	truncateScope.Plan.GetDdl().Definition = &plan2.DataDefinition_TruncateTable{
		TruncateTable: &plan2.TruncateTable{
			Database: "test",
			Table:    "dept",
			TableId:  1,
		},
	}
	alterScope, alterCompile := newAlterCopyPessimisticGateConcurrencyFixture(
		t, ctrl, alterExec, "alter", 0,
	)

	truncateSourceErr := errors.New("stop truncate after source lock")
	alterSourceErr := errors.New("stop alter after source lock")
	stubs := gostub.New()
	stubs.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error {
		return nil
	})
	stubs.Stub(&lockMoTable, func(c *Compile, _ string, _ string, _ lock.LockMode) error {
		if c == truncateCompile {
			coordinator.recorder.record("truncate:source-table")
			coordinator.truncateSourceEntered <- struct{}{}
			<-coordinator.releaseTruncateSource
			return truncateSourceErr
		}
		coordinator.recorder.record("alter:source-table")
		return alterSourceErr
	})
	stubs.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return errors.New("unexpected physical table lock")
	})
	defer stubs.Reset()

	truncateDone := make(chan error, 1)
	alterDone := make(chan error, 1)
	go func() {
		truncateDone <- truncateScope.TruncateTable(truncateCompile)
	}()
	requireRecv(t, coordinator.truncateGateEntered, "truncate gate")
	go func() { alterDone <- alterScope.AlterTableCopy(alterCompile) }()
	requireRecv(t, coordinator.physicalStarted, "alter physical work")
	requireNoEvent(t, coordinator.recorder.snapshot(), "alter:source")

	close(coordinator.releaseTruncateGate)
	select {
	case err := <-truncateDone:
		t.Fatalf("truncate returned before source lock: %v", err)
	default:
	}
	select {
	case <-coordinator.truncateSourceEntered:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for truncate source lock")
	}
	close(coordinator.releaseAlterGate)
	requireRecv(t, coordinator.alterGateEntered, "alter gate")
	coordinator.releaseTruncateSource <- struct{}{}

	require.ErrorIs(t, <-truncateDone, truncateSourceErr)
	require.ErrorIs(t, <-alterDone, alterSourceErr)
	events := coordinator.recorder.snapshot()
	truncateGateIndex := slices.Index(events, "truncate:gate")
	truncateSourceIndex := slices.Index(events, "truncate:source-table")
	alterGateIndex := slices.Index(events, "alter:gate")
	alterSourceIndex := slices.Index(events, "alter:source-table")
	require.GreaterOrEqual(t, truncateGateIndex, 0)
	require.GreaterOrEqual(t, truncateSourceIndex, 0)
	require.Less(t, truncateGateIndex, truncateSourceIndex)
	require.GreaterOrEqual(t, alterGateIndex, 0)
	require.GreaterOrEqual(t, alterSourceIndex, 0)
	require.Less(t, alterGateIndex, alterSourceIndex)
}

func TestAlterCopyFailsAfterPhysicalWorkWithoutReleasingGate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		gateErr   error
		sourceErr error
		wantErr   error
	}{
		{
			name:    "gate failure",
			gateErr: errors.New("lifecycle gate failed"),
			wantErr: errors.New("lifecycle gate failed"),
		},
		{
			name:      "source lock failure",
			sourceErr: errors.New("source lock failed"),
			wantErr:   errors.New("source lock failed"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			exec := &alterCopyInsertSpyExecutor{
				insertSQL: "insert into dept_copy select * from dept",
				errs: map[string]error{
					databranchutils.LineageOwnerLifecyclePessimisticLockSQL(): tc.gateErr,
				},
			}
			scope, c := newAlterCopyPessimisticGateConcurrencyFixture(
				t, ctrl, exec, tc.name, 0,
			)
			stubs := gostub.New()
			stubs.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error {
				return tc.sourceErr
			})
			defer stubs.Reset()

			err := scope.AlterTableCopy(c)
			require.ErrorContains(t, err, tc.wantErr.Error())
			insertIndex := slices.Index(exec.executedSQLs, exec.insertSQL)
			gateIndex := slices.Index(
				exec.executedSQLs, databranchutils.LineageOwnerLifecyclePessimisticLockSQL(),
			)
			require.GreaterOrEqual(t, insertIndex, 0)
			require.GreaterOrEqual(t, gateIndex, 0)
			require.Less(t, insertIndex, gateIndex)
			if tc.gateErr != nil {
				require.Len(t, exec.executedSQLs, gateIndex+1)
			}
		})
	}
}

func requireRecv(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		return
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func requireNoEvent(t *testing.T, events []string, prefix string) {
	t.Helper()
	for _, event := range events {
		if strings.HasPrefix(event, prefix) {
			t.Fatalf("unexpected event before gate: %s", event)
		}
	}
}

func stubAlterCopySourceLocks(t *testing.T, err error) {
	t.Helper()
	stubs := gostub.New()
	stubs.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error { return err })
	stubs.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error { return err })
	stubs.Stub(&lockTable, func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
		return err
	})
	t.Cleanup(stubs.Reset)
}

func TestLockAlterCopySourceRetryContracts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, tc := range []struct {
		name          string
		optimistic    bool
		tableErr      error
		relationErr   error
		wantLockCalls []string
	}{
		{
			name:       "optimistic transaction does not take source locks",
			optimistic: true,
		},
		{
			name:          "catalog table retry is reported as definition change",
			tableErr:      moerr.NewTxnNeedRetryNoCtx(),
			wantLockCalls: []string{"database", "table", "relation"},
		},
		{
			name:          "physical table retry is reported as definition change",
			relationErr:   moerr.NewTxnNeedRetryNoCtx(),
			wantLockCalls: []string{"database", "table", "relation"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := &alterCopyLateOwnerExecutor{t: t}
			c := newAlterCopyPrecheckCompile(t, ctrl, exec)
			txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
			txnMode := txn.TxnMode_Pessimistic
			if tc.optimistic {
				txnMode = txn.TxnMode_Optimistic
			}
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{
				Mode:      txnMode,
				Isolation: txn.TxnIsolation_SI,
			}).AnyTimes()
			c.proc.Base.TxnOperator = txnOperator

			var lockCalls []string
			stubs := gostub.New()
			stubs.Stub(&lockMoDatabase, func(*Compile, string, lock.LockMode) error {
				lockCalls = append(lockCalls, "database")
				return nil
			})
			stubs.Stub(&lockMoTable, func(*Compile, string, string, lock.LockMode) error {
				lockCalls = append(lockCalls, "table")
				return tc.tableErr
			})
			stubs.Stub(&lockTable,
				func(context.Context, engine.Engine, *process.Process, engine.Relation, string, bool) error {
					lockCalls = append(lockCalls, "relation")
					return tc.relationErr
				},
			)
			t.Cleanup(stubs.Reset)

			qry := &plan2.AlterTable{TableDef: &plan.TableDef{Name: "dept"}}
			err := c.lockAlterCopySource(
				mock_frontend.NewMockDatabase(ctrl), "test", "dept",
				mock_frontend.NewMockRelation(ctrl), qry,
			)

			if tc.optimistic {
				require.NoError(t, err)
			} else {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
			}
			require.Equal(t, tc.wantLockCalls, lockCalls)
		})
	}
}

type alterCopyLateOwnerExecutor struct {
	t                *testing.T
	mp               *mpool.MPool
	ownerPublished   bool
	physicalStarted  bool
	metadataSQL      string
	snapshotSQL      string
	dropSourceSQL    string
	sourceRenamed    bool
	onGate           func()
	sourceChanges    engine.ChangesHandle
	sourceDefVersion uint32
}

func (e *alterCopyLateOwnerExecutor) alterCopySourceChanges() engine.ChangesHandle {
	return e.sourceChanges
}

func (e *alterCopyLateOwnerExecutor) alterCopySourceDefVersion() uint32 {
	if e.sourceDefVersion == 0 {
		return 7
	}
	return e.sourceDefVersion
}

func (e *alterCopyLateOwnerExecutor) markSourceRenamed() {
	e.sourceRenamed = true
}

func (e *alterCopyLateOwnerExecutor) historyResult() executor.Result {
	return newAlterCopyFixedResult(
		e.t, e.mp, types.T_int32.ToType(), []int32{1},
	)
}

func (e *alterCopyLateOwnerExecutor) historySQL(sql string) bool {
	return sql == alterDataBranchHistoricalSnapshotSourceSQL("", "test", "dept", 1) ||
		sql == alterDataBranchHistoricalSnapshotSourceProbeSQL("", "test", "dept", 1, false, 0) ||
		sql == alterDataBranchHistoricalPitrSourceSQL("", "test", "dept", 1) ||
		sql == alterDataBranchHistoricalPitrSourceProbeSQL("", "test", "dept", 1, false, 0)
}

func (e *alterCopyLateOwnerExecutor) Exec(
	_ context.Context,
	sql string,
	_ executor.Options,
) (executor.Result, error) {
	if sql == "insert into dept_copy select * from dept" {
		e.physicalStarted = true
	}
	if sql == databranchutils.LineageOwnerLifecyclePessimisticLockSQL() ||
		sql == databranchutils.LineageOwnerLifecycleLockSQL() {
		if !e.ownerPublished {
			e.ownerPublished = true
			if e.onGate != nil {
				e.onGate()
			}
		}
		return executor.Result{}, nil
	}
	if e.ownerPublished && e.historySQL(sql) {
		return e.historyResult(), nil
	}
	if strings.HasPrefix(sql, "drop table `test`.`dept`") {
		e.dropSourceSQL = sql
	}
	if strings.HasPrefix(sql, "insert into mo_catalog.mo_branch_metadata") {
		e.metadataSQL = sql
		return executor.Result{}, nil
	}
	if strings.HasPrefix(sql, "insert into mo_catalog.mo_snapshots") {
		e.snapshotSQL = sql
		return executor.Result{}, errors.New("stop after snapshot lineage publication")
	}
	return executor.Result{}, nil
}

func (e *alterCopyLateOwnerExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	return execFunc(executor.NewMemTxnExecutor(func(sql string) (executor.Result, error) {
		return e.Exec(ctx, sql, opts)
	}, opts.Txn()))
}

func TestAlterCopyLateLineagePublicationUsesCopyTimestamp(t *testing.T) {
	const copyTS = int64(123456789)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := &alterCopyLateOwnerExecutor{t: t}
	stubAlterCopySourceLocks(t, nil)
	scope, compile := newAlterCopyGateConcurrencyFixture(t, ctrl, exec, "late-owner", copyTS)
	exec.mp = compile.proc.Mp()

	err := scope.AlterTableCopy(compile)
	require.ErrorContains(t, err, "stop after snapshot lineage publication")
	require.Equal(
		t,
		"insert into mo_catalog.mo_branch_metadata values(2, 123456789, 1, 0, 'alter', false)",
		exec.metadataSQL,
	)
	require.Contains(t, exec.snapshotSQL, "'__mo_branch_2', 123456789")
}

func TestAlterCopyRejectsSourceReplacementAfterGate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sourceChanged := &atomic.Bool{}
	exec := &alterCopyLateOwnerExecutor{
		t:      t,
		onGate: func() { sourceChanged.Store(true) },
	}
	stubAlterCopySourceLocks(t, nil)
	scope, compile := newAlterCopyGateConcurrencyFixture(
		t, ctrl, exec, "source-replaced", 123456789,
		func() uint64 {
			if sourceChanged.Load() {
				return 99
			}
			return 1
		},
	)
	exec.mp = compile.proc.Mp()

	err := scope.AlterTableCopy(compile)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	require.Empty(t, exec.metadataSQL)
	require.Empty(t, exec.snapshotSQL)
}

func TestAlterCopyRejectsSourceDataChangeAfterCopy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := &batch.Batch{}
	data.SetRowCount(1)
	sourceChanges := &alterCopyOneRowChangesHandle{data: data}
	stubAlterCopySourceLocks(t, nil)
	exec := &alterCopyLateOwnerExecutor{
		t:             t,
		sourceChanges: sourceChanges,
	}
	scope, compile := newAlterCopyGateConcurrencyFixture(
		t, ctrl, exec, "source-data-changed", 123456789,
	)
	exec.mp = compile.proc.Mp()

	err := scope.AlterTableCopy(compile)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	require.True(t, sourceChanges.closed)
	require.True(t, sourceChanges.nextUsed)
	require.Empty(t, exec.metadataSQL)
	require.Empty(t, exec.snapshotSQL)
}

func TestCloneUnaffectedIndexesRetriesWhenSourceIndexDisappears(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	indexDef := &plan.IndexDef{
		IndexName:          "ix",
		IndexTableName:     "__mo_index_ix",
		TableExist:         true,
		Unique:             true,
		IndexAlgoTableType: "secondary",
	}
	sourceDef := &plan.TableDef{Name: "dept", Indexes: []*plan.IndexDef{indexDef}}
	copyDef := &plan.TableDef{Name: "dept_copy", Indexes: []*plan.IndexDef{indexDef}}
	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().GetTableDef(gomock.Any()).Return(copyDef)

	db := mock_frontend.NewMockDatabase(ctrl)
	db.EXPECT().Relation(gomock.Any(), indexDef.IndexTableName, nil).
		Return(nil, moerr.NewNoSuchTable(c.proc.Ctx, "test", indexDef.IndexTableName))
	eng := c.e.(*mock_frontend.MockEngine)
	eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(db, nil)

	err := cloneUnaffectedIndexes(
		c, "test", map[string]bool{"ix": true}, nil, copyRel, sourceDef, nil,
	)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
}

func TestAlterCopySkipsSourceChangesForRelationCreatedInCurrentTxn(t *testing.T) {
	proc := testutil.NewProcess(t)
	compile := &Compile{proc: proc}
	rel := &alterCopyCreatedInCurrentTxnRelation{}

	changed, err := compile.alterCopySourceChangedSinceCopy(rel, 123456789)
	require.NoError(t, err)
	require.False(t, changed)
	require.False(t, rel.collectCalled)
}

func TestAlterCopyRejectsSameIDSourceDefChangeAfterGate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := &alterCopyLateOwnerExecutor{t: t}
	exec.onGate = func() { exec.sourceDefVersion = 8 }
	stubAlterCopySourceLocks(t, nil)
	scope, compile := newAlterCopyGateConcurrencyFixture(
		t, ctrl, exec, "same-id-def-changed", 123456789,
	)
	exec.mp = compile.proc.Mp()

	err := scope.AlterTableCopy(compile)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
	require.True(t, exec.physicalStarted)
	require.True(t, exec.ownerPublished)
	require.Empty(t, exec.dropSourceSQL)
	require.False(t, exec.sourceRenamed)
	require.Empty(t, exec.metadataSQL)
	require.Empty(t, exec.snapshotSQL)
}

func TestAlterCopyRejectsSameStatementReplacementDiscoveredAfterGate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	exec := &alterCopyLateOwnerExecutor{t: t}
	stubAlterCopySourceLocks(t, nil)
	scope, compile := newAlterCopyGateConcurrencyFixture(
		t, ctrl, exec, "late-replacement", 123456789,
	)
	exec.mp = compile.proc.Mp()
	alterTable := scope.Plan.GetDdl().GetAlterTable()
	alterTable.TableDef.Cols = []*plan.ColDef{
		{Name: "a", ColId: 1, Seqnum: 0},
		{Name: "b", ColId: 2, Seqnum: 1},
	}
	alterTable.CopyTableDef.Cols = []*plan.ColDef{
		{Name: "a", ColId: 1, Seqnum: 0},
		{Name: "c", ColId: ^uint64(0), Seqnum: 0},
	}
	alterTable.ChangeTblColIdMap = map[uint64]*plan.ColDef{1: {Name: "a"}}
	columnName, replaced := alterCopySameStatementColumnReplacement(alterTable)
	require.True(t, replaced)
	require.Equal(t, "c", columnName)

	err := scope.AlterTableCopy(compile)
	require.ErrorContains(t, err, "cannot drop and add column 'c' in the same statement")
	require.Empty(t, exec.metadataSQL)
	require.Empty(t, exec.snapshotSQL)
}

func TestScopeAlterTableCopyInsertTmpDataPipelineFlush(t *testing.T) {
	insertErr := errors.New("stop after insert-copy")
	lockDatabaseStub := gostub.Stub(&lockMoDatabase,
		func(_ *Compile, _ string, _ lock.LockMode) error { return nil })
	defer lockDatabaseStub.Reset()
	lockTableMetadataStub := gostub.Stub(&lockMoTable,
		func(_ *Compile, _, _ string, _ lock.LockMode) error { return nil })
	defer lockTableMetadataStub.Reset()
	lockRelationStub := gostub.Stub(&lockTable,
		func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return nil
		})
	defer lockRelationStub.Reset()

	for _, tc := range []struct {
		name               string
		skipPkDedup        bool
		nilCtxBeforeInsert bool
		wantPipelineFlush  bool
	}{
		{
			name:               "skip pk dedup false",
			skipPkDedup:        false,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  false,
		},
		{
			name:               "skip pk dedup true",
			skipPkDedup:        true,
			nilCtxBeforeInsert: false,
			wantPipelineFlush:  true,
		},
		{
			name:               "skip pk dedup true with nil proc ctx",
			skipPkDedup:        true,
			nilCtxBeforeInsert: true,
			wantPipelineFlush:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			proc := testutil.NewProcess(t)
			proc.Base.SessionInfo.Buf = buffer.New()
			proc.Base.SessionInfo.TimeZone = time.Local

			serviceID := "alter-copy-pipeline-flush-" + tc.name
			lockSvc := mock_lock.NewMockLockService(ctrl)
			lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
			proc.Base.LockService = lockSvc
			require.Equal(t, serviceID, proc.GetService())

			const accountID = catalog.System_Account
			ctx := defines.AttachAccountId(context.Background(), accountID)
			proc.Ctx = ctx
			proc.ReplaceTopCtx(ctx)

			txnCli, txnOp := newTestTxnClientAndOpWithModeIsolation(
				ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_SI,
			)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp
			txnOp.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().
				Return(timestamp.Timestamp{}).AnyTimes()

			tableDef := &plan.TableDef{
				TblId: 1,
				Name:  "dept",
			}
			copyTableDef := &plan.TableDef{
				TblId: 2,
				Name:  "dept_copy",
			}
			alterTable := &plan2.AlterTable{
				Database:          "test",
				TableDef:          tableDef,
				CopyTableDef:      copyTableDef,
				CreateTmpTableSql: "create table dept_copy",
				InsertTmpDataSql:  "insert into dept_copy select * from dept",
				Options:           &plan2.AlterCopyOpt{SkipPkDedup: tc.skipPkDedup},
			}
			s := &Scope{
				Magic: AlterTable,
				Plan: &plan.Plan{
					Plan: &plan2.Plan_Ddl{
						Ddl: &plan2.DataDefinition{
							DdlType: plan2.DataDefinition_ALTER_TABLE,
							Definition: &plan2.DataDefinition_AlterTable{
								AlterTable: alterTable,
							},
						},
					},
				},
			}

			originRel := mock_frontend.NewMockRelation(ctrl)
			originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
			originRel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef).AnyTimes()
			originRel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
			originRel.EXPECT().CopyTableDef(gomock.Any()).
				Return(plan.DeepCopyTableDef(tableDef, true)).Times(1)

			copyRel := mock_frontend.NewMockRelation(ctrl)
			if tc.nilCtxBeforeInsert {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).DoAndReturn(func(context.Context) *plan.TableDef {
					proc.Ctx = nil
					return &plan.TableDef{
						TblId: 2,
						Name:  "dept_copy",
					}
				})
			} else {
				copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(&plan.TableDef{
					TblId: 2,
					Name:  "dept_copy",
				}).AnyTimes()
			}

			mockDb := mock_frontend.NewMockDatabase(ctrl)
			mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
			mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

			eng := mock_frontend.NewMockEngine(ctrl)
			eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

			spyExec := &alterCopyInsertSpyExecutor{
				insertSQL: alterTable.InsertTmpDataSql,
				insertErr: insertErr,
			}
			rt := moruntime.DefaultRuntime()
			rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
			moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

			c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
			c.pn = s.Plan
			c.disableLock = true
			origCtx := proc.Ctx

			err := s.AlterTableCopy(c)
			require.ErrorIs(t, err, insertErr)
			require.NotNil(t, spyExec.insertCtx)
			assert.Equal(t, tc.wantPipelineFlush, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)

			insertAccountID, err := defines.GetAccountId(spyExec.insertCtx)
			require.NoError(t, err)
			assert.Equal(t, accountID, insertAccountID)

			if tc.nilCtxBeforeInsert {
				require.NotNil(t, proc.Ctx)
				require.NotSame(t, spyExec.insertCtx, proc.Ctx)
				require.Same(t, proc.GetTopContext(), proc.Ctx)

				restoredAccountID, err := defines.GetAccountId(proc.Ctx)
				require.NoError(t, err)
				assert.Equal(t, accountID, restoredAccountID)
			} else {
				require.Same(t, origCtx, proc.Ctx)
			}
			assert.NotEqual(t, true, proc.Ctx.Value(ioutil.PipelineFlushKey))

			if tc.skipPkDedup {
				require.Same(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
			} else {
				require.Nil(t, spyExec.insertOption.AlterCopyDedupOpt())
			}
		})
	}
}

func TestGetAlterCopyPkPrecheck(t *testing.T) {
	for _, tc := range []struct {
		name             string
		tableDef         *plan.TableDef
		copyTableDef     *plan.TableDef
		changeColMap     map[uint64]*plan.ColDef
		skipPkDedup      bool
		wantCols         []string
		wantCheckNotNull bool
	}{
		{
			name: "add pk on nullable original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{ColId: 1, Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			changeColMap:     map[uint64]*plan.ColDef{1: {Name: "col4"}},
			wantCols:         []string{"col4"},
			wantCheckNotNull: true,
		},
		{
			name: "add pk on not null original column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{ColId: 1, Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			changeColMap: map[uint64]*plan.ColDef{1: {Name: "col4"}},
			wantCols:     []string{"col4"},
		},
		{
			name: "same name pk replacement is not a copied source column",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{ColId: 1, Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{ColId: 2, Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
		{
			name: "static skip pk dedup needs no precheck",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			skipPkDedup: true,
		},
		{
			name: "pk column is not copied from original table",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "new_col", Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "new_col", Names: []string{"new_col"}},
			},
		},
		{
			name: "pk column type change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 16}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_int32)}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
		{
			name: "pk column width change can change dedup key value",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", Typ: plan.Type{Id: int32(types.T_varchar), Width: 32}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{Name: "col4", NotNull: true, Primary: true, Typ: plan.Type{Id: int32(types.T_varchar), Width: 8}}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
		{
			name: "generated pk is recomputed during copy",
			tableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{
					Name: "col4",
					Typ:  plan.Type{Id: int32(types.T_int32)},
					GeneratedCol: &plan2.GeneratedCol{
						IsStored: true,
					},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
			copyTableDef: &plan.TableDef{
				Cols: []*plan.ColDef{{
					Name: "col4",
					Typ:  plan.Type{Id: int32(types.T_int32)},
					GeneratedCol: &plan2.GeneratedCol{
						IsStored: true,
					},
				}},
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qry := &plan2.AlterTable{
				TableDef:          tc.tableDef,
				CopyTableDef:      tc.copyTableDef,
				ChangeTblColIdMap: tc.changeColMap,
				Options: &plan2.AlterCopyOpt{
					SkipPkDedup:     tc.skipPkDedup,
					TargetTableName: "dept_copy",
				},
			}
			pkCols, checkNotNull := getAlterCopyPkPrecheck(qry)
			assert.Equal(t, tc.wantCols, pkCols)
			assert.Equal(t, tc.wantCheckNotNull, checkNotNull)
		})
	}
}

func TestScopeAlterTableCopyPrecheckPrimaryKeyThenSkipDedup(t *testing.T) {
	lockDatabaseStub := gostub.Stub(&lockMoDatabase,
		func(_ *Compile, _ string, _ lock.LockMode) error { return nil })
	defer lockDatabaseStub.Reset()
	lockTableMetadataStub := gostub.Stub(&lockMoTable,
		func(_ *Compile, _, _ string, _ lock.LockMode) error { return nil })
	defer lockTableMetadataStub.Reset()
	lockRelationStub := gostub.Stub(&lockTable,
		func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return nil
		})
	defer lockRelationStub.Reset()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-pk-precheck"
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc
	require.Equal(t, serviceID, proc.GetService())

	const accountID = catalog.System_Account
	ctx := defines.AttachAccountId(context.Background(), accountID)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOpWithModeIsolation(
		ctrl, txn.TxnMode_Pessimistic, txn.TxnIsolation_SI,
	)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp
	txnOp.(*mock_frontend.MockTxnOperator).EXPECT().SnapshotTS().
		Return(timestamp.Timestamp{}).AnyTimes()

	tableDef := &plan.TableDef{
		TblId: 1,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{ColId: 1, Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
	}
	copyTableDef := &plan.TableDef{
		TblId: 2,
		Name:  "dept_copy",
		Cols: []*plan.ColDef{
			{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
	}
	alterTable := &plan2.AlterTable{
		Database:          "test",
		TableDef:          tableDef,
		CopyTableDef:      copyTableDef,
		CreateTmpTableSql: "create table dept_copy",
		InsertTmpDataSql:  "insert into dept_copy select * from dept",
		ChangeTblColIdMap: map[uint64]*plan.ColDef{1: {Name: "col4"}},
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
	}
	s := &Scope{
		Magic: AlterTable,
		Plan: &plan.Plan{
			Plan: &plan2.Plan_Ddl{
				Ddl: &plan2.DataDefinition{
					DdlType: plan2.DataDefinition_ALTER_TABLE,
					Definition: &plan2.DataDefinition_AlterTable{
						AlterTable: alterTable,
					},
				},
			},
		},
	}

	originRel := mock_frontend.NewMockRelation(ctrl)
	originRel.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
	originRel.EXPECT().GetTableDef(gomock.Any()).Return(tableDef).AnyTimes()
	originRel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()
	originRel.EXPECT().CopyTableDef(gomock.Any()).
		Return(plan.DeepCopyTableDef(tableDef, true)).Times(1)

	copyRel := mock_frontend.NewMockRelation(ctrl)
	copyRel.EXPECT().CopyTableDef(gomock.Any()).Return(copyTableDef).AnyTimes()

	mockDb := mock_frontend.NewMockDatabase(ctrl)
	mockDb.EXPECT().Relation(gomock.Any(), "dept", gomock.Any()).Return(originRel, nil).AnyTimes()
	mockDb.EXPECT().Relation(gomock.Any(), "dept_copy", gomock.Any()).Return(copyRel, nil).AnyTimes()

	eng := mock_frontend.NewMockEngine(ctrl)
	eng.EXPECT().Database(gomock.Any(), "test", gomock.Any()).Return(mockDb, nil).AnyTimes()

	insertErr := errors.New("stop after insert-copy")
	spyExec := &alterCopyInsertSpyExecutor{
		insertSQL: alterTable.InsertTmpDataSql,
		insertErr: insertErr,
	}
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, spyExec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = s.Plan
	c.disableLock = true

	err := s.AlterTableCopy(c)
	require.ErrorIs(t, err, insertErr)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotNil(t, spyExec.insertCtx)
	assert.Equal(t, true, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)
	require.NotSame(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
	require.True(t, spyExec.insertOption.AlterCopyDedupOpt().SkipPkDedup)
	require.Equal(t, alterTable.Options.TargetTableName, spyExec.insertOption.AlterCopyDedupOpt().TargetTableName)
	assert.Equal(t, []string{
		alterDataBranchParticipationSQL(1),
		alterDataBranchHistoricalSnapshotSourceProbeSQL("", "test", "dept", 1, false, 0),
		alterDataBranchHistoricalPitrSourceProbeSQL("", "test", "dept", 1, false, 0),
		alterDataBranchHistoricalSnapshotSourceProbeSQL("", "test", "dept", 1, false, 0),
		alterDataBranchHistoricalPitrSourceProbeSQL("", "test", "dept", 1, false, 0),
		alterTable.CreateTmpTableSql,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterTable.InsertTmpDataSql,
	}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsNull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkNullCheckSQL: newAlterCopyConstNullResult(c.proc.Mp(), types.T_int32.ToType()),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrConstraintViolation))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupRejectsDuplicate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	spyExec.results = map[string]executor.Result{
		alterCopyTestPkDuplicateCheckSQL: newAlterCopyFixedResult(t, c.proc.Mp(), types.T_int32.ToType(), []int32{7}),
	}

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.Error(t, err)
	require.Nil(t, opt)
	assert.True(t, moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry))
	assert.False(t, alterTable.Options.SkipPkDedup)
	assert.Equal(t, []string{alterCopyTestPkNullCheckSQL, alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupCanSkipNullCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	alterTable.TableDef.Cols[0].NotNull = true
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	opt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, opt)
	assert.True(t, opt.SkipPkDedup)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, alterTable.Options, opt)
	assert.Equal(t, []string{alterCopyTestPkDuplicateCheckSQL}, spyExec.executedSQLs)
}

func TestPrecheckAlterCopyPkDedupDoesNotMutatePlanOption(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	alterTable := testAlterCopyAddPrimaryKeyPlan()
	spyExec := &alterCopyInsertSpyExecutor{}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)

	firstOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, firstOpt)
	require.True(t, firstOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)

	secondOpt, err := c.precheckAlterCopyPkDedup("test", "dept", alterTable)
	require.NoError(t, err)
	require.NotNil(t, secondOpt)
	require.True(t, secondOpt.SkipPkDedup)
	require.False(t, alterTable.Options.SkipPkDedup)
	require.NotSame(t, firstOpt, secondOpt)

	assert.Equal(t, []string{
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
		alterCopyTestPkNullCheckSQL,
		alterCopyTestPkDuplicateCheckSQL,
	}, spyExec.executedSQLs)
}

func testAlterCopyAddPrimaryKeyPlan() *plan2.AlterTable {
	return &plan2.AlterTable{
		Database: "test",
		TableDef: &plan.TableDef{
			Name: "dept",
			Cols: []*plan.ColDef{
				{ColId: 1, Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.FakePrimaryKeyColName},
		},
		CopyTableDef: &plan.TableDef{
			Name: "dept_copy",
			Cols: []*plan.ColDef{
				{Name: "col4", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "col4", Names: []string{"col4"}},
		},
		Options: &plan2.AlterCopyOpt{
			SkipPkDedup:     false,
			TargetTableName: "dept_copy",
		},
		ChangeTblColIdMap: map[uint64]*plan.ColDef{1: {Name: "col4"}},
	}
}

func newAlterCopyPrecheckCompile(
	t *testing.T,
	ctrl *gomock.Controller,
	exec executor.SQLExecutor,
	serviceSuffix ...string,
) *Compile {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-precheck-" + t.Name()
	if len(serviceSuffix) > 0 {
		serviceID += "-" + serviceSuffix[0]
	}
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOpWithModeIsolation(
		ctrl,
		txn.TxnMode_Pessimistic,
		txn.TxnIsolation_SI,
		alterCopyAutoIncrEpochWorkspace{
			Workspace: &Ws{},
			supported: true,
		},
	)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, exec)
	moruntime.SetupServiceBasedRuntime(proc.GetService(), rt)

	eng := mock_frontend.NewMockEngine(ctrl)
	c := NewCompile("test", "test", "alter table dept", "", "", eng, proc, nil, false, nil, time.Now())
	c.pn = &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
			},
		},
	}
	return c
}

func newAlterCopyConstNullResult(mp *mpool.MPool, typ types.Type) executor.Result {
	bat := batch.NewWithSize(1)
	bat.SetRowCount(1)
	bat.Vecs[0] = vector.NewConstNull(typ, 1, mp)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func newAlterCopyFixedResult[T any](t *testing.T, mp *mpool.MPool, typ types.Type, values []T) executor.Result {
	memRes := executor.NewMemResult([]types.Type{typ}, mp)
	memRes.NewBatchWithRowCount(len(values))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, values))
	return memRes.GetResult()
}

func TestLoadAlterDataBranchHistoricalSourcesUsesPitrCatalogType(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	results := map[string]executor.Result{
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(
			t, mp, nil, nil, nil, nil, nil, nil,
		),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp,
			[]string{"database", "table"},
			[]string{"tenant", "tenant"},
			[]string{"db_hour", "db_day"},
			[]string{"", "tbl"},
			[]uint64{101, 102},
			[]uint8{1, 100},
			[]string{"h", "d"},
		),
	}

	sources, err := loadAlterDataBranchHistoricalSourcesWithQuery(
		func(sql string) (executor.Result, error) {
			res, ok := results[sql]
			require.True(t, ok, "unexpected lineage source query: %s", sql)
			return res, nil
		},
		now,
	)
	require.NoError(t, err)
	require.Equal(t, []databranchutils.HistoricalSource{
		{
			Level:        "database",
			AccountName:  "tenant",
			DatabaseName: "db_hour",
			ObjectID:     101,
			OldestTS:     now.Add(-time.Hour).UnixNano(),
		},
		{
			Level:        "table",
			AccountName:  "tenant",
			DatabaseName: "db_day",
			TableName:    "tbl",
			ObjectID:     102,
			OldestTS:     now.AddDate(0, 0, -100).UnixNano(),
		},
	}, sources)
}

func TestCompactExpiredAlterDataBranchLineage(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	const (
		metadataSQL = "select table_id, p_table_id, clone_ts, creator, level, table_deleted from mo_catalog.mo_branch_metadata for update"
		edgeSQL     = "select sname, ts, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind = 'branch'"
		snapshotSQL = "select ts, level, account_name, database_name, table_name, obj_id from mo_catalog.mo_snapshots where kind = 'user'"
		pitrSQL     = "select level, account_name, database_name, table_name, obj_id, pitr_length, pitr_unit from mo_catalog.mo_pitr where pitr_status = 1"
	)

	for _, tc := range []struct {
		name          string
		pitrLength    uint8
		wantDeletes   bool
		wantSQLSuffix []string
	}{
		{
			name:        "expired PITR releases ALTER edge",
			pitrLength:  24,
			wantDeletes: true,
			wantSQLSuffix: []string{
				"delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')",
				"delete from mo_catalog.mo_branch_metadata where table_id in (2) and (level = 'alter' or level like 'alter:%')",
			},
		},
		{
			name:        "active PITR retains ALTER edge",
			pitrLength:  72,
			wantDeletes: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			spyExec := &alterCopyInsertSpyExecutor{results: make(map[string]executor.Result)}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			mp := c.proc.Mp()

			spyExec.results[metadataSQL] = newAlterLineageMetadataResult(
				t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
				[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
			)
			spyExec.results[edgeSQL] = newAlterLineageEdgeResult(
				t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
				[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
			)
			spyExec.results[snapshotSQL] = newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil)
			spyExec.results[pitrSQL] = newAlterLineagePitrSourceResult(
				t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
				[]uint64{1}, []uint8{tc.pitrLength}, []string{"h"},
			)

			require.NoError(t, c.compactExpiredAlterDataBranchLineage(now))
			want := []string{metadataSQL, edgeSQL, snapshotSQL, pitrSQL}
			if tc.wantDeletes {
				want = append(want, tc.wantSQLSuffix...)
			}
			require.Equal(t, want, spyExec.executedSQLs)
		})
	}
}

func TestCompactExpiredAlterDataBranchLineageWithExecutorStopsOnLifecycleGateError(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	wantErr := errors.New("lifecycle gate failed")
	var executed []string
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		if sql == catalog.SnapshotLifecycleGateSQL {
			return executor.Result{}, wantErr
		}
		switch sql {
		case catalog.FeatureRegistryCatalogSharedGateSQL:
			return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{272476}), nil
		case metadataSQL:
			return newAlterLineageMetadataResult(
				t, mp, []uint64{2}, []uint64{1}, []int64{now.Add(-48 * time.Hour).UnixNano()},
				[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{true},
			), nil
		case alterDataBranchLineageEdgeSQL():
			return newAlterLineageEdgeResult(t, mp, nil, nil, nil, nil, nil, nil), nil
		case alterDataBranchSnapshotSourceSQL():
			return newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil), nil
		case alterDataBranchPitrSourceSQL():
			return newAlterLineagePitrSourceResult(t, mp, nil, nil, nil, nil, nil, nil, nil), nil
		default:
			return executor.Result{}, nil
		}
	})

	err := compactExpiredAlterDataBranchLineageWithExecutor(
		context.Background(), sqlExecutor, now,
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, []string{
		metadataSQL,
		alterDataBranchLineageEdgeSQL(),
		alterDataBranchSnapshotSourceSQL(),
		alterDataBranchPitrSourceSQL(),
		catalog.FeatureRegistryCatalogSharedGateSQL,
		catalog.SnapshotLifecycleGateSQL,
	}, executed)
}

type lineageGCTestExecutor struct {
	t                   *testing.T
	mp                  *mpool.MPool
	remaining           []uint64
	expectedBatchSize   int
	gateErr             error
	onGate              func()
	opts                []executor.Options
	transactions        [][]string
	statementOpts       [][]executor.StatementOption
	committedBatchSizes []int
	rolledBack          int
	execCtxs            []context.Context
	waitForContextEnd   bool
	frontierErr         error
	frontierCalls       int
	registryID          uint64
}

type lineageGCTestFrontierExecutor struct {
	executor.SQLExecutor
	frontierErr   error
	frontierCalls int
}

func (e *lineageGCTestFrontierExecutor) advanceLineageGCAppliedSnapshot(context.Context, client.TxnOperator) error {
	e.frontierCalls++
	return e.frontierErr
}

func (e *lineageGCTestExecutor) advanceLineageGCAppliedSnapshot(context.Context, client.TxnOperator) error {
	e.frontierCalls++
	return e.frontierErr
}

type lineageGCTestTxnExecutor struct {
	owner       *lineageGCTestExecutor
	txnIndex    int
	deleteCount int
}

func (e *lineageGCTestTxnExecutor) Use(string) {}

func (e *lineageGCTestTxnExecutor) LockTable(string) error { return nil }

func (e *lineageGCTestTxnExecutor) Txn() client.TxnOperator { return nil }

func (e *lineageGCTestTxnExecutor) Exec(
	sql string,
	opts executor.StatementOption,
) (executor.Result, error) {
	e.owner.transactions[e.txnIndex] = append(e.owner.transactions[e.txnIndex], sql)
	e.owner.statementOpts[e.txnIndex] = append(e.owner.statementOpts[e.txnIndex], opts)
	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	switch sql {
	case catalog.FeatureRegistryCatalogSharedGateSQL:
		if e.owner.registryID == 0 {
			e.owner.registryID = 272476
		}
		return newAlterCopyFixedResult(e.owner.t, e.owner.mp, types.T_uint64.ToType(), []uint64{e.owner.registryID}), nil
	case catalog.SnapshotLifecycleGateSQL:
		if e.owner.onGate != nil {
			e.owner.onGate()
		}
		if e.owner.gateErr != nil {
			return executor.Result{}, e.owner.gateErr
		}
		return newAlterCopyFixedResult(e.owner.t, e.owner.mp, types.T_uint64.ToType(), []uint64{1}), nil
	case metadataSQL:
		rowCount := len(e.owner.remaining)
		parents := make([]uint64, rowCount)
		cloneTSs := make([]int64, rowCount)
		creators := make([]uint64, rowCount)
		levels := make([]string, rowCount)
		deleted := make([]bool, rowCount)
		for i := range rowCount {
			cloneTSs[i] = 1
			creators[i] = uint64(catalog.System_Account)
			levels[i] = databranchutils.AlterLineageLevel
			deleted[i] = true
		}
		return newAlterLineageMetadataResult(
			e.owner.t, e.owner.mp, e.owner.remaining, parents, cloneTSs, creators, levels, deleted,
		), nil
	case alterDataBranchLineageEdgeSQL():
		return newAlterLineageEdgeResult(e.owner.t, e.owner.mp, nil, nil, nil, nil, nil, nil), nil
	case alterDataBranchSnapshotSourceSQL():
		return newAlterLineageSnapshotSourceResult(e.owner.t, e.owner.mp, nil, nil, nil, nil, nil, nil), nil
	case alterDataBranchPitrSourceSQL():
		return newAlterLineagePitrSourceResult(e.owner.t, e.owner.mp, nil, nil, nil, nil, nil, nil, nil), nil
	case databranchutils.LineageOwnerLifecycleLockSQL():
		return executor.Result{}, nil
	default:
		if strings.HasPrefix(sql, "delete from mo_catalog.mo_branch_metadata") {
			e.deleteCount = min(e.owner.expectedBatchSize, len(e.owner.remaining))
		}
		return executor.Result{}, nil
	}
}

func (e *lineageGCTestExecutor) Exec(
	context.Context, string, executor.Options,
) (executor.Result, error) {
	return executor.Result{}, nil
}

func (e *lineageGCTestExecutor) ExecTxn(
	ctx context.Context,
	execFunc func(executor.TxnExecutor) error,
	opts executor.Options,
) error {
	e.execCtxs = append(e.execCtxs, ctx)
	if e.waitForContextEnd {
		<-ctx.Done()
		e.rolledBack++
		return ctx.Err()
	}
	txnIndex := len(e.transactions)
	e.opts = append(e.opts, opts)
	e.transactions = append(e.transactions, nil)
	e.statementOpts = append(e.statementOpts, nil)
	txn := &lineageGCTestTxnExecutor{owner: e, txnIndex: txnIndex}
	err := execFunc(txn)
	if err != nil {
		e.rolledBack++
		return err
	}
	if txn.deleteCount > 0 {
		e.remaining = e.remaining[txn.deleteCount:]
		e.committedBatchSizes = append(e.committedBatchSizes, txn.deleteCount)
	}
	return nil
}

func newLineageGCTestExecutor(
	t *testing.T,
	remaining []uint64,
	batchSize int,
) *lineageGCTestExecutor {
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	return &lineageGCTestExecutor{
		t:                 t,
		mp:                c.proc.Mp(),
		remaining:         append([]uint64(nil), remaining...),
		expectedBatchSize: batchSize,
	}
}

func TestDataBranchLineageGCRejectsStaleDiscovery(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	metadataSQL := fmt.Sprintf("select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA)
	for _, tc := range []struct {
		name         string
		swapRegistry bool
	}{
		{name: "late PITR protects old edge"},
		{name: "replaced registry invalidates lock", swapRegistry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gateReached := false
			deletes := 0
			base := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
				switch sql {
				case metadataSQL:
					return newAlterLineageMetadataResult(t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
						[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false}), nil
				case alterDataBranchLineageEdgeSQL():
					return newAlterLineageEdgeResult(t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
						[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1}), nil
				case alterDataBranchSnapshotSourceSQL():
					return newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil), nil
				case alterDataBranchPitrSourceSQL():
					if gateReached && !tc.swapRegistry {
						return newAlterLineagePitrSourceResult(t, mp, []string{"table"}, []string{"tenant"},
							[]string{"db"}, []string{"tbl"}, []uint64{1}, []uint8{72}, []string{"h"}), nil
					}
					return newAlterLineagePitrSourceResult(t, mp, nil, nil, nil, nil, nil, nil, nil), nil
				case catalog.FeatureRegistryCatalogSharedGateSQL:
					id := uint64(272476)
					if gateReached && tc.swapRegistry {
						id++
					}
					return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{id}), nil
				case catalog.SnapshotLifecycleGateSQL:
					gateReached = true
					return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{1}), nil
				default:
					if strings.HasPrefix(sql, "delete from mo_catalog.") {
						deletes++
					}
					return executor.Result{}, nil
				}
			})
			withFrontier := &lineageGCTestFrontierExecutor{SQLExecutor: base}
			err := compactExpiredAlterDataBranchLineageWithExecutor(context.Background(), withFrontier, now)
			if tc.swapRegistry {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, gateReached, "ungated plan must contain a candidate")
			require.Equal(t, 1, withFrontier.frontierCalls)
			require.Zero(t, deletes, "a stale plan must never delete a protected edge")
		})
	}
}

func TestDataBranchLineageGCExecutorMakesDurableProgressAcrossRuns(t *testing.T) {
	const batchSize = 2
	spyExec := newLineageGCTestExecutor(t, []uint64{1, 2, 3, 4, 5}, batchSize)
	run := dataBranchLineageGCExecutor(spyExec, batchSize)

	require.NoError(t, run(context.Background(), nil))
	require.Equal(t, []uint64{3, 4, 5}, spyExec.remaining)
	require.Equal(t, []int{2}, spyExec.committedBatchSizes)
	require.NoError(t, run(context.Background(), nil))
	require.Equal(t, []uint64{5}, spyExec.remaining)
	require.Equal(t, []int{2, 2}, spyExec.committedBatchSizes)
	require.NoError(t, run(context.Background(), nil))
	require.Empty(t, spyExec.remaining)
	require.Equal(t, []int{2, 2, 1}, spyExec.committedBatchSizes)
	require.Zero(t, spyExec.rolledBack)

	gateSQL := databranchutils.LineageOwnerLifecycleLockSQL()
	metadataDeletes := make([]string, 0, len(spyExec.committedBatchSizes))
	for txnIndex, sqls := range spyExec.transactions {
		deadline, ok := spyExec.execCtxs[txnIndex].Deadline()
		require.True(t, ok, "each invocation must have a hard task-work budget")
		require.WithinDuration(t, time.Now().Add(dataBranchLineageGCTimeBudget), deadline, 5*time.Second)
		require.True(t, spyExec.opts[txnIndex].HasLockWaitTimeout())
		require.Equal(t, dataBranchLineageGCLockWaitTimeout, spyExec.opts[txnIndex].LockWaitTimeout())
		require.True(t, spyExec.opts[txnIndex].HasTxnIsolation())
		require.Equal(t, txn.TxnIsolation_RC, spyExec.opts[txnIndex].TxnIsolation())
		require.True(t, spyExec.opts[txnIndex].HasTxnMode())
		require.Equal(t, txn.TxnMode_Pessimistic, spyExec.opts[txnIndex].TxnMode())
		gateIndex := slices.Index(sqls, catalog.SnapshotLifecycleGateSQL)
		if gateIndex < 0 {
			// The final empty discovery transaction performs no mutation.
			require.Len(t, sqls, 1)
			continue
		}
		require.Equal(t, 5, gateIndex, "provisional discovery precedes C/G admission")
		require.Equal(t, catalog.FeatureRegistryCatalogSharedGateSQL, sqls[4])
		require.Equal(t, lock.WaitPolicy_FastFail, spyExec.statementOpts[txnIndex][gateIndex].WaitPolicy())
		require.Equal(t, catalog.FeatureRegistryCatalogSharedGateSQL, sqls[6])
		require.Equal(t, gateSQL, sqls[11], "retain the explicit-branch MVCC validation write")
		require.Len(t, sqls[gateIndex+1:], 8, "authoritative rescan and bounded deletion")
		metadataDeletes = append(metadataDeletes, sqls[13])
	}
	require.Equal(t, []string{
		"delete from mo_catalog.mo_branch_metadata where table_id in (1,2) and (level = 'alter' or level like 'alter:%')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (3,4) and (level = 'alter' or level like 'alter:%')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (5) and (level = 'alter' or level like 'alter:%')",
	}, metadataDeletes)
}

func TestDataBranchLineageGCExecutorRechecksAndBoundsMutationAtScale(t *testing.T) {
	const candidateCount = 2049
	remaining := make([]uint64, candidateCount)
	for i := range remaining {
		remaining[i] = uint64(i + 1)
	}
	spyExec := newLineageGCTestExecutor(t, remaining, dataBranchLineageGCBatchSize)

	require.NoError(t, dataBranchLineageGCExecutor(spyExec, dataBranchLineageGCBatchSize)(context.Background(), nil))
	require.Len(t, spyExec.transactions, 1,
		"one invocation must not amplify full-catalog discovery across batches")
	require.Len(t, spyExec.transactions[0], 14,
		"one provisional scan, one authoritative scan and one bounded delete pair")
	require.Len(t, spyExec.committedBatchSizes, 1)
	require.Equal(t, dataBranchLineageGCBatchSize, spyExec.committedBatchSizes[0])
	require.Len(t, spyExec.remaining, candidateCount-dataBranchLineageGCBatchSize)
}

func TestDataBranchLineageGCExecutorDefersOnLocalTimeBudget(t *testing.T) {
	spyExec := newLineageGCTestExecutor(t, []uint64{1}, 1)
	spyExec.waitForContextEnd = true

	require.NoError(t,
		dataBranchLineageGCExecutorWithBudget(spyExec, 1, time.Millisecond)(context.Background(), nil))
	require.Equal(t, 1, spyExec.rolledBack)
	require.Equal(t, []uint64{1}, spyExec.remaining)
}

func TestDataBranchLineageGCExecutorDefersAfterContentionRollback(t *testing.T) {
	for _, contentionErr := range []error{
		moerr.NewLockConflictNoCtx(),
		moerr.NewLockWaitTimeoutNoCtx(),
		moerr.NewTxnNeedRetryNoCtx(),
		moerr.NewTxnNeedRetryWithDefChangedNoCtx(),
	} {
		spyExec := newLineageGCTestExecutor(t, []uint64{1}, 1)
		spyExec.gateErr = contentionErr
		require.NoError(t, dataBranchLineageGCExecutor(spyExec, 1)(context.Background(), nil))
		require.Equal(t, 1, spyExec.rolledBack)
		require.Equal(t, []uint64{1}, spyExec.remaining)
		require.Equal(t, catalog.SnapshotLifecycleGateSQL, spyExec.transactions[0][5])
		require.Len(t, spyExec.transactions[0], 6)
		require.Equal(t, lock.WaitPolicy_FastFail, spyExec.statementOpts[0][5].WaitPolicy())
	}
}

func TestDataBranchLineageGCExecutorParentContextPrecedesContention(t *testing.T) {
	for _, tc := range []struct {
		name          string
		cause         error
		contentionErr error
	}{
		{
			name:          "canceled parent with lock conflict",
			cause:         context.Canceled,
			contentionErr: moerr.NewLockConflictNoCtx(),
		},
		{
			name:          "expired parent with lock timeout",
			cause:         context.DeadlineExceeded,
			contentionErr: moerr.NewLockWaitTimeoutNoCtx(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			spyExec := newLineageGCTestExecutor(t, []uint64{1}, 1)
			spyExec.gateErr = tc.contentionErr
			spyExec.onGate = func() { cancel(tc.cause) }
			err := dataBranchLineageGCExecutor(spyExec, 1)(ctx, nil)
			require.ErrorIs(t, err, tc.cause)
			require.Equal(t, 1, spyExec.rolledBack)
			require.Equal(t, []uint64{1}, spyExec.remaining)
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	spyExec := newLineageGCTestExecutor(t, []uint64{1}, 1)
	require.ErrorIs(t, dataBranchLineageGCExecutor(spyExec, 1)(ctx, nil), context.Canceled)
	require.Empty(t, spyExec.transactions, "an ended parent must stop before transaction admission")
}

func TestDataBranchLineageGCExecutorDoesNotSuppressOtherErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "remote owner timeout", err: moerr.NewRemoteLockWaitTimeoutNoCtx()},
		{name: "execution failure", err: errors.New("gc failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spyExec := newLineageGCTestExecutor(t, []uint64{1}, 1)
			spyExec.gateErr = tc.err
			err := dataBranchLineageGCExecutor(spyExec, 1)(context.Background(), nil)
			require.Error(t, err)
			if moerr.IsMoErrCode(tc.err, moerr.ErrRemoteLockWaitTimeout) {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrRemoteLockWaitTimeout))
			} else {
				require.ErrorIs(t, err, tc.err)
			}
			require.Equal(t, 1, spyExec.rolledBack)
			require.Equal(t, []uint64{1}, spyExec.remaining)
		})
	}
}

func TestOwnerCatalogDropEntryPointsStopAtLifecycleAdmissionFailure(t *testing.T) {
	gateSQL := databranchutils.LineageOwnerLifecycleLockSQL()
	wantErr := errors.New("lifecycle gate failed")
	for _, tc := range []struct {
		name string
		run  func(*Scope, *Compile) error
	}{
		{
			name: "drop database",
			run: func(s *Scope, c *Compile) error {
				s.Plan = &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
					Definition: &plan2.DataDefinition_DropDatabase{
						DropDatabase: &plan2.DropDatabase{Database: "db"},
					},
				}}}
				return s.DropDatabase(c)
			},
		},
		{
			name: "drop table",
			run: func(s *Scope, c *Compile) error {
				return dropTableScope(&plan2.DropTable{
					Database: "db",
					Table:    "tbl",
					TableDef: &plan2.TableDef{},
				}).DropTable(c)
			},
		},
		{
			name: "drop pitr",
			run: func(s *Scope, c *Compile) error {
				s.Plan = &plan2.Plan{Plan: &plan2.Plan_Ddl{Ddl: &plan2.DataDefinition{
					Definition: &plan2.DataDefinition_DropPitr{
						DropPitr: &plan2.DropPitr{Name: "pitr"},
					},
				}}}
				return s.DropPitr(c)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			spyExec := &alterCopyInsertSpyExecutor{errs: map[string]error{gateSQL: wantErr}}
			c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
			err := tc.run(&Scope{}, c)
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, []string{gateSQL}, spyExec.executedSQLs)
		})
	}
}

func TestCompactExpiredAlterDataBranchLineageWithExecutorPropagatesDeleteError(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()
	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	wantErr := errors.New("delete failed")
	snapshotDeleteSQL := "delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')"
	baseExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		switch sql {
		case catalog.FeatureRegistryCatalogSharedGateSQL:
			return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{272476}), nil
		case catalog.SnapshotLifecycleGateSQL:
			return newAlterCopyFixedResult(t, mp, types.T_uint64.ToType(), []uint64{1}), nil
		case metadataSQL:
			return newAlterLineageMetadataResult(t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
				[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false}), nil
		case alterDataBranchLineageEdgeSQL():
			return newAlterLineageEdgeResult(t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
				[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1}), nil
		case alterDataBranchSnapshotSourceSQL():
			return newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil), nil
		case alterDataBranchPitrSourceSQL():
			return newAlterLineagePitrSourceResult(t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
				[]uint64{1}, []uint8{24}, []string{"h"}), nil
		case snapshotDeleteSQL:
			return executor.Result{}, wantErr
		default:
			return executor.Result{}, nil
		}
	})
	sqlExecutor := &lineageGCTestFrontierExecutor{SQLExecutor: baseExecutor}

	require.ErrorIs(t,
		compactExpiredAlterDataBranchLineageWithExecutor(context.Background(), sqlExecutor, now),
		wantErr,
	)
}

func newAlterLineageMetadataResult(
	t *testing.T,
	mp *mpool.MPool,
	tableIDs, parentIDs []uint64,
	cloneTSs []int64,
	creators []uint64,
	levels []string,
	deleted []bool,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType(),
		types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_bool.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(tableIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, tableIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, parentIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 2, cloneTSs))
	require.NoError(t, executor.AppendFixedRows(memRes, 3, creators))
	require.NoError(t, executor.AppendStringRows(memRes, 4, levels))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, deleted))
	return memRes.GetResult()
}

func newAlterLineageEdgeResult(
	t *testing.T,
	mp *mpool.MPool,
	names []string,
	cloneTSs []int64,
	accounts, databases, tables []string,
	objectIDs []uint64,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_int64.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(names))
	require.NoError(t, executor.AppendStringRows(memRes, 0, names))
	require.NoError(t, executor.AppendFixedRows(memRes, 1, cloneTSs))
	require.NoError(t, executor.AppendStringRows(memRes, 2, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 3, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 4, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, objectIDs))
	return memRes.GetResult()
}

func newAlterLineageSnapshotSourceResult(
	t *testing.T,
	mp *mpool.MPool,
	cloneTSs []int64,
	levels, accounts, databases, tables []string,
	objectIDs []uint64,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_int64.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(cloneTSs))
	require.NoError(t, executor.AppendFixedRows(memRes, 0, cloneTSs))
	require.NoError(t, executor.AppendStringRows(memRes, 1, levels))
	require.NoError(t, executor.AppendStringRows(memRes, 2, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 3, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 4, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, objectIDs))
	return memRes.GetResult()
}

func newAlterLineagePitrSourceResult(
	t *testing.T,
	mp *mpool.MPool,
	levels, accounts, databases, tables []string,
	objectIDs []uint64,
	lengths []uint8,
	units []string,
) executor.Result {
	memRes := executor.NewMemResult([]types.Type{
		types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType(),
		types.T_varchar.ToType(), types.T_uint64.ToType(), types.T_uint8.ToType(),
		types.T_varchar.ToType(),
	}, mp)
	memRes.NewBatchWithRowCount(len(levels))
	require.NoError(t, executor.AppendStringRows(memRes, 0, levels))
	require.NoError(t, executor.AppendStringRows(memRes, 1, accounts))
	require.NoError(t, executor.AppendStringRows(memRes, 2, databases))
	require.NoError(t, executor.AppendStringRows(memRes, 3, tables))
	require.NoError(t, executor.AppendFixedRows(memRes, 4, objectIDs))
	require.NoError(t, executor.AppendFixedRows(memRes, 5, lengths))
	require.NoError(t, executor.AppendStringRows(memRes, 6, units))
	return memRes.GetResult()
}

func TestScope_AlterTableInplace(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{
				ColId: 0,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 1,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	alterTable := &plan2.AlterTable{
		Database: "test",
		TableDef: tableDef,
		Actions: []*plan2.AlterTable_Action{
			{
				Action: &plan2.AlterTable_Action_AddIndex{
					AddIndex: &plan2.AlterTableAddIndex{
						DbName:                "test",
						TableName:             "dept",
						OriginTablePrimaryKey: "deptno",
						IndexTableExist:       true,
						IndexInfo: &plan2.CreateTable{
							TableDef: &plan.TableDef{
								Indexes: []*plan.IndexDef{
									{
										IndexName:      "idx",
										Parts:          []string{"dname", "__mo_alias_deptno"},
										Unique:         false,
										IndexTableName: "__mo_index_secondary_0193d918",
										TableExist:     true,
									},
								},
							},
							IndexTables: []*plan.TableDef{
								{
									Name: "__mo_index_secondary_0193d918-3e7b",
									Cols: []*plan.ColDef{
										{
											Name: "__mo_index_idx_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          61,
												NotNullable: false,
												AutoIncr:    false,
												Width:       65535,
												Scale:       0,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
										{
											Name: "__mo_index_pri_col",
											Alg:  plan2.CompressType_Lz4,
											Typ: plan.Type{
												Id:          27,
												NotNullable: false,
												AutoIncr:    false,
												Width:       32,
												Scale:       -1,
											},
											NotNull: false,
											Default: &plan2.Default{
												NullAbility: false,
											},
											Pkidx: 0,
										},
									},
									Pkey: &plan2.PrimaryKeyDef{
										PkeyColName: "__mo_index_idx_col",
										Names:       []string{"__mo_index_idx_col"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock mo_tables", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			cstrDef := &engine.ConstraintDef{}
			cstrDef.Cts = make([]engine.Constraint, 0)
			return cstrDef, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableInplace(c))
	})
}

func TestScope_AlterTableCopy(t *testing.T) {
	tableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept",
		Cols: []*plan.ColDef{
			{
				ColId: 0,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 1,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       15,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	copyTableDef := &plan.TableDef{
		TblId: 282826,
		Name:  "dept_copy_0193dcb4-4c07-77d8",
		Cols: []*plan.ColDef{
			{
				ColId: 1,
				Name:  "deptno",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          27,
					NotNullable: false,
					AutoIncr:    true,
					Width:       32,
					Scale:       -1,
				},
				Default: &plan2.Default{},
				NotNull: true,
				Primary: true,
				Pkidx:   0,
			},
			{
				ColId: 2,
				Name:  "dname",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       20,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId: 3,
				Name:  "loc",
				Alg:   plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          61,
					NotNullable: false,
					AutoIncr:    false,
					Width:       50,
					Scale:       0,
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
			{
				ColId:  4,
				Name:   "__mo_rowid",
				Hidden: true,
				Alg:    plan2.CompressType_Lz4,
				Typ: plan.Type{
					Id:          101,
					NotNullable: true,
					AutoIncr:    false,
					Width:       0,
					Scale:       0,
					Table:       "dept",
				},
				Default: &plan2.Default{},
				NotNull: false,
				Primary: false,
				Pkidx:   0,
			},
		},
		TableType: "r",
		Createsql: `create table dept (deptno int unsigned auto_increment comment "部门编号", dname varchar(15) comment "部门名称", loc varchar(50) comment "部门所在位置", index idxloc (loc), primary key (deptno)) comment = '部门表'`,
		Pkey: &plan.PrimaryKeyDef{
			Cols:        nil,
			PkeyColId:   0,
			PkeyColName: "deptno",
			Names:       []string{"deptno"},
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:      "idxloc",
				Parts:          []string{"loc", "__mo_alias_deptno"},
				Unique:         false,
				IndexTableName: "__mo_index_secondary_0193dc98-4148-74f4-808a",
				TableExist:     true,
			},
		},
		Defs: []*plan2.TableDef_DefType{
			{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: []*plan.Property{
							{
								Key:   "relkind",
								Value: "r",
							},
						},
					},
				},
			},
		},
	}

	alterTable := &plan2.AlterTable{
		Database:     "test",
		TableDef:     tableDef,
		CopyTableDef: copyTableDef,
	}

	cplan := &plan.Plan{
		Plan: &plan2.Plan_Ddl{
			Ddl: &plan2.DataDefinition{
				DdlType: plan2.DataDefinition_ALTER_TABLE,
				Definition: &plan2.DataDefinition_AlterTable{
					AlterTable: alterTable,
				},
			},
		},
	}

	s := &Scope{
		Magic:     AlterTable,
		Plan:      cplan,
		TxnOffset: 0,
	}

	sql := `alter table dept add index idx(dname)`

	convey.Convey("create table lock mo_database", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
		})
		defer lockMoDb.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table1", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewParseErrorNoCtx("table \"__mo_index_unique_0192748f-6868-7182-a6de-2e457c2975c6\" does not exist")
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})

	convey.Convey("create table lock index table2", t, func() {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		proc := testutil.NewProcess(t)
		proc.Base.SessionInfo.Buf = buffer.New()

		ctx := context.Background()
		proc.Ctx = context.Background()
		txnCli, txnOp := newTestTxnClientAndOpWithPessimistic(ctrl)
		proc.Base.TxnClient = txnCli
		proc.Base.TxnOperator = txnOp
		proc.ReplaceTopCtx(ctx)

		relation := mock_frontend.NewMockRelation(ctrl)
		relation.EXPECT().GetTableID(gomock.Any()).Return(uint64(1)).AnyTimes()
		relation.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{}).AnyTimes()

		mockDb := mock_frontend.NewMockDatabase(ctrl)
		mockDb.EXPECT().GetDatabaseId(gomock.Any()).Return("12").AnyTimes()
		mockDb.EXPECT().Relation(gomock.Any(), gomock.Any(), gomock.Any()).Return(relation, nil).AnyTimes()

		eng := mock_frontend.NewMockEngine(ctrl)
		eng.EXPECT().Database(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockDb, nil).AnyTimes()

		getConstraintDef := gostub.Stub(&GetConstraintDef, func(_ context.Context, _ engine.Relation) (*engine.ConstraintDef, error) {
			return nil, nil
		})
		defer getConstraintDef.Reset()

		lockMoDb := gostub.Stub(&lockMoDatabase, func(_ *Compile, _ string, _ lock.LockMode) error {
			return nil
		})
		defer lockMoDb.Reset()

		lockMoTbl := gostub.Stub(&lockMoTable, func(_ *Compile, _ string, _ string, _ lock.LockMode) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockMoTbl.Reset()

		lockTbl := gostub.Stub(&lockTable, func(_ context.Context, _ engine.Engine, _ *process.Process, _ engine.Relation, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockTbl.Reset()

		lockIdxTbl := gostub.Stub(&lockIndexTable, func(_ context.Context, _ engine.Database, _ engine.Engine, _ *process.Process, _ string, _ bool) error {
			return moerr.NewTxnNeedRetryNoCtx()
		})
		defer lockIdxTbl.Reset()

		c := NewCompile("test", "test", sql, "", "", eng, proc, nil, false, nil, time.Now())
		assert.Error(t, s.AlterTableCopy(c))
	})
}
