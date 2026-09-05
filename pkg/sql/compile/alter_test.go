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
	"math"
	"slices"
	"strings"
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
		"", database, table, oldTableID, false,
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

func TestAdvanceAlterDataBranchLineageSnapshotRejectsOverflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().SnapshotTS().Return(timestamp.Timestamp{
		PhysicalTime: math.MaxInt64 - int64(time.Microsecond) + 1,
	})

	proc := testutil.NewProcess(t)
	proc.Base.TxnOperator = txnOp
	c := &Compile{proc: proc}
	_, err := c.advanceAlterDataBranchLineageSnapshot()
	require.ErrorContains(t, err, "timestamp limit")
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
				[]uint64{reverseMarker},
				map[uint64]*plan2.ColDef{1: {ColId: 101}},
				10,
				11,
			))
			require.Equal(t, uint64(10), sourceForeignKey.ForeignTbl)
		})
	}
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
		autoSvc.EXPECT().SetOffset(c.proc.Ctx, copyDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator()),
		autoSvc.EXPECT().SetOffset(c.proc.Ctx, copyDef.TblId, 1, "renamed_id", uint64(500), c.proc.GetTxnOperator()),
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
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `new_id` > 0 then `new_id` else 0 end), 0) as unsigned) from `test`.`dept_copy`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{0}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	c.proc.SetResolveVariableFunc(func(name string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		switch name {
		case "auto_increment_offset":
			require.True(t, isSystemVar)
			require.False(t, isGlobalVar)
			return int64(10), nil
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
	copyRel.EXPECT().GetDBID(gomock.Any()).Return(uint64(1))
	copyRel.EXPECT().AlterTable(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
			require.Equal(t, []*api.AlterTableReq{
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 9, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		c.proc.Ctx, copyDef.TblId, 1, "new_id", uint64(9), c.proc.GetTxnOperator(),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
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
		c.proc.Ctx, copyDef.TblId, 1, "new_id", uint64(7), c.proc.GetTxnOperator(),
	)
	incrservice.SetAutoIncrementServiceByID(c.proc.GetService(), autoSvc)

	require.NoError(t, c.reconcileAlterCopyAutoIncrement(
		"test", srcDef, copyDef, copyRel, false, newAlterAutoIncrementResetCleanup(c),
	))
	require.Equal(t, []string{maxSQL}, spyExec.executedSQLs)
	require.Zero(t, resultMP.CurrNB())
}

func TestReconcileAlterCopyAutoIncrementReappliesConfiguredFreshColumnAlongsideRetainedColumn(t *testing.T) {
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
	c.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
		switch name {
		case "lower_case_table_names":
			return int64(1), nil
		case "auto_increment_offset":
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
				api.NewUpdateAutoIncrementReq(1, copyDef.TblId, 9, 0),
			}, reqs)
			return nil
		},
	)
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	gomock.InOrder(
		autoSvc.EXPECT().SetOffset(
			c.proc.Ctx, copyDef.TblId, 0, "old_id", uint64(50), c.proc.GetTxnOperator(),
		),
		autoSvc.EXPECT().SetOffset(
			c.proc.Ctx, copyDef.TblId, 1, "new_id", uint64(9), c.proc.GetTxnOperator(),
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
		c.proc.Ctx, copyDef.TblId, 0, "id", uint64(500), c.proc.GetTxnOperator(),
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
		c.proc.Ctx, copyDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator(),
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

func TestAppendAlterAutoIncrementReqsUsesStableColumnIndexAfterRename(t *testing.T) {
	ctrl := gomock.NewController(t)
	resultMP := mpool.MustNewZero()
	maxSQL := "select cast(coalesce(max(case when `renamed_id` > 0 then `renamed_id` else 0 end), 0) as unsigned) from `resolved_db`.`dept`"
	spyExec := &alterCopyInsertSpyExecutor{results: map[string]executor.Result{
		maxSQL: newAlterCopyFixedResult(t, resultMP, types.T_uint64.ToType(), []uint64{140}),
	}}
	c := newAlterCopyPrecheckCompile(t, ctrl, spyExec)
	tableDef := &plan.TableDef{
		TblId: 7,
		Name:  "dept",
		Cols: []*plan.ColDef{{
			Name: "renamed_id",
			Typ:  plan.Type{Id: int32(types.T_uint64), AutoIncr: true},
		}},
	}
	autoSvc := mock_frontend.NewMockAutoIncrementService(ctrl)
	autoSvc.EXPECT().SetOffset(
		c.proc.Ctx,
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
		c.proc.Ctx, tableDef.TblId, 0, "new_id", uint64(140), c.proc.GetTxnOperator(),
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
		ctx, tableDef.TblId, 0, "id", uint64(99), c.proc.GetTxnOperator(),
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
	autoSvc.EXPECT().SetOffset(ctx, copyDef.TblId, 0, "first", uint64(99), c.proc.GetTxnOperator()).DoAndReturn(
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

func TestScopeAlterTableCopyInsertTmpDataPipelineFlush(t *testing.T) {
	insertErr := errors.New("stop after insert-copy")

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

			txnCli, txnOp := newTestTxnClientAndOp(ctrl)
			proc.Base.TxnClient = txnCli
			proc.Base.TxnOperator = txnOp

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
			originRel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()

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

	txnCli, txnOp := newTestTxnClientAndOp(ctrl)
	proc.Base.TxnClient = txnCli
	proc.Base.TxnOperator = txnOp

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
	originRel.EXPECT().TableDefs(gomock.Any()).Return(nil, nil).AnyTimes()

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

	err := s.AlterTableCopy(c)
	require.ErrorIs(t, err, insertErr)
	assert.False(t, alterTable.Options.SkipPkDedup)
	require.NotNil(t, spyExec.insertCtx)
	assert.Equal(t, true, spyExec.insertCtx.Value(ioutil.PipelineFlushKey) == true)
	require.NotSame(t, alterTable.Options, spyExec.insertOption.AlterCopyDedupOpt())
	require.True(t, spyExec.insertOption.AlterCopyDedupOpt().SkipPkDedup)
	require.Equal(t, alterTable.Options.TargetTableName, spyExec.insertOption.AlterCopyDedupOpt().TargetTableName)
	assert.Equal(t, []string{
		databranchutils.LineageOwnerLifecycleLockSQL(),
		alterDataBranchParticipationSQL(1),
		alterDataBranchHistoricalSnapshotSourceSQL("", "test", "dept", 1),
		alterDataBranchHistoricalPitrSourceSQL("", "test", "dept", 1),
		alterDataBranchHistoricalSnapshotSourceProbeSQL("", "test", "dept", 1, false),
		alterDataBranchHistoricalPitrSourceProbeSQL("", "test", "dept", 1, false),
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
) *Compile {
	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()
	proc.Base.SessionInfo.TimeZone = time.Local

	serviceID := "alter-copy-precheck-" + t.Name()
	lockSvc := mock_lock.NewMockLockService(ctrl)
	lockSvc.EXPECT().GetConfig().Return(lockservice.Config{ServiceID: serviceID}).AnyTimes()
	proc.Base.LockService = lockSvc

	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	proc.Ctx = ctx
	proc.ReplaceTopCtx(ctx)

	txnCli, txnOp := newTestTxnClientAndOp(
		ctrl,
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

func TestCompactExpiredAlterDataBranchLineageWithExecutor(t *testing.T) {
	now := time.Date(2026, time.July, 17, 12, 0, 0, 0, time.UTC)
	cloneTS := now.Add(-48 * time.Hour).UnixNano()
	ctrl := gomock.NewController(t)
	c := newAlterCopyPrecheckCompile(t, ctrl, &alterCopyInsertSpyExecutor{})
	mp := c.proc.Mp()

	metadataSQL := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, creator, level, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	results := map[string]executor.Result{
		databranchutils.LineageOwnerLifecycleLockSQL(): {},
		metadataSQL: newAlterLineageMetadataResult(
			t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
			[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
		),
		alterDataBranchLineageEdgeSQL(): newAlterLineageEdgeResult(
			t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
			[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
		),
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
			[]uint64{1}, []uint8{24}, []string{"h"},
		),
	}
	var executed []string
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		executed = append(executed, sql)
		return results[sql], nil
	})

	require.NoError(t, compactExpiredAlterDataBranchLineageWithExecutor(context.Background(), sqlExecutor, now))
	require.Equal(t, []string{
		metadataSQL,
		alterDataBranchLineageEdgeSQL(),
		alterDataBranchSnapshotSourceSQL(),
		alterDataBranchPitrSourceSQL(),
		databranchutils.LineageOwnerLifecycleLockSQL(),
		"delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (2) and (level = 'alter' or level like 'alter:%')",
	}, executed)
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
		if sql == databranchutils.LineageOwnerLifecycleLockSQL() {
			return executor.Result{}, wantErr
		}
		switch sql {
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
		databranchutils.LineageOwnerLifecycleLockSQL(),
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
		if e.owner.onGate != nil {
			e.owner.onGate()
		}
		return executor.Result{}, e.owner.gateErr
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
		require.Equal(t, txn.TxnIsolation_SI, spyExec.opts[txnIndex].TxnIsolation())
		gateIndex := slices.Index(sqls, gateSQL)
		if gateIndex < 0 {
			// The final empty discovery transaction performs no mutation.
			require.Len(t, sqls, 1)
			continue
		}
		require.Equal(t, 4, gateIndex, "unbounded discovery must precede lifecycle admission")
		require.Equal(t, lock.WaitPolicy_FastFail, spyExec.statementOpts[txnIndex][gateIndex].WaitPolicy())
		require.Len(t, sqls[gateIndex+1:], 2, "only the bounded delete pair may execute while owning the gate")
		metadataDeletes = append(metadataDeletes, sqls[gateIndex+2])
	}
	require.Equal(t, []string{
		"delete from mo_catalog.mo_branch_metadata where table_id in (1,2) and (level = 'alter' or level like 'alter:%')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (3,4) and (level = 'alter' or level like 'alter:%')",
		"delete from mo_catalog.mo_branch_metadata where table_id in (5) and (level = 'alter' or level like 'alter:%')",
	}, metadataDeletes)
}

func TestDataBranchLineageGCExecutorScansOnceAndBoundsMutationAtScale(t *testing.T) {
	const candidateCount = 2049
	remaining := make([]uint64, candidateCount)
	for i := range remaining {
		remaining[i] = uint64(i + 1)
	}
	spyExec := newLineageGCTestExecutor(t, remaining, dataBranchLineageGCBatchSize)

	require.NoError(t, dataBranchLineageGCExecutor(spyExec, dataBranchLineageGCBatchSize)(context.Background(), nil))
	require.Len(t, spyExec.transactions, 1,
		"one invocation must not amplify full-catalog discovery across batches")
	require.Len(t, spyExec.transactions[0], 7,
		"one fixed-SI discovery, one gate write, and one delete pair are the complete work unit")
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
		require.Equal(t, databranchutils.LineageOwnerLifecycleLockSQL(), spyExec.transactions[0][4])
		require.Len(t, spyExec.transactions[0], 5)
		require.Equal(t, lock.WaitPolicy_FastFail, spyExec.statementOpts[0][4].WaitPolicy())
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
				return s.dropTableSingle(c, &plan2.DropTable{
					Database: "db",
					Table:    "tbl",
					TableDef: &plan2.TableDef{},
				})
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
	results := map[string]executor.Result{
		databranchutils.LineageOwnerLifecycleLockSQL(): {},
		metadataSQL: newAlterLineageMetadataResult(
			t, mp, []uint64{2}, []uint64{1}, []int64{cloneTS},
			[]uint64{uint64(catalog.System_Account)}, []string{databranchutils.AlterLineageLevel}, []bool{false},
		),
		alterDataBranchLineageEdgeSQL(): newAlterLineageEdgeResult(
			t, mp, []string{databranchutils.BranchSnapshotName(2)}, []int64{cloneTS},
			[]string{"tenant"}, []string{"db"}, []string{"tbl"}, []uint64{1},
		),
		alterDataBranchSnapshotSourceSQL(): newAlterLineageSnapshotSourceResult(t, mp, nil, nil, nil, nil, nil, nil),
		alterDataBranchPitrSourceSQL(): newAlterLineagePitrSourceResult(
			t, mp, []string{"table"}, []string{"tenant"}, []string{"db"}, []string{"tbl"},
			[]uint64{1}, []uint8{24}, []string{"h"},
		),
	}
	wantErr := errors.New("delete failed")
	snapshotDeleteSQL := "delete from mo_catalog.mo_snapshots where kind = 'branch' and sname in ('__mo_branch_2')"
	sqlExecutor := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if sql == snapshotDeleteSQL {
			return executor.Result{}, wantErr
		}
		return results[sql], nil
	})

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
