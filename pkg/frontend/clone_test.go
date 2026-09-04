// Copyright 2025 Matrix Origin
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

package frontend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

func TestWithCloneLockContext(t *testing.T) {
	proc := newValidateSession(t).proc
	oldCtx, cancel := context.WithCancel(context.Background())
	cancel()
	proc.Ctx = oldCtx

	lockCtx := context.WithValue(context.Background(), struct{}{}, "current")
	wantErr := errors.New("lock failed")
	err := withCloneLockContext(proc, lockCtx, func() error {
		require.Same(t, lockCtx, proc.Ctx)
		require.NoError(t, proc.Ctx.Err())
		return wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Same(t, oldCtx, proc.Ctx)
}

func TestResolveUdfInCallerTxnContext(t *testing.T) {
	ctx := context.Background()
	require.False(t, resolvesUdfInCallerTxn(ctx))
	require.True(t, resolvesUdfInCallerTxn(withResolveUdfInCallerTxn(ctx)))
}

func TestRewriteCloneSequenceCreateSQL(t *testing.T) {
	got, err := rewriteCloneSequenceCreateSQL(
		"create sequence `source-db`.`seq-name` as bigint increment by 3 minvalue 1 maxvalue 99 start with 7 no cycle",
		"target-db",
		"seq-name",
		0,
	)
	require.NoError(t, err)
	require.Equal(t,
		"create sequence `target-db`.`seq-name` as bigint increment by 3 minvalue 1 maxvalue 99 start with 7 no cycle",
		got)

	_, err = rewriteCloneSequenceCreateSQL("create table t(a int)", "target-db", "seq-name", 0)
	require.ErrorContains(t, err, "expected *tree.CreateSequence")
}

func TestCloneCatalogLockBatch(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	baseline := mp.CurrNB()

	bat, err := cloneCatalogLockBatch(ses.proc, 7, "db", "tbl")
	require.NoError(t, err)
	require.Len(t, bat.Vecs, 1)
	require.Equal(t, 1, bat.Vecs[0].Length())
	require.NotEmpty(t, bat.Vecs[0].GetBytesAt(0))
	bat.Vecs[0].Free(mp)
	require.Equal(t, baseline, mp.CurrNB())
}

func TestGeneratedCloneRestoreSnapshotTS(t *testing.T) {
	for _, test := range []struct {
		name       string
		optionBits uint32
		want       int64
	}{
		{name: "autocommit keeps generated timestamp", want: 42},
		{name: "explicit transaction keeps shared visibility", optionBits: OPTION_BEGIN},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses := &Session{feSessionImpl: feSessionImpl{
				txnHandler: &TxnHandler{optionBits: test.optionBits},
			}}
			require.Equal(t, test.want, generatedCloneRestoreSnapshotTS(ses, 42))
		})
	}
}

type cloneSnapshotResolutionCompilerContext struct {
	*plan2.MockCompilerContext
	ctx        context.Context
	resolveErr error
}

func (c *cloneSnapshotResolutionCompilerContext) GetContext() context.Context {
	return c.ctx
}

func (c *cloneSnapshotResolutionCompilerContext) GetSnapshot() *plan2.Snapshot {
	return nil
}

func (c *cloneSnapshotResolutionCompilerContext) ResolveSnapshotWithSnapshotName(string) (*plan2.Snapshot, error) {
	return nil, c.resolveErr
}

func TestGetOpAndToAccountIDNormalizesMissingNamedSnapshot(t *testing.T) {
	const snapshotName = "missing_snapshot"
	ctx := context.Background()

	resolverContext := &cloneSnapshotResolutionCompilerContext{
		MockCompilerContext: plan2.NewMockCompilerContext(false),
		ctx:                 ctx,
		resolveErr:          moerr.NewInternalErrorf(ctx, "find 0 snapshot records by name(%s), expect only 1", snapshotName),
	}
	atTsExpr := &tree.AtTimeStamp{
		Type:         tree.ATTIMESTAMPSNAPSHOT,
		SnapshotName: snapshotName,
		Expr:         tree.NewNumVal(snapshotName, snapshotName, false, tree.P_char),
	}
	_, resolveErr := plan2.NewQueryBuilder(
		plan.Query_INSERT, resolverContext, false, true,
	).ResolveTsHint(atTsExpr)
	require.Error(t, resolveErr)
	require.True(t, plan2.IsSnapshotNotFound(resolveErr))

	originalResolver := resolveSnapshotForClone
	resolveSnapshotForClone = func(*Session, *tree.AtTimeStamp) (*plan2.Snapshot, error) {
		return nil, resolveErr
	}
	t.Cleanup(func() {
		resolveSnapshotForClone = originalResolver
	})

	_, _, snapshot, err := getOpAndToAccountId(ctx, nil, nil, nil, atTsExpr)
	require.Nil(t, snapshot)
	require.EqualError(t, err, "invalid input: snapshot 'missing_snapshot' not found")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	require.NotContains(t, err.Error(), "internal error")
	require.NotContains(t, err.Error(), "snapshot records")
}

func TestGetOpAndToAccountIDPropagatesOtherSnapshotErrors(t *testing.T) {
	ctx := context.Background()
	wantErr := moerr.NewInternalError(ctx, "snapshot catalog unavailable")
	atTsExpr := &tree.AtTimeStamp{
		Type:         tree.ATTIMESTAMPSNAPSHOT,
		SnapshotName: "snapshot",
	}

	originalResolver := resolveSnapshotForClone
	resolveSnapshotForClone = func(*Session, *tree.AtTimeStamp) (*plan2.Snapshot, error) {
		return nil, wantErr
	}
	t.Cleanup(func() {
		resolveSnapshotForClone = originalResolver
	})

	_, _, snapshot, err := getOpAndToAccountId(ctx, nil, nil, nil, atTsExpr)
	require.Nil(t, snapshot)
	require.ErrorIs(t, err, wantErr)
}

func TestCloneForeignKeyChecksRestoresMigrationReplayability(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	for _, test := range []struct {
		name       string
		tracked    bool
		replayable bool
	}{
		{name: "untracked"},
		{name: "tracked replayable", tracked: true, replayable: true},
		{name: "tracked unreplayable", tracked: true, replayable: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ses := newTestSession(t, ctrl)
			t.Cleanup(ses.Close)
			ses.migrationSystemVarReplayable = make(map[string]bool)
			if test.tracked {
				ses.markMigrationSystemVarReplayable(
					"foreign_key_checks", test.replayable)
			}

			oldReplayable, hadReplayability :=
				ses.getMigrationSystemVarReplayability("foreign_key_checks")
			ses.markMigrationSystemVarReplayable("foreign_key_checks", false)
			ses.restoreMigrationSystemVarReplayability(
				"foreign_key_checks", oldReplayable, hadReplayability)

			gotReplayable, gotReplayability :=
				ses.getMigrationSystemVarReplayability("foreign_key_checks")
			require.Equal(t, test.tracked, gotReplayability)
			if test.tracked {
				require.Equal(t, test.replayable, gotReplayable)
			}
			require.Equal(t, test.tracked && !test.replayable,
				ses.hasUnreplayableMigrationSystemVars())
		})
	}
}

func TestShouldLockDataBranchCloneSource(t *testing.T) {
	timestampSource := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
	}
	namedSnapshotSource := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{
			Name: "snap",
		},
	}

	require.True(t, shouldLockDataBranchCloneSource(nil))
	require.True(t, shouldLockDataBranchCloneSource(timestampSource))
	require.False(t, shouldLockDataBranchCloneSource(namedSnapshotSource))
}

func TestShouldRevalidateTimestampDataBranchCloneSource(t *testing.T) {
	timestampSource := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	namedSnapshotSource := &plan.Snapshot{
		TS:        &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{Name: "snap"},
	}

	require.False(t, shouldRevalidateTimestampDataBranchCloneSource(
		context.Background(), timestampSource,
	))
	dataBranchCtx := context.WithValue(
		context.Background(), dataBranchCloneLockCtxKey{}, true,
	)
	require.True(t, shouldRevalidateTimestampDataBranchCloneSource(
		dataBranchCtx, timestampSource,
	))
	require.False(t, shouldRevalidateTimestampDataBranchCloneSource(
		dataBranchCtx, namedSnapshotSource,
	))
}

func TestHandleCloneTableRejectsUnsupportedTemporaryOptionsBeforeExecution(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*tree.CloneTable)
		wantErr string
	}{
		{
			name: "to account",
			prepare: func(stmt *tree.CloneTable) {
				stmt.ToAccountOpt = &tree.ToAccountOpt{AccountName: "target"}
			},
			wantErr: "CREATE TEMPORARY TABLE ... CLONE cannot be used with TO ACCOUNT",
		},
		{
			name: "copy grants",
			prepare: func(stmt *tree.CloneTable) {
				stmt.CopyGrants = true
			},
			wantErr: "CREATE TEMPORARY TABLE ... CLONE cannot be used with COPY GRANTS",
		},
		{
			name: "if not exists copy grants",
			prepare: func(stmt *tree.CloneTable) {
				stmt.CreateTable.IfNotExists = true
				stmt.CopyGrants = true
			},
			wantErr: "CREATE TEMPORARY TABLE ... CLONE cannot be used with COPY GRANTS",
		},
		{
			name: "to account and copy grants",
			prepare: func(stmt *tree.CloneTable) {
				stmt.ToAccountOpt = &tree.ToAccountOpt{AccountName: "target"}
				stmt.CopyGrants = true
			},
			wantErr: "CREATE TEMPORARY TABLE ... CLONE cannot be used with TO ACCOUNT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt := tree.NewCloneTable()
			t.Cleanup(stmt.Free)
			stmt.CreateTable.Temporary = true
			test.prepare(stmt)

			// Nil execution dependencies prove this semantic rejection happens before
			// opening a background transaction, resolving a snapshot/account, or
			// publishing any temporary-table state.
			_, err := handleCloneTable(nil, nil, stmt, nil, nil)
			require.ErrorContains(t, err, test.wantErr)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
		})
	}
}

func TestRemoveFailedTemporaryCloneAlias(t *testing.T) {
	newSession := func() *Session {
		return &Session{
			tempTables:    make(map[string]string),
			tempTablesRev: make(map[string]string),
		}
	}

	t.Run("failed clone removes its newly registered alias", func(t *testing.T) {
		ses := newSession()
		ses.AddTempTable("clone_db", "temp_dst", "physical_temp_dst")

		removeFailedTemporaryCloneAlias(ses, "clone_db", "temp_dst", false, errors.New("clone failed"))

		_, exists := ses.GetTempTable("clone_db", "temp_dst")
		require.False(t, exists)
	})

	t.Run("failed if not exists clone preserves a preexisting alias", func(t *testing.T) {
		ses := newSession()
		ses.AddTempTable("clone_db", "temp_dst", "physical_temp_dst")

		removeFailedTemporaryCloneAlias(ses, "clone_db", "temp_dst", true, errors.New("later failure"))

		realName, exists := ses.GetTempTable("clone_db", "temp_dst")
		require.True(t, exists)
		require.Equal(t, "physical_temp_dst", realName)
	})

	t.Run("successful clone preserves its new alias", func(t *testing.T) {
		ses := newSession()
		ses.AddTempTable("clone_db", "temp_dst", "physical_temp_dst")

		removeFailedTemporaryCloneAlias(ses, "clone_db", "temp_dst", false, nil)

		realName, exists := ses.GetTempTable("clone_db", "temp_dst")
		require.True(t, exists)
		require.Equal(t, "physical_temp_dst", realName)
	})
}

func TestShouldLockNamedDataBranchCloneSnapshot(t *testing.T) {
	ctx := context.WithValue(context.Background(), dataBranchCloneLockCtxKey{}, true)
	named := &plan.Snapshot{
		TS:        &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{Name: "snap"},
	}
	require.True(t, shouldLockNamedDataBranchCloneSnapshot(ctx, named))
	require.False(t, shouldLockNamedDataBranchCloneSnapshot(context.Background(), named))
	require.False(t, shouldLockNamedDataBranchCloneSnapshot(ctx,
		&plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}},
	))
}

func TestLockNamedDataBranchCloneSnapshot(t *testing.T) {
	ctx := context.WithValue(context.Background(), dataBranchCloneLockCtxKey{}, true)
	snapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{
			Name:  "snap",
			Level: "table",
			ObjId: 7,
		},
	}
	lockSQL, err := namedDataBranchCloneSnapshotLockSQL(ctx, "snap")
	require.NoError(t, err)
	require.Equal(t,
		"select * from mo_catalog.mo_snapshots where sname = 'snap' for update",
		lockSQL,
	)
	_, err = namedDataBranchCloneSnapshotLockSQL(ctx, "invalid'snapshot")
	require.Error(t, err)
	require.NoError(t, lockNamedDataBranchCloneSnapshot(
		context.Background(), &backgroundExecTest{}, snapshot,
	))

	t.Run("matching snapshot is locked", func(t *testing.T) {
		bh := &backgroundExecTest{}
		bh.init()
		bh.sql2result[lockSQL] = newMrsForSnapshotRecord(
			"id", "snap", 42, "table", "acc", "db", "tbl", 7,
		)
		require.NoError(t, lockNamedDataBranchCloneSnapshot(ctx, bh, snapshot))
		require.Equal(t, []string{lockSQL}, bh.executedSQLs)
	})

	for _, tc := range []struct {
		name   string
		record *MysqlResultSet
	}{
		{name: "drop won the row lock", record: newMrsEmpty()},
		{name: "same name was recreated", record: newMrsForSnapshotRecord(
			"new-id", "snap", 43, "table", "acc", "db", "tbl", 7,
		)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bh := &backgroundExecTest{}
			bh.init()
			bh.sql2result[lockSQL] = tc.record
			err := lockNamedDataBranchCloneSnapshot(ctx, bh, snapshot)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
		})
	}
}

func TestValidateNamedDataBranchCloneSnapshotRecordRejectsIdentityDrift(t *testing.T) {
	snapshot := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
		ExtraInfo: &plan.SnapshotExtraInfo{
			Name: "snap", Level: "table", ObjId: 7,
		},
	}
	matching := &snapshotRecord{
		snapshotName: "snap", ts: 42, level: "table", objId: 7,
	}
	require.NoError(t, validateNamedDataBranchCloneSnapshotRecord(snapshot, matching))

	for _, tc := range []struct {
		name     string
		snapshot *plan.Snapshot
		record   *snapshotRecord
	}{
		{name: "missing record", snapshot: snapshot},
		{name: "missing snapshot", record: matching},
		{name: "missing timestamp", snapshot: &plan.Snapshot{ExtraInfo: snapshot.ExtraInfo}, record: matching},
		{name: "missing extra info", snapshot: &plan.Snapshot{TS: snapshot.TS}, record: matching},
		{name: "name changed", snapshot: snapshot, record: &snapshotRecord{snapshotName: "other", ts: 42, level: "table", objId: 7}},
		{name: "level changed", snapshot: snapshot, record: &snapshotRecord{snapshotName: "snap", ts: 42, level: "database", objId: 7}},
		{name: "object changed", snapshot: snapshot, record: &snapshotRecord{snapshotName: "snap", ts: 42, level: "table", objId: 8}},
		{name: "internal branch snapshot", snapshot: snapshot, record: &snapshotRecord{snapshotName: "snap", ts: 42, level: "table", objId: 7, kind: branchSnapshotKind}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNamedDataBranchCloneSnapshotRecord(tc.snapshot, tc.record)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged))
		})
	}
}

func TestBranchDAGSelectSQLLocksTimestampValidation(t *testing.T) {
	require.NotContains(t, branchDAGSelectSQL(false), "for update")
	require.Equal(t,
		"select table_id, clone_ts, p_table_id, level, table_deleted from "+
			"mo_catalog.mo_branch_metadata for update",
		branchDAGSelectSQL(true),
	)
}

func TestTimestampDataBranchCloneWaitsForAlterPublication(t *testing.T) {
	timestampSource := &plan.Snapshot{
		TS: &timestamp.Timestamp{PhysicalTime: 42},
	}
	var catalogRow sync.RWMutex
	catalogRow.Lock() // COPY ALTER holds the exclusive publication lock.

	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- withDataBranchCloneSourceLock(timestampSource, func() error {
			close(entered)
			catalogRow.RLock()
			defer catalogRow.RUnlock()
			return validateTimestampDataBranchSourceAfterLock(
				timestampSource,
				func(at *plan.Snapshot) (uint64, error) {
					if at != nil {
						return 1, nil // timestamp selected the old generation
					}
					return 2, nil // ALTER published the new current generation
				},
				func() (*databranchutils.DataBranchDAG, error) {
					return databranchutils.NewDAG(nil), nil
				},
			)
		})
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("timestamp clone did not enter the shared source-lock path")
	}
	select {
	case err := <-done:
		t.Fatalf("timestamp clone bypassed the ALTER publication lock: %v", err)
	default:
	}

	catalogRow.Unlock()
	select {
	case err := <-done:
		require.ErrorContains(t, err, "timestamp source generation is not connected")
	case <-time.After(time.Second):
		t.Fatal("timestamp clone did not resume after ALTER released the lock")
	}
}

func TestValidateTimestampDataBranchSourceIDs(t *testing.T) {
	t.Run("same generation", func(t *testing.T) {
		require.NoError(t, validateTimestampDataBranchSourceIDs(1, 1, nil))
	})

	t.Run("alter first without lineage is rejected", func(t *testing.T) {
		dag := databranchutils.NewDAG(nil)
		err := validateTimestampDataBranchSourceIDs(1, 2, dag)
		require.ErrorContains(t, err, "timestamp source generation is not connected")
	})

	t.Run("preserved alter lineage is accepted", func(t *testing.T) {
		dag := databranchutils.NewDAG([]databranchutils.DataBranchMetadata{
			{TableID: 2, PTableID: 1, LineageOnly: true},
		})
		require.NoError(t, validateTimestampDataBranchSourceIDs(1, 2, dag))
	})
}

func TestValidateTimestampDataBranchSourceAfterLock(t *testing.T) {
	timestampSource := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	var resolved []*plan.Snapshot
	dagLoaded := false
	err := validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(at *plan.Snapshot) (uint64, error) {
			resolved = append(resolved, at)
			if at != nil {
				return 1, nil
			}
			return 2, nil
		},
		func() (*databranchutils.DataBranchDAG, error) {
			dagLoaded = true
			return databranchutils.NewDAG([]databranchutils.DataBranchMetadata{
				{TableID: 2, PTableID: 1, LineageOnly: true},
			}), nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, []*plan.Snapshot{timestampSource, nil}, resolved)
	require.True(t, dagLoaded)
}

func TestValidateTimestampDataBranchSourceAfterLockFailures(t *testing.T) {
	timestampSource := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	wantErr := errors.New("lookup failed")

	require.NoError(t, validateTimestampDataBranchSourceAfterLock(
		&plan.Snapshot{ExtraInfo: &plan.SnapshotExtraInfo{Name: "named"}},
		func(*plan.Snapshot) (uint64, error) { return 0, wantErr },
		func() (*databranchutils.DataBranchDAG, error) { return nil, wantErr },
	))
	require.ErrorIs(t, validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(*plan.Snapshot) (uint64, error) { return 0, wantErr },
		func() (*databranchutils.DataBranchDAG, error) { return nil, nil },
	), wantErr)

	resolveCalls := 0
	require.ErrorIs(t, validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(*plan.Snapshot) (uint64, error) {
			resolveCalls++
			if resolveCalls == 2 {
				return 0, wantErr
			}
			return 1, nil
		},
		func() (*databranchutils.DataBranchDAG, error) { return nil, nil },
	), wantErr)

	require.NoError(t, validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(*plan.Snapshot) (uint64, error) { return 1, nil },
		func() (*databranchutils.DataBranchDAG, error) {
			t.Fatal("same-generation validation must not load lineage")
			return nil, nil
		},
	))

	resolveCalls = 0
	require.ErrorIs(t, validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(*plan.Snapshot) (uint64, error) {
			resolveCalls++
			return uint64(resolveCalls), nil
		},
		func() (*databranchutils.DataBranchDAG, error) { return nil, wantErr },
	), wantErr)
}

func TestTimestampDataBranchDatabaseRevalidatesEveryLifecycleTableAfterAllLocks(t *testing.T) {
	timestampSource := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	var catalogRows sync.RWMutex
	catalogRows.Lock() // COPY ALTER holds one source row exclusively.

	enteredLockPath := make(chan struct{})
	allLocksHeld := make(chan struct{})
	validated := make(chan string, 3)
	done := make(chan error, 1)
	go func() {
		// Database clone acquires all source locks before revalidating any table.
		// The external row is part of this fence because view dependency sorting
		// consults its catalog metadata after the timestamp advances.
		close(enteredLockPath)
		catalogRows.RLock()
		close(allLocksHeld)
		defer catalogRows.RUnlock()
		source := cloneDatabaseSource{srcTblInfos: []*tableInfo{
			{dbName: "db", tblName: "t1"},
			{dbName: "db", tblName: "v", typ: view},
			{dbName: "db", tblName: "external", relKind: catalog.SystemExternalRel},
			{dbName: "db", tblName: "t2"},
		}}
		done <- forEachCloneDatabaseSourceTable(source, func(table *tableInfo) error {
			err := validateTimestampDataBranchSourceAfterLock(
				timestampSource,
				func(at *plan.Snapshot) (uint64, error) {
					if at != nil {
						return 1, nil
					}
					return 1, nil
				},
				func() (*databranchutils.DataBranchDAG, error) {
					return databranchutils.NewDAG(nil), nil
				},
			)
			if err != nil {
				return err
			}
			validated <- table.tblName
			return nil
		})
	}()

	<-enteredLockPath
	select {
	case <-allLocksHeld:
		t.Fatal("database clone acquired all locks before ALTER released its row")
	default:
	}
	catalogRows.Unlock()
	select {
	case <-allLocksHeld:
	case <-time.After(time.Second):
		t.Fatal("database clone did not acquire all source locks")
	}
	require.NoError(t, <-done)
	require.ElementsMatch(t, []string{"t1", "external", "t2"}, []string{<-validated, <-validated, <-validated})
}

func TestTimestampDataBranchValidationLockCoversPublication(t *testing.T) {
	timestampSource := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 42}}
	var lineageRows sync.Mutex
	validated := make(chan struct{})
	gcStarted := make(chan struct{})
	gcDone := make(chan struct{})

	lineageRows.Lock() // SELECT ... FOR UPDATE, held until clone transaction commit.
	err := validateTimestampDataBranchSourceAfterLock(
		timestampSource,
		func(at *plan.Snapshot) (uint64, error) {
			if at != nil {
				return 1, nil
			}
			return 2, nil
		},
		func() (*databranchutils.DataBranchDAG, error) {
			return databranchutils.NewDAG([]databranchutils.DataBranchMetadata{
				{TableID: 2, PTableID: 1, LineageOnly: true},
			}), nil
		},
	)
	require.NoError(t, err)
	close(validated)

	go func() {
		close(gcStarted)
		lineageRows.Lock() // Compaction uses the same FOR UPDATE lock.
		lineageRows.Unlock()
		close(gcDone)
	}()

	<-gcStarted
	select {
	case <-gcDone:
		t.Fatal("lineage compaction passed validation before branch publication")
	default:
	}
	<-validated
	// updateBranchMetaTable and createBranchProtectSnapshot run before this
	// transaction commits and releases the lineage-row lock.
	lineageRows.Unlock()
	select {
	case <-gcDone:
	case <-time.After(time.Second):
		t.Fatal("lineage compaction did not resume after branch publication")
	}
}

type failingBeginBackgroundExec struct {
	BackgroundExec
	err        error
	closeCalls int
}

func (b *failingBeginBackgroundExec) ClearExecResultSet() {}

func (b *failingBeginBackgroundExec) Exec(context.Context, string) error {
	return b.err
}

func (b *failingBeginBackgroundExec) Close() {
	b.closeCalls++
}

func TestGetBackExecutorClosesWhenBeginFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().TxnOptions().Return(txn.TxnOptions{}).Times(2)
	ses.proc.Base.TxnOperator = txnOp

	beginErr := errors.New("begin failed")
	backExec := &failingBeginBackgroundExec{err: beginErr}
	oldNewBackgroundExec := NewBackgroundExec
	t.Cleanup(func() { NewBackgroundExec = oldNewBackgroundExec })
	forcedPessimisticRC := false
	NewBackgroundExec = func(_ context.Context, _ FeSession, opts ...*BackgroundExecOption) BackgroundExec {
		for _, opt := range opts {
			forcedPessimisticRC = forcedPessimisticRC || opt != nil && opt.forcePessimisticRC
		}
		return backExec
	}

	returned, cleanup, err := getBackExecutor(context.Background(), ses)
	require.ErrorIs(t, err, beginErr)
	require.Nil(t, returned)
	require.Nil(t, cleanup)
	require.False(t, forcedPessimisticRC)
	require.Equal(t, 1, backExec.closeCalls)

	returned, cleanup, err = getBackExecutor(
		context.Background(), ses, &BackgroundExecOption{forcePessimisticRC: true},
	)
	require.ErrorIs(t, err, beginErr)
	require.Nil(t, returned)
	require.Nil(t, cleanup)
	require.True(t, forcedPessimisticRC)
	require.Equal(t, 2, backExec.closeCalls)
}

func TestGetBackExecutorWithTxnHandlerUsesTxnHandlerBeginState(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	staleTxn := mock_frontend.NewMockTxnOperator(ctrl)
	sharedTxn := mock_frontend.NewMockTxnOperator(ctrl)
	sharedTxn.EXPECT().SetFootPrints(gomock.Any(), gomock.Any()).AnyTimes()
	ses.proc.Base.TxnOperator = staleTxn
	ses.GetTxnHandler().txnOp = sharedTxn
	ses.GetTxnHandler().SetOptionBits(OPTION_BEGIN)

	bh, cleanup, err := getBackExecutorWithTxnHandler(context.Background(), ses)
	require.NoError(t, err)
	require.NotNil(t, bh)
	require.NotNil(t, cleanup)
	back := bh.(*backExec)
	require.Same(t, sharedTxn, back.backSes.GetTxnHandler().GetTxn())
	require.True(t, back.backSes.GetTxnHandler().IsShareTxn())
	require.Same(t, staleTxn, ses.proc.GetTxnOperator())
	require.NoError(t, cleanup(nil))
}

func TestHandleCloneDatabaseWithSourceIfNotExistsSkipsExistingTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	ctx := defines.AttachAccountId(context.Background(), sysAccountID)
	stub := gostub.Stub(&lockCloneDatabaseTarget, func(context.Context, *Session, BackgroundExec, uint32, string) error {
		return nil
	})
	t.Cleanup(stub.Reset)
	execCtx := newTestExecCtx(ctx, ctrl)
	er := mock_frontend.NewMockExecResult(ctrl)
	er.EXPECT().GetRowCount().Return(uint64(1))
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet()
	bh.EXPECT().Exec(
		gomock.Any(),
		"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'destination' AND account_id = 0 LIMIT 1",
	).DoAndReturn(func(gotCtx context.Context, _ string) error {
		accountID, err := defines.GetAccountId(gotCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(sysAccountID), accountID)
		return nil
	})
	bh.EXPECT().GetExecResultSet().Return([]interface{}{er})

	receipts, err := handleCloneDatabaseWithSource(
		execCtx,
		ses,
		bh,
		&tree.CloneDatabase{
			IfNotExists: true,
			DstDatabase: tree.Identifier("destination"),
			SrcDatabase: tree.Identifier("missing_source"),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, receipts)
}

func TestHandleCloneDatabaseWithSourceIfNotExistsPropagatesLookupErrors(t *testing.T) {
	stmt := &tree.CloneDatabase{
		IfNotExists: true,
		DstDatabase: tree.Identifier("destination"),
		SrcDatabase: tree.Identifier("source"),
	}

	t.Run("target account context is missing", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)

		_, err := handleCloneDatabaseWithSource(
			newTestExecCtx(context.Background(), ctrl),
			ses,
			mock_frontend.NewMockBackgroundExec(ctrl),
			stmt,
			nil,
		)
		require.Error(t, err)
	})

	t.Run("destination existence query fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)

		wantErr := errors.New("destination lookup failed")
		stub := gostub.Stub(&lockCloneDatabaseTarget, func(context.Context, *Session, BackgroundExec, uint32, string) error {
			return nil
		})
		t.Cleanup(stub.Reset)
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet()
		bh.EXPECT().Exec(
			gomock.Any(),
			"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'destination' AND account_id = 0 LIMIT 1",
		).Return(wantErr)

		_, err := handleCloneDatabaseWithSource(
			newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl),
			ses,
			bh,
			stmt,
			nil,
		)
		require.ErrorIs(t, err, wantErr)
	})
}

func TestHandleCloneDatabaseWithSourceAuthorizesTargetBeforeIfNotExistsCheck(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	t.Cleanup(ses.Close)

	lockCalled := false
	stub := gostub.Stub(&lockCloneDatabaseTarget, func(context.Context, *Session, BackgroundExec, uint32, string) error {
		lockCalled = true
		return nil
	})
	t.Cleanup(stub.Reset)

	_, err := handleCloneDatabaseWithSource(
		newTestExecCtx(defines.AttachAccountId(context.Background(), 1), ctrl),
		ses,
		mock_frontend.NewMockBackgroundExec(ctrl),
		&tree.CloneDatabase{
			IfNotExists: true,
			DstDatabase: tree.Identifier("destination"),
			SrcDatabase: tree.Identifier("source"),
			ToAccountOpt: &tree.ToAccountOpt{
				AccountName: tree.Identifier("target"),
			},
		},
		&cloneDatabaseSource{
			opAccountId: 1,
			toAccountId: 2,
			snapshot:    &plan.Snapshot{},
		},
	)
	require.EqualError(t, err, "internal error: only sys can clone table to another account")
	require.False(t, lockCalled)
}

func TestHandleCloneDatabaseWithSourceRestoresRoutines(t *testing.T) {
	newSource := func() *cloneDatabaseSource {
		return &cloneDatabaseSource{
			srcResolveDBName: "source",
			opAccountId:      sysAccountID,
			toAccountId:      sysAccountID,
			userDefinedFuncs: []userDefinedFunctionDefinition{{
				name:    "f_answer",
				args:    "{}",
				retType: "int",
				body:    "select 42",
				lang:    "sql",
				sqlMode: "",
				dbName:  "source",
			}},
			storedProcedures: []storedProcedureDefinition{{
				name:    "p_answer",
				args:    "[]",
				lang:    "sql",
				body:    "begin select 42; end",
				sqlMode: "",
				dbName:  "source",
			}},
		}
	}
	newStatement := func() *tree.CloneDatabase {
		return &tree.CloneDatabase{
			DstDatabase: tree.Identifier("destination"),
			SrcDatabase: tree.Identifier("source"),
			AtTsExpr:    &tree.AtTimeStamp{},
		}
	}

	t.Run("restores functions before procedures after creating destination", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)
		bh := &backgroundExecTest{}
		bh.init()
		checkSQL := getSqlForCheckProcedureExistence("p_answer", "destination")
		bh.sql2result[checkSQL] = newMrsForPasswordOfUser(nil)

		_, err := handleCloneDatabaseWithSource(
			newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl),
			ses, bh, newStatement(), newSource(),
		)
		require.NoError(t, err)
		require.Len(t, bh.executedSQLs, 4)
		require.Equal(t, "create database `destination`", bh.executedSQLs[0])
		require.Contains(t, bh.executedSQLs[1], "insert into mo_catalog.mo_user_defined_function")
		require.Equal(t, checkSQL, bh.executedSQLs[2])
		require.Contains(t, bh.executedSQLs[3], "insert into mo_catalog.mo_stored_procedure")
	})

	t.Run("propagates procedure restoration failures", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)
		bh := &backgroundExecTest{}
		bh.init()
		checkSQL := getSqlForCheckProcedureExistence("p_answer", "destination")
		wantErr := errors.New("procedure lookup failed")
		bh.sql2err[checkSQL] = wantErr

		_, err := handleCloneDatabaseWithSource(
			newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl),
			ses, bh, newStatement(), newSource(),
		)
		require.ErrorIs(t, err, wantErr)
		require.Len(t, bh.executedSQLs, 3)
		require.Equal(t, "create database `destination`", bh.executedSQLs[0])
		require.Contains(t, bh.executedSQLs[1], "insert into mo_catalog.mo_user_defined_function")
		require.Equal(t, checkSQL, bh.executedSQLs[2])
	})

	t.Run("rejects imported functions before target database creation", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ses := newTestSession(t, ctrl)
		t.Cleanup(ses.Close)

		source := newSource()
		source.userDefinedFuncs[0] = userDefinedFunctionDefinition{
			name: "f_imported", args: "{}", retType: "int", lang: "python",
			body:   `{"handler":"f_imported","import":true,"body":"shared:udf/source/f_imported.py"}`,
			dbName: "source",
		}
		bh := &backgroundExecTest{}
		bh.init()

		_, err := handleCloneDatabaseWithSource(
			newTestExecCtx(defines.AttachAccountId(context.Background(), sysAccountID), ctrl),
			ses, bh, newStatement(), source,
		)
		require.ErrorContains(t, err, "imported python function f_imported is not supported")
		require.Empty(t, bh.executedSQLs)
	})
}

func TestCheckCloneDatabaseTargetSerializesConcurrentIfNotExistsDecisions(t *testing.T) {
	firstLocked := make(chan struct{})
	secondWaiting := make(chan struct{})
	releaseFirst := make(chan struct{})
	var lockCalls int
	var lockCallsMu sync.Mutex
	stub := gostub.Stub(&lockCloneDatabaseTarget, func(context.Context, *Session, BackgroundExec, uint32, string) error {
		lockCallsMu.Lock()
		lockCalls++
		call := lockCalls
		lockCallsMu.Unlock()
		if call == 1 {
			close(firstLocked)
			return nil
		}
		close(secondWaiting)
		<-releaseFirst
		return nil
	})
	t.Cleanup(stub.Reset)

	firstCtrl := gomock.NewController(t)
	firstResult := mock_frontend.NewMockExecResult(firstCtrl)
	firstResult.EXPECT().GetRowCount().Return(uint64(0))
	firstBH := mock_frontend.NewMockBackgroundExec(firstCtrl)
	firstBH.EXPECT().ClearExecResultSet()
	firstBH.EXPECT().Exec(
		gomock.Any(),
		"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'destination' AND account_id = 7 LIMIT 1",
	).Return(nil)
	firstBH.EXPECT().GetExecResultSet().Return([]interface{}{firstResult})

	secondCtrl := gomock.NewController(t)
	secondResult := mock_frontend.NewMockExecResult(secondCtrl)
	secondResult.EXPECT().GetRowCount().Return(uint64(1))
	secondBH := mock_frontend.NewMockBackgroundExec(secondCtrl)
	secondBH.EXPECT().ClearExecResultSet()
	secondBH.EXPECT().Exec(
		gomock.Any(),
		"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'destination' AND account_id = 7 LIMIT 1",
	).Return(nil)
	secondBH.EXPECT().GetExecResultSet().Return([]interface{}{secondResult})

	type result struct {
		exists bool
		err    error
	}
	firstDone := make(chan result, 1)
	go func() {
		exists, err := checkCloneDatabaseTarget(context.Background(), nil, firstBH, 7, "destination")
		firstDone <- result{exists: exists, err: err}
	}()
	<-firstLocked
	first := <-firstDone
	require.NoError(t, first.err)
	require.False(t, first.exists)

	secondDone := make(chan result, 1)
	go func() {
		exists, err := checkCloneDatabaseTarget(context.Background(), nil, secondBH, 7, "destination")
		secondDone <- result{exists: exists, err: err}
	}()
	<-secondWaiting
	select {
	case second := <-secondDone:
		t.Fatalf("second clone decided before the first transaction released the target lock: %+v", second)
	default:
	}

	close(releaseFirst)
	second := <-secondDone
	require.NoError(t, second.err)
	require.True(t, second.exists)
}

func TestCheckCloneDatabaseTargetReturnsLockErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "unretryable", err: errors.New("target lock failed")},
		{name: "retryable without owning background transaction", err: moerr.NewTxnNeedRetryNoCtx()},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			stub := gostub.Stub(&lockCloneDatabaseTarget, func(context.Context, *Session, BackgroundExec, uint32, string) error {
				return test.err
			})
			t.Cleanup(stub.Reset)

			exists, err := checkCloneDatabaseTarget(
				context.Background(), nil, mock_frontend.NewMockBackgroundExec(ctrl), 7, "destination",
			)
			require.False(t, exists)
			require.ErrorIs(t, err, test.err)
		})
	}
}

func TestNewCloneDatabaseTargetLockProcessRequiresBackgroundExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, err := newCloneDatabaseTargetLockProcess(
		context.Background(), nil, mock_frontend.NewMockBackgroundExec(ctrl),
	)
	require.EqualError(t, err, "internal error: database clone target lock requires a background executor")
}

func TestLockCloneDatabaseTargetRequiresBackgroundExecutor(t *testing.T) {
	ctrl := gomock.NewController(t)
	err := lockCloneDatabaseTarget(
		context.Background(), nil, mock_frontend.NewMockBackgroundExec(ctrl), 7, "destination",
	)
	require.EqualError(t, err, "internal error: database clone target lock requires a background executor")
}

func TestIsCloneDatabaseTargetLockRetry(t *testing.T) {
	require.True(t, isCloneDatabaseTargetLockRetry(moerr.NewTxnNeedRetryNoCtx()))
	require.True(t, isCloneDatabaseTargetLockRetry(moerr.NewTxnNeedRetryWithDefChangedNoCtx()))
	require.False(t, isCloneDatabaseTargetLockRetry(errors.New("not retryable")))
}

func Test_prepareCloneViewSnapshot(t *testing.T) {
	original := &plan.Snapshot{
		Tenant: &plan.SnapshotTenant{TenantID: 1001},
	}

	rewritten := prepareCloneViewSnapshot(original, 42)
	require.NotNil(t, rewritten)
	require.NotNil(t, rewritten.TS)
	require.Equal(t, int64(42), rewritten.TS.PhysicalTime)
	require.Equal(t, uint32(1001), rewritten.Tenant.TenantID)
	require.Nil(t, original.TS)

	valid := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 99},
		Tenant: &plan.SnapshotTenant{TenantID: 2002},
	}
	require.Same(t, valid, prepareCloneViewSnapshot(valid, 42))

	fromNil := prepareCloneViewSnapshot(nil, 24)
	require.NotNil(t, fromNil)
	require.NotNil(t, fromNil.TS)
	require.Equal(t, int64(24), fromNil.TS.PhysicalTime)
	require.Nil(t, fromNil.Tenant)

	require.Nil(t, prepareCloneViewSnapshot(nil, 0))
}

func Test_rewriteCloneViewInfos(t *testing.T) {
	fallbackKey := "pub_db#"
	viewMap := map[string]*tableInfo{
		genKey("pub_db", "v1"): {
			dbName:    "pub_db",
			tblName:   "v1",
			typ:       view,
			createSql: "create view `pub_db`.`v1` as select 'pub_db' as marker, a from `pub_db`.`t1`",
		},
		fallbackKey: {
			dbName:    "pub_db",
			tblName:   "legacy_v",
			typ:       view,
			createSql: "create view `pub_db`.`legacy_v` as select 1",
		},
	}
	sortedViews := []string{
		genKey("other_db", "dep_v"),
		fallbackKey,
		genKey("pub_db", "v1"),
	}

	rewrittenViewMap, rewrittenViews, err := rewriteCloneViewInfos(viewMap, sortedViews, "pub_db", "clone_db", 1)
	require.NoError(t, err)
	require.Equal(t, []string{
		genKey("other_db", "dep_v"),
		"clone_db#",
		genKey("clone_db", "v1"),
	}, rewrittenViews)

	info, ok := rewrittenViewMap[genKey("clone_db", "v1")]
	require.True(t, ok)
	require.Equal(t, "clone_db", info.dbName)
	require.Equal(t, "create view `clone_db`.`v1` as select 'pub_db' as `marker`, `a` from `clone_db`.`t1`;", info.createSql)

	fallbackInfo, ok := rewrittenViewMap["clone_db#"]
	require.True(t, ok)
	require.Equal(t, "clone_db", fallbackInfo.dbName)
	require.Equal(t, "create view `clone_db`.`legacy_v` as select 1;", fallbackInfo.createSql)

	require.Equal(t, "pub_db", viewMap[genKey("pub_db", "v1")].dbName)
	require.Equal(t, "create view `pub_db`.`v1` as select 'pub_db' as marker, a from `pub_db`.`t1`", viewMap[genKey("pub_db", "v1")].createSql)
	require.Equal(t, "pub_db", viewMap[fallbackKey].dbName)
	require.Equal(t, "create view `pub_db`.`legacy_v` as select 1", viewMap[fallbackKey].createSql)
}

func Test_rewriteCloneCreateSQL_RewritesOnlyTableNames(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		`create view pub_db.v as
				with c as (select * from pub_db.cte_t)
				select pub_db as pub_db,
			       (select max(id) from pub_db.proj_t) as m,
			       case when exists (select 1 from pub_db.case_t) then 1 else 0 end as c,
			       ((select max(id) from pub_db.null_t) is null) as n
			  from c
			  join pub_db.join_t as j on j.id in (select id from pub_db.on_t)
			 where exists (select 1 from pub_db.where_t)
			 group by pub_db
			having count(*) > (select count(*) from pub_db.having_t)`,
		"pub_db",
		"clone_db",
		1,
	)
	require.NoError(t, err)
	require.Contains(t, got, "create view `clone_db`.`v`")
	require.Contains(t, got, "from `clone_db`.`cte_t`")
	require.Contains(t, got, "from `clone_db`.`proj_t`")
	require.Contains(t, got, "from `clone_db`.`case_t`")
	require.Contains(t, got, "from `clone_db`.`null_t`")
	require.Contains(t, got, "join `clone_db`.`join_t`")
	require.Contains(t, got, "from `clone_db`.`on_t`")
	require.Contains(t, got, "from `clone_db`.`where_t`")
	require.Contains(t, got, "from `clone_db`.`having_t`")
	require.NotContains(t, got, "`pub_db`.`cte_t`")
	require.NotContains(t, got, "`pub_db`.`proj_t`")
	require.NotContains(t, got, "`pub_db`.`case_t`")
	require.NotContains(t, got, "`pub_db`.`null_t`")
	require.NotContains(t, got, "`pub_db`.`join_t`")
	require.NotContains(t, got, "`pub_db`.`on_t`")
	require.NotContains(t, got, "`pub_db`.`where_t`")
	require.NotContains(t, got, "`pub_db`.`having_t`")
	require.NotContains(t, got, "select `clone_db` as")
	require.Contains(t, got, "as `pub_db`")
}

func Test_rewriteCloneCreateSQL_RewritesQualifiedColumnsAndOrderingSubqueries(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view src.v as select src.t.a from src.t order by (select max(b) from src.u)",
		"src",
		"dst",
		1,
	)
	require.NoError(t, err)
	require.Contains(t, got, "create view `dst`.`v`")
	require.Contains(t, got, "select `dst`.`t`.`a` from `dst`.`t`")
	require.Contains(t, got, "from `dst`.`u`")
	require.NotContains(t, got, "`src`.`t`.`a`")
	require.NotContains(t, got, "from `src`.`t`")
	require.NotContains(t, got, "from `src`.`u`")
}

func Test_rewriteCloneCreateSQL_PreservesUnqualifiedViewFormat(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view v1 as select * from t1;",
		"pub_db",
		"clone_db",
		1,
	)
	require.NoError(t, err)
	require.Equal(t, "create view v1 as select * from t1;", got)
}

func Test_rewriteCloneCreateSQL_QuotesUserViewIdentifiers(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view `quote'db`.`quote view` as select `quote col` from `quote'db`.`quote table`",
		"quote'db",
		"clone db",
		1,
	)
	require.NoError(t, err)
	require.Equal(t, "create view `clone db`.`quote view` as select `quote col` from `clone db`.`quote table`;", got)

	_, err = rewriteCloneCreateSQL(got, "clone db", "next db", 1)
	require.NoError(t, err)
}

func Test_rewriteCloneCreateSQL_QuotesSystemViewIdentifiers(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view information_schema.v as select mt.`constraint` from mo_catalog.mo_tables as mt",
		"information_schema",
		"information_schema_new",
		1,
	)
	require.NoError(t, err)
	require.Contains(t, got, "`mt`.`constraint`")
	require.NotContains(t, got, "mt.constraint")

	_, err = rewriteCloneCreateSQL(got, "information_schema_new", "information_schema_next", 1)
	require.NoError(t, err)
}

func Test_rewriteCloneCreateSQL_UsesLocalOnlyInformationSchemaMetadataViews(t *testing.T) {
	for _, tc := range []struct {
		name              string
		ddl               string
		privateFunction   string
		localCatalogToken string
	}{
		{
			name:              "tables",
			ddl:               sysview.InformationSchemaTablesDDL,
			privateFunction:   "mo_subscription_tables()",
			localCatalogToken: "from `__mo_visible_tables` as `tbl`",
		},
		{
			name:              "columns",
			ddl:               sysview.InformationSchemaColumnsDDL,
			privateFunction:   "mo_subscription_columns()",
			localCatalogToken: "from `mo_catalog`.`mo_columns` as `mc`",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := rewriteCloneCreateSQL(
				tc.ddl,
				"information_schema",
				"information_schema_new",
				1,
			)
			require.NoError(t, err)
			require.Contains(t, got, "create view `information_schema_new`")
			require.Contains(t, got, tc.localCatalogToken)
			require.NotContains(t, got, tc.privateFunction)
			require.NotContains(t, got, " reg_match ")
			require.Contains(t, got, "regexp_like(")

			_, err = rewriteCloneCreateSQL(got, "information_schema_new", "information_schema_next", 1)
			require.NoError(t, err)
		})
	}
}

func Test_rewriteCloneCreateSQL_PreservesCaseSensitiveIdentifiers(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view `SrcDB`.`ViewName` as select `ID` from `SrcDB`.`TableName`",
		"SrcDB",
		"DstDB",
		0,
	)
	require.NoError(t, err)
	require.Equal(t, "create view `DstDB`.`ViewName` as select `ID` from `DstDB`.`TableName`;", got)
}

func Test_rewriteCloneCreateSQL_PreservesModeTwoNames(t *testing.T) {
	got, err := rewriteCloneCreateSQL(
		"create view `SrcDB`.`ViewName` as select `SrcDB`.`TableName`.`ID` from `SrcDB`.`TableName`",
		"SrcDB",
		"DstDB",
		2,
	)
	require.NoError(t, err)
	require.Equal(t,
		"create view `DstDB`.`ViewName` as select `DstDB`.`TableName`.`ID` from `DstDB`.`TableName`;",
		got,
	)
}
