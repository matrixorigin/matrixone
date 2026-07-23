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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestWithDataBranchCloneLockContext(t *testing.T) {
	proc := newValidateSession(t).proc
	oldCtx, cancel := context.WithCancel(context.Background())
	cancel()
	proc.Ctx = oldCtx

	lockCtx := context.WithValue(context.Background(), struct{}{}, "current")
	wantErr := errors.New("lock failed")
	err := withDataBranchCloneLockContext(proc, lockCtx, func() error {
		require.Same(t, lockCtx, proc.Ctx)
		require.NoError(t, proc.Ctx.Err())
		return wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Same(t, oldCtx, proc.Ctx)
}

func TestDataBranchCloneCatalogLockBatch(t *testing.T) {
	ses := newValidateSession(t)
	mp := ses.proc.Mp()
	baseline := mp.CurrNB()

	bat, err := dataBranchCloneCatalogLockBatch(ses.proc, 7, "db", "tbl")
	require.NoError(t, err)
	require.Len(t, bat.Vecs, 1)
	require.Equal(t, 1, bat.Vecs[0].Length())
	require.NotEmpty(t, bat.Vecs[0].GetBytesAt(0))
	bat.Vecs[0].Free(mp)
	require.Equal(t, baseline, mp.CurrNB())
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
			createSql: "create view `pub_db`.`v1` as select * from `pub_db`.`t1`",
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

	rewrittenViewMap, rewrittenViews := rewriteCloneViewInfos(viewMap, sortedViews, "pub_db", "clone_db")
	require.Equal(t, []string{
		genKey("other_db", "dep_v"),
		"clone_db#",
		genKey("clone_db", "v1"),
	}, rewrittenViews)

	info, ok := rewrittenViewMap[genKey("clone_db", "v1")]
	require.True(t, ok)
	require.Equal(t, "clone_db", info.dbName)
	require.Equal(t, "create view `clone_db`.`v1` as select * from `clone_db`.`t1`", info.createSql)

	fallbackInfo, ok := rewrittenViewMap["clone_db#"]
	require.True(t, ok)
	require.Equal(t, "clone_db", fallbackInfo.dbName)
	require.Equal(t, "create view `clone_db`.`legacy_v` as select 1", fallbackInfo.createSql)

	require.Equal(t, "pub_db", viewMap[genKey("pub_db", "v1")].dbName)
	require.Equal(t, "create view `pub_db`.`v1` as select * from `pub_db`.`t1`", viewMap[genKey("pub_db", "v1")].createSql)
	require.Equal(t, "pub_db", viewMap[fallbackKey].dbName)
	require.Equal(t, "create view `pub_db`.`legacy_v` as select 1", viewMap[fallbackKey].createSql)
}
