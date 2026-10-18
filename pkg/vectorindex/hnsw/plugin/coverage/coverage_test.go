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

package coverage

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	indexcoverage "github.com/matrixorigin/matrixone/pkg/indexplugin/coverage"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	veccache "github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/stretchr/testify/require"
)

func TestBuildCoversSource(t *testing.T) {
	for _, tc := range []struct {
		name   string
		build  int64
		source types.TS
		want   bool
	}{
		{name: "behind", build: 99, source: types.BuildTS(100, 0), want: false},
		{name: "equal", build: 100, source: types.BuildTS(100, 0), want: true},
		{name: "ahead", build: 101, source: types.BuildTS(100, 7), want: true},
		{name: "unknown build", build: 0, source: types.BuildTS(100, 0), want: false},
		{name: "empty source", build: 100, source: types.TS{}, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, BuildCoversSource(tc.build, tc.source))
		})
	}
}

func TestBuildCoversSnapshotRejectsNewerResidentGeneration(t *testing.T) {
	for _, tc := range []struct {
		name         string
		build        int64
		read, source types.TS
		want         bool
	}{
		{name: "future", build: 101, read: types.BuildTS(100, 1), source: types.BuildTS(99, 0)},
		{name: "same physical logical zero", build: 100, read: types.BuildTS(100, 0), source: types.BuildTS(99, 0)},
		{name: "same physical logical one", build: 100, read: types.BuildTS(100, 1), source: types.BuildTS(99, 0)},
		{name: "same physical logical seven", build: 100, read: types.BuildTS(100, 7), source: types.BuildTS(99, 0)},
		{name: "older physical", build: 99, read: types.BuildTS(100, 1), source: types.BuildTS(99, 0), want: true},
		{name: "source equality", build: 100, read: types.BuildTS(101, 0), source: types.BuildTS(100, 0), want: true},
		{name: "source physical gap", build: 99, read: types.BuildTS(101, 0), source: types.BuildTS(100, 0)},
		{name: "source logical gap", build: 99, read: types.BuildTS(100, 1), source: types.BuildTS(99, 1)},
		{name: "unknown build", read: types.BuildTS(100, 1), source: types.BuildTS(99, 0)},
		{name: "negative build", build: -1, read: types.BuildTS(100, 1), source: types.BuildTS(99, 0)},
		{name: "empty read", build: 99, source: types.BuildTS(99, 0)},
		{name: "empty source", build: 99, read: types.BuildTS(100, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, BuildCoversSnapshot(tc.build, tc.read, tc.source)) })
	}
}

type fakeTxn struct{ client.TxnOperator }

func (fakeTxn) SnapshotTS() timestamp.Timestamp { return timestamp.Timestamp{} }

func (f fakeTxn) CloneSnapshotOp(timestamp.Timestamp) client.TxnOperator { return f }

func metadataResult(t *testing.T, buildTS int64) executor.Result {
	t.Helper()
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], buildTS, false, mp))
	bat.SetRowCount(1)
	return executor.Result{Mp: mp, Batches: []*batch.Batch{bat}}
}

func TestCoversSnapshotUsesDurableMetadataWhenCacheIsCold(t *testing.T) {
	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })

	previousExec := execWithResult
	t.Cleanup(func() { execWithResult = previousExec })
	execWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return metadataResult(t, 100), nil
	}

	req := indexcoverage.Request{
		CNUUID:             "cn0",
		Txn:                fakeTxn{},
		TableID:            1,
		IndexDef:           &plan.IndexDef{IndexName: "idx_hnsw"},
		IndexStorageTable:  "cold_hnsw_storage",
		IndexMetadataDB:    "db",
		IndexMetadataTable: "cold_hnsw_meta",
		Snapshot:           types.BuildTS(101, 0),
		SourceCommitTS: func(context.Context, types.TS) (types.TS, error) {
			return types.BuildTS(100, 0), nil
		},
	}

	covered, buildTS, err := Hooks{}.CoversSnapshot(context.Background(), req)
	require.NoError(t, err)
	require.True(t, covered)
	require.Equal(t, types.BuildTS(100, 0), buildTS)
	req.Snapshot = types.BuildTS(100, 1)
	covered, _, err = Hooks{}.CoversSnapshot(context.Background(), req)
	require.NoError(t, err)
	require.False(t, covered, "equal physical provenance is ambiguous even when metadata is snapshot-visible")
}

func TestCoversSnapshotRejectsGenerationNewerThanReadSnapshot(t *testing.T) {
	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })

	previousExec := execWithResult
	t.Cleanup(func() { execWithResult = previousExec })
	execWithResult = func(_ context.Context, _ string, _ string, _ client.TxnOperator) (executor.Result, error) {
		return metadataResult(t, 300), nil
	}
	req := indexcoverage.Request{
		CNUUID:             "cn0",
		Txn:                fakeTxn{},
		TableID:            1,
		IndexDef:           &plan.IndexDef{IndexName: "idx_hnsw"},
		IndexStorageTable:  "cold_hnsw_storage",
		IndexMetadataDB:    "db",
		IndexMetadataTable: "cold_hnsw_meta",
		Snapshot:           types.BuildTS(200, 0),
		SourceCommitTS: func(context.Context, types.TS) (types.TS, error) {
			return types.BuildTS(300, 0), nil
		},
	}

	covered, searched, err := Hooks{}.CoversSnapshot(context.Background(), req)
	require.NoError(t, err)
	require.False(t, covered)
	require.Equal(t, types.BuildTS(300, 0), searched)
}

func TestCoversSnapshotFailsClosedOnMetadataOrSourceErrors(t *testing.T) {
	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })

	base := indexcoverage.Request{
		CNUUID:             "cn0",
		Txn:                fakeTxn{},
		TableID:            1,
		IndexDef:           &plan.IndexDef{IndexName: "idx_hnsw"},
		IndexStorageTable:  "cold_hnsw_storage",
		IndexMetadataDB:    "db",
		IndexMetadataTable: "cold_hnsw_meta",
	}

	previousExec := execWithResult
	t.Cleanup(func() { execWithResult = previousExec })

	covered, _, err := Hooks{}.CoversSnapshot(context.Background(), indexcoverage.Request{})
	require.NoError(t, err)
	require.False(t, covered)

	execWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, errors.New("metadata unavailable")
	}
	base.SourceCommitTS = func(context.Context, types.TS) (types.TS, error) {
		return types.BuildTS(100, 0), nil
	}
	covered, _, err = Hooks{}.CoversSnapshot(context.Background(), base)
	require.NoError(t, err)
	require.False(t, covered)

	// An empty metadata result is also an unknown generation, not coverage.
	mp := mpool.MustNewZero()
	empty := batch.NewWithSize(1)
	empty.SetRowCount(0)
	execWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{Mp: mp, Batches: []*batch.Batch{empty}}, nil
	}
	covered, _, err = Hooks{}.CoversSnapshot(context.Background(), base)
	require.NoError(t, err)
	require.False(t, covered)

	execWithResult = func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return metadataResult(t, 100), nil
	}
	base.SourceCommitTS = func(context.Context, types.TS) (types.TS, error) {
		return types.TS{}, errors.New("source watermark unavailable")
	}
	covered, _, err = Hooks{}.CoversSnapshot(context.Background(), base)
	require.Error(t, err)
	require.False(t, covered)
}

func TestExecWithResultFailsWithoutInternalExecutor(t *testing.T) {
	sid := t.Name()
	rt := moruntime.NewRuntime(metadata.ServiceType_CN, sid, nil)
	moruntime.SetupServiceBasedRuntime(sid, rt)
	t.Cleanup(func() { moruntime.CompareAndDeleteServiceBasedRuntime(sid, rt) })

	_, err := execWithResultImpl(context.Background(), "select 1", sid, fakeTxn{})
	require.Error(t, err)
}

func TestCoversSnapshotColdHistoricalTransaction(t *testing.T) {
	previousCache := veccache.Cache
	veccache.Cache = veccache.NewVectorIndexCache()
	t.Cleanup(func() { veccache.Cache = previousCache })
	previousExec := execWithResult
	t.Cleanup(func() { execWithResult = previousExec })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	snapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	cloned := &metadataSnapshotTxn{snapshot: snapshot}
	parent := &metadataSnapshotTxn{snapshot: timestamp.Timestamp{PhysicalTime: 200}, clone: cloned}
	var execCtx, sourceCtx context.Context
	var execTxn client.TxnOperator
	execWithResult = func(ctx context.Context, _ string, _ string, op client.TxnOperator) (executor.Result, error) {
		execCtx = ctx
		execTxn = op
		return metadataResult(t, 99), nil
	}
	req := indexcoverage.Request{
		CNUUID: "cn0", Txn: parent, TableID: 1, IndexDef: &plan.IndexDef{IndexName: "idx_hnsw"},
		IndexStorageTable: t.Name(), IndexMetadataDB: "db", IndexMetadataTable: "meta",
		Snapshot: types.TimestampToTS(snapshot), ScanSnapshotTS: &snapshot,
		SourceCommitTS: func(ctx context.Context, _ types.TS) (types.TS, error) {
			sourceCtx = ctx
			return types.BuildTS(99, 0), nil
		},
	}
	covered, _, err := Hooks{}.CoversSnapshot(ctx, req)
	require.NoError(t, err)
	require.True(t, covered)
	require.Equal(t, snapshot, parent.clonedAt)
	require.Same(t, cloned, execTxn)
	require.Same(t, ctx, execCtx)
	require.Same(t, ctx, sourceCtx)
}

type metadataSnapshotTxn struct {
	client.TxnOperator
	snapshot, clonedAt timestamp.Timestamp
	clone              *metadataSnapshotTxn
}

func (t *metadataSnapshotTxn) SnapshotTS() timestamp.Timestamp { return t.snapshot }
func (t *metadataSnapshotTxn) CloneSnapshotOp(ts timestamp.Timestamp) client.TxnOperator {
	t.clonedAt = ts
	return t.clone
}
