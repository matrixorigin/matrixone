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

package disttae

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var errSpillWrite = errors.New("injected post-persist spill write failure")
var errSpillDelete = errors.New("injected spill delete failure")

type spillFaultFS struct {
	fileservice.FileService
	sync.Mutex
	writes      []string
	failWriteAt int
	failDeletes bool
	cancelWrite context.CancelFunc
}

func (fs *spillFaultFS) Write(ctx context.Context, vector fileservice.IOVector) error {
	if err := fs.FileService.Write(ctx, vector); err != nil {
		return err
	}
	fs.Lock()
	fs.writes = append(fs.writes, vector.FilePath)
	fail := len(fs.writes) == fs.failWriteAt
	fs.Unlock()
	if fail {
		if fs.cancelWrite != nil {
			fs.cancelWrite()
		}
		return errSpillWrite
	}
	return nil
}

func (fs *spillFaultFS) Delete(ctx context.Context, names ...string) error {
	fs.Lock()
	fail := fs.failDeletes
	fs.Unlock()
	if fail {
		return errSpillDelete
	}
	return fs.FileService.Delete(ctx, names...)
}

type spillTestRelation struct {
	engine.Relation
	def *plan.TableDef
}

func (r spillTestRelation) GetTableDef(context.Context) *plan.TableDef { return r.def }

func spillTestBatch(t *testing.T, mp *mpool.MPool, value int64) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Attrs = []string{catalog.Row_ID, "a"}
	bat.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.RandomRowid(), false, mp))
	require.NoError(t, vector.AppendFixed(bat.Vecs[1], value, false, mp))
	bat.SetRowCount(1)
	return bat
}

func newSpillTestFixture(
	t *testing.T, tablesCount int,
) (*Transaction, *workspaceSpillAttempt, map[tableKey]engine.Relation, int64) {
	t.Helper()
	proc := testutil.NewProc(t)
	t.Cleanup(proc.Free)
	baseline := proc.Mp().CurrNB()
	workspace := newTxnWorkspace()
	txn := &Transaction{engine: &Engine{}, proc: proc, workspace: workspace}
	def := &plan.TableDef{
		Name: "t", Cols: []*plan.ColDef{
			{ColId: 0, Name: "a", Seqnum: 0, Typ: plan.Type{Id: int32(types.T_int64)}},
			{ColId: 1, Name: catalog.Row_ID, Seqnum: 1, Typ: plan.Type{Id: int32(types.T_Rowid)}},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColId: 0, PkeyColName: "a", Names: []string{"a"}},
	}
	ids := make([]workspaceMutationID, 0, tablesCount)
	tables := make(map[tableKey]engine.Relation)
	for i := 0; i < tablesCount; i++ {
		name := string(rune('a' + i))
		ids = append(ids, workspace.append(Entry{
			typ: INSERT, accountId: 1, databaseId: 2,
			tableId: uint64(3 + i), databaseName: "db", tableName: name,
			bat: spillTestBatch(t, proc.Mp(), int64(i+1)),
		}))
		tables[tableKey{accountId: 1, databaseId: 2, dbName: "db", name: name}] =
			spillTestRelation{def: def}
	}
	attempt, err := workspace.beginSpill(ids)
	require.NoError(t, err)
	return txn, attempt, tables, baseline
}

func TestWorkspaceSpillFailureRetainsEveryUnpublishedObject(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	baseFS := newCleanFS(t)
	fs := &spillFaultFS{
		FileService: baseFS, failWriteAt: 2, failDeletes: true,
		cancelWrite: cancel,
	}
	txn, attempt, tables, baseline := newSpillTestFixture(t, 2)
	workspace := txn.workspace
	staged, stageErr := txn.stageWorkspaceSpill(ctx, fs, INSERT, tables, attempt)
	require.ErrorIs(t, stageErr, errSpillWrite)
	require.ErrorIs(t, stageErr, errSpillDelete)
	require.Len(t, staged.objects, 1, "the first object was staged, not published")
	txn.Lock()
	cleanupErr := txn.cleanStagedWorkspaceSpillLocked(ctx, staged)
	txn.Unlock()
	require.ErrorIs(t, cleanupErr, errSpillDelete)
	require.Len(t, txn.engine.workspaceSpillCleanup.pending, 2)
	require.ErrorIs(t, txn.engine.Close(), errSpillDelete)
	require.Len(t, txn.engine.workspaceSpillCleanup.pending, 2,
		"failed engine shutdown must not discard the cleanup owner")
	for _, mutation := range workspace.mutations {
		require.Empty(t, mutation.entry.fileName, "failed spill must not publish")
	}
	fs.Lock()
	written := append([]string(nil), fs.writes...)
	fs.failDeletes = false
	fs.Unlock()
	require.Len(t, written, 2)
	for _, name := range written {
		_, err := baseFS.StatFile(context.Background(), name)
		require.NoError(t, err)
	}
	require.NoError(t, txn.engine.workspaceSpillCleanup.retry(context.Background()))
	require.Empty(t, txn.engine.workspaceSpillCleanup.pending)
	for _, name := range written {
		_, err := baseFS.StatFile(context.Background(), name)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
	}
	attempt.Close()
	require.NoError(t, workspace.close(txn.proc.Mp()))
	require.Equal(t, baseline, txn.proc.Mp().CurrNB())
}

func TestWorkspaceSpillSuccessTransfersObjectOwnership(t *testing.T) {
	ctx := context.Background()
	fs := newCleanFS(t)
	txn, attempt, tables, baseline := newSpillTestFixture(t, 1)
	staged, err := txn.stageWorkspaceSpill(ctx, fs, INSERT, tables, attempt)
	require.NoError(t, err)
	require.Len(t, staged.objects, 1)
	require.Len(t, staged.cleanup, 1)
	_, err = txn.workspace.commitSpill(attempt, staged.objects)
	require.NoError(t, err)
	name := staged.cleanup[0].name
	_, err = fs.StatFile(ctx, name)
	require.NoError(t, err, "published object must not be aborted")
	require.Empty(t, txn.engine.workspaceSpillCleanup.pending)
	attempt.Close()
	require.NoError(t, txn.workspace.close(txn.proc.Mp()))
	require.Equal(t, baseline, txn.proc.Mp().CurrNB())
	require.NoError(t, fs.Delete(ctx, name))
}
