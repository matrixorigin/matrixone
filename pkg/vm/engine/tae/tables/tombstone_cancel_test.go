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

package tables

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

var errCommitTSReadReleased = errors.New("blocked commit timestamp read released")

// blockCommitTSRead blocks one persisted column; other reads use the real FS.
type blockCommitTSRead struct {
	fileservice.FileService
	armed   atomic.Bool
	name    string
	offset  int64
	size    int64
	entered chan struct{}
	release chan struct{}
}

func (f *blockCommitTSRead) Read(ctx context.Context, v *fileservice.IOVector) error {
	if f.armed.Load() && v.FilePath == f.name {
		for _, entry := range v.Entries {
			if entry.Offset == f.offset && entry.Size == f.size {
				select {
				case f.entered <- struct{}{}:
				default:
				}
				select {
				case <-ctx.Done():
					return context.Cause(ctx)
				case <-f.release:
					return errCommitTSReadReleased
				}
			}
		}
	}
	return f.FileService.Read(ctx, v)
}

func TestPersistedTombstoneCommitTSReadCancellation(t *testing.T) {
	ctx := context.Background()
	fs := &blockCommitTSRead{
		FileService: testutil.NewSharedFS(),
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	smallPool := containers.NewVectorPool("cancel-small", 4, containers.WithMPool(mp))
	transientPool := containers.NewVectorPool("cancel-transient", 4, containers.WithMPool(mp))
	defer smallPool.Destory()
	defer transientPool.Destory()
	rt := dbutils.NewRuntime(
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeSmallPool(smallPool),
		dbutils.WithRuntimeTransientPool(transientPool),
	)

	targetObject := objectio.NewObjectid()
	input := batch.NewWithSize(3)
	input.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[2] = vector.NewVec(types.T_TS.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], types.NewRowIDWithObjectIDBlkNumAndRowID(targetObject, 0, 1), false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(1), false, mp))
	require.NoError(t, vector.AppendFixed(input.Vecs[2], types.BuildTS(10, 0), false, mp))
	input.SetRowCount(1)
	defer input.Clean(mp)

	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, objectio.TombstoneSeqnums_DN_Created, true)
	require.NoError(t, err)
	writer.SetAppendable()
	writer.SetPrimaryKeyWithType(uint16(objectio.TombstonePrimaryKeyIdx), index.HBF, index.ObjectPrefixFn, index.BlockPrefixFn)
	_, err = writer.WriteBatch(input)
	require.NoError(t, err)
	blocks, _, err := writer.Sync(ctx)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	block := blocks[0]
	require.Greater(t, block.GetMetaColumnCount(), block.GetMaxSeqnum()+1)
	commitColumn := block.ColumnMeta(block.GetMetaColumnCount() - 1)
	require.Equal(t, uint8(types.T_TS), commitColumn.DataType())
	extent := commitColumn.Location()
	fs.name = name.String()
	fs.offset, fs.size = int64(extent.Offset()), int64(extent.Length())

	stats := writer.GetObjectStats(objectio.WithAppendable())
	c := catalog.MockCatalog(NewDataFactory(rt, ""))
	defer c.Close()
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	c.OnReplayObjectBatch_V2(db.GetID(), table.GetID(), ioutil.ObjectType_Tombstone, stats, types.BuildTS(10, 0), types.TS{})
	entry, err := table.GetObjectByID(stats.ObjectName().ObjectId(), true)
	require.NoError(t, err)
	require.True(t, entry.IsAppendable())
	require.True(t, entry.ObjectPersisted())

	blockID := types.NewBlockidWithObjectID(&targetObject, 0)
	txn := txnbase.MockTxnReaderWithStartTS(types.BuildTS(20, 0))
	fixedUsed, varlenUsed := transientPool.Used(false)
	scanCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	fs.armed.Store(true)
	result := make(chan error, 1)
	go func() {
		var deletes *nulls.Nulls
		result <- entry.GetObjectData().FillBlockTombstones(scanCtx, txn, &blockID, &deletes, 0, mp)
	}()
	select {
	case <-fs.entered:
	case err = <-result:
		t.Fatalf("tombstone scan ended before commit timestamp read: %v", err)
	case <-time.After(15 * time.Second):
		cancel(nil)
		close(fs.release)
		<-result
		t.Fatal("commit timestamp read did not start")
	}

	cause := errors.New("cancel tombstone scan")
	cancel(cause)
	select {
	case err = <-result:
	case <-time.After(3 * time.Second):
		close(fs.release)
		err = <-result
		t.Fatalf("commit timestamp read ignored cancellation: %v", err)
	}
	require.ErrorIs(t, err, cause)
	afterFixed, afterVarlen := transientPool.Used(false)
	require.Equal(t, fixedUsed, afterFixed)
	require.Equal(t, varlenUsed, afterVarlen)

	fs.armed.Store(false)
	var deletes *nulls.Nulls
	require.NoError(t, entry.GetObjectData().FillBlockTombstones(ctx, txn, &blockID, &deletes, 0, mp))
	require.Equal(t, 1, deletes.Count())
	require.True(t, deletes.Contains(1))
}
