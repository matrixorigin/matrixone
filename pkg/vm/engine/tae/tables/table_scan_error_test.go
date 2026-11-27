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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/require"
)

// hybridScanObject is a data object whose Scan hands out a prepared batch
// and whose FillBlockTombstones (as a tombstone object) can fail.
type hybridScanObject struct {
	data.Object
	scans   int
	partial *containers.Batch
	scanErr error
	fillErr error
}

func (o *hybridScanObject) Scan(
	_ context.Context,
	bat **containers.Batch,
	_ txnif.TxnReader,
	_ any,
	_ uint16,
	_ []int,
	_ *mpool.MPool,
) error {
	o.scans++
	if o.partial != nil && *bat == nil {
		*bat = o.partial
	}
	return o.scanErr
}

func (o *hybridScanObject) FillBlockTombstones(
	_ context.Context,
	_ txnif.TxnReader,
	_ *objectio.Blockid,
	_ **nulls.Nulls,
	_ uint64,
	_ *mpool.MPool,
) error {
	return o.fillErr
}

func newHybridScanTable(t *testing.T) (*catalog.TableEntry, func()) {
	c := catalog.MockCatalog(nil)
	db, err := c.CreateDBEntry("db", "", "", nil)
	require.NoError(t, err)
	table, err := db.CreateTableEntry(catalog.MockSchema(1, 0), nil, nil)
	require.NoError(t, err)
	return table, func() { _ = c.Close() }
}

func addHybridScanObject(
	t *testing.T, table *catalog.TableEntry, obj *hybridScanObject, tombstone bool,
) objectio.ObjectId {
	id := objectio.NewObjectid()
	stats := objectio.NewObjectStatsWithObjectID(&id, false, false, false)
	_, err := table.CreateCommittedObject(
		types.BuildTS(1, 0),
		&objectio.CreateObjOpt{Stats: stats, IsTombstone: tombstone},
		func(*catalog.ObjectEntry) data.Object { return obj },
	)
	require.NoError(t, err)
	return id
}

func partialScanBatch(mp *mpool.MPool) *containers.Batch {
	bat := containers.NewBatch()
	values := containers.MakeVector(types.T_int64.ToType(), mp)
	values.MustAppendValue(int64(1), false)
	bat.AddVector("value", values)
	return bat
}

func TestHybridScanByBlockClosesBatchWhenScanFails(t *testing.T) {
	table, done := newHybridScanTable(t)
	defer done()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	partial := partialScanBatch(mp)
	obj := &hybridScanObject{partial: partial, scanErr: errors.New("injected scan failure")}
	id := addHybridScanObject(t, table, obj, false)
	blkID := objectio.NewBlockidWithObjectID(&id, 0)

	var bat *containers.Batch
	err := HybridScanByBlock(context.Background(), table, nil, &bat, table.GetLastestSchema(false), []int{0}, &blkID, mp)
	require.EqualError(t, err, "injected scan failure")
	require.Nil(t, bat)
	require.Nil(t, partial.Vecs[0], "the partly built batch is closed")
	require.Zero(t, mp.CurrNB())
}

func TestHybridScanByBlockClosesBatchWhenTombstonesFail(t *testing.T) {
	table, done := newHybridScanTable(t)
	defer done()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	partial := partialScanBatch(mp)
	id := addHybridScanObject(t, table, &hybridScanObject{partial: partial}, false)
	addHybridScanObject(t, table, &hybridScanObject{fillErr: errors.New("injected tombstone failure")}, true)
	blkID := objectio.NewBlockidWithObjectID(&id, 0)

	var bat *containers.Batch
	err := HybridScanByBlock(
		context.Background(), table, txnbase.MockTxnReaderWithNow(), &bat,
		table.GetLastestSchema(false), []int{0}, &blkID, mp,
	)
	require.EqualError(t, err, "injected tombstone failure")
	require.Nil(t, bat, "a closed batch is not handed back")
	require.Nil(t, partial.Vecs[0])
	require.Zero(t, mp.CurrNB())
}

func TestHybridScanByBlockKeepsCallerBatchBeforeScan(t *testing.T) {
	table, done := newHybridScanTable(t)
	defer done()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	callers := partialScanBatch(mp)
	defer callers.Close()
	missing := objectio.NewObjectid()
	blkID := objectio.NewBlockidWithObjectID(&missing, 0)

	bat := callers
	err := HybridScanByBlock(context.Background(), table, nil, &bat, table.GetLastestSchema(false), []int{0}, &blkID, mp)
	require.Error(t, err)
	require.Same(t, callers, bat, "an error before Scan leaves the caller's batch alone")
	require.NotNil(t, callers.Vecs[0])
}
