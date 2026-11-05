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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/stretchr/testify/require"
)

// Materialization assigns new rowids, so transaction deletes must be applied
// while the source rowids still identify the rows.
func TestTableMetaReaderMaterializesVisibleRows(t *testing.T) {
	txn, _, fs, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	disarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	defer disarm()
	ids := []types.Rowid{buildTestMaterializedSnapshotRowID(t, 0), buildTestMaterializedSnapshotRowID(t, 1)}
	state := newTestMaterializedSnapshotPartitionState(t, mp, []testMaterializedSnapshotRow{
		{rowID: ids[0], ts: types.BuildTS(1, 0), id: 1, payload: 10},
		{rowID: ids[1], ts: types.BuildTS(1, 0), id: 2, payload: 20},
		{rowID: buildTestMaterializedSnapshotRowID(t, 2), ts: types.BuildTS(3, 0), id: 3, payload: 30},
	})
	appendDelete := func(tableID uint64, id types.Rowid, key int64) {
		b := batch.NewWithSize(2)
		t.Cleanup(func() { b.Clean(mp) })
		b.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		b.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		b.Vecs[1] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(b.Vecs[0], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], key, false, mp))
		b.SetRowCount(1)
		txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, accountId: 1, databaseId: tbl.db.databaseId, tableId: tableID, databaseName: "db", tableName: "r32tbl", bat: b})
	}
	materialize := func(offset int) []int64 {
		r := &TableMetaReader{table: tbl, fs: fs, snapshot: types.BuildTS(2, 0), txnOffset: offset, pState: state, sinkerOption: txn.SinkerCapacityOption()}
		out := colexec.AllocCNS3ResultBat(false)
		defer out.Clean(mp)
		seqs, typs, attrs, _, _ := colexec.GetSequmsAttrsSortKeyIdxFromTableDef(tbl.tableDef)
		_, err := r.collectDataOfAObjsAndInMem(ctx, mp, out, seqs, attrs, typs, readutil.ObjListRelData{})
		require.NoError(t, err)
		if out.RowCount() == 0 {
			return nil
		}
		stats := r32FirstOutBatchStats(t, out)
		reader, err := ioutil.NewObjectReader(fs, stats.ObjectLocation())
		require.NoError(t, err)
		var keys []int64
		for i := uint16(0); i < uint16(stats.BlkCnt()); i++ {
			loaded, release, err := reader.LoadColumns(ctx, []uint16{0}, nil, i, mp)
			require.NoError(t, err)
			keys = append(keys, vector.MustFixedColWithTypeCheck[int64](loaded.Vecs[0])...)
			release()
		}
		return keys
	}
	appendDelete(r32TableID+1, ids[1], 2)
	initial := len(txn.writes)
	appendDelete(r32TableID, ids[0], 1)
	boundary := len(txn.writes)
	appendDelete(r32TableID, ids[1], 2)
	require.ElementsMatch(t, []int64{1, 2}, materialize(initial), "deletes after boundary are invisible")
	require.ElementsMatch(t, []int64{2}, materialize(boundary), "only the live source row is rewritten")
	require.Empty(t, materialize(len(txn.writes)), "all deleted rows produce no data")
}
