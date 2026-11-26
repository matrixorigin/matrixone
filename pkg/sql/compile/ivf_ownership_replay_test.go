// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

// A single logical snapshot can read the same still-visible appendable object
// from memory or from its flush snapshot file, depending on replica progress.
func TestIVFEntriesOwnershipAcrossReplayPhases(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	fs := testutil.NewSharedFS()
	defer fs.Close(ctx)
	const pk = "known-workload-pk"
	create, insert, snapshot := types.BuildTS(10, 0), types.BuildTS(20, 0), types.BuildTS(30, 0)
	flushStart, flushCommit := types.BuildTS(35, 0), types.BuildTS(40, 0)

	objID := types.Objectid{1, 2, 3}
	for plan2.IVFObjectIDHashToRange(objID, 2) != 1 {
		objID[0]++
		if objID[0] == 0 {
			t.Fatal("deterministic fixture could not find a partition-1 object id")
		}
	}
	blockID := objectio.NewBlockidWithObjectID(&objID, 0)
	rowID := types.NewRowid(&blockID, 0)
	row := batch.NewWithSize(3)
	row.Attrs = []string{"__mo_rowid", "__mo_commit_ts", "pk"}
	row.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	row.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	row.Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	defer row.Clean(mp)
	require.NoError(t, vector.AppendFixed(row.Vecs[0], rowID, false, mp))
	require.NoError(t, vector.AppendFixed(row.Vecs[1], insert, false, mp))
	require.NoError(t, vector.AppendBytes(row.Vecs[2], []byte(pk), false, mp))
	row.SetRowCount(1)
	proto, err := batch.BatchToProtoBatch(row)
	require.NoError(t, err)
	packer := types.NewPacker()
	defer packer.Close()
	before := logtailreplay.NewPartitionState("pre-apply", false, 1234, false)
	before.HandleRowsInsert(ctx, proto, 0, packer, mp)
	mem := before.NewRowsIter(snapshot, nil, false)
	require.True(t, mem.Next())
	decoded := mem.Entry().Batch
	defer decoded.Clean(mp)
	require.Equal(t, pk, decoded.Vecs[2].GetStringAt(0))
	require.NoError(t, mem.Close())
	after := before.Copy()

	// Reproduce the old appendable object's persisted representation (same ID,
	// original row commit TS) with a real writer, as flushObjTask does.
	persisted := batch.NewWithSize(2)
	persisted.Attrs = []string{"pk", "__mo_commit_ts"}
	persisted.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	persisted.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	defer persisted.Clean(mp)
	require.NoError(t, vector.AppendBytes(persisted.Vecs[0], []byte(pk), false, mp))
	require.NoError(t, vector.AppendFixed(persisted.Vecs[1], insert, false, mp))
	persisted.SetRowCount(1)
	name := objectio.BuildObjectNameWithObjectID(&objID)
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, []uint16{0, objectio.SEQNUM_COMMITTS}, false)
	require.NoError(t, err)
	writer.SetAppendable()
	_, err = writer.WriteBatch(persisted)
	require.NoError(t, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(t, err)
	snapshotStats := writer.GetObjectStats(objectio.WithAppendable())
	require.True(t, snapshotStats.GetAppendable())
	require.Equal(t, objID, *snapshotStats.ObjectName().ObjectId())
	// Replay the real live object-list format consumed from the TN, including
	// the flush transaction start TS used as its in-memory eviction boundary.
	objectList := batch.NewWithSize(2 + len(logtail.ObjectInfoAttr))
	objectList.Attrs = append([]string{"__mo_rowid", "__mo_commit_ts"}, logtail.ObjectInfoAttr...)
	objectList.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	objectList.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	for i, typ := range logtail.ObjectInfoTypes {
		objectList.Vecs[2+i] = vector.NewVec(typ)
	}
	defer objectList.Clean(mp)
	newID := types.Objectid{9, 8, 7}
	appendObject := func(stats objectio.ObjectStats, createdAt, deletedAt types.TS) {
		require.NoError(t, vector.AppendFixed(objectList.Vecs[0], types.Rowid{}, false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[1], flushCommit, false, mp))
		require.NoError(t, vector.AppendBytes(objectList.Vecs[2], stats[:], false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[3], uint64(12), false, mp))
		require.NoError(t, vector.AppendFixed(objectList.Vecs[4], uint64(1234), false, mp))
		for i, ts := range []types.TS{createdAt, deletedAt, flushStart, flushCommit, flushCommit} {
			require.NoError(t, vector.AppendFixed(objectList.Vecs[5+i], ts, false, mp))
		}
	}
	appendObject(snapshotStats, create, flushCommit)
	appendObject(ivfTestImmutableStats(t, &newID), flushCommit, types.TS{})
	objectList.SetRowCount(2)
	objects, err := batch.BatchToProtoBatch(objectList)
	require.NoError(t, err)
	after.HandleDataObjectList(ctx, &api.Entry{EntryType: api.Entry_DataObject, DatabaseId: 12, TableId: 1234, Bat: objects}, fs, mp)
	require.Zero(t, after.ApproxInMemRows(), "applied flush removes committed rows from the replay memtable")
	postFlushMem := after.NewRowsIter(snapshot, nil, false)
	require.False(t, postFlushMem.Next(), "replayed snapshot has no in-memory copy of the key")
	require.NoError(t, postFlushMem.Close())
	require.Equal(t, 1, before.ApproxInMemRows(), "other replica still precedes the flush")

	keys := func(p *logtailreplay.PartitionState, cnidx, cncnt int32, partitioned, remote bool) []string {
		t.Helper()
		node := ivfTestEntriesScanNode()
		if partitioned {
			node.IndexReaderParam = partitionedIndexReaderParam(node.IndexReaderParam, cncnt, cnidx)
		}
		// Match the planner's actual entries-scan marker rather than just a table type.
		if !plan2.IsIvfSearchEntriesInternalScan(node) {
			t.Fatal("test node is not a real IVF entries scan")
		}
		policy := engine.DataCollectPolicy(engine.Policy_CollectCommittedPersistedData)
		if !remote {
			policy = localRangesPolicy(node, cnidx)
		}
		var out []string
		if policy&engine.Policy_CollectCommittedInmemData != 0 {
			iter := p.NewRowsIter(snapshot, nil, false)
			defer iter.Close()
			for iter.Next() {
				out = append(out, iter.Entry().Batch.Vecs[2].GetStringAt(0))
			}
		}
		scope := Scope{NodeInfo: engine.Node{CNCNT: cncnt, CNIDX: cnidx}, DataSource: &Source{node: node}, IsRemote: remote}
		rsp := scope.rangesShuffleParam()
		blocks := objectio.PreAllocBlockInfoSlice(2)
		err := readutil.FilterTxnObjects(ctx, engine.RangesParam{Policy: policy, Rsp: rsp}, snapshot.ToTimestamp(), nil, nil, nil, nil, nil, p, nil, nil, &blocks, nil, fs, false)
		require.NoError(t, err)
		for i := 0; i < blocks.Len(); i++ {
			loc := blocks.Get(i).MetaLocation()
			require.Equal(t, objID, loc.ObjectId(), "new replacement is not visible at the retained snapshot")
			cols := containers.NewVectors(2)
			_, release, _, readErr := ioutil.LoadColumnsData(ctx, []uint16{0, objectio.SEQNUM_COMMITTS}, []types.Type{types.T_varchar.ToType(), types.T_TS.ToType()}, fs, loc, cols, mp, 0)
			require.NoError(t, readErr)
			func() {
				defer release()
				commits := vector.MustFixedColWithTypeCheck[types.TS](&cols[1])
				for i, ts := range commits {
					if ts.LE(&snapshot) {
						out = append(out, cols[0].GetStringAt(i))
					}
				}
			}()
		}
		return out
	}

	for _, route := range []struct {
		name           string
		partitioned    bool
		physicalRemote bool
	}{
		{name: "parallel-table-function-locally-nested-scan", partitioned: true},
		{name: "filtered-generic-fanout", physicalRemote: true},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, phases := range []struct {
				name         string
				owner, other *logtailreplay.PartitionState
			}{
				{"pre-pre", before, before},
				{"post-post", after, after},
				{"pre-post", before, after},
				{"post-pre", after, before},
			} {
				t.Run(phases.name, func(t *testing.T) {
					require.Equal(t, []string{pk}, keys(phases.owner, 0, 2, route.partitioned, false))
					require.Empty(t, keys(phases.other, 1, 2, route.partitioned, route.physicalRemote))
				})
			}
		})
	}
	t.Run("one-CN", func(t *testing.T) {
		require.Equal(t, []string{pk}, keys(before, 0, 1, false, false))
		require.Equal(t, []string{pk}, keys(after, 0, 1, false, false))
	})
	t.Run("immutable-distribution-unchanged", func(t *testing.T) {
		partitionsSeen := map[int32]bool{}
		for x := 1; x <= 64; x++ {
			id := types.Objectid{byte(x), 23, 64}
			stats := ivfTestImmutableStats(t, &id)
			require.False(t, stats.GetAppendable())
			var kept []int32
			for idx := int32(0); idx < 3; idx++ {
				scope := Scope{NodeInfo: engine.Node{CNCNT: 3, CNIDX: idx}, DataSource: &Source{node: ivfTestEntriesScanNode()}}
				if !plan2.ShouldSkipObjByShuffle(scope.rangesShuffleParam(), &stats) {
					kept = append(kept, idx)
					partitionsSeen[idx] = true
				}
			}
			require.Equal(t, []int32{int32(plan2.IVFObjectIDHashToRange(id, 3))}, kept)
		}
		require.Len(t, partitionsSeen, 3)
	})
	t.Logf("snapshot=%s flush-start=%s flush-commit=%s object=%s hash-partition=%d", snapshot.ToString(), flushStart.ToString(), flushCommit.ToString(), objID.String(), plan2.IVFObjectIDHashToRange(objID, 2))
}

// Both distributed IVF paths use ordinary pipeline instructions. The scan's
// existing partition fields suffice to determine who may read appendable rows.
func TestIVFDistributedScopesKeepNativeOperators(t *testing.T) {
	c, node := ivfCandidateCompileFixture(t, 2, nil)
	scopes, err := c.compileIvfSearchParallel(node)
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	require.Len(t, scopes, 2)
	scan := newScope(Remote)
	t.Cleanup(scan.release)
	scan.Proc = testutil.NewProcess(t)
	t.Cleanup(scan.Proc.Free)
	scan.Proc.BuildPipelineContext(scan.Proc.GetTopContext())
	scan.DataSource = &Source{node: ivfTestEntriesScanNode()}
	scan.NodeInfo = engine.Node{CNCNT: 2, CNIDX: 1}
	scan.setRootOperator(table_scan.NewArgument())
	for _, tc := range []struct {
		scope *Scope
		op    vm.OpType
	}{{scopes[1], vm.TableFunction}, {scan, vm.TableScan}} {
		data, err := encodeScope(tc.scope)
		require.NoError(t, err)
		var p pipeline.Pipeline
		require.NoError(t, p.Unmarshal(data))
		require.Len(t, p.InstructionList, 1)
		require.Equal(t, int32(tc.op), p.InstructionList[0].GetOp())
	}
}

func ivfTestImmutableStats(t *testing.T, id *types.Objectid) objectio.ObjectStats {
	t.Helper()
	stats := objectio.NewObjectStatsWithObjectID(id, false, false, false)
	require.NoError(t, objectio.SetObjectStatsSize(stats, 100))
	require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
	return *stats
}

func ivfTestEntriesScanNode() *plan.Node {
	return &plan.Node{
		NodeType:         plan.Node_TABLE_SCAN,
		TableDef:         &plan.TableDef{Name: "__mo_index_secondary_witness", TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
		IndexReaderParam: &plan.IndexReaderParam{OrigFuncName: "l2_distance"},
	}
}
