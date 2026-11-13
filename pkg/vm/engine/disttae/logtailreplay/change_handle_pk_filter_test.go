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

package logtailreplay

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

// Tombstones store the deleted user PK at seqnum 1, independently of its
// position in the data schema. Both paths must prune the unrelated block, while
// an unfiltered consumer must still read both blocks.
func TestHistoricalChangesPKBlockPruning(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		tombstone, appendable bool
		clusterBy             bool
	}{
		{name: "data"},
		{name: "tombstone", tombstone: true},
		{name: "appendable data", appendable: true},
		{name: "appendable tombstone", tombstone: true, appendable: true},
		{name: "cluster-by data", clusterBy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName,
				fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer fs.Close(context.Background())

			const dataPKSeqnum = 17
			var writer *ioutil.BlockWriter
			switch {
			case tc.tombstone:
				writer = ioutil.ConstructTombstoneWriter(objectio.HiddenColumnSelection_CommitTS, fs)
			case tc.clusterBy:
				writer = ioutil.ConstructWriter(0,
					[]uint16{dataPKSeqnum, dataPKSeqnum + 1, objectio.SEQNUM_COMMITTS}, 1, false, false, fs)
			default:
				writer = ioutil.ConstructWriter(0,
					[]uint16{dataPKSeqnum, objectio.SEQNUM_COMMITTS}, 0, true, false, fs)
			}
			for _, key := range []int32{11, 99} {
				b := batch.NewWithSize(2)
				b.Vecs[0] = vector.NewVec(types.T_int32.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[0], key, false, mp))
				b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[1], types.BuildTS(100, 0), false, mp))
				if tc.clusterBy {
					clusterKey := vector.NewVec(types.T_int32.ToType())
					require.NoError(t, vector.AppendFixed(clusterKey, key+100, false, mp))
					b.Vecs = []*vector.Vector{b.Vecs[0], clusterKey, b.Vecs[1]}
				}
				if tc.tombstone {
					rowids := vector.NewVec(types.T_Rowid.ToType())
					require.NoError(t, vector.AppendFixed(rowids, types.Rowid{}, false, mp))
					b.Vecs = append([]*vector.Vector{rowids}, b.Vecs...)
				}
				b.SetRowCount(1)
				_, err = writer.WriteBatch(b)
				b.Clean(mp)
				require.NoError(t, err)
			}
			_, _, err = writer.Sync(context.Background())
			require.NoError(t, err)
			obj := &objectio.ObjectEntry{ObjectStats: writer.Stats(), CreateTime: types.BuildTS(110, 0)}
			if tc.appendable {
				objectio.WithAppendable()(&obj.ObjectStats)
			}
			zm := index.NewZM(types.T_int32, 0)
			requestedKey := int32(11)
			index.UpdateZM(zm, types.EncodeInt32(&requestedKey))

			for _, filtered := range []bool{false, true} {
				h := &AObjectHandle{
					p: &baseHandle{changesHandle: &ChangeHandler{
						primarySeqnum:            dataPKSeqnum,
						enableCommitTSBlockPrune: true,
						strictCommitTSBlockPrune: true,
					}},
					fs:          fs,
					isTombstone: tc.tombstone,
					start:       types.BuildTS(50, 0),
					end:         types.BuildTS(200, 0),
					blockPlans:  make(map[string]*aobjBlockPlan),
				}
				if filtered {
					h.p.changesHandle.pkFilter = &engine.PKFilter{
						Segments: [][]byte{zm}, PrimarySeqnum: dataPKSeqnum, ObjectZMIsPK: !tc.clusterBy,
					}
				}
				if tc.clusterBy && filtered {
					require.False(t, index.AnySegmentOverlaps(obj.SortKeyZoneMap(), h.p.changesHandle.pkFilter.Segments),
						"the cluster key is disjoint from the requested PK even though its physical type matches")
				}
				require.True(t, objectMayMatchPK(*obj, h.p.changesHandle.pkFilter, tc.tombstone),
					"object pruning must preserve the block containing the requested PK")
				read, err := h.shouldReadBlock(context.Background(), obj, 0)
				require.NoError(t, err)
				require.True(t, read, "the requested PK must remain readable")
				read, err = h.shouldReadBlock(context.Background(), obj, 1)
				require.NoError(t, err)
				require.Equal(t, !filtered, read, "only a filtered consumer may skip PK 99")
			}
		})
	}
}

func TestHistoricalChangesObjectPKPruning(t *testing.T) {
	requested := int32(11)
	other := int32(99)
	segment := index.NewZM(types.T_int32, 0)
	index.UpdateZM(segment, types.EncodeInt32(&requested))
	otherZM := index.NewZM(types.T_int32, 0)
	index.UpdateZM(otherZM, types.EncodeInt32(&other))
	filter := &engine.PKFilter{Segments: [][]byte{segment}, ObjectZMIsPK: true}
	for _, tc := range []struct {
		name      string
		zm        index.ZM
		filter    *engine.PKFilter
		tombstone bool
		wantRead  bool
	}{
		{name: "matching PK", zm: segment, filter: filter, wantRead: true},
		{name: "unrelated PK", zm: otherZM, filter: filter},
		{name: "unfiltered", zm: otherZM, wantRead: true},
		{name: "unknown object zonemap", zm: index.NewZM(types.T_int32, 0), filter: filter, wantRead: true},
		{name: "cluster-by is not the user PK", zm: otherZM,
			filter: &engine.PKFilter{Segments: [][]byte{segment}}, wantRead: true},
		{name: "tombstone sort key is not the user PK", zm: otherZM, filter: filter,
			tombstone: true, wantRead: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj := objectio.ObjectEntry{}
			require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&obj.ObjectStats, tc.zm))
			require.Equal(t, tc.wantRead, objectMayMatchPK(obj, tc.filter, tc.tombstone))
		})
	}
}

// Exercise the range-recovery constructor and actual object reads together:
// payload pruning must not erase the metadata needed to follow a replacement.
func TestHistoricalChangesPKFilterReplacementChain(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		cnPredecessor, coarsePredecessor               bool
		missingMetadata, missingFile, missingFollowing bool
	}{
		{name: "unknown appendable predecessor"},
		{name: "coarse appendable predecessor", coarsePredecessor: true},
		{name: "unknown CN predecessor", cnPredecessor: true},
		{name: "coarse CN predecessor", cnPredecessor: true, coarsePredecessor: true},
		{name: "missing successor metadata", missingMetadata: true},
		{name: "missing matching successor file", missingFile: true},
		{name: "disjoint intermediate still requires successor", missingFollowing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			fs, err := fileservice.NewMemoryFS(defines.SharedFileServiceName, fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer fs.Close(ctx)
			write := func(key int32, create, commit int64) *objectio.ObjectEntry {
				t.Helper()
				writer := ioutil.ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS}, 0, true, false, fs)
				b := batch.NewWithSize(2)
				b.Vecs[0] = vector.NewVec(types.T_int32.ToType())
				b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[0], key, false, mp))
				require.NoError(t, vector.AppendFixed(b.Vecs[1], types.BuildTS(commit, 0), false, mp))
				b.SetRowCount(1)
				_, err := writer.WriteBatch(b)
				b.Clean(mp)
				require.NoError(t, err)
				_, _, err = writer.Sync(ctx)
				require.NoError(t, err)
				return &objectio.ObjectEntry{ObjectStats: writer.Stats(), CreateTime: types.BuildTS(create, 0)}
			}
			wanted := write(11, 2, 2)
			replacementKey := int32(99)
			if tc.missingFile {
				replacementKey = 11
			}
			replacement := write(replacementKey, 7, 3)
			predecessor := makeTestObjectEntry(t, 1, !tc.cnPredecessor, tc.cnPredecessor, types.BuildTS(3, 0))
			predecessor.DeleteTime = types.BuildTS(7, 0)
			if tc.coarsePredecessor {
				zm := index.NewZM(types.T_int32, 0)
				for _, key := range []int32{11, 99} {
					index.UpdateZM(zm, types.EncodeInt32(&key))
				}
				require.NoError(t, objectio.SetObjectStatsSortKeyZoneMap(&predecessor.ObjectStats, zm))
			}
			if tc.missingFollowing {
				predecessor.DeleteTime = types.BuildTS(5, 0)
				replacement.CreateTime = types.BuildTS(5, 0)
				replacement.DeleteTime = types.BuildTS(7, 0)
			}
			if tc.missingFile {
				require.NoError(t, fs.Delete(ctx, replacement.ObjectName().String()))
			}
			state := NewPartitionState("", false, 1, false)
			state.start = types.BuildTS(8, 0)
			state.dataObjectsNameIndex.Set(*predecessor)
			state.dataObjectsNameIndex.Set(*wanted)
			if !tc.missingMetadata {
				state.dataObjectsNameIndex.Set(*replacement)
			}
			zm := index.NewZM(types.T_int32, 0)
			key := int32(11)
			index.UpdateZM(zm, types.EncodeInt32(&key))
			filter := &engine.PKFilter{Segments: [][]byte{zm}, PrimarySeqnum: 0, ObjectZMIsPK: true}
			for _, filtered := range []bool{false, true} {
				readCtx := ctx
				if filtered {
					readCtx = engine.WithPKFilter(ctx, filter)
				}
				h, err := NewChangesHandlerWithPartitionStateRange(readCtx, state,
					types.BuildTS(1, 0), types.BuildTS(10, 0), false, CoarseMaxRow, 0, mp, fs)
				if tc.missingMetadata || tc.missingFile || tc.missingFollowing {
					require.Error(t, err, "filtering must not hide an unresolvable replacement")
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrFileNotFound))
					require.Nil(t, h)
					continue
				}
				require.NoError(t, err, "filtered=%t", filtered)
				type version struct {
					key int32
					ts  types.TS
				}
				var got []version
				for {
					data, tombstone, _, err := h.Next(readCtx, mp)
					require.NoError(t, err)
					require.Nil(t, tombstone)
					if data == nil {
						break
					}
					keys := vector.MustFixedColWithTypeCheck[int32](data.Vecs[0])
					commit := vector.MustFixedColWithTypeCheck[types.TS](data.Vecs[len(data.Vecs)-1])
					for i, key := range keys {
						got = append(got, version{key, commit[i]})
					}
					data.Clean(mp)
				}
				require.NoError(t, h.Close())
				want := []version{{11, types.BuildTS(2, 0)}}
				if !filtered {
					want = append(want, version{99, types.BuildTS(3, 0)})
				}
				require.ElementsMatch(t, want, got, "filtered=%t", filtered)
			}
		})
	}
}
