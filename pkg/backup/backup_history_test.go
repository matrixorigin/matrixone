// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

func TestExecBackupPreservesHistoryAcrossInheritedGCWindow(t *testing.T) {
	for _, proven := range []bool{false, true} {
		name := "legacy metadata"
		if proven {
			name = "committed scan proof"
		}
		t.Run(name, func(t *testing.T) {

			ctx := t.Context()
			src, restored, second := newBackupLocalFS(t, "original"), newBackupLocalFS(t, "restored"), newBackupLocalFS(t, "second-backup")
			pool := common.CheckpointAllocator
			create, snap, drop := types.BuildTS(25, 0), types.BuildTS(27, 0), types.BuildTS(30, 0)
			globalEnd, restore := types.BuildTS(40, 0), types.BuildTS(40, 1)
			id := objectio.NewObjectid()
			name := objectio.BuildObjectNameWithObjectID(&id)
			writer, err := ioutil.NewBlockWriterNew(src, name, 0, []uint16{0}, false)
			require.NoError(t, err)
			data := batch.NewWithSize(1)
			data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			defer data.Clean(pool)
			require.NoError(t, vector.AppendFixed(data.Vecs[0], int64(42), false, pool))
			data.SetRowCount(1)
			_, err = writer.WriteBatch(data)
			require.NoError(t, err)
			_, _, err = writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats()
			require.True(t, logtail.ObjectIsSnapshotRefers(&stats, nil, &create, &drop, []types.TS{snap}))
			cat := catalog.MockCatalog(nil)
			defer cat.Close()
			db, err := cat.CreateDBEntry("history", "", "", nil)
			require.NoError(t, err)
			table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
			require.NoError(t, err)
			entry, err := table.CreateCommittedObject(create, &objectio.CreateObjOpt{Stats: &stats}, nil)
			require.NoError(t, err)
			catalog.MockDroppedObjectEntry2List(entry, drop)
			global, err := logtail.GlobalCheckpointDataFactory(globalEnd, 20*time.Nanosecond, src)(cat)
			require.NoError(t, err)
			defer global.Close()
			globalLoc, _, err := global.Sync(ctx, src)
			require.NoError(t, err)
			writeBackupCheckpointMetadata(t, src, types.TS{}, globalEnd, globalLoc, checkpoint.ET_Global)
			// The GC scanner has only covered 1..20. The object was created later.
			writeGCMetadataWithProof(t, ctx, src, types.BuildTS(1, 0), types.BuildTS(20, 0), proven)
			backup := func(from, to fileservice.FileService, start, end types.TS) {
				metas, err := fileservice.SortedList(from.List(ctx, "ckp"))
				require.NoError(t, err)
				metaNames := make(map[string]struct{})
				for _, file := range metas {
					metaNames[file.Name] = struct{}{}
				}
				previousEntries, err := checkpoint.ListSnapshotCheckpoint(ctx, "", from, start, metaNames)
				require.NoError(t, err)
				special, err := logtail.BackupCheckpointDataFactory(start, end, from)(cat)
				require.NoError(t, err)
				defer special.Close()
				loc, _, err := special.Sync(ctx, from)
				require.NoError(t, err)
				names := []string{"2026-09-30 00:00:00",
					fmt.Sprintf("%s:%d:%s:%s:%s", loc, logtail.CheckpointCurrentVersion, end.ToString(), loc, start.ToString()),
				}
				for _, e := range previousEntries {
					names = append(names, fmt.Sprintf("%s:%d", e.GetLocation(), e.GetVersion()))
				}
				names = append(names, fmt.Sprintf("%s:%d", loc, logtail.CheckpointCurrentVersion))
				require.NoError(t, execBackup(ctx, "", from, to, names, 1, types.TS{}, "full", nil, nil, nil))
				for _, completion := range []string{taeList, taeSum} {
					_, err = to.StatFile(ctx, completion)
					require.NoError(t, err, "successful backup published its completion files")
				}
			}
			backup(src, restored, restore, types.BuildTS(50, 0))
			_, err = restored.StatFile(ctx, name.String())
			require.NoError(t, err, "first backup conservatively copies lifecycle outside scanned range")
			firstReader, err := ioutil.NewObjectReader(restored, stats.ObjectLocation(), objectio.WithDataCachePolicyOption(fileservice.SkipAllCache), objectio.WithMetaCachePolicyOption(fileservice.SkipAllCache))
			require.NoError(t, err)
			firstRows, firstRelease, err := firstReader.LoadColumns(ctx, []uint16{0}, []types.Type{types.T_int64.ToType()}, 0, pool)
			require.NoError(t, err)
			require.Equal(t, []int64{42}, vector.MustFixedColNoTypeCheck[int64](firstRows.Vecs[0]))
			firstRelease()
			gcMetas, err := ioutil.ListTSRangeFilesInGCDir(ctx, restored)
			require.NoError(t, err)
			require.Len(t, gcMetas, 1)
			require.Equal(t, types.BuildTS(20, 0), *gcMetas[0].GetStart())
			require.Equal(t, globalEnd, *gcMetas[0].GetEnd())
			// Recovery advances to the global checkpoint, while the committed scan
			// evidence remains 1..20. A second backup must not invent coverage of 20..40.
			backup(restored, second, types.BuildTS(50, 1), types.BuildTS(60, 0))
			metas, err := fileservice.SortedList(second.List(ctx, "ckp"))
			require.NoError(t, err)
			metaNames := make(map[string]struct{})
			for _, file := range metas {
				metaNames[file.Name] = struct{}{}
			}
			selected, err := checkpoint.ListSnapshotCheckpoint(ctx, "", second, snap, metaNames)
			require.NoError(t, err)
			var references int
			for _, e := range selected {
				reader, err := logtail.GetCheckpointReader(ctx, "", second, e.GetLocation(), e.GetVersion())
				require.NoError(t, err)
				require.NoError(t, reader.ForEachRow(ctx, func(_ uint32, _, _ uint64, _ int8, s objectio.ObjectStats, c, d types.TS, _ types.Rowid) error {
					if s.ObjectName().String() == name.String() && snap.GE(&c) && (d.IsEmpty() || snap.LT(&d)) {
						references++
					}
					return nil
				}))
			}
			require.Greater(t, references, 0, "snapshot consumer still selects object references in copied checkpoint")
			reader, err := ioutil.NewObjectReader(second, stats.ObjectLocation(), objectio.WithDataCachePolicyOption(fileservice.SkipAllCache), objectio.WithMetaCachePolicyOption(fileservice.SkipAllCache))
			require.NoError(t, err)
			rows, release, err := reader.LoadColumns(ctx, []uint16{0}, []types.Type{types.T_int64.ToType()}, 0, pool)
			if release != nil {
				defer release()
			}
			require.NoError(t, err)
			require.Equal(t, []int64{42}, vector.MustFixedColNoTypeCheck[int64](rows.Vecs[0]))

		})
	}
}
