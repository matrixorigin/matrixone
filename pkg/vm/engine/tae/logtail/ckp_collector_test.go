// Copyright 2021 Matrix Origin
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

package logtail

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCollectorsExcludeUnpersistedObjects(t *testing.T) {
	for _, tombstone := range []bool{false, true} {
		name := "data"
		objectType := int8(ioutil.ObjectType_Data)
		if tombstone {
			name = "tombstone"
			objectType = ioutil.ObjectType_Tombstone
		}
		t.Run(name, func(t *testing.T) {
			c := catalog.MockCatalog(tables.NewDataFactory(nil, ""))
			defer c.Close()
			db, err := c.CreateDBEntry("checkpoint_objects", "", "", nil)
			require.NoError(t, err)
			table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
			require.NoError(t, err)
			createTS, dropTS := types.BuildTS(650, 0), types.BuildTS(850, 0)
			newStats := func(appendable bool) *objectio.ObjectStats {
				id := objectio.NewObjectid()
				return objectio.NewObjectStatsWithObjectID(&id, appendable, false, false)
			}
			// An independently published appendable carrier can precede its first
			// committed append. It has no file and its payload belongs to WAL replay.
			pending := newStats(true)
			_, err = table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: pending, IsTombstone: tombstone}, nil)
			require.NoError(t, err)
			immutable := newStats(false)
			_, err = table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: immutable, IsTombstone: tombstone}, nil)
			require.NoError(t, err)
			flushed := newStats(true)
			entry, err := table.CreateCommittedObject(createTS, &objectio.CreateObjOpt{Stats: flushed, IsTombstone: tombstone}, nil)
			require.NoError(t, err)
			catalog.MockDroppedObjectEntry2List(entry, dropTS)
			// Legacy checkpoint objects may lack an extent. Their authoritative
			// persisted flag must retain them; an extent heuristic would lose data.
			legacy := newStats(true)
			c.OnReplayObjectBatch_V2(db.ID, table.ID, objectType, *legacy, createTS, types.TS{})

			for _, kind := range []string{"incremental", "global"} {
				t.Run(kind, func(t *testing.T) {
					var collector *BaseCollector_V2
					switch kind {
					case "global":
						collector = &NewGlobalCollector_V2(nil, types.BuildTS(1000, 0), 500*time.Nanosecond).BaseCollector_V2
					default:
						collector = NewBaseCollector_V2(types.BuildTS(600, 0), types.BuildTS(1000, 0), 0, nil)
					}
					defer collector.Close()
					require.NoError(t, collector.Collect(c))
					counts := make(map[string]int)
					for row := 0; row < collector.data.batch.RowCount(); row++ {
						var stats objectio.ObjectStats
						stats.UnMarshal(collector.data.batch.Vecs[ckputil.TableObjectsAttr_ID_Idx].GetBytesAt(row))
						counts[stats.ObjectName().String()]++
					}
					expected := map[string]int{immutable.ObjectName().String(): 1, flushed.ObjectName().String(): 2, legacy.ObjectName().String(): 1}

					require.Equal(t, expected, counts)
					deletes := vector.MustFixedColNoTypeCheck[types.TS](collector.data.batch.Vecs[ckputil.TableObjectsAttr_DeleteTS_Idx])
					nonEmpty := 0
					for _, ts := range deletes {
						if !ts.IsEmpty() {
							nonEmpty++
							require.Equal(t, dropTS, ts)
						}
					}
					require.Equal(t, 1, nonEmpty)
				})
			}
		})
	}
}
