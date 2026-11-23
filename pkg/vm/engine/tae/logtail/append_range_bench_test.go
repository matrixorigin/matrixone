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

package logtail

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

// Use the real table operator, response builder and MVCC collector. This seam
// omits payload copying so the benchmark isolates no-change older-object work.
type rangeBenchmarkObject struct {
	data.Object
	history *updates.AppendMVCCHandle
}

func (d *rangeBenchmarkObject) GetAppendMaxCommitTS() (types.TS, bool) { return types.TS{}, false }
func (d *rangeBenchmarkObject) ScanInMemory(_ context.Context, _ map[uint32]*containers.BatchWithVersion, start, end types.TS, mp *mpool.MPool) error {
	d.history.RLock()
	_, commits, aborts := d.history.CollectAppendLocked(start, end, mp)
	d.history.RUnlock()
	if commits != nil {
		commits.Close()
		aborts.Close()
	}
	return nil
}
func collectRangeBenchmark(table *catalog.TableEntry, from, to types.TS) error {
	b := NewTableLogtailRespBuilder(context.Background(), "", from, to, table)
	defer b.Close()
	op := BoundTableOperator{tbl: table}
	op.visitor = b
	if err := op.iterObject(from, to, false); err != nil {
		return err
	}
	response, err := b.BuildResp()
	runtime.KeepAlive(response)
	return err
}
func BenchmarkOlderObjectRangeCollection(b *testing.B) {
	for _, objects := range []int{1, 64} {
		for _, nodes := range []int{64, 8192} {
			b.Run(fmt.Sprintf("objects%d/nodes%d", objects, nodes), func(b *testing.B) {
				c := catalog.MockCatalog(nil)
				b.Cleanup(func() {
					if err := c.Close(); err != nil {
						b.Error(err)
					}
				})
				db, err := c.CreateDBEntry("range_benchmark", "", "", nil)
				if err != nil {
					b.Fatal(err)
				}
				table, err := db.CreateTableEntry(catalog.MockSchema(2, 0), nil, nil)
				if err != nil {
					b.Fatal(err)
				}
				for object := range objects + 1 {
					h := updates.NewAppendMVCCHandle(nil)
					if object < objects {
						for row := range nodes {
							node := updates.NewAppendNode(nil, uint32(row), uint32(row+1), false, h)
							node.Start = types.BuildTS(int64(100+row), 0)
							node.Prepare, node.End = node.Start, node.Start
							h.OnReplayAppendNode(node)
						}
					}
					from := types.BuildTS(int64(nodes+200), 0)
					created := types.BuildTS(int64(object+1), 0)
					if object == objects {
						created = from
					}
					id := objectio.NewObjectid()
					stats := objectio.NewObjectStatsWithObjectID(&id, true, false, false)
					_, err = table.CreateCommittedObject(created, &objectio.CreateObjOpt{Stats: stats}, func(*catalog.ObjectEntry) data.Object { return &rangeBenchmarkObject{history: h} })
					if err != nil {
						b.Fatal(err)
					}
				}
				from, to := types.BuildTS(int64(nodes+200), 0), types.BuildTS(int64(nodes+201), 0)
				// Warm the initial response/index construction outside scored iterations.
				for range 2 {
					if err := collectRangeBenchmark(table, from, to); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				for b.Loop() {
					if err := collectRangeBenchmark(table, from, to); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
