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

package gc

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/stretchr/testify/require"
)

type logicalCheckpointRunner struct {
	checkpoint.Runner
	registered []string
}

func (r *logicalCheckpointRunner) AddCheckpointMetaFile(name string) {
	r.registered = append(r.registered, name)
}

func TestMergeCheckpointPreservesLogicalDML(t *testing.T) {
	for _, version := range []uint32{13, 14} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx := context.Background()
			fs, err := fileservice.NewLocalFS(ctx, "shared", t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			defer fs.Close(ctx)
			mp := mpool.MustNewZero()
			defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
			end := types.BuildTS(100, 0)
			entry := writeScanCheckpoint(t, fs, mp, 0, 100)
			if version == 14 {
				ranges := ckputil.MakeTableRangeBatch()
				defer ranges.Clean(mp)
				logical := batch.NewWithSize(3)
				logical.Vecs[0] = vector.NewVec(types.T_uint64.ToType())
				logical.Vecs[1] = vector.NewVec(types.T_uint64.ToType())
				logical.Vecs[2] = vector.NewVec(types.T_TS.ToType())
				defer logical.Clean(mp)
				for _, row := range []struct {
					db, table uint64
					ts        types.TS
				}{{0, 0, end}, {1, 42, types.BuildTS(10, 0)}} {
					require.NoError(t, vector.AppendFixed(logical.Vecs[0], row.db, false, mp))
					require.NoError(t, vector.AppendFixed(logical.Vecs[1], row.table, false, mp))
					require.NoError(t, vector.AppendFixed(logical.Vecs[2], row.ts, false, mp))
				}
				logical.SetRowCount(2)
				name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
				writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, false)
				require.NoError(t, err)
				_, err = writer.WriteBatch(ranges)
				require.NoError(t, err)
				_, _, err = writer.WriteSubBatch(logical, objectio.CkpMetaStart)
				require.NoError(t, err)
				blocks, _, err := writer.Sync(ctx)
				require.NoError(t, err)
				loc := objectio.BuildLocation(name, blocks[0].GetExtent(), 0, blocks[0].GetID())
				entry.SetLocation(loc, loc)
				entry.SetVersion(version)
			}
			runner := &logicalCheckpointRunner{}
			bf := bloomfilter.New(1, 0.01)
			defer bf.Free()
			_, files, merged, raw, err := MergeCheckpoint(ctx, "logical-history", "", []*checkpoint.CheckpointEntry{entry}, &bf, &end, runner, mp, fs)
			if raw != nil {
				defer raw.Clean(mp)
			}
			require.NoError(t, err)
			require.NotEmpty(t, files)
			require.Len(t, runner.registered, 1)
			require.Equal(t, version, merged.GetVersion())
			loaded, err := logtail.ReadCheckpointLogicalDML(ctx, merged.GetVersion(), merged.GetLocation(), fs, mp)
			require.NoError(t, err)
			if version == 13 {
				require.Nil(t, loaded)
				return
			}
			defer loaded.Clean(mp)
			require.Equal(t, []uint64{0, 42}, vector.MustFixedColNoTypeCheck[uint64](loaded.Vecs[1]))
			require.Equal(t, []types.TS{end, types.BuildTS(10, 0)}, vector.MustFixedColNoTypeCheck[types.TS](loaded.Vecs[2]))
		})
	}
}
