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

package disttae

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestColumnSummaryZoneMap(t *testing.T) {
	for _, tc := range []struct {
		name     string
		blocks   [][]*bool
		legacy   bool
		min, max any
	}{
		{"false", [][]*bool{{boolPointer(false), boolPointer(false)}}, false, false, false},
		{"false null", [][]*bool{{boolPointer(false), nil}}, false, false, false},
		{"multiblock false", [][]*bool{{boolPointer(false), nil}, {boolPointer(false), nil}}, false, false, false},
		{"mixed", [][]*bool{{boolPointer(false), boolPointer(true)}}, false, false, true},
		{"legacy mixed", [][]*bool{{boolPointer(false), boolPointer(true)}}, true, false, true},
		{"legacy multiblock", [][]*bool{{boolPointer(false), nil}, {boolPointer(true), nil}}, true, false, true},
		{"all true", [][]*bool{{boolPointer(true), boolPointer(true)}}, false, true, true},
		{"all null", [][]*bool{{nil, nil}}, false, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			fs, err := fileservice.NewLocalFS(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
			require.NoError(t, err)
			t.Cleanup(func() { fs.Close(ctx) })
			mp := mpool.MustNewZero()
			t.Cleanup(func() { defer mpool.DeleteMPool(mp); require.Zero(t, mp.CurrNB()) })
			writer := ioutil.ConstructWriter(0, []uint16{0}, 0, false, false, fs)
			for _, rows := range tc.blocks {
				func() {
					b := batch.NewWithSize(1)
					defer b.Clean(mp)
					b.Vecs[0] = vector.NewVec(types.T_bool.ToType())
					for _, v := range rows {
						value := false
						if v != nil {
							value = *v
						}
						require.NoError(t, vector.AppendFixed(b.Vecs[0], value, v == nil, mp))
					}
					b.SetRowCount(len(rows))
					_, err := writer.WriteBatch(b)
					require.NoError(t, err)
				}()
			}
			_, _, err = writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats()
			loc := stats.ObjectLocation()
			original, err := objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
			require.NoError(t, err)
			meta := append(objectio.ObjectMeta(nil), original...)
			data := meta.MustDataMeta()
			if tc.legacy {
				data.MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				for i := uint32(0); i < data.BlockCount(); i++ {
					data.GetBlockMeta(i).MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				}
			}
			before := bytes.Clone(meta)
			originalBefore := bytes.Clone(original)
			cols := []*plan.ColDef{{Seqnum: 0, Typ: plan.Type{Id: int32(types.T_bool)}}}
			zms := make([]objectio.ZoneMap, 1)
			tableTypes := make([]uint8, 1)
			for repeat := 0; repeat < 2; repeat++ {
				require.NoError(t, updateColumnSummaries(ctx, data, loc, cols, zms, tableTypes, fs, mp))
			}
			exact := zms[0]
			require.Equal(t, uint8(types.T_bool), tableTypes[0])
			require.Equal(t, tc.min, exact.GetMin())
			require.Equal(t, tc.max, exact.GetMax())
			// 返回值将被跨对象合并，绝不能别名到文件服务共享元数据。
			if exact.IsInited() {
				exact.GetMinBuf()[0] ^= 1
			}
			require.Equal(t, before, []byte(meta))
			require.Equal(t, originalBefore, []byte(original))
			if tc.name == "false" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				require.Error(t, updateColumnSummaries(canceled, data, loc, cols, zms, tableTypes, fs, mp))
			}
		})
	}
}

func boolPointer(v bool) *bool { return &v }

func TestColumnSummaryZoneMapMetadataOnly(t *testing.T) {
	for _, tc := range []struct {
		typ   types.T
		value any
	}{
		{types.T_bool, true}, {types.T_int32, int32(7)},
	} {
		data := objectio.BuildMetaData(1, 1)
		data.MustGetColumn(0).SetZoneMap(index.BuildZM(tc.typ, types.EncodeValue(tc.value, tc.typ)))
		before := bytes.Clone(data)
		// 无文件服务也能完成：精确全 true 和非 BOOL 不应读列数据。
		exact, err := columnSummaryZoneMap(context.Background(), data, nil, 0, nil, nil)
		require.NoError(t, err)
		require.Equal(t, tc.value, exact.GetMax())
		exact.GetMaxBuf()[0] ^= 1
		require.Equal(t, before, []byte(data))
	}
}
