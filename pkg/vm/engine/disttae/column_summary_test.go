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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestMaxAndMinColumnZoneMap(t *testing.T) {
	ctx := context.Background()
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, tc := range []struct {
		name   string
		values []bool
		nulls  []bool
		legacy bool
		min    bool
		max    bool
		inited bool
	}{
		{name: "new false", values: []bool{false, false}, inited: true},
		{name: "new true", values: []bool{true, true}, min: true, max: true, inited: true},
		{name: "new mixed", values: []bool{false, true}, max: true, inited: true},
		{name: "legacy mixed", values: []bool{false, true}, legacy: true, max: true, inited: true},
		{name: "nullable mixed", values: []bool{false, true}, nulls: []bool{true, false}, min: true, max: true, inited: true},
		{name: "all null", values: []bool{false, false}, nulls: []bool{true, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer := ioutil.ConstructWriter(0, []uint16{0, 1}, 0, false, false, fs)
			// 两个物理块验证非首块的 true 不会丢失，缓存引用必须逐块释放。
			for i, value := range tc.values {
				func() {
					bat := batch.NewWithSize(2)
					defer bat.Clean(mp)
					bat.Vecs[0] = vector.NewVec(types.T_bool.ToType())
					bat.Vecs[1] = vector.NewVec(types.T_int8.ToType())
					isNull := len(tc.nulls) > 0 && tc.nulls[i]
					require.NoError(t, vector.AppendFixed(bat.Vecs[0], value, isNull, mp))
					require.NoError(t, vector.AppendFixed(bat.Vecs[1], int8(7), false, mp))
					bat.SetRowCount(1)
					_, err := writer.WriteBatch(bat)
					require.NoError(t, err)
				}()
			}
			_, _, err := writer.Sync(ctx)
			require.NoError(t, err)
			stats := writer.GetObjectStats()
			location := stats.ObjectLocation()
			loaded, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
			require.NoError(t, err)
			private := objectio.ObjectMeta(bytes.Clone(loaded))
			meta := private.MustDataMeta()
			if tc.legacy {
				meta.MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				for i := uint32(0); i < meta.BlockCount(); i++ {
					meta.GetBlockMeta(i).MustGetColumn(0).SetZoneMap(index.BuildZM(types.T_bool, []byte{0}))
				}
			}
			before := bytes.Clone(private)
			zm, err := maxAndMinColumnZoneMap(ctx, fs, location, meta, 0, mp)
			require.NoError(t, err)
			require.Equal(t, tc.inited, zm.IsInited())
			if tc.inited {
				require.Equal(t, tc.min, types.DecodeBool(zm.GetMinBuf()))
				require.Equal(t, tc.max, types.DecodeBool(zm.GetMaxBuf()))
			}
			zm[0] ^= 1
			require.Equal(t, before, []byte(private), "不改写共享元数据或借出缓存向量")
			other, err := maxAndMinColumnZoneMap(ctx, fs, location, meta, 1, mp)
			require.NoError(t, err)
			require.Equal(t, int8(7), other.GetMin())
			require.Equal(t, int8(7), other.GetMax())
			other[0] ^= 1
			require.Equal(t, before, []byte(private))
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = maxAndMinColumnZoneMap(cancelled, fs, location, meta, 0, mp)
			require.Error(t, err)
		})
	}
}
