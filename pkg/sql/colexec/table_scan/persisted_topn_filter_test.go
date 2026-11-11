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

package table_scan

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

// Only tombstone access is used by this persisted BlockIO fixture.
type topNFilterDataSource struct{ engine.DataSource }

func (*topNFilterDataSource) GetTombstones(context.Context, *objectio.Blockid) (objectio.Bitmap, error) {
	return objectio.GetReusableBitmap(), nil
}

func (*topNFilterDataSource) ApplyTombstones(_ context.Context, _ *objectio.Blockid, rows []int64, _ engine.TombstoneApplyPolicy) ([]int64, error) {
	return rows, nil
}

func TestPersistedTopNReaderFilter(t *testing.T) {
	ctx := context.Background()
	capacity := toml.ByteSize(8 << 20)
	fs, err := fileservice.NewLocalFS2(ctx, defines.SharedFileServiceName, t.TempDir(),
		fileservice.CacheConfig{MemoryCapacity: &capacity}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { fs.Close(ctx) })
	fs.SetAsyncUpdate(false)
	colTypes := []types.Type{types.T_int32.ToType(), types.T_array_float32.ToType(), types.T_varchar.ToType()}
	info := func() objectio.BlockInfo {
		mp := mpool.MustNewZero()
		defer mpool.DeleteMPool(mp)
		input := batch.NewWithSize(len(colTypes))
		defer input.Clean(mp)
		for i, typ := range colTypes {
			input.Vecs[i] = vector.NewVec(typ)
		}
		for row := 0; row < 8; row++ {
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(100+row), false, mp))
			require.NoError(t, vector.AppendBytes(input.Vecs[1], types.ArrayToBytes([]float32{float32(row), 0}), false, mp))
			require.NoError(t, vector.AppendBytes(input.Vecs[2], []byte(fmt.Sprintf("payload-%d", row)), false, mp))
		}
		input.SetRowCount(8)
		writer := ioutil.ConstructWriter(0, []uint16{0, 1, 2}, -1, false, false, fs)
		_, err := writer.WriteBatch(input)
		require.NoError(t, err)
		_, _, err = writer.Sync(ctx)
		require.NoError(t, err)
		stats := writer.GetObjectStats()
		return stats.ConstructBlockInfo(0)
	}()

	for _, tc := range []struct {
		name    string
		topN    bool
		filters []*pbplan.Expr
		want    []int32
	}{
		{"eager control", false, []*pbplan.Expr{lateTestCompare(t, "=", 0, 106)}, []int32{106}},
		{"unmaterialized embedding", true, []*pbplan.Expr{lateTestCompare(t, "=", 0, 106)}, []int32{106}},
		{"successive filters", true, []*pbplan.Expr{lateTestCompare(t, ">", 0, 104), lateTestCompare(t, "<", 0, 107)}, []int32{105, 106}},
		{"all rows survive", true, []*pbplan.Expr{lateTestCompare(t, ">", 0, 99)}, []int32{100, 101, 102, 103, 104, 105, 106, 107}},
		{"no rows survive", true, []*pbplan.Expr{lateTestCompare(t, ">", 0, 200)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()) })
			t.Cleanup(proc.Free)
			txnOperator := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
			txnOperator.EXPECT().Txn().Return(txn.TxnMeta{}).AnyTimes()
			proc.Base.TxnOperator = txnOperator
			out := batch.NewWithSize(len(colTypes))
			t.Cleanup(func() { out.Clean(proc.Mp()) })
			for i, typ := range colTypes {
				out.Vecs[i] = vector.NewOffHeapVecWithType(typ)
			}
			cache := containers.NewVectors(len(colTypes) + 1)
			t.Cleanup(func() { cache.Free(proc.Mp()) })
			var top *objectio.IndexReaderTopOp
			if tc.topN {
				top = &objectio.IndexReaderTopOp{ColPos: 1, Limit: 8, Typ: types.T_array_float32,
					NumVec: types.ArrayToBytes([]float32{0, 0}), MetricType: metric.Metric_L2Distance,
					DistHeap: make(objectio.Float64Heap, 0, 8)}
			}
			require.NoError(t, blockio.BlockDataReadInner(ctx, &info, &topNFilterDataSource{},
				[]uint16{0, 1, 2}, colTypes, -1, types.TS{}, nil, top, fileservice.Policy(0),
				out, cache, proc.Mp(), fs))
			out.SetRowCount(out.Vecs[0].Length()) // same publication as blockDataRead
			require.Equal(t, 8, out.RowCount())
			if tc.topN {
				require.Zero(t, out.Vecs[1].Length(), "real TopN producer leaves the embedding unmaterialized")
				require.Len(t, out.Vecs, len(colTypes)+1)
			} else {
				require.Equal(t, 8, out.Vecs[1].Length())
			}
			scan := &TableScan{FilterExprs: tc.filters}
			t.Cleanup(func() { scan.Free(proc, false, nil) })
			require.NoError(t, scan.Prepare(proc))
			_, err := scan.applyReaderFilter(proc, out, nil)
			require.NoError(t, err)
			require.Equal(t, len(tc.want), out.RowCount())
			if len(tc.want) == 0 {
				return
			}
			require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[int32](out.Vecs[0]))
			require.Equal(t, len(tc.want), out.Vecs[2].Length())
			for i, id := range tc.want {
				row := id - 100
				require.Equal(t, fmt.Sprintf("payload-%d", row), out.Vecs[2].GetStringAt(i))
				if tc.topN {
					require.Zero(t, out.Vecs[1].Length())
					require.Equal(t, len(tc.want), out.Vecs[3].Length())
					require.Equal(t, float64(row*row), vector.MustFixedColWithTypeCheck[float64](out.Vecs[3])[i])
				} else {
					require.Equal(t, types.ArrayToBytes([]float32{float32(row), 0}), out.Vecs[1].GetBytesAt(i))
				}
			}
		})
	}
}
