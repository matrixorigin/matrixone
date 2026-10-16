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

package ioutil

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJSONSubcolumnsVisibilityAndSelection(t *testing.T) {
	ctx := context.Background()
	fs, e := fileservice.NewMemoryFS("json-visibility", fileservice.DisabledCacheConfig, nil)
	require.NoError(t, e)
	defer fs.Close(ctx)
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) }()
	b := batch.NewWithSize(3)
	b.Vecs[0] = vector.NewVec(types.T_json.ToType())
	b.Vecs[1] = vector.NewVec(types.T_TS.ToType())
	b.Vecs[2] = vector.NewVec(types.T_bool.ToType())
	defer b.Clean(mp)
	for i := 0; i < 1000; i++ {
		j, e := bytejson.ParseFromStringExact(fmt.Sprintf(`{"id":%d,"cost":9007199254740993.001}`, i))
		require.NoError(t, e)
		require.NoError(t, vector.AppendByteJson(b.Vecs[0], j, i%19 == 0, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], types.BuildTS(int64(i), 0), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[2], i%13 == 0, false, mp))
	}
	b.SetRowCount(1000)
	w := ConstructWriter(0, []uint16{0, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT}, -1, false, false, fs)
	w.SetAppendable()
	_, e = w.WriteBatch(b)
	require.NoError(t, e)
	_, _, e = w.Sync(ctx)
	require.NoError(t, e)
	stats := w.GetObjectStats(objectio.WithAppendable())
	loc := stats.ObjectLocation()
	projection := objectio.WithJSONPathProjection(ctx, objectio.JSONPathProjection{0: {"$.id", "$.cost"}})
	out := vector.NewVec(types.T_json.ToType())
	defer out.Free(mp)
	snapshot := types.BuildTS(500, 0)
	mask, _, e := LoadColumnsDataInto(projection, []uint16{0}, []types.Type{types.T_json.ToType()}, fs, loc, []*vector.Vector{out}, nil, &snapshot, mp, fileservice.SkipAllCache)
	require.NoError(t, e)
	defer mask.Release()
	require.Equal(t, 1000, out.Length())
	for i := 0; i < 1000; i++ {
		require.Equal(t, i > 500 || i%13 == 0, mask.Contains(uint64(i)))
		require.Equal(t, i%19 == 0, out.IsNull(uint64(i)))
		if !out.IsNull(uint64(i)) {
			require.Equal(t, b.Vecs[0].GetBytesAt(i), out.GetBytesAt(i))
		}
	}
	selected := vector.NewVec(types.T_json.ToType())
	defer selected.Free(mp)
	sels := []int64{999, 0, 42, 13, 500}
	_, _, e = LoadColumnsDataInto(projection, []uint16{0}, []types.Type{types.T_json.ToType()}, fs, loc, []*vector.Vector{selected}, sels, nil, mp, fileservice.SkipAllCache)
	require.NoError(t, e)
	for i, row := range sels {
		require.Equal(t, b.Vecs[0].IsNull(uint64(row)), selected.IsNull(uint64(i)))
		if !selected.IsNull(uint64(i)) {
			require.Equal(t, b.Vecs[0].GetBytesAt(int(row)), selected.GetBytesAt(i))
		}
	}
}
