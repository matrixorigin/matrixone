// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package disttae

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func replayTestBatch(t *testing.T, mp *mpool.MPool, ids ...uint64) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(7)
	for i, typ := range []types.T{types.T_uint64, types.T_uint64, types.T_uint32, types.T_uint64, types.T_varchar, types.T_uint64, types.T_varchar} {
		b.Vecs[i] = vector.NewVec(typ.ToType())
	}
	for _, id := range ids {
		require.NoError(t, vector.AppendFixed(b.Vecs[0], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], id, false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[2], uint32(1), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[3], uint64(1), false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[4], []byte("db"), false, mp))
		require.NoError(t, vector.AppendFixed(b.Vecs[5], id, false, mp))
		require.NoError(t, vector.AppendBytes(b.Vecs[6], []byte{byte('a' + id)}, false, mp))
	}
	b.SetRowCount(len(ids))
	return b
}
func TestConsumeReplayColumns(t *testing.T) {
	t.Run("split and empty", func(t *testing.T) {
		mp := mpool.MustNewZero()
		bs := []*batch.Batch{replayTestBatch(t, mp, 1, 2), replayTestBatch(t, mp), replayTestBatch(t, mp, 2), replayTestBatch(t, mp, 2, 3, 4)}
		var groups [][]uint64
		err := consumeReplayColumns(context.Background(), bs, mp, types.TS{}, func(b *batch.Batch) {
			var got []uint64
			for i := 0; i < b.RowCount(); i++ {
				got = append(got, replayKey(b, i, 2).table)
			}
			groups = append(groups, got)
		})
		require.NoError(t, err)
		require.Equal(t, [][]uint64{{1}, {2, 2, 2}, {3, 4}}, groups)
		require.Zero(t, mp.CurrNB())
	})
	for _, position := range []int{0, 1, 2} {
		t.Run(string(rune('0'+position)), func(t *testing.T) {
			mp := mpool.MustNewZero()
			bs := make([]*batch.Batch, 2, 3)
			bs[0], bs[1] = replayTestBatch(t, mp, 1), replayTestBatch(t, mp, 2)
			bs = append(bs, nil)
			copy(bs[position+1:], bs[position:])
			bs[position] = nil
			err := consumeReplayColumns(context.Background(), bs, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
			require.Error(t, err)
			require.Zero(t, mp.CurrNB())
		})
	}
	t.Run("order", func(t *testing.T) {
		mp := mpool.MustNewZero()
		err := consumeReplayColumns(context.Background(), []*batch.Batch{replayTestBatch(t, mp, 2, 1)}, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
	})
	t.Run("cancel", func(t *testing.T) {
		mp := mpool.MustNewZero()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := consumeReplayColumns(ctx, []*batch.Batch{replayTestBatch(t, mp, 1)}, mp, types.TS{}, func(*batch.Batch) { t.Fatal("unexpected insert") })
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, mp.CurrNB())
	})
}
