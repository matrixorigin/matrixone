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

package disttae

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
)

func TestToPBEntryPreservesRepresentation(t *testing.T) {
	mp := mpool.MustNewZero()
	bat := batch.NewWithSize(4)
	batAttrs := []string{"fixed", "varlen", "const", "null"}
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(1), false, mp))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType(), mp)
	longValue := bytes.Repeat([]byte("v"), types.VarlenaInlineSize+17)
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], longValue, false, mp))
	bat.Vecs[2], _ = vector.NewConstFixed(types.T_int64.ToType(), int64(7), 1, mp)
	bat.Vecs[3] = vector.NewVec(types.T_int64.ToType(), mp)
	require.NoError(t, vector.AppendFixed(bat.Vecs[3], int64(0), true, mp))
	bat.SetRowCount(1)
	t.Cleanup(func() { bat.Clean(mp) })

	pb, err := toPBEntry(Entry{typ: ALTER, bat: bat, attrs: batAttrs})
	require.NoError(t, err)
	require.Equal(t, api.Entry_Alter, pb.EntryType)
	require.Equal(t, batAttrs, pb.Attrs)
	rows, err := batch.EncodedRowCount(pb.Bat)
	require.NoError(t, err)
	require.Equal(t, 1, rows)

	// The encoding is a copy: the sender may reuse its vectors.
	require.NotEmpty(t, bat.Vecs[1].GetArea())
	clear(bat.Vecs[0].GetData())
	clear(bat.Vecs[1].GetArea())

	view, err := batch.ViewBinary(pb.Bat)
	require.NoError(t, err)
	require.Len(t, view.Vecs, 4)
	require.Equal(t, 1, view.RowCount())
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](view.Vecs[0], 0))
	require.Equal(t, longValue, view.Vecs[1].GetBytesAt(0))
	require.True(t, view.Vecs[2].IsConst())
	require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](view.Vecs[2], 0))
	require.False(t, view.Vecs[3].IsConst())
	require.True(t, view.Vecs[3].IsNull(0))
}

func TestToPBEntryEncodesEmptyBatch(t *testing.T) {
	pb, err := toPBEntry(Entry{typ: ALTER, bat: new(batch.Batch)})
	require.NoError(t, err)
	require.Nil(t, pb.Attrs)
	view, err := batch.ViewBinary(pb.Bat)
	require.NoError(t, err)
	require.Empty(t, view.Vecs)
	require.Zero(t, view.RowCount())
}

func BenchmarkToPBEntry(b *testing.B) {
	for _, size := range []int{1, 2, 8, 16, 32} {
		bat := newDisttaeProtoBenchmarkBatch(size)
		b.Run(fmt.Sprintf("vectors=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				pb, err := toPBEntry(Entry{typ: ALTER, bat: bat})
				if err != nil {
					b.Fatal(err)
				}
				if len(pb.Bat) == 0 {
					b.Fatal("empty encoding")
				}
			}
		})
	}
}

func newDisttaeProtoBenchmarkBatch(size int) *batch.Batch {
	bat := batch.NewWithSize(size)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.NewVecWithData(types.T_int64.ToType(), 1, make([]byte, types.T_int64.ToType().TypeSize()), nil, mpool.MustNewZero())
	}
	bat.SetRowCount(1)
	return bat
}
