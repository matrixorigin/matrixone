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

package blockio

import (
	"context"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/stretchr/testify/require"
)

func BenchmarkBlockDataReadInnerDeletes1(b *testing.B)  { benchBlockReadWithDeletes(b, 0.01) }
func BenchmarkBlockDataReadInnerDeletes10(b *testing.B) { benchBlockReadWithDeletes(b, 0.10) }

// benchBlockReadWithDeletes reads a full 8192-row block of four int64 columns
// whose tombstones delete a fraction p of the rows, as a scan does.
func benchBlockReadWithDeletes(b *testing.B, p float64) {
	const rows = 8192
	const ncols = 4
	ctx := context.Background()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	seqnums := make([]uint16, ncols)
	colTypes := make([]types.Type, ncols)
	input := batch.NewWithSize(ncols)
	for c := range ncols {
		seqnums[c] = uint16(c)
		colTypes[c] = types.T_int64.ToType()
		input.Vecs[c] = vector.NewVec(colTypes[c], mp)
		for r := range rows {
			require.NoError(b, vector.AppendFixed(input.Vecs[c], int64(r*ncols+c), false, mp))
		}
	}
	input.SetRowCount(rows)
	writer := ioutil.ConstructWriter(0, seqnums, -1, false, false, fs)
	_, err := writer.WriteBatch(input)
	require.NoError(b, err)
	_, _, err = writer.Sync(ctx)
	require.NoError(b, err)
	stats := writer.GetObjectStats()
	info := stats.ConstructBlockInfo(0)
	input.Clean(mp)

	rng := rand.New(rand.NewSource(1))
	var deleted []uint64
	for r := range rows {
		if rng.Float64() < p {
			deleted = append(deleted, uint64(r))
		}
	}
	ds := &blockReadTestDataSource{deleted: deleted}

	output := batch.NewWithSize(ncols)
	for c := range ncols {
		output.Vecs[c] = vector.NewVec(colTypes[c], mp)
	}
	cacheVectors := containers.NewVectors(ncols)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, vec := range output.Vecs {
			vec.CleanOnlyData()
		}
		if err := BlockDataReadInner(
			ctx, &info, ds, seqnums, colTypes, -1, types.TS{}, nil, nil,
			fileservice.Policy(0), output, cacheVectors, mp, fs,
		); err != nil {
			b.Fatal(err)
		}
		if output.Vecs[0].Length() != rows-len(deleted) {
			b.Fatalf("got %d rows", output.Vecs[0].Length())
		}
	}
	b.StopTimer()
	cacheVectors.Free(mp)
	for _, vec := range output.Vecs {
		vec.Free(mp)
	}
}
