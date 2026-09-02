// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockio

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	lateMaterializationBenchmarkRows         = 1024
	lateMaterializationBenchmarkPayloadBytes = 4096
	lateMaterializationBenchmarkSelectEvery  = 128
	vectorRangeTopNBenchmarkRows             = 8192
	vectorRangeTopNBenchmarkDimensions       = 768
	vectorRangeTopNBenchmarkLimit            = 10
)

// BenchmarkBlockDataReadPersistedLateMaterialization measures the object-reader
// boundary used by IVF INCLUDE scans: an inexpensive predicate column plus a
// 4 KiB payload column. The low-selectivity case models the reported INCLUDE
// filter, while all-match is the control for an eligible predicate that rejects
// nothing. Both variants produce the same rows; payload-bytes/op records the
// logical payload materialized into the result, rather than filesystem cache I/O.
func BenchmarkBlockDataReadPersistedLateMaterialization(b *testing.B) {
	fixture := newLateMaterializationBenchmarkFixture(b)
	b.Cleanup(fixture.close)

	for _, benchmark := range []struct {
		name     string
		late     bool
		all      bool
		payloads int
	}{
		{
			name:     "eager_low_selectivity",
			payloads: lateMaterializationBenchmarkRows,
		},
		{
			name:     "late_low_selectivity",
			late:     true,
			payloads: lateMaterializationBenchmarkRows / lateMaterializationBenchmarkSelectEvery,
		},
		{
			name:     "eager_all_match",
			all:      true,
			payloads: lateMaterializationBenchmarkRows,
		},
		{
			name:     "late_all_match",
			late:     true,
			all:      true,
			payloads: lateMaterializationBenchmarkRows,
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(benchmark.payloads * lateMaterializationBenchmarkPayloadBytes))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := fixture.read(benchmark.late, benchmark.all)
				if err != nil {
					b.Fatal(err)
				}
				expected := lateMaterializationBenchmarkRows / lateMaterializationBenchmarkSelectEvery
				if benchmark.all {
					expected = lateMaterializationBenchmarkRows
				}
				if rows != expected {
					b.Fatalf("got %d rows, want %d", rows, expected)
				}
			}
			b.ReportMetric(
				float64(benchmark.payloads*lateMaterializationBenchmarkPayloadBytes),
				"payload-bytes/op",
			)
		})
	}
}

type lateMaterializationBenchmarkFixture struct {
	ctx      context.Context
	fs       fileservice.FileService
	info     objectio.BlockInfo
	columns  []uint16
	colTypes []types.Type
	ds       engine.DataSource
}

func newLateMaterializationBenchmarkFixture(b *testing.B) *lateMaterializationBenchmarkFixture {
	b.Helper()
	fs := testutil.NewSharedFS()
	mp := mpool.MustNewZero()
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	payload := makeBenchmarkPayload(lateMaterializationBenchmarkPayloadBytes)
	for row := 0; row < lateMaterializationBenchmarkRows; row++ {
		if err := vector.AppendFixed(input.Vecs[0], int32(row), false, mp); err != nil {
			b.Fatal(err)
		}
		payload[0] = byte(row)
		if err := vector.AppendBytes(input.Vecs[1], payload, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	input.SetRowCount(lateMaterializationBenchmarkRows)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	if _, err := writer.WriteBatch(input); err != nil {
		b.Fatal(err)
	}
	if _, _, err := writer.Sync(context.Background()); err != nil {
		b.Fatal(err)
	}
	stats := writer.GetObjectStats()
	input.Clean(mp)
	if bytes := mp.CurrNB(); bytes != 0 {
		b.Fatalf("writer mpool retained %d bytes", bytes)
	}
	mpool.DeleteMPool(mp)

	return &lateMaterializationBenchmarkFixture{
		ctx:      context.Background(),
		fs:       fs,
		info:     stats.ConstructBlockInfo(0),
		columns:  []uint16{0, 1},
		colTypes: []types.Type{types.T_int32.ToType(), types.T_text.ToType()},
		ds:       &blockReadTestDataSource{},
	}
}

func (f *lateMaterializationBenchmarkFixture) close() {}

func (f *lateMaterializationBenchmarkFixture) read(late, all bool) (int, error) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	output := batch.NewWithSize(len(f.columns))
	for i, typ := range f.colTypes {
		output.Vecs[i] = vector.NewOffHeapVecWithType(typ)
	}
	defer output.Clean(mp)
	cacheVectors := containers.NewVectors(len(f.columns) + 2)
	defer cacheVectors.Free(mp)

	filter := func(bat *batch.Batch, loaded []int) (engine.ReaderFilterResult, error) {
		if all {
			return engine.ReaderFilterResult{All: true}, nil
		}
		values := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
		sels := make([]int64, 0, len(values)/lateMaterializationBenchmarkSelectEvery)
		for row, value := range values {
			if value%lateMaterializationBenchmarkSelectEvery == 0 {
				sels = append(sels, int64(row))
			}
		}
		for _, pos := range loaded {
			bat.Vecs[pos].Shrink(sels, false)
		}
		bat.SetRowCount(len(sels))
		return engine.ReaderFilterResult{Sels: sels}, nil
	}

	if late {
		_, err := BlockDataReadWithFilter(
			f.ctx, &f.info, f.ds, f.columns, f.colTypes, -1, timestamp.Timestamp{},
			nil, nil, objectio.BlockReadFilter{}, fileservice.Policy(0),
			"ivfflat-include-benchmark", output, cacheVectors, mp, f.fs,
			[]int{0}, filter,
		)
		return output.RowCount(), err
	}
	err := BlockDataRead(
		f.ctx, &f.info, f.ds, f.columns, f.colTypes, -1, timestamp.Timestamp{},
		nil, nil, objectio.BlockReadFilter{}, nil, fileservice.Policy(0),
		"ivfflat-include-benchmark", output, cacheVectors, mp, f.fs,
	)
	if err != nil {
		return 0, err
	}
	if _, err = filter(output, []int{0, 1}); err != nil {
		return 0, err
	}
	return output.RowCount(), nil
}

func makeBenchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	state := uint32(1)
	for i := range payload {
		state = state*1664525 + 1013904223
		payload[i] = byte(state >> 24)
	}
	return payload
}

// BenchmarkBlockDataReadPersistedVectorRangeTopN measures the persisted-object
// boundary behind bounded IVF entry scans. The local fallback materializes the
// complete embedding column before computing Top-K; the storage path computes
// the same result while the cached embedding is pinned and returns only the
// selected scalar columns plus distances.
func BenchmarkBlockDataReadPersistedVectorRangeTopN(b *testing.B) {
	for _, benchmark := range []struct {
		name                   string
		storageTopK            bool
		membershipEvery        int
		materializedEmbeddings int
	}{
		{
			name:                   "local_full_vector_materialization",
			materializedEmbeddings: vectorRangeTopNBenchmarkRows,
		},
		{
			name:        "storage_bounded_topk",
			storageTopK: true,
		},
		{
			name:                   "local_exact_membership",
			membershipEvery:        8,
			materializedEmbeddings: vectorRangeTopNBenchmarkRows / 8,
		},
		{
			name:            "storage_exact_membership_topk",
			storageTopK:     true,
			membershipEvery: 8,
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			fixture := newVectorRangeTopNBenchmarkFixture(b)
			b.Cleanup(fixture.close)
			if err := fixture.read(benchmark.storageTopK, benchmark.membershipEvery); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := fixture.read(benchmark.storageTopK, benchmark.membershipEvery); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(
				float64(benchmark.materializedEmbeddings*vectorRangeTopNBenchmarkDimensions*4),
				"embedding-bytes/op",
			)
		})
	}
}

type vectorRangeTopNBenchmarkFixture struct {
	ctx         context.Context
	fs          fileservice.FileService
	info        objectio.BlockInfo
	columns     []uint16
	columnTypes []types.Type
	ds          engine.DataSource
}

func newVectorRangeTopNBenchmarkFixture(b *testing.B) *vectorRangeTopNBenchmarkFixture {
	b.Helper()
	ctx := context.Background()
	cacheCapacity := toml.ByteSize(64 << 20)
	fs, err := fileservice.NewLocalFS2(
		ctx,
		defines.SharedFileServiceName,
		b.TempDir(),
		fileservice.CacheConfig{MemoryCapacity: &cacheCapacity},
		nil,
	)
	if err != nil {
		b.Fatal(err)
	}
	fs.SetAsyncUpdate(false)

	mp := mpool.MustNewZero()
	vectorType := types.New(types.T_array_float32, vectorRangeTopNBenchmarkDimensions, 0)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.Vecs[1] = vector.NewVec(vectorType)
	entry := make([]float32, vectorRangeTopNBenchmarkDimensions)
	for row := 0; row < vectorRangeTopNBenchmarkRows; row++ {
		entry[0] = float32(row + 1)
		if err := vector.AppendFixed(input.Vecs[0], int64(row+1), false, mp); err != nil {
			b.Fatal(err)
		}
		if err := vector.AppendArray(input.Vecs[1], entry, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	input.SetRowCount(vectorRangeTopNBenchmarkRows)
	writer := ioutil.ConstructWriter(0, []uint16{0, 1}, -1, false, false, fs)
	if _, err := writer.WriteBatch(input); err != nil {
		b.Fatal(err)
	}
	if _, _, err := writer.Sync(ctx); err != nil {
		b.Fatal(err)
	}
	stats := writer.GetObjectStats()
	input.Clean(mp)
	if bytes := mp.CurrNB(); bytes != 0 {
		b.Fatalf("writer mpool retained %d bytes", bytes)
	}
	mpool.DeleteMPool(mp)

	return &vectorRangeTopNBenchmarkFixture{
		ctx:         ctx,
		fs:          fs,
		info:        stats.ConstructBlockInfo(0),
		columns:     []uint16{0, objectio.SEQNUM_ROWID, 1},
		columnTypes: []types.Type{types.T_int64.ToType(), objectio.RowidType, vectorType},
		ds:          &blockReadTestDataSource{},
	}
}

func (f *vectorRangeTopNBenchmarkFixture) close() {
	f.fs.Close(f.ctx)
}

func (f *vectorRangeTopNBenchmarkFixture) newTop() *objectio.IndexReaderTopOp {
	return &objectio.IndexReaderTopOp{
		ColPos:         2,
		Limit:          vectorRangeTopNBenchmarkLimit,
		Typ:            types.T_array_float32,
		NumVec:         types.ArrayToBytes(make([]float32, vectorRangeTopNBenchmarkDimensions)),
		MetricType:     metric.Metric_L2sqDistance,
		UpperBoundType: plan.BoundType_INCLUSIVE,
		UpperBound:     128 * 128,
	}
}

func (f *vectorRangeTopNBenchmarkFixture) read(storageTopK bool, membershipEvery int) (retErr error) {
	mp := mpool.MustNewZero()
	defer func() {
		if bytes := mp.CurrNB(); bytes != 0 && retErr == nil {
			retErr = moerr.NewInternalErrorNoCtxf("vector benchmark mpool retained %d bytes", bytes)
		}
		mpool.DeleteMPool(mp)
	}()
	output := batch.NewWithSize(len(f.columns))
	for i, typ := range f.columnTypes {
		output.Vecs[i] = vector.NewOffHeapVecWithType(typ)
	}
	defer output.Clean(mp)
	cacheVectors := containers.NewVectors(len(f.columns) + 1)
	defer cacheVectors.Free(mp)

	top := f.newTop()
	var selectRows []int64
	if membershipEvery > 0 {
		selectRows = make([]int64, 0, vectorRangeTopNBenchmarkRows/membershipEvery)
		for row := 0; row < vectorRangeTopNBenchmarkRows; row += membershipEvery {
			selectRows = append(selectRows, int64(row))
		}
	}
	if storageTopK {
		if err := BlockDataReadInner(
			f.ctx, &f.info, f.ds, f.columns, f.columnTypes, 1, types.TS{}, selectRows,
			top, fileservice.Policy(0), output, cacheVectors, mp, f.fs,
		); err != nil {
			return err
		}
		if output.Vecs[0].Length() != vectorRangeTopNBenchmarkLimit || output.Vecs[2].Length() != 0 {
			return moerr.NewInternalErrorNoCtxf(
				"storage vector benchmark returned %d rows and %d embeddings from %d block rows",
				output.Vecs[0].Length(), output.Vecs[2].Length(), f.info.MetaLocation().Rows(),
			)
		}
		return validateVectorRangeTopNBenchmarkResult(
			vector.MustFixedColWithTypeCheck[int64](output.Vecs[0]),
			vector.MustFixedColWithTypeCheck[float64](output.Vecs[len(f.columns)]),
			membershipEvery,
		)
	}

	if err := BlockDataReadInner(
		f.ctx, &f.info, f.ds, f.columns, f.columnTypes, 1, types.TS{}, selectRows,
		nil, fileservice.Policy(0), output, cacheVectors, mp, f.fs,
	); err != nil {
		return err
	}
	wantEmbeddings := vectorRangeTopNBenchmarkRows
	if membershipEvery > 0 {
		wantEmbeddings = len(selectRows)
	}
	if output.Vecs[2].Length() != wantEmbeddings {
		return moerr.NewInternalErrorNoCtxf(
			"local vector benchmark materialized %d embeddings, expected %d",
			output.Vecs[2].Length(), wantEmbeddings,
		)
	}
	rows, distances, err := objectio.TopNVector(f.ctx, nil, output.Vecs[2], top)
	if err != nil {
		return err
	}
	if len(rows) != vectorRangeTopNBenchmarkLimit || len(distances) != vectorRangeTopNBenchmarkLimit {
		return moerr.NewInternalErrorNoCtxf(
			"local vector benchmark returned %d rows and %d distances",
			len(rows), len(distances),
		)
	}
	ids := vector.MustFixedColWithTypeCheck[int64](output.Vecs[0])
	selectedIDs := make([]int64, len(rows))
	for i, row := range rows {
		selectedIDs[i] = ids[row]
	}
	return validateVectorRangeTopNBenchmarkResult(selectedIDs, distances, membershipEvery)
}

func validateVectorRangeTopNBenchmarkResult(ids []int64, distances []float64, membershipEvery int) error {
	if len(ids) != vectorRangeTopNBenchmarkLimit || len(distances) != vectorRangeTopNBenchmarkLimit {
		return moerr.NewInternalErrorNoCtxf(
			"vector benchmark result has %d ids and %d distances", len(ids), len(distances),
		)
	}
	for i := range ids {
		wantID := int64(i + 1)
		if membershipEvery > 0 {
			wantID = int64(i*membershipEvery + 1)
		}
		wantDistance := float64(wantID * wantID)
		if ids[i] != wantID || distances[i] != wantDistance {
			return moerr.NewInternalErrorNoCtxf(
				"vector benchmark result %d is (%d, %v), expected (%d, %v)",
				i, ids[i], distances[i], wantID, wantDistance,
			)
		}
	}
	return nil
}
