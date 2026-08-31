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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	lateMaterializationBenchmarkRows         = 1024
	lateMaterializationBenchmarkPayloadBytes = 4096
	lateMaterializationBenchmarkSelectEvery  = 128
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
