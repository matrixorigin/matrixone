// Copyright 2026 Matrix Origin
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

package python

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func benchmarkInputVector(b *testing.B, typ types.Type, rows int) (*vector.Vector, *mpool.MPool) {
	b.Helper()
	mp := mpool.MustNewZeroNoFixed()
	input := vector.NewVec(typ)
	for row := 0; row < rows; row++ {
		var err error
		switch typ.Oid {
		case types.T_int64:
			err = vector.AppendFixed(input, int64(row), false, mp)
		case types.T_varchar:
			err = vector.AppendBytes(input, []byte("matrixone-python-udf"), false, mp)
		default:
			b.Fatalf("unsupported benchmark type %s", typ)
		}
		if err != nil {
			input.Free(mp)
			mpool.DeleteMPool(mp)
			b.Fatal(err)
		}
	}
	return input, mp
}

func BenchmarkEncodeInputBatchFixedWidth(b *testing.B) {
	benchmarkEncodeInputBatch(b, types.T_int64.ToType())
}

func BenchmarkEncodeInputBatchFixedWidthRebuildBaseline(b *testing.B) {
	const rows = 8192
	input, mp := benchmarkInputVector(b, types.T_int64.ToType(), rows)
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	encoder, err := newInputBatchEncoder([]*vector.Vector{input}, []types.Type{types.T_int64.ToType()})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = encoder.close() }()

	b.ReportAllocs()
	b.SetBytes(int64(rows))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := encodeInputBatchRebuildBaseline(
			encoder,
			0, rows, DefaultMaxBatchBytes, rows,
		)
		if err != nil {
			b.Fatal(err)
		}
		if batch.Rows != rows || len(batch.Frames) != 2 {
			b.Fatalf("unexpected encoded batch rows=%d frames=%d", batch.Rows, len(batch.Frames))
		}
	}
}

func BenchmarkEncodeInputBatchVariableWidth(b *testing.B) {
	benchmarkEncodeInputBatch(b, types.New(types.T_varchar, 64, 0))
}

func BenchmarkEncodeInputBatchVariableWidthRebuildBaseline(b *testing.B) {
	const rows = 8192
	typ := types.New(types.T_varchar, 64, 0)
	input, mp := benchmarkInputVector(b, typ, rows)
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	encoder, err := newInputBatchEncoder([]*vector.Vector{input}, []types.Type{typ})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = encoder.close() }()

	b.ReportAllocs()
	b.SetBytes(int64(rows))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := encodeInputBatchRebuildBaseline(
			encoder,
			0, rows, DefaultMaxBatchBytes, rows,
		)
		if err != nil {
			b.Fatal(err)
		}
		if batch.Rows != rows || len(batch.Frames) != 2 {
			b.Fatalf("unexpected encoded batch rows=%d frames=%d", batch.Rows, len(batch.Frames))
		}
	}
}

func benchmarkEncodeInputBatch(b *testing.B, typ types.Type) {
	const rows = 8192
	input, mp := benchmarkInputVector(b, typ, rows)
	defer func() {
		input.Free(mp)
		mpool.DeleteMPool(mp)
	}()
	encoder, err := newInputBatchEncoder([]*vector.Vector{input}, []types.Type{typ})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = encoder.close() }()

	b.ReportAllocs()
	b.SetBytes(int64(rows))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := encodeInputBatchWithEncoder(
			encoder, 0, rows,
			DefaultMaxBatchBytes, rows,
		)
		if err != nil {
			b.Fatal(err)
		}
		if batch.Rows != rows || len(batch.Frames) != 2 {
			b.Fatalf("unexpected encoded batch rows=%d frames=%d", batch.Rows, len(batch.Frames))
		}
	}
}

// encodeInputBatchRebuildBaseline is a benchmark-only reference for repeated
// Arrow record construction. It intentionally rebuilds the record and its
// input encoder for every size probe while sharing the reusable Flight writer
// with production. This measures record construction and probe avoidance
// without counting repeated schema serialization as a second optimization.
func encodeInputBatchRebuildBaseline(
	encoder *inputBatchEncoder,
	start, remaining, maxBytes, maxRows int64,
) (encodedRecordBatch, error) {
	if remaining > maxRows {
		remaining = maxRows
	}
	tryEncode := func(rows int64) ([]ArrowFrame, error) {
		record, _, err := BuildInputRecordRange(encoder.inputs, encoder.args, int(start), int(rows))
		if err != nil {
			return nil, err
		}
		frames, encodeErr := encoder.encode(record, maxBytes)
		record.Release()
		return frames, encodeErr
	}
	return chooseInputBatch(remaining, maxBytes, tryEncode)
}
