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

package spillutil

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// BenchmarkSpillReadMergedSmallRecords models the many small per-bucket records
// produced by a low-threshold shuffled join. One logical output batch requires
// decoding and merging 64 physical records.
func BenchmarkSpillReadMergedSmallRecords(b *testing.B) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()

	values := make([]int64, 128)
	for i := range values {
		values[i] = int64(i)
	}
	source := testutil.NewBatchWithVectors([]*vector.Vector{
		testutil.NewVector(
			len(values),
			types.T_int64.ToType(),
			proc.Mp(),
			false,
			values,
		),
	}, nil)
	defer source.Clean(proc.Mp())

	record := marshalTestSpillRecord(source)
	payload := make([]byte, 0, 64*len(record))
	for range 64 {
		payload = append(payload, record...)
	}
	path := filepath.Join(b.TempDir(), "small-records.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		b.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()

	state := newTestSpillAllocationAccount(b, 64<<20, 4_096)
	reader := BucketReader{
		fd:           file,
		mergeRecords: true,
		allocation:   state.allocation,
	}
	reuse := batch.NewOffHeapWithSize(0)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, err = file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if reader.reader != nil {
			if err = reader.reader.Reset(file); err != nil {
				b.Fatal(err)
			}
		}
		reader.headerPending = false
		decoded, err := reader.ReadBatch(proc, reuse)
		if err != nil {
			b.Fatal(err)
		}
		if decoded.RowCount() != 64*len(values) {
			b.Fatalf("decoded rows = %d", decoded.RowCount())
		}
	}
	b.StopTimer()
	reuse.Clean(proc.Mp())
	reader.Close()
	finalizeTestSpillAllocationAccount(b, state)
}
