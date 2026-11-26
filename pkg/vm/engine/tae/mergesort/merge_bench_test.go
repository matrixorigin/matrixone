// Copyright 2021 Matrix Origin
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

package mergesort

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/mocks"
)

// mergeBenchInput is 4 appendable batches of 8192 rows with an int64 sort key
// that interleaves across the batches, an int64, a decimal128 and a varchar
// column. Every 10th row is deleted.
func mergeBenchInput() ([]*containers.Batch, []uint32) {
	const batches, rows = 4, 8192
	bats := make([]*containers.Batch, batches)
	kept := 0
	for b := range bats {
		bat := containers.NewBatch()
		key := containers.NewVector(types.T_int64.ToType())
		i64 := containers.NewVector(types.T_int64.ToType())
		dec := containers.NewVector(types.New(types.T_decimal128, 38, 2))
		str := containers.NewVector(types.T_varchar.ToType())
		for r := 0; r < rows; r++ {
			key.Append(int64(r*batches+b), false)
			i64.Append(int64(r), false)
			dec.Append(types.Decimal128{B0_63: uint64(r)}, false)
			str.Append([]byte(fmt.Sprintf("payload-%08d-%d", r, b)), false)
			if r%10 == 3 {
				bat.Delete(r)
			} else {
				kept++
			}
		}
		bat.AddVector("key", key)
		bat.AddVector("i64", i64)
		bat.AddVector("dec", dec)
		bat.AddVector("str", str)
		bats[b] = bat
	}
	var layout []uint32
	for left := kept; left > 0; left -= 8192 {
		layout = append(layout, uint32(min(left, 8192)))
	}
	return bats, layout
}

func BenchmarkMergeAObjGather(b *testing.B) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	bats, layout := mergeBenchInput()
	defer closeBatches(bats)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, release, _, err := MergeAObj(context.Background(), pool, bats, 0, layout)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}

func BenchmarkReshapeBatchesGather(b *testing.B) {
	pool := &testPool{pool: mocks.GetTestVectorPool()}
	bats, layout := mergeBenchInput()
	defer closeBatches(bats)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, release, _, err := ReshapeBatches(bats, layout, pool)
		if err != nil {
			b.Fatal(err)
		}
		release()
	}
}
