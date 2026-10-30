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

package batch

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// The control uses unique columns and applies the same selection directly,
// without Batch's duplicate-pointer and preparation checks.
func BenchmarkShrinkOwnedColumns(b *testing.B) {
	for _, columns := range []int{8, 128, 1024, 1025, 2048} {
		for _, direct := range []bool{false, true} {
			b.Run(fmt.Sprintf("columns=%d/direct=%t", columns, direct), func(b *testing.B) {
				mp := mpool.MustNewZero()
				bat := NewWithSize(columns)
				for i := range bat.Vecs {
					bat.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
					if err := vector.AppendFixedList(bat.Vecs[i], []int64{1, 2, 3}, nil, mp); err != nil {
						b.Fatal(err)
					}
				}
				b.Cleanup(func() { bat.Clean(mp); mpool.DeleteMPool(mp) })
				sels := []int64{1, 2}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, vec := range bat.Vecs {
						vec.SetLength(3)
					}
					bat.SetRowCount(3)
					if direct {
						for _, vec := range bat.Vecs {
							if err := vec.Shrink(sels, false, mp); err != nil {
								b.Fatal(err)
							}
						}
					} else if err := bat.Shrink(sels, false, mp); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
