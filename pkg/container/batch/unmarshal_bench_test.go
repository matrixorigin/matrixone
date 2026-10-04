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

func BenchmarkBatchUnmarshalReceiverReuse(b *testing.B) {
	for _, columnCount := range []int{1, 8, 32, 64, 128} {
		b.Run(fmt.Sprintf("%d_columns", columnCount), func(b *testing.B) {
			mp := mpool.MustNewZero()
			source := NewWithSize(columnCount)
			for i := range source.Vecs {
				source.Vecs[i] = vector.NewVec(types.T_int64.ToType())
				if err := vector.AppendFixed(source.Vecs[i], int64(i), false, mp); err != nil {
					b.Fatal(err)
				}
			}
			source.SetRowCount(1)
			encoded, err := source.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			source.Clean(mp)

			target := new(Batch)
			if err := target.UnmarshalBinary(encoded); err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { target.Clean(nil) })

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := target.UnmarshalBinary(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
