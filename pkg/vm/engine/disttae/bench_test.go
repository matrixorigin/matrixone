// Copyright 2022 Matrix Origin
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
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkPartitionInsert(b *testing.B) {
	ctx := context.Background()
	for _, numRows := range []int{
		1,
		128,
		4096,
	} {
		b.Run(fmt.Sprintf("num-rows-%v", numRows), func(b *testing.B) {

			mp := mpool.MustNewZero()
			partition := NewPartition(nil, false)
			v0 := make([]types.Rowid, numRows)
			v1 := make([]types.TS, numRows)
			v2 := make([]int64, numRows)
			v3 := make([]string, numRows)
			zs := make([]int64, numRows)
			nums := rand.Perm(numRows)
			for i := 0; i < numRows; i++ {
				buf := make([]byte, 16)
				binary.PutVarint(buf, int64(i))

				var rowID types.Rowid
				copy(rowID[:], buf)
				v0[i] = rowID

				var ts types.TS
				copy(ts[:], buf)
				v1[i] = ts

				v2[i] = int64(nums[i])
				v3[i] = fmt.Sprintf("%v", nums[i])
			}
			for i := 0; i < len(zs); i++ {
				zs[i] = 1
			}
			vecs := make([]*vector.Vector, 4)
			vecs[0] = testutil.NewVector(numRows, types.T_Rowid.ToType(), mp, false, v0)
			vecs[1] = testutil.NewVector(numRows, types.T_TS.ToType(), mp, false, v1)
			vecs[2] = testutil.NewVector(numRows, types.T_int64.ToType(), mp, false, v2)
			vecs[3] = testutil.NewVector(numRows, types.T_varchar.ToType(), mp, false, v3)
			bat := testutil.NewBatchWithVectors(vecs, zs)
			defer bat.Clean(mp)
			bat.Attrs = []string{"0", "1", "2", "3"}
			protoBatch, err := batch.BatchToProtoBatch(bat)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := partition.Insert(ctx, -1, protoBatch, false); err != nil {
					b.Fatal(err)
				}
			}

		})
	}

}
