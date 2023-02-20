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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func BenchmarkPartitionInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {

		mp := mpool.MustNewZero()
		part := NewPartition(nil)
		ctx := context.Background()
		numRows := 100
		v0 := make([]int64, numRows)
		v1 := make([]string, numRows)
		v2 := make([]types.Rowid, numRows)
		v3 := make([]types.TS, numRows)
		zs := make([]int64, numRows)
		for i := 0; i < len(zs); i++ {
			zs[i] = 1
		}
		vecs := make([]*vector.Vector, 4)
		for i := 0; i < 1000*numRows; i += numRows {
			{
				for j := 0; j < numRows; j++ {
					v0[j] = int64(j + i)
					v1[j] = fmt.Sprintf("%v", j+i)
					x := uint64(i + j)
					copy(v2[j][:], types.EncodeUint64(&x))
					copy(v3[j][:], types.EncodeUint64(&x))
				}
			}
			vecs[0] = testutil.NewVector(1000, types.New(types.T_Rowid, 0, 0, 0), mp, false, v2)
			vecs[1] = testutil.NewVector(1000, types.New(types.T_TS, 0, 0, 0), mp, false, v3)
			vecs[2] = testutil.NewVector(1000, types.New(types.T_int64, 0, 0, 0), mp, false, v0)
			vecs[3] = testutil.NewVector(1000, types.New(types.T_varchar, 0, 0, 0), mp, false, v0)
			bat := testutil.NewBatchWithVectors(vecs, zs)
			bat.Attrs = []string{"0", "1", "2", "3"}
			ebat, _ := batch.BatchToProtoBatch(bat)
			if err := part.Insert(ctx, -1, ebat, false); err != nil {
				panic(err)
			}
			bat.Clean(mp)
		}

	}

}
