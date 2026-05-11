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

package ioutil

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func BenchmarkHiddenColumnOverhead(b *testing.B) {
	benchmarkHiddenColumnOverheadRows(b, 8192)
}

func benchmarkHiddenColumnOverheadRows(b *testing.B, rows int) {
	b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
		for _, tc := range []struct {
			name string
			vec  *vector.Vector
		}{
			{name: "rowid", vec: buildBenchmarkRowIDVector(b, rows)},
			{name: "ts", vec: buildBenchmarkTSVector(b, rows)},
		} {
			columnData := containers.ToTNVector(tc.vec, common.DefaultAllocator)
			b.Run(tc.name+"/inspect", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					builder := NewObjectColumnMetasBuilder(1)
					builder.InspectVector(0, columnData, false)
				}
			})
			b.Run(tc.name+"/zm", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					zm := index.NewZM(tc.vec.GetType().Oid, tc.vec.GetType().Scale)
					if err := index.BatchUpdateZM(zm, columnData.GetDownstreamVector()); err != nil {
						b.Fatal(err)
					}
					index.SetZMSum(zm, columnData.GetDownstreamVector())
				}
			})
			b.Run(tc.name+"/full", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					builder := NewObjectColumnMetasBuilder(1)
					builder.InspectVector(0, columnData, false)
					zm := index.NewZM(tc.vec.GetType().Oid, tc.vec.GetType().Scale)
					if err := index.BatchUpdateZM(zm, columnData.GetDownstreamVector()); err != nil {
						b.Fatal(err)
					}
					index.SetZMSum(zm, columnData.GetDownstreamVector())
					builder.UpdateZm(0, zm)
					_, _ = builder.Build()
				}
			})
		}
	})
}

func buildBenchmarkRowIDVector(b *testing.B, rows int) *vector.Vector {
	b.Helper()
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_Rowid.ToType())
	var blk types.Blockid
	for i := 0; i < rows; i++ {
		rowid := types.NewRowid(&blk, uint32(i))
		if err := vector.AppendFixed(vec, rowid, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	return vec
}

func buildBenchmarkTSVector(b *testing.B, rows int) *vector.Vector {
	b.Helper()
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_TS.ToType())
	for i := 0; i < rows; i++ {
		ts := types.BuildTS(int64(i+1), 0)
		if err := vector.AppendFixed(vec, ts, false, mp); err != nil {
			b.Fatal(err)
		}
	}
	return vec
}
