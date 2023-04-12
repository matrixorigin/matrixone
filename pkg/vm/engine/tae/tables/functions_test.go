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

package tables

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func BenchmarkFunctions(b *testing.B) {
	vec := containers.MockVector2(types.T_int64.ToType(), 10000, 0)
	defer vec.Close()
	vec2 := containers.MakeVector(vec.GetType())
	defer vec2.Close()
	for i := 9999999; i < 9999999+1000; i++ {
		vec2.Append(int64(i))
	}

	// op := containers.MakeForeachVectorOp(vec2.GetType().Oid, dedupNABlkFunctions, vec, nil, nil)
	// containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op)

	b.Run("old-dedup-int64", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec2.ForeachShallow(dedupNABlkClosure(vec, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-int64", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec2.GetType().Oid, dedupNABlkFunctions, vec, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op, nil)
		}
	})

	vec3 := containers.MockVector(types.T_decimal128.ToType(), 20000, true, nil)
	defer vec3.Close()
	vec4 := vec3.CloneWindow(0, 10000)
	defer vec4.Close()
	vec5 := vec3.CloneWindow(11000, 100)
	defer vec5.Close()
	b.Run("old-dedup-d128", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec5.ForeachShallow(dedupNABlkClosure(vec4, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-d128", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec4.GetType().Oid, dedupNABlkFunctions, vec4, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec5, 0, vec5.Length(), op, nil)
		}
	})

	vec6 := containers.MockVector2(types.T_varchar.ToType(), 12000, 0)
	defer vec6.Close()
	vec7 := vec6.CloneWindow(0, 10000)
	defer vec7.Close()
	vec8 := vec6.CloneWindow(10500, 10)
	defer vec8.Close()

	b.Run("old-dedup-chars", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec8.ForeachShallow(dedupNABlkClosure(vec7, nil, nil, nil), nil)
		}
	})
	b.Run("new-dedup-chars", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, dedupNABlkFunctions, vec7, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil)
		}
	})

	b.Run("old-dedup-achars", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec8.ForeachShallow(dedupABlkClosureFactory(nil)(vec7, nil, nil, nil), nil)
		}
	})

	// op := containers.MakeForeachVectorOp(vec7.GetType().Oid, dedupAlkFunctions, vec7, nil, nil, nil, types.TS{})
	// containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op)
	b.Run("new-dedup-achars", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec7.GetType().Oid, dedupAlkFunctions, vec7, nil, nil, nil, types.TS{})
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec8, 0, vec8.Length(), op, nil)
		}
	})
}
