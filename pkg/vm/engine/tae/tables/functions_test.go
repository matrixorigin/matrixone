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

	// op := containers.MakeForeachVectorOp(vec2.GetType().Oid, dedupFunctions, vec, nil, nil)
	// containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op)

	b.Run("old-dedup", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			vec2.ForeachShallow(dedupClosure(vec, types.TS{}, nil, nil), nil)
		}
	})
	b.Run("new-dedup", func(b *testing.B) {
		op := containers.MakeForeachVectorOp(vec2.GetType().Oid, dedupFunctions, vec, nil, nil)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			containers.ForeachVectorWindow(vec2, 0, vec2.Length(), op)
		}
	})
}
