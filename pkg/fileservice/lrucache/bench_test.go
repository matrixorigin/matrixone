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

package lrucache

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// BenchmarkGetOrSet gets a random key of twice as many as fit, and sets it when absent.
func BenchmarkGetOrSet(b *testing.B) {
	const keys = 1 << 16
	paths := make([]string, keys)
	for i := range paths {
		paths[i] = "/cache/" + strconv.Itoa(i) + ".data"
	}
	c := NewWithPrepareEvict[string, struct{}](fscache.ConstCapacity(keys/2*4096), nil, nil, nil, nil)
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewPCG(rand.Uint64(), 0))
		for pb.Next() {
			k := paths[r.IntN(keys)]
			if _, ok := c.Get(ctx, k); !ok {
				c.Set(ctx, k, struct{}{}, 4096)
			}
		}
	})
}
