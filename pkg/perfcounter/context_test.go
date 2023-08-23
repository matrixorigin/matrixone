// Copyright 2023 Matrix Origin
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

package perfcounter

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithCounterSetFrom(t *testing.T) {
	var c1, c2 CounterSet
	ctx := WithCounterSet(context.Background(), &c1, &c2)
	ctx2 := WithCounterSetFrom(context.Background(), ctx)
	Update(ctx2, func(set *CounterSet) {
		set.DistTAE.Logtail.Entries.Add(1)
	})
	assert.Equal(t, int64(1), c1.DistTAE.Logtail.Entries.Load())
}

func makeCounterSetArray(cnt int) []*CounterSet {
	var s []*CounterSet

	for i := 0; i < cnt; i++ {
		s = append(s, new(CounterSet))
	}

	return s
}

func BenchmarkWithCounterSetNonNested(b *testing.B) {
	sets := makeCounterSetArray(100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < b.N; i++ {
		WithCounterSet(ctx, sets...)
	}
}

func BenchmarkWithCounterSetNested(b *testing.B) {
	sets := makeCounterSetArray(100)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < b.N; i++ {
		ctx = WithCounterSet(ctx, sets...)
	}
}

func TestAddDuplicateCounterSetToContext(t *testing.T) {
	cs1 := new(CounterSet)
	cs2 := new(CounterSet)

	counterSets := CounterSets{}
	counterSets[cs1] = struct{}{}
	counterSets[cs2] = struct{}{}

	ctx1 := context.WithValue(context.Background(), CtxKeyCounters, counterSets)

	// storing an existing counter set into ctx
	ctx2 := WithCounterSet(ctx1, cs1)
	require.Equal(t, ctx1, ctx2)

	// storing a new counter set into ctx
	cs3 := new(CounterSet)
	ctx3 := WithCounterSet(ctx1, cs3)
	require.NotEqual(t, ctx1, ctx3)
}
