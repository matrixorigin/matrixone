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

//go:build !race

// Measurement-only allocation budget: race-runtime bookkeeping invalidates
// the numbers, so this file is excluded from race builds. The functional
// parse behavior itself is race-covered by the ordinary reader tests.

package external

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
)

// TestKafkaParseOneMessageAllocBudget is the regression gate for the
// per-message parse cost: a rebuilt 320 KiB parser per message (the shape
// this replaces) costs ~330 KB/op and would exceed this budget ~100x.
func TestKafkaParseOneMessageAllocBudget(t *testing.T) {
	r, _ := newBenchKafkaReader(t, sqlkafka.FormatCSV)
	msg := KafkaMsgMeta{Offset: 1, Value: "1,a"}
	ctx := context.Background()

	// warm up scratch buffers
	for i := 0; i < 10; i++ {
		_, err := r.parseOneMessage(ctx, r.csv.param, &msg)
		require.NoError(t, err)
	}

	allocs := testing.AllocsPerRun(200, func() {
		if _, err := r.parseOneMessage(ctx, r.csv.param, &msg); err != nil {
			t.Fatal(err)
		}
	})
	require.LessOrEqual(t, allocs, 8.0, "per-message parse must not construct parser state")

	const iters = 2000
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	for i := 0; i < iters; i++ {
		if _, err := r.parseOneMessage(ctx, r.csv.param, &msg); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	perOp := float64(after.TotalAlloc-before.TotalAlloc) / iters
	require.Less(t, perOp, 4096.0,
		"per-message parse of a 3-byte record must cost bytes proportional to the record, got %.0f B/op", perOp)
}
