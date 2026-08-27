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

package lockservice

import (
	"context"
	"fmt"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"go.uber.org/zap/zapcore"
)

// BenchmarkLockUnlockWithoutConflict pins the full common transaction cycle.
// Both sides of an A/B comparison use this exact benchmark source so test-only
// logging and deadline changes cannot be mistaken for an implementation cost.
func BenchmarkLockUnlockWithoutConflict(b *testing.B) {
	runLockServiceTestsWithLevel(
		b,
		zapcore.ErrorLevel,
		[]string{"s1"},
		10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			b.StopTimer()
			service := services[0]
			ctx := context.Background()
			txnIDs := make([][]byte, b.N)
			rows := make([][]byte, b.N)
			for idx := range b.N {
				txnIDs[idx] = []byte(fmt.Sprintf("lock-unlock-bench-txn-%d", idx))
				rows[idx] = []byte(fmt.Sprintf("lock-unlock-bench-row-%d", idx))
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for idx := range b.N {
				if _, err := service.Lock(
					ctx,
					2670600,
					[][]byte{rows[idx]},
					txnIDs[idx],
					pb.LockOptions{},
				); err != nil {
					b.Fatal(err)
				}
				if err := service.Unlock(
					ctx,
					txnIDs[idx],
					timestamp.Timestamp{},
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		},
		nil,
	)
}

// BenchmarkUnlockWithoutConflict isolates the transaction-close half of the
// ordinary one-table path. Setup is outside the timer so admission, ledger
// cleanup and pooled-object reset regressions are not hidden by Lock work.
func BenchmarkUnlockWithoutConflict(b *testing.B) {
	runLockServiceTestsWithLevel(
		b,
		zapcore.ErrorLevel,
		[]string{"s1"},
		10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			b.StopTimer()
			service := services[0]
			ctx := context.Background()
			txnIDs := make([][]byte, b.N)
			for idx := range b.N {
				txnIDs[idx] = []byte(fmt.Sprintf("unlock-bench-txn-%d", idx))
				row := []byte(fmt.Sprintf("unlock-bench-row-%d", idx))
				if _, err := service.Lock(
					ctx,
					2670601,
					[][]byte{row},
					txnIDs[idx],
					newTestRowExclusiveOptions(),
				); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for _, txnID := range txnIDs {
				if err := service.Unlock(
					ctx,
					txnID,
					timestamp.Timestamp{},
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		},
		nil,
	)
}

// BenchmarkRemoteMultiTableUnlock isolates the regression shape from #27628:
// one transaction releases several physical tables on the same remote owner.
// The protocol gate provides an in-process A/B: v29 exercises the table-scoped
// fallback and v31 exercises the bounded batch without changing workload data.
func BenchmarkRemoteMultiTableUnlock(b *testing.B) {
	benchmarks := []struct {
		name    string
		version int64
	}{
		{name: "legacy-v29", version: defines.MORPCVersion29},
		{name: "batch-v31", version: defines.MORPCVersion31},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkRemoteMultiTableUnlock(b, benchmark.version)
		})
	}
}

func benchmarkRemoteMultiTableUnlock(b *testing.B, protocolVersion int64) {
	const (
		firstTable = uint64(2762800)
		tableCount = 8
	)
	runLockServiceTestsWithLevel(
		b,
		zapcore.ErrorLevel,
		[]string{"owner", "origin"},
		10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			b.StopTimer()
			rt := moruntime.ServiceRuntime("")
			if rt == nil {
				b.Fatal("missing service runtime")
			}
			oldVersion, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			if !ok {
				b.Fatal("missing protocol version")
			}
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, protocolVersion)
			defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)

			ctx := context.Background()
			owner := services[0]
			origin := services[1]
			options := newTestRowExclusiveOptions()

			// Pin all physical table generations to the same owner before the
			// measured transactions acquire them remotely.
			for offset := range tableCount {
				table := firstTable + uint64(offset)
				txnID := []byte(fmt.Sprintf("remote-unlock-seed-%d", offset))
				if _, err := owner.Lock(ctx, table, [][]byte{{0}}, txnID, options); err != nil {
					b.Fatal(err)
				}
				if err := owner.Unlock(ctx, txnID, timestamp.Timestamp{}); err != nil {
					b.Fatal(err)
				}
			}

			txnIDs := make([][]byte, b.N)
			for idx := range b.N {
				txnIDs[idx] = []byte(fmt.Sprintf("remote-unlock-bench-%d", idx))
				row := []byte(fmt.Sprintf("remote-unlock-row-%d", idx))
				for offset := range tableCount {
					if _, err := origin.Lock(
						ctx,
						firstTable+uint64(offset),
						[][]byte{row},
						txnIDs[idx],
						options,
					); err != nil {
						b.Fatal(err)
					}
				}
			}

			b.ReportAllocs()
			b.ReportMetric(tableCount, "tables/op")
			b.ResetTimer()
			b.StartTimer()
			for _, txnID := range txnIDs {
				if err := origin.Unlock(ctx, txnID, timestamp.Timestamp{}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		},
		nil,
	)
}

func BenchmarkLockWithoutConflict(b *testing.B) {
	runLockServiceTestsWithLevel(
		b,
		zapcore.ErrorLevel,
		[]string{"s1"},
		10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			b.StopTimer()
			service := services[0]
			ctx := context.Background()
			txnIDs := make([][]byte, b.N)
			rows := make([][]byte, b.N)
			for idx := range b.N {
				txnIDs[idx] = []byte(fmt.Sprintf("lock-bench-txn-%d", idx))
				rows[idx] = []byte(fmt.Sprintf("lock-bench-row-%d", idx))
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.StartTimer()
			for idx := range b.N {
				if _, err := service.Lock(
					ctx,
					2670602,
					[][]byte{rows[idx]},
					txnIDs[idx],
					newTestRowExclusiveOptions(),
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			for _, txnID := range txnIDs {
				if err := service.Unlock(
					ctx,
					txnID,
					timestamp.Timestamp{},
				); err != nil {
					b.Fatal(err)
				}
			}
		},
		nil,
	)
}

// BenchmarkExclusiveLockBudgetAcrossBatches measures the steady-state cost of
// a large Exclusive transaction after it first crosses the cumulative row-lock
// budget. Rows are intentionally delivered in sub-budget batches, matching the
// execution shape of bulk DML such as LOAD DATA.
func BenchmarkExclusiveLockBudgetAcrossBatches(b *testing.B) {
	const (
		table     = uint64(2670603)
		budget    = 128
		batchSize = 64
		batches   = 32
	)
	runLockServiceTestsWithLevel(
		b,
		zapcore.ErrorLevel,
		[]string{"s1"},
		10*time.Second,
		func(_ *lockTableAllocator, services []*service) {
			b.StopTimer()
			service := services[0]
			ctx := context.Background()
			rows := make([][][]byte, batches)
			for batch := range rows {
				rows[batch] = make([][]byte, batchSize)
				for row := range rows[batch] {
					rows[batch][row] = []byte(fmt.Sprintf(
						"bulk-row-%08d", batch*batchSize+row))
				}
			}
			txnIDs := make([][]byte, b.N)
			for idx := range txnIDs {
				txnIDs[idx] = []byte(fmt.Sprintf("bulk-lock-bench-txn-%d", idx))
			}

			b.ReportAllocs()
			b.ReportMetric(batchSize*batches, "rows/op")
			b.ResetTimer()
			b.StartTimer()
			for _, txnID := range txnIDs {
				for _, batch := range rows {
					if _, err := service.Lock(
						ctx,
						table,
						batch,
						txnID,
						newTestRowExclusiveOptions(),
					); err != nil {
						b.Fatal(err)
					}
				}
				if err := service.Unlock(
					ctx,
					txnID,
					timestamp.Timestamp{},
				); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		},
		func(c *Config) {
			c.MaxLockRowCount = budget
			c.MaxFixedSliceSize = 4096
		},
	)
}
