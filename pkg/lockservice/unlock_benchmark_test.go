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
