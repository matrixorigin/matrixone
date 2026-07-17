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

package client

import (
	"context"
	"testing"

	commonruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"go.uber.org/zap/zapcore"
)

func BenchmarkTxnNewRollback(b *testing.B) {
	b.Run("serial", func(b *testing.B) {
		c := newBenchmarkTxnClient(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			op, err := c.New(ctx, timestamp.Timestamp{})
			if err != nil {
				b.Fatal(err)
			}
			if err := op.Rollback(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		c := newBenchmarkTxnClient(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				op, err := c.New(ctx, timestamp.Timestamp{})
				if err != nil {
					b.Fatal(err)
				}
				if err := op.Rollback(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkTxnNewAppendCallbackRollback(b *testing.B) {
	callback := NewTxnEventCallback(func(context.Context, TxnOperator, TxnEvent, any) error {
		return nil
	})
	b.Run("serial", func(b *testing.B) {
		c := newBenchmarkTxnClient(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			op, err := c.New(ctx, timestamp.Timestamp{})
			if err != nil {
				b.Fatal(err)
			}
			op.AppendEventCallback(ClosedEvent, callback)
			if err := op.Rollback(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("parallel", func(b *testing.B) {
		c := newBenchmarkTxnClient(b)
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				op, err := c.New(ctx, timestamp.Timestamp{})
				if err != nil {
					b.Fatal(err)
				}
				op.AppendEventCallback(ClosedEvent, callback)
				if err := op.Rollback(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkTxnNewAppendTraceCallbacksRollback(b *testing.B) {
	events := []EventType{
		WaitActiveEvent, UpdateSnapshotEvent, CommitEvent, RollbackEvent,
		CommitResponseEvent, CommitWaitApplyEvent, UnlockEvent, RangesEvent,
		BuildPlanEvent, ExecuteSQLEvent, CompileEvent, TableScanEvent,
		WorkspaceWriteEvent, WorkspaceAdjustEvent,
	}
	callback := NewTxnEventCallback(func(context.Context, TxnOperator, TxnEvent, any) error {
		return nil
	})
	c := newBenchmarkTxnClient(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op, err := c.New(ctx, timestamp.Timestamp{})
		if err != nil {
			b.Fatal(err)
		}
		for _, event := range events {
			op.AppendEventCallback(event, callback)
		}
		if err := op.Rollback(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchmarkTxnClient(b *testing.B) TxnClient {
	b.Helper()
	rt := commonruntime.NewRuntime(
		metadata.ServiceType_CN,
		"",
		logutil.GetPanicLoggerWithLevel(zapcore.ErrorLevel),
		commonruntime.WithClock(clock.NewHLCClock(func() int64 { return 1 }, 0)),
	)
	commonruntime.SetupServiceBasedRuntime("", rt)
	c := NewTxnClient("", newTestTxnSender())
	c.Resume()
	b.Cleanup(func() {
		if err := c.Close(); err != nil {
			b.Fatal(err)
		}
	})
	return c
}
