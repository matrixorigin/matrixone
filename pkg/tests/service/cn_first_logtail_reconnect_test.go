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

package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestCNFirstReplayDisconnect(t *testing.T) {
	cluster, err := NewCluster(context.Background(), t, DefaultOptions().WithRootDataDir(t.TempDir()))
	require.NoError(t, err)
	c := cluster.(*testCluster)
	var receiverPaused atomic.Bool
	previousReporter := logutil.GetReportZapFunc()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Message == "logtail.consumer.receiver.paused" {
			receiverPaused.Store(true)
		}
		return previousReporter(enc, entry, fields)
	}})
	logutil.SetupMOLogger(&logutil.LogConfig{Level: "info", Format: "json", DisableLog: true})
	t.Cleanup(func() {
		logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReporter})
		logutil.SetupMOLogger(&logutil.LogConfig{Level: "fatal", Format: "console"})
	})
	t.Cleanup(func() { require.NoError(t, c.Close()) })

	// Use the normal fixture's startup stages, but do not enter bootstrap's
	// five-minute wait until the logtail recovery oracle has passed.
	require.NoError(t, c.acquireAdmissionLocked())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	require.NoError(t, c.startLogServices(ctx))
	require.NoError(t, c.startTNServices(ctx))
	if fault.Enable() {
		t.Cleanup(func() { fault.Disable() })
	}
	addFault := func(name, action string, arg int64, target string) {
		require.NoError(t, fault.AddFaultPoint(ctx, name, ":::", action, arg, target, false))
		t.Cleanup(func() {
			_, err := fault.RemoveFaultPoint(context.Background(), name)
			require.NoError(t, err)
		})
	}
	addFault(objectio.FJ_CNReplayCacheErr, "wait", 0, "")
	const waiters = "first-replay-test-waiters"
	addFault(waiters, "getwaiters", 0, objectio.FJ_CNReplayCacheErr)
	c.initCNServices(c.fileservices, 0)
	require.Eventually(t, func() bool {
		n, _, ok := fault.TriggerFault(waiters)
		return ok && n == 1
	}, 15*time.Second, 10*time.Millisecond, "CN must reach its first catalog replay")
	addFault(objectio.FJ_CNRecvErr, "echo", 100000, "")
	require.Eventually(t, receiverPaused.Load, 15*time.Second, 10*time.Millisecond,
		"receiver must pause before replay completes")
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNRecvErr)
	require.NoError(t, err)
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNReplayCacheErr)
	require.NoError(t, err)

	cn := c.cn.svcs[0].(*cnService)
	owner := cn.svc
	fence, _ := owner.GetClock().Now()
	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 30*time.Second)
	defer recoveryCancel()
	_, err = owner.GetTimestampWaiter().GetTimestamp(recoveryCtx, fence)
	require.NoError(t, err, "a fresh TN timestamp must arrive after the first-replay disconnect")
	require.NoError(t, cn.Start())
	exec := owner.GetSQLExecutor()
	for _, sql := range []string{
		"create database first_replay",
		"create table first_replay.t (id int primary key)",
		"insert into first_replay.t values (1), (2)",
	} {
		result, err := exec.Exec(ctx, sql, executor.Options{}.WithWaitCommittedLogApplied())
		require.NoError(t, err)
		result.Close()
	}
	result, err := exec.Exec(ctx, "select id from first_replay.t order by id", executor.Options{})
	require.NoError(t, err)
	defer result.Close()
	var ids []int32
	result.ReadRows(func(_ int, cols []*vector.Vector) bool {
		ids = append(ids, executor.GetFixedRows[int32](cols[0])...)
		return true
	})
	require.Equal(t, []int32{1, 2}, ids)
}
