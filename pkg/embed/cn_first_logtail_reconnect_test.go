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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestCNFirstReplayDisconnect(t *testing.T) {
	const child = "MO_TEST_FIRST_REPLAY_DISCONNECT_CHILD"
	const childRoot = "MO_TEST_FIRST_REPLAY_DISCONNECT_ROOT"
	if os.Getenv(child) != "1" {
		// Replay/receive fault points are process-wide. Keep them away from the
		// package's shared basicCluster, including during full-package runs.
		dir := t.TempDir()
		log, err := os.Create(filepath.Join(dir, "child.log"))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, log.Close()) })
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 160*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCNFirstReplayDisconnect$", "-test.count=1", "-test.v", "-test.timeout=150s")
		cmd.Env = append(os.Environ(), child+"=1", childRoot+"="+dir)
		cmd.Stdout, cmd.Stderr = log, log
		err = cmd.Run()
		if err != nil {
			if info, statErr := log.Stat(); statErr == nil {
				length := min(info.Size(), 64<<10)
				tail := make([]byte, int(length))
				_, _ = log.ReadAt(tail, info.Size()-length)
				t.Logf("isolated first-replay child log tail:\n%s", tail)
			}
		}
		require.NoError(t, ctx.Err(), "isolated first-replay child exceeded its deadline")
		require.NoError(t, err, "isolated first-replay child failed")
		t.Log("isolated child verified replay recovery, fresh TN timestamp, and prepared SQL results [1,2] then [2]")
		return
	}
	dataPath := os.Getenv(childRoot)
	require.NotEmpty(t, dataPath)
	c, err := NewCluster(WithTesting(), WithDataPath(dataPath), WithCNCount(1))
	started := make(chan struct{})
	var startupErr error
	startupLaunched := false
	if c != nil {
		t.Cleanup(func() {
			// Fault cleanups run first. Join Start before Close, which uses the
			// same cluster lock. A failed start cannot make cleanup wait forever.
			if startupLaunched {
				select {
				case <-started:
				case <-time.After(30 * time.Second):
					t.Error("cluster Start did not terminate after fault removal")
					return
				}
			}
			require.NoError(t, c.Close())
		})
	}
	require.NoError(t, err)

	var receiverPaused atomic.Bool
	previousReporter := logutil.GetReportZapFunc()
	logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: func(enc zapcore.Encoder, entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
		if entry.Message == "logtail.consumer.receiver.paused" {
			receiverPaused.Store(true)
		}
		return previousReporter(enc, entry, fields)
	}})
	t.Cleanup(func() {
		logutil.SetLogReporter(&logutil.TraceReporter{ReportZap: previousReporter})
	})
	if fault.Enable() {
		t.Cleanup(func() { fault.Disable() })
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
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
	startupLaunched = true
	go func() {
		startupErr = c.Start()
		close(started)
	}()
	require.Eventually(t, func() bool {
		n, _, ok := fault.TriggerFault(waiters)
		return ok && n == 1
	}, 30*time.Second, 10*time.Millisecond, "CN must reach its first catalog replay")
	addFault(objectio.FJ_CNRecvErr, "echo", 100000, "")
	require.Eventually(t, receiverPaused.Load, 15*time.Second, 10*time.Millisecond,
		"receiver must pause before replay completes")
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNRecvErr)
	require.NoError(t, err)
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNReplayCacheErr)
	require.NoError(t, err)
	select {
	case <-started:
		require.NoError(t, startupErr)
	case <-ctx.Done():
		t.Fatal("first-replay disconnect prevented cluster startup")
	}

	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	owner := cn.RawService().(cnservice.Service)
	fence, _ := owner.GetClock().Now()
	recoveryCtx, recoveryCancel := context.WithTimeout(ctx, 15*time.Second)
	defer recoveryCancel()
	_, err = owner.GetTimestampWaiter().GetTimestamp(recoveryCtx, fence)
	require.NoError(t, err, "recovered CN must receive a fresh TN timestamp")

	db, err := sql.Open("mysql", fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	for _, query := range []string{
		"create database first_replay",
		"create table first_replay.t (id int primary key)",
		"insert into first_replay.t values (1), (2)",
	} {
		_, err := db.ExecContext(ctx, query)
		require.NoError(t, err)
	}

	// Reuse the recovered frontend and two-row fixture for the prepared-reader
	// consumer: one protocol statement, two executions, distinct row oracles.
	stmt, err := db.PrepareContext(ctx, "select id from first_replay.t where id > ? order by id")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stmt.Close()) })
	for minimum, want := range [][]int32{{1, 2}, {2}} {
		func() {
			rows, err := stmt.QueryContext(ctx, minimum)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			var got []int32
			for rows.Next() {
				var id int32
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
		}()
	}
}
