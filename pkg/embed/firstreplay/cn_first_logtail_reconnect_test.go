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

package firstreplay

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/testutil/clusteradmission"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// The cluster owns cleanup even when an intermediate startup step fails.
func startReplayControlPlane(startLog, startTN, waitTN func() error) error {
	for _, step := range []struct {
		name string
		run  func() error
	}{{"log", startLog}, {"TN", startTN}, {"TN shard readiness", waitTN}} {
		if err := step.run(); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
	}
	return nil
}

func waitForReplayTN(ctx context.Context, client interface {
	GetClusterDetails(context.Context) (logservicepb.ClusterDetails, error)
}) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var lastErr error
	for {
		details, err := client.GetClusterDetails(ctx)
		if err != nil {
			lastErr = err
		} else {
			for _, store := range details.TNStores {
				if len(store.Shards) > 0 {
					return nil
				}
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("waiting for TN shard: %w", errors.Join(ctx.Err(), lastErr))
		case <-timer.C:
		}
	}
}

// Do not release the admission lease or remove storage until startup has
// completed and every service has actually closed. The two final operations
// are independent after that point, so report both errors if either fails.
func cleanupReplayResources(waitStartup func(), closeCluster, removeData, releaseAdmission func() error) error {
	waitStartup()
	if err := closeCluster(); err != nil {
		return fmt.Errorf("close replay cluster: %w", err)
	}
	return errors.Join(removeData(), releaseAdmission())
}

type replayStartup struct {
	done chan struct{}
	err  error
}

func startReplayCN(start func() error) *replayStartup {
	startup := &replayStartup{done: make(chan struct{})}
	go func() {
		defer close(startup.done)
		startup.err = start()
	}()
	return startup
}

func TestCNFirstReplayDisconnect(t *testing.T) {
	// This package has its own test process: replay faults cannot affect the
	// shared embed cluster, and admission remains exclusive across processes.
	// Reserve time for replay and cleanup instead of charging admission wait to
	// the replay deadline below.
	dataPath, err := os.MkdirTemp("", "mo-first-replay-")
	require.NoError(t, err)
	var c embed.Cluster
	var admission *clusteradmission.Lease
	var startup *replayStartup
	t.Cleanup(func() {
		// Fault cleanups run first; the package timeout is the outer guard if
		// an operator cannot stop. Keep the data directory on a close error.
		err := cleanupReplayResources(func() {
			if startup != nil {
				<-startup.done
			}
		}, func() error {
			if c == nil {
				return nil
			}
			return c.Close()
		}, func() error {
			return os.RemoveAll(dataPath)
		}, func() error {
			return admission.Release()
		})
		require.NoError(t, err)
	})

	admissionCtx := t.Context()
	if deadline, ok := t.Deadline(); ok {
		reserve := min(150*time.Second, time.Until(deadline)/2)
		var cancel context.CancelFunc
		admissionCtx, cancel = context.WithDeadline(admissionCtx, deadline.Add(-reserve))
		defer cancel()
	}
	admission, err = clusteradmission.Acquire(admissionCtx)
	require.NoError(t, err, "acquire test cluster admission")
	c, err = embed.NewCluster(embed.WithTesting(), embed.WithDataPath(dataPath), embed.WithCNCount(1))
	require.NoError(t, err)

	var logOp, tnOp, cnOp embed.ServiceOperator
	counts := make(map[metadata.ServiceType]int)
	c.ForeachServices(func(op embed.ServiceOperator) bool {
		counts[op.ServiceType()]++
		switch op.ServiceType() {
		case metadata.ServiceType_LOG:
			logOp = op
		case metadata.ServiceType_TN:
			tnOp = op
		case metadata.ServiceType_CN:
			cnOp = op
		}
		return true
	})
	require.Equal(t, 1, counts[metadata.ServiceType_LOG])
	require.Equal(t, 1, counts[metadata.ServiceType_TN])
	require.Equal(t, 1, counts[metadata.ServiceType_CN])
	t.Log("starting log and TN before measuring first CN replay")
	require.NoError(t, startReplayControlPlane(logOp.Start, tnOp.Start, func() error {
		readyCtx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		defer cancel()
		// The TN runtime already exists; the first CN has not started yet.
		for {
			attemptCtx, cancelAttempt := context.WithTimeout(readyCtx, 5*time.Second)
			client, err := logservice.NewCNHAKeeperClient(attemptCtx, tnOp.ServiceID(), cnOp.GetServiceConfig().HAKeeperClient)
			cancelAttempt()
			if err == nil {
				return func() (err error) {
					defer func() { err = errors.Join(err, client.Close()) }()
					return waitForReplayTN(readyCtx, client)
				}()
			}
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-readyCtx.Done():
				timer.Stop()
				return errors.Join(readyCtx.Err(), err)
			case <-timer.C:
			}
		}
	}))
	t.Log("TN shard is ready; starting the first CN")

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
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
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
	startup = startReplayCN(cnOp.Start)
	require.Eventually(t, func() bool {
		select {
		case <-startup.done:
			return true
		default:
		}
		n, _, ok := fault.TriggerFault(waiters)
		return ok && n == 1
	}, 30*time.Second, 10*time.Millisecond, "CN must reach its first catalog replay")
	select {
	case <-startup.done:
		require.NoError(t, startup.err, "cluster startup failed before first replay")
		t.Fatal("cluster started without reaching the first replay fault")
	default:
	}
	addFault(objectio.FJ_CNRecvErr, "echo", 100000, "")
	require.Eventually(t, receiverPaused.Load, 15*time.Second, 10*time.Millisecond,
		"receiver must pause before replay completes")
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNRecvErr)
	require.NoError(t, err)
	_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_CNReplayCacheErr)
	require.NoError(t, err)
	select {
	case <-startup.done:
		require.NoError(t, startup.err)
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
