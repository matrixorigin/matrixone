// Copyright 2021 - 2024 Matrix Origin
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

package dml

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// TestIssue25025DistributedInsertSelectNoDeadlock reproduces issue #25025:
// a distributed (multi-CN) INSERT...SELECT with a shuffle join must not
// deadlock in the pipeline teardown (Dispatch.Reset -> PipelineSpool.Close
// waiting forever on csDoneSignal).
//
// We force multi-CN execution and run the shuffle INSERT...SELECT many times,
// each guarded by a watchdog. If any run does not finish in time we dump the
// goroutines; a stack stuck in pSpool.(*PipelineSpool).Close is the signature
// of the bug.
func TestIssue25025DistributedInsertSelectNoDeadlock(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
			defer cancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			mustExec(t, ctx, exec, "create database "+db, "")
			opt := executor.Options{}.WithDatabase(db)

			mustExec(t, ctx, exec, "create table a (k int, v varchar(64))", db)
			mustExec(t, ctx, exec, "create table b (k int, w varchar(64))", db)
			mustExec(t, ctx, exec, "create table tgt (k int, v varchar(64), w varchar(64))", db)

			// moderate data with overlapping join keys to force a real shuffle.
			mustExec(t, ctx, exec,
				"insert into a select result % 20000, concat('v', result) from generate_series(1, 200000) g", db)
			mustExec(t, ctx, exec,
				"insert into b select result % 20000, concat('w', result) from generate_series(1, 200000) g", db)

			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(false)

			const iterations = 40
			for i := 0; i < iterations; i++ {
				mustExec(t, ctx, exec, "truncate table tgt", db)

				insertSQL := "insert into tgt select a.k, a.v, b.w from a join b on a.k = b.k"
				done := make(chan error, 1)
				go func() {
					res, e := exec.Exec(ctx, insertSQL, opt)
					if e == nil {
						res.Close()
					}
					done <- e
				}()

				select {
				case e := <-done:
					require.NoError(t, e, "iteration %d", i)
				case <-time.After(90 * time.Second):
					dumpGoroutines(t)
					t.Fatalf("iteration %d: distributed INSERT...SELECT did not finish in 90s "+
						"(suspected pSpool.Close teardown deadlock, issue #25025)", i)
				}
			}
		},
	)
}

// TestIssue25025TeardownDeadlockOnCancel reproduces the real failure mode:
// a distributed INSERT...SELECT whose context is cancelled mid-flight (the
// KILL / mid-query error scenario). The teardown must not leave a goroutine
// stuck forever in pSpool.(*PipelineSpool).Close waiting on csDoneSignal.
func TestIssue25025TeardownDeadlockOnCancel(t *testing.T) {
	embed.RunBaseClusterTests(
		func(c embed.Cluster) {
			baseCtx, baseCancel := context.WithTimeout(context.Background(), time.Minute*10)
			defer baseCancel()

			cn1, err := c.GetCNService(0)
			require.NoError(t, err)
			exec := testutils.GetSQLExecutor(cn1)

			db := testutils.GetDatabaseName(t)
			mustExec(t, baseCtx, exec, "create database "+db, "")
			opt := executor.Options{}.WithDatabase(db)

			mustExec(t, baseCtx, exec, "create table a (k int, v varchar(64))", db)
			mustExec(t, baseCtx, exec, "create table b (k int, w varchar(64))", db)
			mustExec(t, baseCtx, exec, "create table tgt (k int, v varchar(64), w varchar(64))", db)
			mustExec(t, baseCtx, exec,
				"insert into a select result % 20000, concat('v', result) from generate_series(1, 200000) g", db)
			mustExec(t, baseCtx, exec,
				"insert into b select result % 20000, concat('w', result) from generate_series(1, 200000) g", db)

			plan.SetForceScanOnMultiCN(true)
			defer plan.SetForceScanOnMultiCN(false)

			insertSQL := "insert into tgt select a.k, a.v, b.w from a join b on a.k = b.k"

			// cancel at increasing offsets to hit different teardown windows.
			for i := 0; i < 60; i++ {
				runCtx, runCancel := context.WithCancel(baseCtx)
				done := make(chan struct{})
				go func() {
					res, e := exec.Exec(runCtx, insertSQL, opt)
					if e == nil {
						res.Close()
					}
					close(done)
				}()

				// cancel mid-flight (the insert itself takes ~300-400ms).
				delay := time.Duration(10+(i%30)*10) * time.Millisecond
				time.Sleep(delay)
				runCancel()

				select {
				case <-done:
				case <-time.After(30 * time.Second):
					dumpGoroutines(t)
					t.Fatalf("iteration %d (cancel@%v): exec did not return in 30s after cancel", i, delay)
				}
				runCancel()
			}

			// after all cancelled runs, give teardown a grace period then look
			// for goroutines leaked in pSpool.Close (the issue #25025 deadlock).
			time.Sleep(8 * time.Second)
			if n := countStuckSpoolClose(t); n > 0 {
				dumpGoroutines(t)
				t.Fatalf("issue #25025 REPRODUCED: %d goroutine(s) stuck in pSpool.(*PipelineSpool).Close", n)
			}
		},
	)
}

func countStuckSpoolClose(t *testing.T) int {
	buf := make([]byte, 8<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	cnt := 0
	for _, g := range strings.Split(stacks, "\n\n") {
		if strings.Contains(g, "pSpool.(*PipelineSpool).Close") {
			cnt++
		}
	}
	t.Logf("stuck pSpool.Close goroutines: %d", cnt)
	return cnt
}

func mustExec(t *testing.T, ctx context.Context, exec executor.SQLExecutor, sql, db string) {
	opt := executor.Options{}
	if db != "" {
		opt = opt.WithDatabase(db)
	}
	res, err := exec.Exec(ctx, sql, opt)
	require.NoError(t, err, "sql: %s", sql)
	res.Close()
}

func dumpGoroutines(t *testing.T) {
	buf := make([]byte, 4<<20)
	n := runtime.Stack(buf, true)
	stacks := string(buf[:n])
	t.Logf("=== goroutine dump (issue #25025) ===\n%s", stacks)
	if strings.Contains(stacks, "pSpool.(*PipelineSpool).Close") {
		t.Logf(">>> REPRODUCED: goroutine stuck in pSpool.(*PipelineSpool).Close")
	}
	fmt.Println("goroutine count signature lines:")
	for _, line := range strings.Split(stacks, "\n") {
		if strings.Contains(line, "PipelineSpool).Close") ||
			strings.Contains(line, "MergeRun") ||
			strings.Contains(line, "WaitingEnd") {
			fmt.Println("   ", strings.TrimSpace(line))
		}
	}
}
