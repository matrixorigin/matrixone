// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type restartCatalogOperation struct {
	sql        string
	account    uint32
	accountSet bool
}

type restartCatalogRecorder struct {
	*blockingRestartPublicationExecutor
	mu         sync.Mutex
	operations []restartCatalogOperation
}

func (r *restartCatalogRecorder) record(ctx context.Context, sql string) {
	account, err := defines.GetAccountId(ctx)
	r.mu.Lock()
	r.operations = append(r.operations, restartCatalogOperation{sql: sql, account: account, accountSet: err == nil})
	r.mu.Unlock()
}

func (r *restartCatalogRecorder) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	_, err := r.ExecWithStatus(ctx, sql, opts)
	return err
}

func (r *restartCatalogRecorder) ExecWithStatus(ctx context.Context, sql string, opts ie.SessionOverrideOptions) (ie.InternalExecStatus, error) {
	r.record(ctx, sql)
	return r.blockingRestartPublicationExecutor.ExecWithStatus(ctx, sql, opts)
}

func (r *restartCatalogRecorder) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	r.record(ctx, sql)
	return r.blockingRestartPublicationExecutor.Query(ctx, sql, opts)
}

// The larger restart test exercises a real race. Pin the alternative where
// publication has committed running before timeout persistence reads the
// catalog, so the fallback and its ownership fence have explicit oracles.
func TestRestartTimeoutRepairsLateRunningCatalogState(t *testing.T) {
	const childMarker = "MO_TEST_CDC_TIMEOUT_FALLBACK_CHILD"
	child := os.Getenv(childMarker) == "error-controls"
	var once sync.Once
	var childOutput []byte
	var childErr error
	runErrorControls := func() {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		args := []string{"-test.run=^TestRestartTimeoutRepairsLateRunningCatalogState$", "-test.v", "-test.count=1", "-test.timeout=20s"}
		if testing.CoverMode() != "" {
			coverageDir := ""
			if f := flag.Lookup("test.gocoverdir"); f != nil {
				coverageDir = f.Value.String()
			}
			if coverageDir == "" {
				coverageDir = os.Getenv("GOCOVERDIR")
			}
			if coverageDir == "" {
				childErr = errors.New("the child must contribute to the parent coverage profile")
				return
			}
			args = append(args, "-test.gocoverdir="+coverageDir)
		}
		command := exec.CommandContext(ctx, os.Args[0], args...)
		command.Env = append(os.Environ(), childMarker+"=error-controls")
		childOutput, childErr = command.CombinedOutput()
	}
	cases := []struct {
		name, initial, owner, final string
		fallback, wantError         bool
	}{
		{"timed out and running", cdc.CDCState_Running, "timed out", cdc.CDCState_Failed, true, false},
		{"missing owner", cdc.CDCState_Running, "missing", cdc.CDCState_Running, false, true},
		{"pending owner", cdc.CDCState_Running, "pending", cdc.CDCState_Running, false, true},
		{"conflicting paused state", cdc.CDCState_Paused, "timed out", cdc.CDCState_Paused, true, true},
		{"still restarting", cdc.CDCState_Restarting, "timed out", cdc.CDCState_Failed, false, false},
	}
	for _, tc := range cases {
		if child && !tc.wantError {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantError && !child {
				// These errors consume a process-wide logging budget. A single
				// child isolates them and is also used for selected subtests.
				once.Do(runErrorControls)
				require.NoError(t, childErr, "isolated error controls: %s", childOutput)
				name := strings.ReplaceAll(tc.name, " ", "_")
				require.Contains(t, string(childOutput), "--- PASS: TestRestartTimeoutRepairsLateRunningCatalogState/"+name)
				return
			}
			store := newBlockingRestartPublicationExecutor()
			// This test fixes the catalog's starting state; it does not park a
			// publication. An unexpected running update must not hang it.
			store.unblock()
			store.state = tc.initial
			recorder := &restartCatalogRecorder{blockingRestartPublicationExecutor: store}
			const accountID uint64 = 1
			const taskID = "restart-timeout-fallback"
			timeoutErr := errors.New("test restart startup timeout")
			exec := &CDCTaskExecutor{
				spec:                          &task.CreateCdcDetails{TaskId: taskID, Accounts: []*task.Account{{Id: accountID}}},
				restartCatalogExecutorFactory: func() ie.InternalExecutor { return recorder },
				restartStartupTimeout:         time.Second,
				lifecycleTaskScheduler: func(_ string, run func(context.Context)) error {
					run(context.Background())
					return nil
				},
			}
			var attempt *cdcStartAttempt
			if tc.owner != "missing" {
				attempt = &cdcStartAttempt{}
				if tc.owner == "timed out" {
					require.True(t, attempt.timeoutRestart())
				}
			}
			exec.recordRestartTimeoutAsync(attempt, timeoutErr)
			err, timedOut := exec.waitForRestartCatalogPersistence(time.Second)
			require.False(t, timedOut)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.final, store.getState())
			store.stateMu.Lock()
			message := store.errMsg
			store.stateMu.Unlock()
			if tc.final == cdc.CDCState_Failed {
				require.Equal(t, timeoutErr.Error(), message)
			} else {
				require.Empty(t, message)
			}
			update := func(from string) string {
				return cdc.CDCSQLBuilder.UpdateTaskStateAndErrMsgByStateSQL(accountID, taskID, cdc.CDCState_Failed, timeoutErr.Error(), from)
			}
			query := cdc.CDCSQLBuilder.GetTaskStateSQL(accountID, taskID)
			want := []string{update(cdc.CDCState_Restarting)}
			if tc.initial != cdc.CDCState_Restarting {
				want = append(want, query)
			}
			if tc.fallback {
				want = append(want, update(cdc.CDCState_Running))
				if tc.initial != cdc.CDCState_Running {
					want = append(want, query)
				}
			}
			recorder.mu.Lock()
			got := append([]restartCatalogOperation(nil), recorder.operations...)
			recorder.mu.Unlock()
			require.Len(t, got, len(want))
			for i, op := range got {
				require.Equal(t, want[i], op.sql)
				require.True(t, op.accountSet)
				require.Equal(t, uint32(catalog.System_Account), op.account)
			}
		})
	}
}
