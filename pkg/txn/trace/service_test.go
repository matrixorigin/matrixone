// Copyright 2024 Matrix Origin
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

package trace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type blockingTraceOperation struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingTraceOperation) wait(ctx context.Context) error {
	close(b.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.release:
		return context.DeadlineExceeded
	}
}

type blockingTraceExecutor struct {
	executor.SQLExecutor
	op *blockingTraceOperation
}

func (b *blockingTraceExecutor) ExecTxn(ctx context.Context, _ func(executor.TxnExecutor) error, _ executor.Options) error {
	return b.op.wait(ctx)
}

type blockingTraceFileService struct {
	fileservice.FileService
	op *blockingTraceOperation
}

func (b *blockingTraceFileService) Write(ctx context.Context, _ fileservice.IOVector) error {
	return b.op.wait(ctx)
}

func TestTraceWorkerOperationsFollowCancellation(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *service, string) error
	}{
		{"fetch", func(ctx context.Context, s *service, _ string) error {
			_, _, err := s.fetchFeatures(ctx)
			return err
		}},
		{"table filters", func(ctx context.Context, s *service, _ string) error { return s.refreshTableFilters(ctx) }},
		{"transaction filters", func(ctx context.Context, s *service, _ string) error { return s.refreshTxnFilters(ctx) }},
		{"statement filters", func(ctx context.Context, s *service, _ string) error { return s.refreshStatementFilters(ctx) }},
		{"database write", func(ctx context.Context, s *service, _ string) error { return s.writeToMO(ctx, loadAction{}) }},
		{"object write", func(ctx context.Context, s *service, file string) error {
			return s.writeToS3(ctx, loadAction{file: file})
		}},
		{"transaction error", func(ctx context.Context, s *service, _ string) error {
			s.doAddTxnError(ctx, "insert")
			return ctx.Err()
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			op := &blockingTraceOperation{started: make(chan struct{}), release: make(chan struct{})}
			s := &service{
				clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
				executor: &blockingTraceExecutor{op: op},
				dir:      t.TempDir(),
				logger:   runtime.DefaultRuntime().Logger(),
			}
			s.options.fs = &blockingTraceFileService{op: op}
			file := filepath.Join(s.dir, "trace.csv")
			require.NoError(t, os.WriteFile(file, []byte("trace"), 0600))
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				result <- tc.run(ctx, s, file)
			}()
			t.Cleanup(func() {
				cancel()
				close(op.release)
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("trace operation did not exit during cleanup")
				}
			})
			select {
			case <-op.started:
			case <-time.After(10 * time.Second):
				t.Fatal("trace operation did not reach its executor")
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("trace operation did not observe cancellation")
			}
		})
	}
}

func TestTraceWatchStopsDuringFeatureFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		op := &blockingTraceOperation{started: make(chan struct{}), release: make(chan struct{})}
		s := &service{
			stopper:  stopper.NewStopper("test-trace-watch"),
			executor: &blockingTraceExecutor{op: op},
			logger:   runtime.DefaultRuntime().Logger(),
		}
		s.atomic.flushEnabled.Store(true)
		require.NoError(t, s.stopper.RunTask(s.watch))
		stopped := make(chan struct{})
		stop := sync.OnceFunc(func() {
			go func() {
				s.stopper.Stop()
				close(stopped)
			}()
		})
		release := sync.OnceFunc(func() { close(op.release) })
		defer func() {
			release()
			stop()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Error("trace watch did not exit during cleanup")
			}
		}()

		// Advance the real watch ticker in virtual time, then stop while the
		// feature query is blocked in its executor.
		time.Sleep(10 * time.Second)
		synctest.Wait()
		select {
		case <-op.started:
		default:
			t.Fatal("watch did not start the feature fetch")
		}
		stop()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("watch did not stop while its feature fetch was blocked")
		}
	})
}

type testTraceCSV struct{ started chan struct{} }

func (e testTraceCSV) toCSVRecord(_ string, _ *buffer, records []string) {
	records[0] = "event"
	close(e.started)
}

func TestTraceEventWorkerStopsWithFullLoadQueue(t *testing.T) {
	dir := t.TempDir()
	s := &service{dir: dir, logger: runtime.DefaultRuntime().Logger(), loadC: make(chan loadAction, 1)}
	s.options.flushDuration = time.Hour
	s.atomic.flushEnabled.Store(true)
	s.loadC <- loadAction{}
	events := make(chan event, 1)
	started := make(chan struct{})
	events <- event{csv: testTraceCSV{started: started}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleEvent(ctx, 1, "test", events)
	}()
	releaseQueue := sync.OnceFunc(func() { <-s.loadC })
	t.Cleanup(func() {
		cancel()
		releaseQueue()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("trace event worker did not exit during cleanup")
		}
	})
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("trace event was not processed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("trace event worker did not stop with a full load queue")
	}
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, files, 1)
	contents, err := os.ReadFile(filepath.Join(dir, files[0].Name()))
	require.NoError(t, err)
	require.Equal(t, "event\n", string(contents))
}

func TestTraceLoadWorkerStopsWithoutDeletingFailedSpool(t *testing.T) {
	file := filepath.Join(t.TempDir(), "trace.csv")
	require.NoError(t, os.WriteFile(file, []byte("trace"), 0600))
	op := &blockingTraceOperation{started: make(chan struct{}), release: make(chan struct{})}
	s := &service{loadC: make(chan loadAction, 1), logger: runtime.DefaultRuntime().Logger()}
	s.options.writeFunc = func(ctx context.Context, _ loadAction) error { return op.wait(ctx) }
	s.loadC <- loadAction{file: file}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.handleLoad(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		close(op.release)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("trace load worker did not exit during cleanup")
		}
	})
	select {
	case <-op.started:
	case <-time.After(10 * time.Second):
		t.Fatal("trace load did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("trace load did not stop after cancellation")
	}
	require.FileExists(t, file)
}

func TestTraceLoadRetryWaitStopsOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{})
		s := &service{loadC: make(chan loadAction, 1), logger: runtime.DefaultRuntime().Logger()}
		s.options.writeFunc = func(context.Context, loadAction) error {
			close(started)
			return context.DeadlineExceeded
		}
		s.loadC <- loadAction{file: "trace.csv"}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleLoad(ctx)
		}()
		<-started
		// The worker is now blocked in its retry wait; virtual time must not
		// need to advance to the retry deadline for cancellation to release it.
		synctest.Wait()
		cancelledAt := time.Now()
		cancel()
		<-done
		require.Less(t, time.Since(cancelledAt), time.Second)
	})
}

func TestTraceLoadWorkerRemovesSuccessfulSpool(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "trace.csv")
		require.NoError(t, os.WriteFile(file, []byte("trace"), 0600))
		started := make(chan struct{})
		s := &service{loadC: make(chan loadAction, 1), logger: runtime.DefaultRuntime().Logger()}
		s.options.writeFunc = func(context.Context, loadAction) error {
			close(started)
			return nil
		}
		s.loadC <- loadAction{file: file}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.handleLoad(ctx)
		}()
		<-started
		synctest.Wait()
		require.NoFileExists(t, file)
		cancel()
		<-done
	})
}

func Test_updateState(t *testing.T) {
	exec := executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		if strings.HasPrefix(sql, "update trace_features set state = 'running' where name = 'data'") {
			return executor.Result{}, context.DeadlineExceeded
		}
		return executor.Result{}, nil
	})

	serv := &service{
		clock:    clock.NewHLCClock(func() int64 { return 0 }, 0),
		executor: exec,
	}
	err := serv.updateState(FeatureTraceData, "running")
	assert.Error(t, err)
}
