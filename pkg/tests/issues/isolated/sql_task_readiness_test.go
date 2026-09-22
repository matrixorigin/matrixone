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

package isolated

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

// waitSQLTaskReady observes the frontend publication AND storage-read boundary.
// There is no event for this combined condition; only these two startup errors
// are polled. Business statements and unrelated query errors are never retried.
// The caller owns the setup deadline and the borrowed DB's lifetime.
func waitSQLTaskReady(ctx context.Context, db *sql.DB) error {
	return waitSQLTaskReadiness(ctx, func(ctx context.Context) error {
		return observeSQLTaskReadiness(ctx, db)
	}, func(ctx context.Context) error {
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	})
}

func observeSQLTaskReadiness(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "show tasks")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		// Drain the result: a successful QueryContext alone need not mean the
		// complete response was received. No task data is retained or changed.
	}
	return rows.Err()
}

func waitSQLTaskReadiness(ctx context.Context, observe, wait func(context.Context) error) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := observe(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil {
			return nil
		}
		var serverErr *mysql.MySQLError
		if !errors.As(err, &serverErr) ||
			!(serverErr.Number == moerr.ErrInvalidState && serverErr.Message == "invalid state task store not ready" ||
				serverErr.Number == moerr.ErrInternal && serverErr.Message == "internal error: task service not ready yet, please try again later.") {
			return err
		}
		if err := wait(ctx); err != nil {
			return err
		}
	}
}

func TestSQLTaskReadinessWaitsForReadableFrontend(t *testing.T) {
	for _, startupErr := range []*mysql.MySQLError{
		{Number: moerr.ErrInternal, Message: "internal error: task service not ready yet, please try again later."},
		{Number: moerr.ErrInvalidState, Message: "invalid state task store not ready"},
	} {
		t.Run(startupErr.Message, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := taskservice.NewMemTaskStorage()
			defer store.Close()
			waiting := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			joined := make(chan struct{})
			observations, businessCalls := 0, 0
			go func() {
				defer close(joined)
				err := waitSQLTaskReadiness(ctx, func(context.Context) error {
					observations++
					if observations == 1 {
						return startupErr
					}
					_, err := store.QuerySQLTask(ctx)
					return err
				}, func(ctx context.Context) error {
					close(waiting) // The negative observation has been processed.
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				if err == nil {
					businessCalls++
					_, err = store.AddSQLTask(ctx, taskservice.SQLTask{TaskID: 1, TaskName: "once"})
				}
				done <- err
			}()
			defer func() { cancel(); <-joined }()
			select {
			case <-waiting:
			case err := <-done:
				t.Fatalf("readiness released before storage was readable: %v", err)
			case <-ctx.Done():
				t.Fatal("readiness did not reach its controlled wait")
			}
			tasks, err := store.QuerySQLTask(ctx)
			require.NoError(t, err)
			require.Empty(t, tasks, "business action must not run before readiness")
			close(release)
			require.NoError(t, <-done)
			require.Equal(t, 2, observations)
			require.Equal(t, 1, businessCalls)
			tasks, err = store.QuerySQLTask(ctx)
			require.NoError(t, err)
			require.Len(t, tasks, 1)
			require.Equal(t, "once", tasks[0].TaskName)
		})
	}
}

func TestSQLTaskHolderPresenceDoesNotProveReadiness(t *testing.T) {
	// Reproduce the causal startup state without a cluster or scheduling luck:
	// an unsuccessful initial refresh still leaves a published TaskService.
	holder := taskservice.NewTaskServiceHolderWithTaskStorageFactorySelector(
		runtime.DefaultRuntime(),
		func(context.Context, bool) (string, error) { return "", errors.New("no CN yet") },
		func(string, string, string) taskservice.TaskStorageFactory {
			return taskservice.NewFixedTaskStorageFactory(nil)
		})
	defer func() { require.NoError(t, holder.Close()) }()
	require.NoError(t, holder.Create(logservicepb.CreateTaskService{
		User: logservicepb.TaskTableUser{Username: "task", Password: "test"}, TaskDatabase: "mo_task",
	}))
	service, published := holder.Get()
	require.True(t, published)
	require.NotNil(t, service)
	_, err := service.GetStorage().QuerySQLTask(context.Background())
	require.ErrorIs(t, err, taskservice.ErrNotReady)
}

func TestSQLTaskReadinessStopsOnFatalErrorOrCancellation(t *testing.T) {
	for _, err := range []error{
		errors.New("connection failed"),
		&mysql.MySQLError{Number: moerr.ErrInvalidState, Message: "invalid state unrelated"},
		&mysql.MySQLError{Number: 1045, Message: "invalid state task store not ready"},
	} {
		got := waitSQLTaskReadiness(context.Background(), func(context.Context) error { return err }, func(context.Context) error {
			t.Fatal("fatal errors must not be polled")
			return nil
		})
		require.ErrorIs(t, got, err)
	}
	for _, phase := range []string{"entry", "observation", "wait"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "entry" {
				cancel()
			}
			err := waitSQLTaskReadiness(ctx, func(context.Context) error {
				require.NotEqual(t, "entry", phase)
				if phase == "observation" {
					cancel()
					return nil
				}
				return &mysql.MySQLError{Number: moerr.ErrInvalidState, Message: "invalid state task store not ready"}
			}, func(context.Context) error {
				require.Equal(t, "wait", phase)
				cancel()
				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
		})
	}
}

func TestSQLTaskReadinessObservationClosesRows(t *testing.T) {
	for _, readErr := range []error{nil, errors.New("read failed")} {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		func() {
			defer db.Close()
			rows := sqlmock.NewRows([]string{"task_name"}).AddRow("existing_task")
			if readErr != nil {
				rows.RowError(0, readErr)
			}
			mock.ExpectQuery("^show tasks$").WillReturnRows(rows).RowsWillBeClosed()
			require.ErrorIs(t, observeSQLTaskReadiness(context.Background(), db), readErr)
			require.NoError(t, mock.ExpectationsWereMet())
		}()
	}
}
