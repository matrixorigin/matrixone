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

package compile

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/stretchr/testify/require"
)

func TestCDCKeyReadReportsIterationError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		failAt int
	}{{"before_first_row", 0}, {"after_first_row", 1}} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			readErr := errors.New("task key read interrupted")
			rows := sqlmock.NewRows([]string{"task_id"}).AddRow("first").AddRow("second").RowError(tc.failAt, readErr)
			mock.ExpectQuery(cdc.CDCSQLBuilder.GetTaskIdSQL(7, "task")).WillReturnRows(rows)
			keys := make(map[taskservice.CDCTaskKey]struct{})
			count, err := getTaskKeysCount(context.Background(), db, 7, "task", keys, false)
			require.ErrorIs(t, err, readErr)
			require.EqualValues(t, tc.failAt, count)
			require.Len(t, keys, tc.failAt)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestCDCControlStopsOnPartialTaskKeyRead(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	readErr := errors.New("task key read interrupted")
	rows := sqlmock.NewRows([]string{"task_id"}).AddRow("first").AddRow("second").RowError(1, readErr)
	mock.ExpectQuery(cdc.CDCSQLBuilder.GetTaskIdSQL(7, "")).WillReturnRows(rows)
	keys := make(map[taskservice.CDCTaskKey]struct{})
	_, err = onPreUpdateCDCTasks(context.Background(), task.TaskStatus_CancelRequested, keys, db, 7, "", false)
	require.ErrorIs(t, err, readErr)
	require.Len(t, keys, 1)
	require.NoError(t, mock.ExpectationsWereMet())
}
