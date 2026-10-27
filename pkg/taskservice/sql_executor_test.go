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

package taskservice

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingAffectedRows struct{ err error }

func (f failingAffectedRows) LastInsertId() (int64, error) { return 0, nil }
func (f failingAffectedRows) RowsAffected() (int64, error) { return 9, f.err }

func TestExecRowsAffected(t *testing.T) {
	failure := errors.New("planned failure")
	for _, tc := range []struct {
		name    string
		result  sql.Result
		execErr error
		want    int64
		wantErr error
	}{
		{"success", mockRowsAffectedResult{rows: 4}, nil, 4, nil},
		{"execute error", nil, failure, 0, failure},
		{"rows error", failingAffectedRows{failure}, nil, 0, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &mockSqlExecutor{execFn: func(_ context.Context, query string, args ...interface{}) (sql.Result, error) {
				require.Equal(t, "update tasks set state = ?", query)
				require.Equal(t, []interface{}{"ready"}, args)
				return tc.result, tc.execErr
			}}
			rows, err := ExecRowsAffected(context.Background(), tx, "update tasks set state = ?", "ready")
			require.Equal(t, tc.want, rows)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
