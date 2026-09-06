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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

type cloneSnapshotCompilerContext struct {
	*MockCompilerContext
	snapshotErr error
}

func (c *cloneSnapshotCompilerContext) ResolveSnapshotWithSnapshotName(string) (*Snapshot, error) {
	return nil, c.snapshotErr
}

func TestBuildCloneTableSnapshotResolutionErrors(t *testing.T) {
	const snapshotName = "missing_snapshot"

	tests := []struct {
		name        string
		snapshotErr error
		wantErr     string
		wantUserErr bool
	}{
		{
			name: "missing named snapshot",
			snapshotErr: moerr.NewInternalErrorf(
				context.Background(),
				"find 0 snapshot records by name(%s), expect only 1",
				snapshotName,
			),
			wantErr:     "invalid input: snapshot 'missing_snapshot' not found",
			wantUserErr: true,
		},
		{
			name:        "other snapshot resolution error",
			snapshotErr: moerr.NewInternalError(context.Background(), "snapshot catalog unavailable"),
			wantErr:     "internal error: snapshot catalog unavailable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(
				t.Context(),
				dialect.MYSQL,
				"create table clone_target clone tpch.nation {snapshot = 'missing_snapshot'}",
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			ctx := &cloneSnapshotCompilerContext{
				MockCompilerContext: NewMockCompilerContext(false),
				snapshotErr:         test.snapshotErr,
			}
			_, err = BuildPlan(ctx, stmt, false)
			require.EqualError(t, err, test.wantErr)

			if test.wantUserErr {
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.NotContains(t, err.Error(), "internal error")
				require.NotContains(t, err.Error(), "snapshot records")
			}
		})
	}
}
