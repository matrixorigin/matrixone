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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestCheckDatabaseExistsAtSnapshot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := defines.AttachAccountId(context.Background(), 1)
	snapshot := &plan.Snapshot{
		TS:     &timestamp.Timestamp{PhysicalTime: 42},
		Tenant: &plan.SnapshotTenant{TenantID: 7},
	}
	er := mock_frontend.NewMockExecResult(ctrl)
	er.EXPECT().GetRowCount().Return(uint64(1)).AnyTimes()
	bh := mock_frontend.NewMockBackgroundExec(ctrl)
	bh.EXPECT().ClearExecResultSet()
	bh.EXPECT().Exec(
		gomock.Any(),
		"SELECT 1 FROM mo_catalog.mo_database {MO_TS = 42} WHERE datname = 'db''name' LIMIT 1",
	).DoAndReturn(func(gotCtx context.Context, _ string) error {
		accountID, err := defines.GetAccountId(gotCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(7), accountID)
		return nil
	})
	bh.EXPECT().GetExecResultSet().Return([]interface{}{er})

	exists, err := checkDatabaseExistsAtSnapshot(ctx, bh, snapshot, "db'name")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestCheckDatabaseExistsAtSnapshotPropagatesErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("catalog query fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		wantErr := errors.New("catalog query failed")
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet()
		bh.EXPECT().Exec(
			gomock.Any(),
			"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'source' LIMIT 1",
		).Return(wantErr)

		exists, err := checkDatabaseExistsAtSnapshot(ctx, bh, nil, "source")
		require.False(t, exists)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("catalog result is malformed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		bh := mock_frontend.NewMockBackgroundExec(ctrl)
		bh.EXPECT().ClearExecResultSet()
		bh.EXPECT().Exec(
			gomock.Any(),
			"SELECT 1 FROM mo_catalog.mo_database WHERE datname = 'source' LIMIT 1",
		).Return(nil)
		bh.EXPECT().GetExecResultSet().Return([]interface{}{"not an ExecResult"})

		exists, err := checkDatabaseExistsAtSnapshot(ctx, bh, nil, "source")
		require.False(t, exists)
		require.Error(t, err)
	})
}
