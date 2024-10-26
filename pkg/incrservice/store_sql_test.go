// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package incrservice

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/require"
)

func TestDeleteWhenAccountNotExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
		return execFunc(nil)
	}).AnyTimes()

	// account not exists
	var executedSQLs []string
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
		executedSQLs = append(executedSQLs, sql)
		res := executor.Result{}
		return res, nil
	}).AnyTimes()

	s := &sqlStore{
		exec: mockSqlExecutor,
	}

	err := s.Delete(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 1, len(executedSQLs))
}

func TestDeleteWhenAccountExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.TODO()
	ctx = defines.AttachAccountId(ctx, 12)

	mockSqlExecutor := mock_executor.NewMockSQLExecutor(ctrl)
	mockSqlExecutor.EXPECT().ExecTxn(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
		return execFunc(nil)
	}).AnyTimes()
	var executedSQLs []string
	mockSqlExecutor.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
		executedSQLs = append(executedSQLs, sql)
		bat := &batch.Batch{}
		bat.SetRowCount(1)
		res := executor.Result{
			Batches: []*batch.Batch{bat},
		}
		return res, nil
	}).AnyTimes()

	s := &sqlStore{
		exec: mockSqlExecutor,
	}

	err := s.Delete(ctx, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(executedSQLs))
}
