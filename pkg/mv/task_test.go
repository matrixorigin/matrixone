// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.
package mv

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type scheduledTasks struct {
	taskservice.TaskService
	batches [][]task.TaskMetadata
}

func (s *scheduledTasks) CreateBatch(_ context.Context, batch []task.TaskMetadata) error {
	s.batches = append(s.batches, append([]task.TaskMetadata(nil), batch...))
	return nil
}
func TestMaterializedViewReconcileIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		if strings.HasPrefix(query, "select account_id,mv_id,epoch,") {
			require.Contains(t, query, "next_due<=current_timestamp()")
			require.Contains(t, query, "not exists")
			require.Contains(t, query, "limit 128")
			r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType()}, mp)
			r.NewBatchWithRowCount(2)
			require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7, 8}))
			require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99, 99}))
			require.NoError(t, executor.AppendFixedRows(r, 2, []uint64{2, 2}))
			require.NoError(t, executor.AppendFixedRows(r, 3, []int64{123, 123}))
			return r.GetResult(), nil
		}
		require.True(t, strings.HasPrefix(query, "select account_id,mv_id,run_id,") || strings.HasPrefix(query, "select account_id,mv_id from"))
		return executor.Result{}, nil
	})
	service := NewService(sql)
	tasks := &scheduledTasks{}
	require.NoError(t, service.reconcile(context.Background(), tasks))
	require.NoError(t, service.reconcile(context.Background(), tasks))
	require.Len(t, tasks.batches, 2)
	require.Len(t, tasks.batches[0], 2)
	require.Equal(t, tasks.batches[0], tasks.batches[1])
	require.NotEqual(t, tasks.batches[0][0].ID, tasks.batches[0][1].ID)
}
