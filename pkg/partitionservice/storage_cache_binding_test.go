// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package partitionservice

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type metadataCacheWorkspace struct {
	client.Workspace
	revision uint64
	stable   bool
}

func (w *metadataCacheWorkspace) CatalogVisibility() (uint64, bool) {
	return w.revision, w.stable
}

type metadataCacheTxn struct {
	client.TxnOperator
	stamp client.CatalogReadStamp
	cache map[string]any
	onGet func()
}

func (t *metadataCacheTxn) CatalogReadStamp() (client.CatalogReadStamp, error) {
	return t.stamp, nil
}
func (t *metadataCacheTxn) Get(key string) (any, bool) {
	v, ok := t.cache[key]
	if t.onGet != nil {
		t.onGet()
	}
	return v, ok
}
func (t *metadataCacheTxn) Set(key string, value any) { t.cache[key] = value }
func (t *metadataCacheTxn) Delete(key string)         { delete(t.cache, key) }

// The embedded interface deliberately does not expose CatalogReadStamp.
type unstampedMetadataTxn struct{ client.TxnOperator }

func TestPartitionMetadataCacheReadBinding(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	workspace := &metadataCacheWorkspace{stable: true}
	op := &metadataCacheTxn{cache: map[string]any{}, stamp: client.CatalogReadStamp{
		TransactionID: "read-binding", Snapshot: timestamp.Timestamp{PhysicalTime: 10}, Workspace: workspace,
	}}
	name := "before"
	queries := 0
	advanceDuringRead := false
	lookup := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		queries++
		if strings.Contains(query, catalog.MOPartitionTables) {
			if advanceDuringRead {
				op.stamp.Revision++
				advanceDuringRead = false
			}
			return executor.Result{}, nil
		}
		require.Contains(t, query, catalog.MOPartitionMetadata)
		m := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType()}, mp)
		m.NewBatchWithRowCount(1)
		for i, value := range []string{name, "db", "HASH", "a"} {
			require.NoError(t, executor.AppendStringRows(m, i, []string{value}))
		}
		require.NoError(t, executor.AppendFixedRows(m, 4, []uint32{0}))
		return m.GetResult(), nil
	})
	s := &storage{exec: lookup}
	ctx := defines.AttachAccountId(context.Background(), 42)
	got, found, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "before", got.TableName)
	_, _, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, 2, queries, "a stable scope should reuse its metadata")
	name = "after"
	op.stamp.Snapshot = timestamp.Timestamp{PhysicalTime: 11}
	op.stamp.Revision++
	got, found, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "after", got.TableName, "a new snapshot cannot reuse earlier metadata")
	workspace.revision++
	name = "workspace"
	got, _, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "workspace", got.TableName, "a changed workspace cannot reuse earlier metadata")
	name = "other-account"
	got, _, err = s.GetMetadata(defines.AttachAccountId(ctx, 99), 7, op)
	require.NoError(t, err)
	require.Equal(t, "other-account", got.TableName, "different accounts cannot share metadata")
	name = "same-timestamp-new-generation"
	op.stamp.Revision++
	got, _, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "same-timestamp-new-generation", got.TableName, "snapshot ABA must invalidate metadata")
	workspace.revision++
	name = "in-flight"
	advanceDuringRead = true
	_, _, err = s.GetMetadata(ctx, 7, op)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "a changed read generation must retry")
	name = "after-retry"
	got, _, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "after-retry", got.TableName, "a stale in-flight result must not enter the cache")
	op.onGet = func() { op.stamp.Revision++ }
	_, _, err = s.GetMetadata(ctx, 7, op)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged), "a changed cache-hit generation must retry")
	op.onGet = nil
	workspace.stable = false
	name = "unsettled"
	got, _, err = s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "unsettled", got.TableName, "an unstable workspace cannot use the previous cache entry")
	workspace.stable = true
	workspace.revision++
	name = "unstamped-first"
	legacy := &unstampedMetadataTxn{TxnOperator: op}
	beforeQueries := queries
	got, _, err = s.GetMetadata(ctx, 7, legacy)
	require.NoError(t, err)
	require.Equal(t, "unstamped-first", got.TableName)
	name = "unstamped-second"
	got, _, err = s.GetMetadata(ctx, 7, legacy)
	require.NoError(t, err)
	require.Equal(t, "unstamped-second", got.TableName)
	require.Equal(t, beforeQueries+4, queries, "an unstampable transaction must bypass the cache")
}

func TestPartitionMetadataDoesNotCachePartialFailure(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	workspace := &metadataCacheWorkspace{stable: true}
	op := &metadataCacheTxn{cache: map[string]any{}, stamp: client.CatalogReadStamp{
		TransactionID: "partial", Snapshot: timestamp.Timestamp{PhysicalTime: 10}, Workspace: workspace,
	}}
	fail := true
	queries := 0
	lookup := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		queries++
		if strings.Contains(query, catalog.MOPartitionTables) {
			if fail {
				return executor.Result{}, errors.New("injected partition read failure")
			}
			return executor.Result{}, nil
		}
		m := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType()}, mp)
		m.NewBatchWithRowCount(1)
		for i, value := range []string{"t", "db", "HASH", "a"} {
			require.NoError(t, executor.AppendStringRows(m, i, []string{value}))
		}
		require.NoError(t, executor.AppendFixedRows(m, 4, []uint32{0}))
		return m.GetResult(), nil
	})
	s := &storage{exec: lookup}
	ctx := defines.AttachAccountId(context.Background(), 42)
	_, _, err := s.GetMetadata(ctx, 7, op)
	require.ErrorContains(t, err, "injected partition read failure")
	fail = false
	_, found, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 4, queries, "a failed partial read must not populate the cache")
}

func TestPartitionMetadataCacheDoesNotShareMutableResults(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	defer func() { require.Zero(t, mp.CurrNB()) }()
	workspace := &metadataCacheWorkspace{stable: true}
	op := &metadataCacheTxn{cache: map[string]any{}, stamp: client.CatalogReadStamp{
		TransactionID: "ownership", Snapshot: timestamp.Timestamp{PhysicalTime: 10}, Workspace: workspace,
	}}
	expr, err := (&plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}).Marshal()
	require.NoError(t, err)
	queries := 0
	lookup := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		queries++
		if strings.Contains(query, catalog.MOPartitionTables) {
			m := executor.NewMemResult([]types.Type{types.T_uint64.ToType(), types.T_varchar.ToType(),
				types.T_varchar.ToType(), types.T_uint32.ToType(), types.T_varchar.ToType(), types.T_varchar.ToType()}, mp)
			m.NewBatchWithRowCount(1)
			require.NoError(t, executor.AppendFixedRows(m, 0, []uint64{101}))
			require.NoError(t, executor.AppendStringRows(m, 1, []string{"p0_table"}))
			require.NoError(t, executor.AppendStringRows(m, 2, []string{"p0"}))
			require.NoError(t, executor.AppendFixedRows(m, 3, []uint32{0}))
			require.NoError(t, executor.AppendStringRows(m, 4, []string{"a"}))
			require.NoError(t, executor.AppendBytesRows(m, 5, [][]byte{expr}))
			return m.GetResult(), nil
		}
		m := executor.NewMemResult([]types.Type{types.T_varchar.ToType(), types.T_varchar.ToType(),
			types.T_varchar.ToType(), types.T_varchar.ToType(), types.T_uint32.ToType()}, mp)
		m.NewBatchWithRowCount(1)
		for i, value := range []string{"t", "db", "HASH", "a"} {
			require.NoError(t, executor.AppendStringRows(m, i, []string{value}))
		}
		require.NoError(t, executor.AppendFixedRows(m, 4, []uint32{1}))
		return m.GetResult(), nil
	})
	s := &storage{exec: lookup}
	ctx := defines.AttachAccountId(context.Background(), 42)
	first, found, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, first.Partitions, 1)
	first.Partitions[0].Name = "changed"
	first.Partitions[0].Expr.GetCol().ColPos = 42
	second, _, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "p0", second.Partitions[0].Name)
	require.Zero(t, second.Partitions[0].Expr.GetCol().ColPos)
	second.Partitions[0].ExprWithRowID.GetCol().ColPos = 43
	third, _, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, int32(1), third.Partitions[0].ExprWithRowID.GetCol().ColPos)
	require.Equal(t, 2, queries, "the test must exercise cache reuse")
	concurrentCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			select {
			case <-start:
				value, found, err := s.GetMetadata(concurrentCtx, 7, op)
				if err != nil {
					results <- err
					return
				}
				if !found || len(value.Partitions) != 1 || value.Partitions[0].Expr.GetCol() == nil {
					results <- errors.New("cached partition metadata is incomplete")
					return
				}
				value.Partitions[0].Name = "concurrent"
				value.Partitions[0].Expr.GetCol().ColPos = 99
				results <- nil
			case <-concurrentCtx.Done():
				results <- concurrentCtx.Err()
			}
		}()
	}
	close(start)
	for range 2 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-concurrentCtx.Done():
			t.Fatal("concurrent cached metadata reads did not complete")
		}
	}
	last, _, err := s.GetMetadata(ctx, 7, op)
	require.NoError(t, err)
	require.Equal(t, "p0", last.Partitions[0].Name)
	require.Zero(t, last.Partitions[0].Expr.GetCol().ColPos)
}
