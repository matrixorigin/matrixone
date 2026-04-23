// Copyright 2025 Matrix Origin
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

package databranchutils

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type fakeChangesHandle struct {
	data      *batch.Batch
	tombstone *batch.Batch
	hint      engine.ChangesHandle_Hint
	err       error
	closed    bool
	lastCtx   context.Context
}

func (f *fakeChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	f.lastCtx = ctx
	return f.data, f.tombstone, f.hint, f.err
}

func (f *fakeChangesHandle) Close() error {
	f.closed = true
	return nil
}

func TestBranchChangeHandleNextWithoutUnderlying(t *testing.T) {
	h := &BranchChangeHandle{}
	data, tomb, hint, err := h.Next(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, data)
	require.Nil(t, tomb)
	require.Equal(t, engine.ChangesHandle_Hint(0), hint)
}

func TestBranchChangeHandleNextPassthrough(t *testing.T) {
	ctx := context.Background()
	bat := &batch.Batch{}
	fake := &fakeChangesHandle{
		data:      bat,
		tombstone: &batch.Batch{},
		hint:      engine.ChangesHandle_Tail_done,
	}
	h := &BranchChangeHandle{
		handle: fake,
	}

	data, tomb, hint, err := h.Next(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, bat, data)
	require.Equal(t, fake.tombstone, tomb)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
}

func TestBranchChangeHandleNextReappliesPolicies(t *testing.T) {
	fake := &fakeChangesHandle{}
	pkFilter := &engine.PKFilter{
		Segments:      [][]byte{{1, 2, 3}},
		PrimarySeqnum: 7,
	}
	h := &BranchChangeHandle{
		handle:             fake,
		snapshotReadPolicy: engine.SnapshotReadPolicyVisibleState,
		retainRowID:        true,
		pkFilter:           pkFilter,
	}

	_, _, _, err := h.Next(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, engine.SnapshotReadPolicyVisibleState, engine.SnapshotReadPolicyFromContext(fake.lastCtx))
	require.True(t, engine.RetainRowIDFromContext(fake.lastCtx))
	require.Same(t, pkFilter, engine.PKFilterFromContext(fake.lastCtx))
}

func TestBranchChangeHandleNextUnderlyingError(t *testing.T) {
	h := &BranchChangeHandle{
		handle: &fakeChangesHandle{err: moerr.NewInternalErrorNoCtx("next failed")},
	}

	_, _, _, err := h.Next(context.Background(), nil)
	require.Error(t, err)
}

func TestBranchChangeHandleClose(t *testing.T) {
	fake := &fakeChangesHandle{}
	h := &BranchChangeHandle{handle: fake}
	require.NoError(t, h.Close())
	require.True(t, fake.closed)

	empty := &BranchChangeHandle{}
	require.NoError(t, empty.Close())
}

func TestCollectChangesRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(1, 0)
	end := types.BuildTS(2, 0)
	fake := &fakeChangesHandle{}

	rel.EXPECT().CollectChanges(gomock.Any(), from, end, false, gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			_ types.TS,
			_ types.TS,
			_ bool,
			_ *mpool.MPool,
		) (engine.ChangesHandle, error) {
			require.Equal(t, engine.SnapshotReadPolicyVisibleState, engine.SnapshotReadPolicyFromContext(ctx))
			return fake, nil
		},
	)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.NoError(t, err)
	require.IsType(t, &BranchChangeHandle{}, handle)

	actual := handle.(*BranchChangeHandle)
	require.Equal(t, fake, actual.handle)
	require.Equal(t, engine.SnapshotReadPolicyVisibleState, actual.snapshotReadPolicy)
	require.True(t, actual.retainRowID)
}

func TestCollectChangesSkipsEmptyRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(2, 0)
	end := types.BuildTS(1, 0)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.NoError(t, err)

	require.Nil(t, handle)
}

func TestCollectChangesPropagatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(1, 0)
	end := types.BuildTS(3, 0)

	expectedErr := moerr.NewInternalErrorNoCtx("collect failed")
	rel.EXPECT().CollectChanges(gomock.Any(), from, end, false, gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			_ types.TS,
			_ types.TS,
			_ bool,
			_ *mpool.MPool,
		) (engine.ChangesHandle, error) {
			require.Equal(t, engine.SnapshotReadPolicyVisibleState, engine.SnapshotReadPolicyFromContext(ctx))
			return nil, expectedErr
		},
	)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.Nil(t, handle)
	require.ErrorIs(t, err, expectedErr)
}

func TestCollectChangesWithPKFilterPropagatesPoliciesToHandle(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(1, 0)
	end := types.BuildTS(2, 0)
	fake := &fakeChangesHandle{}
	pkFilter := &engine.PKFilter{
		Segments:      [][]byte{{4, 5, 6}},
		PrimarySeqnum: 3,
	}

	rel.EXPECT().CollectChanges(gomock.Any(), from, end, false, gomock.Any()).DoAndReturn(
		func(
			ctx context.Context,
			_ types.TS,
			_ types.TS,
			_ bool,
			_ *mpool.MPool,
		) (engine.ChangesHandle, error) {
			require.Equal(t, engine.SnapshotReadPolicyVisibleState, engine.SnapshotReadPolicyFromContext(ctx))
			require.True(t, engine.RetainRowIDFromContext(ctx))
			require.Same(t, pkFilter, engine.PKFilterFromContext(ctx))
			return fake, nil
		},
	)

	handle, err := CollectChangesWithPKFilter(context.Background(), rel, from, end, nil, pkFilter)
	require.NoError(t, err)
	require.IsType(t, &BranchChangeHandle{}, handle)

	actual := handle.(*BranchChangeHandle)
	require.Equal(t, fake, actual.handle)
	require.Equal(t, engine.SnapshotReadPolicyVisibleState, actual.snapshotReadPolicy)
	require.True(t, actual.retainRowID)
	require.Same(t, pkFilter, actual.pkFilter)
}
