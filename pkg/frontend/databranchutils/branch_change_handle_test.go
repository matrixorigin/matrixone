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
}

func (f *fakeChangesHandle) Next(context.Context, *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
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

func TestBranchChangeHandleNextWithFilter(t *testing.T) {
	ctx := context.Background()
	bat := &batch.Batch{}
	fake := &fakeChangesHandle{
		data:      bat,
		tombstone: &batch.Batch{},
		hint:      engine.ChangesHandle_Tail_done,
	}
	var called bool
	h := &BranchChangeHandle{
		handle: fake,
		filterData: func(b *batch.Batch) error {
			require.Equal(t, bat, b)
			called = true
			return nil
		},
	}

	data, tomb, hint, err := h.Next(ctx, nil)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, bat, data)
	require.Equal(t, fake.tombstone, tomb)
	require.Equal(t, engine.ChangesHandle_Tail_done, hint)
}

func TestBranchChangeHandleNextFilterError(t *testing.T) {
	ctx := context.Background()
	filterErr := moerr.NewInternalErrorNoCtx("filter failed")

	h := &BranchChangeHandle{
		handle: &fakeChangesHandle{data: &batch.Batch{}},
		filterData: func(*batch.Batch) error {
			return filterErr
		},
	}

	_, _, _, err := h.Next(ctx, nil)
	require.ErrorIs(t, err, filterErr)
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

	rel.EXPECT().CollectChanges(gomock.Any(), from, end, false, gomock.Any()).Return(fake, nil)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.NoError(t, err)
	require.IsType(t, &BranchChangeHandle{}, handle)

	actual := handle.(*BranchChangeHandle)
	require.Equal(t, fake, actual.handle)
}

func TestCollectChangesSkipsEmptyRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(2, 0)
	end := types.BuildTS(1, 0)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.NoError(t, err)

	actual := handle.(*BranchChangeHandle)
	require.Nil(t, actual.handle)
}

func TestCollectChangesPropagatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rel := mock_frontend.NewMockRelation(ctrl)
	from := types.BuildTS(1, 0)
	end := types.BuildTS(3, 0)

	expectedErr := moerr.NewInternalErrorNoCtx("collect failed")
	rel.EXPECT().CollectChanges(gomock.Any(), from, end, false, gomock.Any()).Return(nil, expectedErr)

	handle, err := CollectChanges(context.Background(), rel, from, end, nil)
	require.Nil(t, handle)
	require.ErrorIs(t, err, expectedErr)
}
