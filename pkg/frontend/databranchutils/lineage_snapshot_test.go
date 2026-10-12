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

package databranchutils

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

func TestAdvanceLineageSnapshot(t *testing.T) {
	const physical int64 = 10000
	requested := timestamp.Timestamp{PhysicalTime: physical + int64(time.Microsecond)}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	transferErr := moerr.NewInternalErrorNoCtx("transfer failed")
	for _, tc := range []struct {
		name             string
		ctx              context.Context
		physical         int64
		missingWorkspace bool
		updated          int64
		advanceErr       error
		wantErr          string
	}{
		{name: "advanced", ctx: context.Background(), physical: physical, updated: requested.PhysicalTime + 1},
		{name: "overflow", ctx: context.Background(), physical: math.MaxInt64 - int64(time.Microsecond) + 1, wantErr: "timestamp limit"},
		{name: "missing workspace", ctx: context.Background(), physical: physical, missingWorkspace: true, wantErr: "missing lineage transaction workspace"},
		{name: "transfer failure", ctx: context.Background(), physical: physical, advanceErr: transferErr, wantErr: "transfer failed"},
		{name: "cancellation", ctx: canceled, physical: physical, advanceErr: context.Canceled, wantErr: "context canceled"},
		{name: "physical boundary not crossed", ctx: context.Background(), physical: physical, updated: requested.PhysicalTime, wantErr: "failed to advance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: tc.physical, LogicalTime: 100})
			if tc.physical <= math.MaxInt64-int64(time.Microsecond) {
				if tc.missingWorkspace {
					op.EXPECT().GetWorkspace().Return(nil)
				} else {
					ws := mock_frontend.NewMockWorkspace(ctrl)
					op.EXPECT().GetWorkspace().Return(ws)
					ws.EXPECT().AdvanceSnapshot(tc.ctx, requested).Return(tc.advanceErr)
					if tc.advanceErr == nil {
						op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: tc.updated})
					}
				}
			}
			cloneTS, err := AdvanceLineageSnapshot(tc.ctx, op)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Zero(t, cloneTS)
				if tc.advanceErr != nil {
					require.ErrorIs(t, err, tc.advanceErr)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.updated-1, cloneTS)
			}
		})
	}
}
