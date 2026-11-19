// Copyright 2022 Matrix Origin
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

package preinsert

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestGenAutoIncrColUnavailableHistoryRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		changed, signal, refreshed bool
		probeErr, refreshErr       error
		want                       uint16
	}{
		{name: "clear"},
		{name: "ordinary_conflict_keeps_private_cache", changed: true, want: moerr.ErrTxnNeedRetry},
		{name: "unavailable_committed", signal: true, refreshed: true, want: moerr.ErrTxnNeedRetry},
		{name: "unavailable_private", signal: true, want: moerr.ErrInternal},
		{name: "refresh_cancelled", signal: true, refreshErr: context.Canceled},
		{name: "current_data_missing", probeErr: moerr.NewFileNotFoundNoCtx("current"), want: moerr.ErrFileNotFound},
		{name: "probe_cancelled", probeErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			op := mock_frontend.NewMockTxnOperator(ctrl)
			op.EXPECT().GetWorkspace().Return(mock_frontend.NewMockWorkspace(ctrl))
			op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 50})
			eng := mock_frontend.NewMockEngine(ctrl)
			db := mock_frontend.NewMockDatabase(ctrl)
			rel := mock_frontend.NewMockRelation(ctrl)
			eng.EXPECT().Database(gomock.Any(), "d", op).Return(db, nil)
			db.EXPECT().Relation(gomock.Any(), "t", nil).Return(rel, nil)
			incr := mock_frontend.NewMockAutoIncrementService(ctrl)
			incr.EXPECT().GetLastAllocateTS(gomock.Any(), uint64(42), uint32(3), op, "id").Return(timestamp.Timestamp{PhysicalTime: 10}, nil)
			incr.EXPECT().InsertValues(gomock.Any(), uint64(42), uint32(3), op, gomock.Any(), 1, int64(1)).Return(uint64(7), nil)
			probeErr := tc.probeErr
			if tc.signal {
				probeErr = moerr.NewTxnNeedRetry(context.Background())
				incr.EXPECT().ReloadForPKRetry(gomock.Any(), uint64(42), uint32(3), op).Return(tc.refreshed, tc.refreshErr)
			}
			rel.EXPECT().PrimaryKeysMayBeUpserted(gomock.Any(), types.BuildTS(10, 0), types.BuildTS(50, 0), gomock.Any(), int32(0)).Return(tc.changed, probeErr)
			proc := testutil.NewProc(t)
			proc.Base.TxnOperator = op
			proc.Base.IncrService = incr
			proc.Base.SessionInfo.StorageEngine = eng
			pre := &PreInsert{HasAutoCol: true, SchemaName: "d", Attrs: []string{"id"}, EstimatedRowCount: 1, TableDef: &plan.TableDef{Name: "t", TblId: 42, AutoIncrEpoch: 3, Cols: []*plan.ColDef{{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), AutoIncr: true}}}, Pkey: &plan.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}}}}
			pre.ctr.tblId = 42
			bat := batch.NewWithSize(1)
			defer bat.Clean(proc.Mp())
			bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(bat.Vecs[0], int64(0), true, proc.Mp()))
			bat.SetRowCount(1)
			err := genAutoIncrCol(bat, proc, pre)
			if tc.want != 0 {
				require.True(t, moerr.IsMoErrCode(err, tc.want), "%v", err)
			} else if tc.probeErr == context.Canceled || tc.refreshErr == context.Canceled {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
