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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataCatalogFailureIsNotInvalidDefinition(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, failedDatabase := range []string{"binding_db", "source_db"} {
			t.Run(fmt.Sprintf("historical=%t/failure=%s", historical, failedDatabase), func(t *testing.T) {
				f := newViewSchemaOwnedFixture(t)
				f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 100})
				ctrl := gomock.NewController(t)
				storage := mock_frontend.NewMockEngine(ctrl)
				probeStorage := mock_frontend.NewMockEngine(ctrl)
				f.session.txnHandler.storage = storage
				f.session.respr = &NullResp{database: "binding_db"}
				f.session.pool = f.proc.Mp()
				previous := getPuIfPresent(f.session.GetService())
				sv := &config.FrontendParameters{}
				sv.SetDefaultValues()
				setPu(f.session.GetService(), config.NewParameterUnit(sv, probeStorage, nil, nil))
				t.Cleanup(func() { setPu(f.session.GetService(), previous) })

				lower := int64(1)
				definition, err := json.Marshal(plan.ViewData{
					Stmt:            "create view binding_db.v as select x from source_db.source",
					DefaultDatabase: "binding_db", LowerCaseTableNames: &lower,
				})
				require.NoError(t, err)
				defs := []*pb.TableDef{
					{Name: "v", DbName: "binding_db", TblId: 100, Version: 3, TableType: catalog.SystemViewRel,
						ViewSql: &pb.ViewDef{View: string(definition)}},
					{Name: "source", DbName: "source_db", TblId: 101, Version: 3,
						Cols: []*pb.ColDef{{Name: "x", Typ: pb.Type{Id: int32(types.T_int64)}}}},
				}
				failure := errors.New("subscription catalog probe failed")
				var injected error = failure
				failures := 0
				for _, def := range defs {
					database := mock_frontend.NewMockDatabase(ctrl)
					database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
					database.EXPECT().GetDatabaseId(gomock.Any()).Return("8").AnyTimes()
					relation := mock_frontend.NewMockRelation(ctrl)
					database.EXPECT().Relation(gomock.Any(), def.Name, gomock.Any()).Return(relation, nil).AnyTimes()
					relation.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
					relation.EXPECT().GetTableDef(gomock.Any()).Return(def).AnyTimes()
					storage.EXPECT().Database(gomock.Any(), def.DbName, gomock.Any()).Return(database, nil).AnyTimes()
					// 只在真实 getSubscriptionMeta 的存储入口注入；存在性探测、
					// 表解析和 binder 仍走各自生产路径，不靠“第几次调用”定位。
					probeStorage.EXPECT().Database(gomock.Any(), def.DbName, gomock.Any()).DoAndReturn(
						func(_ context.Context, name string, op client.TxnOperator) (engine.Database, error) {
							if historical {
								require.Equal(t, int64(50), op.SnapshotTS().PhysicalTime)
							} else {
								require.Same(t, f.op, op)
							}
							if name == failedDatabase && injected != nil {
								failures++
								return nil, injected
							}
							return database, nil
						}).AnyTimes()
				}
				var snapshot *plan.Snapshot
				if historical {
					snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 50}}
				}
				before, err := f.op.(catalogStampReader).CatalogReadStamp()
				require.NoError(t, err)
				request := f.parent.NewViewMetadataRequest(f.ctx, func(context.Context, string, string, *plan.Snapshot) error { return nil })
				defer request.Close()
				for range 2 {
					result, invalid, err := plan.DescribeViewMetadataCandidate(request.Context(), "binding_db", "v", snapshot)
					result.Release()
					require.Nil(t, result)
					require.ErrorIs(t, err, failure)
					require.False(t, invalid, "临时目录错误不能变成 bulk 可跳过的坏定义")
				}
				require.GreaterOrEqual(t, failures, 2, "失败不得被负缓存代替真实重读")
				injected = nil
				result, invalid, err := plan.DescribeViewMetadataCandidate(request.Context(), "binding_db", "v", snapshot)
				if result != nil {
					defer result.Release()
				}
				require.NoError(t, err, "故障恢复后，同请求不得保留错误或半成品")
				require.False(t, invalid)
				columns, err := result.Columns()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.EqualValues(t, types.T_int64, columns[0].Typ.Id)
				result.Release()
				request.Close()
				after, err := f.op.(catalogStampReader).CatalogReadStamp()
				require.NoError(t, err)
				require.Equal(t, before, after)
				budget, err := f.proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				require.Zero(t, budget.Used())
				require.False(t, budget.Closed())
			})
		}
	}
}
