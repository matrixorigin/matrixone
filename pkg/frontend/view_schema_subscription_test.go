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

package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestViewSchemaSubscriptionCrossDatabaseDescribe(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, historical := range []bool{false, true} {
			for _, reuseRequest := range []bool{false, true} {
				t.Run(fmt.Sprintf("nested=%t/history=%t/reuse=%t", nested, historical, reuseRequest), func(t *testing.T) {
					f := newViewSchemaOwnedFixture(t)
					f.op.SetSnapshotTS(timestamp.Timestamp{PhysicalTime: 100})
					parentSub := &pb.SubscriptionMeta{AccountId: 23, SubName: "sub", DbName: "pub", Tables: "*"}
					f.parent.SetQueryingSubscription(parentSub)
					ctrl := gomock.NewController(t)
					storage := mock_frontend.NewMockEngine(ctrl)
					f.session.txnHandler.storage = storage
					f.session.respr = &NullResp{database: "binding_db"}
					f.session.pool = f.proc.Mp()
					previousPU := getPuIfPresent(f.session.GetService())
					sv := &config.FrontendParameters{}
					sv.SetDefaultValues()
					setPu(f.session.GetService(), config.NewParameterUnit(sv, storage, nil, nil))
					t.Cleanup(func() { setPu(f.session.GetService(), previousPU) })
					lower := int64(1)
					viewDef := func(name, database, query string, id uint64) *pb.TableDef {
						encoded, err := json.Marshal(plan.ViewData{Stmt: "create view " + database + "." + name + " as " + query,
							DefaultDatabase: "", LowerCaseTableNames: &lower})
						require.NoError(t, err)
						return &pb.TableDef{Name: name, DbName: database, TblId: id, Version: 3, TableType: catalog.SystemViewRel,
							ViewSql: &pb.ViewDef{View: string(encoded)}}
					}
					query := "select x from other.source"
					if nested {
						query = "select x from other.inner_v"
					}
					defs := map[string]*pb.TableDef{
						"pub.published_v": viewDef("published_v", "pub", query, 100),
						"other.inner_v":   viewDef("inner_v", "other", "select x from other.source", 101),
						"other.source":    {Name: "source", DbName: "other", TblId: 99, Version: 3, Cols: []*pb.ColDef{{Name: "x", Typ: pb.Type{Id: int32(types.T_int64)}}}},
					}
					var sourceReads []uint32
					for _, dbName := range []string{"pub", "other"} {
						database := mock_frontend.NewMockDatabase(ctrl)
						database.EXPECT().IsSubscription(gomock.Any()).Return(false).AnyTimes()
						database.EXPECT().GetDatabaseId(gomock.Any()).Return("8").AnyTimes()
						storage.EXPECT().Database(gomock.Any(), dbName, gomock.Any()).Return(database, nil).AnyTimes()
						for key, def := range defs {
							if def.DbName != dbName {
								continue
							}
							relation := mock_frontend.NewMockRelation(ctrl)
							database.EXPECT().Relation(gomock.Any(), def.Name, gomock.Any()).Return(relation, nil).AnyTimes()
							relation.EXPECT().GetTableID(gomock.Any()).Return(def.TblId).AnyTimes()
							relation.EXPECT().GetTableDef(gomock.Any()).DoAndReturn(func(ctx context.Context) *pb.TableDef {
								if key == "other.source" {
									account, err := defines.GetAccountId(ctx)
									require.NoError(t, err)
									sourceReads = append(sourceReads, account)
									if account != 23 {
										return &pb.TableDef{Name: "source", DbName: "other", TblId: 13,
											Cols: []*pb.ColDef{{Name: "x", Typ: pb.Type{Id: int32(types.T_varchar)}}}}
									}
								}
								return def
							}).AnyTimes()
						}
					}
					provider := &viewSchemaProvider{parent: f.parent, authorize: func(ctx context.Context, _, _ string, _ *plan.Snapshot) error {
						account, err := defines.GetAccountId(ctx)
						require.NoError(t, err)
						require.Equal(t, uint32(7), account, "root authorization remains subscriber-owned")
						return nil
					}}
					var snapshot *plan.Snapshot
					if historical {
						snapshot = &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 50}, Tenant: &pb.SnapshotTenant{TenantID: 7}}
					}
					beforeSnapshot := plan.DeepCopySnapshot(snapshot)
					r := plan.NewViewSchemaRequest(f.ctx, provider)
					t.Cleanup(r.Close)
					for i := 0; i < 2; i++ {
						if i == 1 && !reuseRequest {
							r.Close()
							r = plan.NewViewSchemaRequest(f.ctx, provider)
							t.Cleanup(r.Close)
						}
						result, err := r.Describe("pub", "published_v", snapshot)
						if result != nil {
							t.Cleanup(result.Release)
						}
						require.NoError(t, err)
						columns, err := result.Columns()
						require.NoError(t, err)
						require.Equal(t, int32(types.T_int64), columns[0].Typ.Id)
						dependencies, err := result.Dependencies()
						require.NoError(t, err)
						found := false
						for _, dep := range dependencies {
							if dep.RelationName == "source" || dep.RelationName == "inner_v" {
								require.Equal(t, uint32(23), dep.AccountID)
								require.Equal(t, "other", dep.DatabaseName)
								found = found || dep.RelationName == "source"
							}
						}
						require.True(t, found)
						result.Release()
						require.Same(t, parentSub, f.parent.GetQueryingSubscription())
						require.Equal(t, beforeSnapshot, snapshot)
					}
					require.NotEmpty(t, sourceReads)
					for _, account := range sourceReads {
						require.Equal(t, uint32(23), account)
					}
				})
			}
		}
	}
}
