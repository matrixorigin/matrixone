//go:build integration

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

package frontend_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/models"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func nativeViewMetadataHasApply(scopes []models.PhyScope) bool {
	var hasApply func(*models.PhyOperator) bool
	hasApply = func(op *models.PhyOperator) bool {
		if op == nil {
			return false
		}
		if strings.Contains(strings.ToLower(op.OpName), "apply") {
			return true
		}
		for _, child := range op.Children {
			if hasApply(child) {
				return true
			}
		}
		return false
	}
	for _, scope := range scopes {
		if hasApply(scope.RootOperator) || nativeViewMetadataHasApply(scope.PreScopes) {
			return true
		}
	}
	return false
}

func TestNativeViewMetadataTwoCNOriginOwnership(t *testing.T) {
	nativeViewSchemaShortTempDir(t)
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(defines.AttachAccount(context.Background(), 0, 0, 0), 3*time.Minute)
	defer cancel()
	first, err := cluster.GetCNService(0)
	require.NoError(t, err)
	second, err := cluster.GetCNService(1)
	require.NoError(t, err)
	const database = "view_metadata_origin"
	exec := func(query string) {
		t.Helper()
		result, err := first.RawService().(cnservice.Service).GetSQLExecutor().Exec(ctx, query, executor.Options{}.WithDatabase(database))
		require.NoError(t, err, query)
		result.Close()
	}
	exec("create database " + database)
	defer exec("drop database " + database)
	exec("create table src (id int not null, label varchar(20))")
	exec("insert into src values (1,'one'),(2,'two')")
	exec("create view middle as select id,label from src")
	exec("create view root as select id,label from middle")
	exec("select mo_ctl('dn','flush','" + database + ".src')")
	oldForce := plan.GetForceScanOnMultiCN()
	defer plan.SetForceScanOnMultiCN(oldForce)
	for _, width := range []int{20, 40} {
		if width == 40 {
			plan.SetForceScanOnMultiCN(false)
			exec("alter table src modify column label varchar(40), algorithm=copy")
			exec("select mo_ctl('dn','flush','" + database + ".src')")
		}
		for _, origin := range []embed.ServiceOperator{first, second} {
			inventory := clusterservice.GetMOCluster(origin.ServiceID())
			refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
			require.True(t, ok)
			peer, localAddr := "", ""
			require.Eventually(t, func() bool {
				if refresher.Refresh(ctx) != nil {
					return false
				}
				count := 0
				inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
					count++
					if service.ServiceID != origin.ServiceID() {
						peer = service.PipelineServiceAddress
					} else {
						localAddr = service.PipelineServiceAddress
					}
					return true
				})
				return count == 2 && peer != "" && localAddr != ""
			}, 30*time.Second, 50*time.Millisecond)
			for _, ap := range []bool{false, true} {
				for _, cacheOff := range []bool{false, true} {
					t.Run(fmt.Sprintf("width=%d/origin=%s/AP=%v/cache_off=%v", width, origin.ServiceID(), ap, cacheOff), func(t *testing.T) {
						plan.SetForceScanOnMultiCN(ap)
						withNativeViewSchemaRead(t, ctx, origin, origin.RawService().(cnservice.Service), database, nil, func(compiler *frontend.TxnCompilerContext, op client.TxnOperator, proc *process.Process) {
							before, revision := nativeViewSchemaStamp(t, op)
							var roots atomic.Int64
							authorize := frontend.NativeViewSchemaRoleAuthorizerForTest(compiler)
							request := compiler.NewViewMetadataRequest(ctx, func(ctx context.Context, db, name string, snapshot *plan.Snapshot) error {
								roots.Add(1)
								return authorize(ctx, db, name, snapshot)
							})
							defer request.Close()
							if cacheOff {
								request.DisableMemoForIntegrationTest()
							}
							query := "select concat(column_name,':',lower(column_type)) as value from information_schema.columns where table_schema='" + database + "' and table_name='root' union all select concat('row:',id) from " + database + ".src order by value"
							originalContext := compiler.GetContext()
							compiler.SetContext(request.Context())
							defer compiler.SetContext(originalContext)
							proc.SetIncrStatementDisabled(true)
							statements, err := mysql.Parse(request.Context(), query, 1)
							require.NoError(t, err)
							defer statements[0].Free()
							p, err := plan.BuildPlan(compiler, statements[0], false)
							require.NoError(t, err)
							func() {
								c := compile.NewCompile(localAddr, database, query, "", "",
									origin.RawService().(cnservice.Service).GetEngine(), proc, statements[0], false, nil, time.Now())
								defer c.Release()
								var rows [][]string
								require.NoError(t, c.Compile(request.Context(), p, func(bat *batch.Batch, _ *perfcounter.CounterSet) error {
									if bat != nil {
										for i := 0; i < bat.RowCount(); i++ {
											rows = append(rows, []string{bat.Vecs[0].GetStringAt(i)})
										}
									}
									return nil
								}))
								_, err := c.Run(0)
								require.NoError(t, err)
								require.Equal(t, [][]string{{"id:int"}, {fmt.Sprintf("label:varchar(%d)", width)}, {"row:1"}, {"row:2"}}, rows)
								c.GenPhyPlan(c)
								physical := c.GetAnalyzeModule().GetPhyPlan()
								require.NotNil(t, physical)
								if ap {
									require.False(t, c.IsTpQuery())
									require.NotEmpty(t, physical.RemoteScope, "must collect an executed remote worker report")
								} else {
									require.True(t, c.IsTpQuery())
									require.Empty(t, physical.RemoteScope)
								}
								require.True(t, nativeViewMetadataHasApply(physical.LocalScope))
								require.False(t, nativeViewMetadataHasApply(physical.RemoteScope), "the metadata APPLY must not lose the origin request on a remote CN")
							}()
							require.Positive(t, roots.Load(), "origin-CN provider must execute; default metadata is not an oracle")
							request.Close()
							after, afterRevision := nativeViewSchemaStamp(t, op)
							require.Equal(t, before, after)
							require.Equal(t, revision, afterRevision)
							generation, err := proc.GetExecutionResourceBudget()
							require.NoError(t, err)
							require.Zero(t, generation.Used())
							require.False(t, generation.Closed())
						})
					})
				}
			}
		}
	}
}
