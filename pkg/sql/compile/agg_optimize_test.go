// Copyright 2021 Matrix Origin
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

package compile

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCheckAggOptimizeBoolMax(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      types.T
		aggs     []string
		notNull  bool
		eligible bool
	}{
		{"nullable max", types.T_bool, []string{"max"}, false, false},
		{"not null max", types.T_bool, []string{"max"}, true, false},
		{"min before max", types.T_bool, []string{"min", "max"}, false, false},
		{"max before min", types.T_bool, []string{"max", "min"}, false, false},
		{"count before max", types.T_bool, []string{"count", "max"}, false, false},
		{"literal count before max", types.T_bool, []string{"literal count", "max"}, false, false},
		{"bool min", types.T_bool, []string{"min"}, false, true},
		{"integer max", types.T_int64, []string{"max"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &plan.Node{TableDef: &plan.TableDef{Cols: []*plan.ColDef{
				{Seqnum: 3, Typ: plan.Type{Id: int32(tc.typ), NotNullable: tc.notNull}},
			}}}
			for _, name := range tc.aggs {
				if name == "literal count" {
					node.AggList = append(node.AggList, &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
						Func: &plan.ObjectRef{ObjName: "count"},
						Args: []*plan.Expr{{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}},
					}}})
					continue
				}
				node.AggList = append(node.AggList, &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: name},
					Args: []*plan.Expr{{Typ: plan.Type{Id: int32(tc.typ)}, Expr: &plan.Expr_Col{
						Col: &plan.ColRef{ColPos: 0},
					}}},
				}}})
			}
			results, resultTypes, columns := checkAggOptimize(node)
			if tc.eligible {
				require.Len(t, results, len(tc.aggs))
				require.Len(t, resultTypes, len(tc.aggs))
				require.Equal(t, map[int]int{0: 3}, columns)
			} else {
				require.Nil(t, results)
				require.Nil(t, resultTypes)
				require.Nil(t, columns)
				if tc.name == "literal count before max" {
					require.Equal(t, "count", node.AggList[0].GetF().Func.ObjName, "回落前不应部分改写聚合")
				}
			}
		})
	}
}

// wantObj: expected overload id after rewrite; 0 means do not assert (e.g. count unchanged).
var wantCountStarObj = function.EncodeOverloadID(int32(function.STARCOUNT), 0)

func TestCheckAggOptimize_CountNotNull(t *testing.T) {
	tests := []struct {
		name              string
		node              *plan.Node
		wantObjName       string
		wantObj           int64 // 0 = skip assert (e.g. count unchanged)
		wantColumnMapSize int
	}{
		{
			name: "COUNT(not_null_col) should rewrite to starcount and set Obj to CountStar",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
						{Name: "name", Seqnum: 1, Typ: plan.Type{NotNullable: false}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 0},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "starcount",
			wantObj:           wantCountStarObj,
			wantColumnMapSize: 0,
		},
		{
			name: "COUNT(lit) should rewrite to starcount and set Obj to CountStar",
			node: &plan.Node{
				TableDef: &plan.TableDef{Cols: []*plan.ColDef{}},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
								},
							},
						},
					},
				},
			},
			wantObjName:       "starcount",
			wantObj:           wantCountStarObj,
			wantColumnMapSize: 0,
		},
		{
			name: "COUNT(nullable_col) should not rewrite",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
						{Name: "name", Seqnum: 1, Typ: plan.Type{NotNullable: false}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{ObjName: "count"},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 1},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "count",
			wantObj:           0,
			wantColumnMapSize: 1,
		},
		{
			name: "COUNT(DISTINCT not_null_col) should not optimize",
			node: &plan.Node{
				TableDef: &plan.TableDef{
					Cols: []*plan.ColDef{
						{Name: "id", Seqnum: 0, Typ: plan.Type{NotNullable: true}},
					},
				},
				AggList: []*plan.Expr{
					{
						Expr: &plan.Expr_F{
							F: &plan.Function{
								Func: &plan.ObjectRef{
									ObjName: "count",
									Obj:     0,
								},
								Args: []*plan.Expr{
									{
										Expr: &plan.Expr_Col{
											Col: &plan.ColRef{ColPos: 0},
										},
									},
								},
							},
						},
					},
				},
			},
			wantObjName:       "",
			wantObj:           0,
			wantColumnMapSize: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set Distinct flag if test name contains "DISTINCT"
			if tt.name == "COUNT(DISTINCT not_null_col) should not optimize" {
				tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.Obj = -9223372036854775808
			}

			partialResults, partialResultTypes, columnMap := checkAggOptimize(tt.node)

			if tt.wantObjName == "" {
				require.Nil(t, partialResults)
				require.Nil(t, partialResultTypes)
				require.Nil(t, columnMap)
			} else {
				require.NotNil(t, partialResults)
				require.NotNil(t, partialResultTypes)
				require.NotNil(t, columnMap)

				actualObjName := tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.ObjName
				require.Equal(t, tt.wantObjName, actualObjName)
				if tt.wantObj != 0 {
					actualObj := tt.node.AggList[0].Expr.(*plan.Expr_F).F.Func.Obj
					require.Equal(t, tt.wantObj, actualObj, "Obj (overload id) must be CountStar so runtime uses countStarExec")
				}
				require.Equal(t, tt.wantColumnMapSize, len(columnMap))
			}
		})
	}
}

func TestScopeAggOptimizeWithSeparateAggregateKeepsInput(t *testing.T) {
	data := readutil.NewBlockListRelationData(0)
	data.AppendBlockInfo(&objectio.EmptyBlockInfo)
	data.AppendBlockInfo(&objectio.BlockInfo{BlockID: types.Blockid{1}})
	node := zmAggNode(nil, zmAggFuncExpr("starcount", zmAggConstExpr(), 0))
	s := &Scope{
		DataSource: &Source{node: node},
		NodeInfo:   engine.Node{Mcpu: 1, Data: data},
	}
	// Any attempt to compute partials would call a method on the missing
	// relation. A separated scan must simply preserve its original input.
	require.NoError(t, s.aggOptimize(&Compile{}, nil, t.Context()))
	require.Same(t, data, s.NodeInfo.Data)
	require.False(t, s.StarCountOnly)
}

type starCountBlockedMetadataFS struct {
	fileservice.FileService
	entered chan<- context.Context
}

func (fs starCountBlockedMetadataFS) Read(ctx context.Context, _ *fileservice.IOVector) error {
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.FileService.S3.Get.Add(1)
		counter.FileService.S3ReadSize.Add(64)
	})
	fs.entered <- ctx
	<-ctx.Done()
	return ctx.Err()
}

func TestScopeSealedCountIOUsesScannerCancellationAndAccounting(t *testing.T) {
	queryCtx, cancelQuery := context.WithCancel(t.Context())
	defer cancelQuery()
	topCtx := defines.AttachAccountId(queryCtx, 42)
	stats := &statistic.StatsInfo{}
	topCtx = statistic.ContextWithStatsInfo(topCtx, stats)
	deadline, end := context.WithTimeout(t.Context(), 5*time.Second)
	defer end()
	child, stop := context.WithCancel(defines.AttachAccountId(deadline, 17))
	defer stop()

	c := NewMockCompile(t)
	t.Cleanup(c.proc.Free)
	s := &Scope{Proc: &process.Process{Ctx: child}}
	scan := &table_scan.TableScan{}
	entered := make(chan context.Context, 1)
	result := make(chan error, 1)
	done := make(chan struct{})
	fs := starCountBlockedMetadataFS{entered: entered}
	id := objectio.NewObjectid()
	name := objectio.BuildObjectNameWithObjectID(&id)
	loc := objectio.BuildLocation(name, objectio.NewExtent(0, 0, 64, 64), 1, 0)

	go func() {
		defer close(done)
		result <- scan.MeasureReaderStartup(s.countSealedBlocksContext(c, topCtx), func(ctx context.Context, analyzer process.Analyzer) error {
			_, err := process.MeasureFilesystemWait(analyzer, func() (objectio.ObjectMeta, error) {
				return objectio.FastLoadObjectMeta(ctx, &loc, false, fs)
			})
			return err
		})
	}()
	defer func() {
		stop()
		cancelQuery()
		<-done
	}()
	select {
	case readCtx := <-entered:
		account, err := defines.GetAccountId(readCtx)
		require.NoError(t, err)
		require.Equal(t, uint32(42), account)
		require.Same(t, stats, statistic.StatsInfoFromContext(readCtx))
	case <-deadline.Done():
		t.Fatal("metadata read did not start")
	}
	stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-deadline.Done():
		t.Fatal("the scope cancellation did not wake the metadata read")
	}
	require.NoError(t, topCtx.Err())

	owner := scan.OpAnalyzer.GetOpStats()
	startup := owner.ResourceDelta()
	require.Positive(t, startup.Usage.ExclusiveActiveNS)
	require.Positive(t, startup.Usage.WaitNS[resource.WaitFilesystem])
	require.Equal(t, uint64(1), startup.Usage.S3Requests[resource.S3Get])
	require.Equal(t, uint64(64), startup.Usage.S3ReadBytes)
	require.Zero(t, owner.CallNum)
	require.Zero(t, owner.TimeConsumed)
	require.Zero(t, owner.WaitTimeConsumed)
	require.Zero(t, owner.S3Get)
	require.NoError(t, scan.Prepare(c.proc))
	t.Cleanup(func() { scan.Free(c.proc, false, nil) })
	require.Equal(t, startup, owner.ResourceDelta())
	require.Zero(t, owner.CallNum)

	// The ordinary reader reports its own interval on the same owner.
	owner.CallNum = 1
	owner.TimeConsumed = 100
	owner.WaitTimeConsumed = 200
	owner.ResourceWaitNS[resource.WaitFilesystem] = 200
	owner.S3Get = 2
	owner.S3ReadSize = 128
	combined := owner.ResourceDelta()
	require.Equal(t, startup.Usage.ExclusiveActiveNS+100, combined.Usage.ExclusiveActiveNS)
	require.Equal(t, startup.Usage.WaitNS[resource.WaitFilesystem]+200, combined.Usage.WaitNS[resource.WaitFilesystem])
	require.Equal(t, uint64(3), combined.Usage.S3Requests[resource.S3Get])
	require.Equal(t, uint64(192), combined.Usage.S3ReadBytes)
	require.Equal(t, 1, owner.CallNum)

	require.NoError(t, scan.Prepare(c.proc))
	require.Empty(t, owner.ResourceDelta(), "reusing the scan must clear the startup interval")
}
