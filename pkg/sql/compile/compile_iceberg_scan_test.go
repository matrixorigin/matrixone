// Copyright 2026 Matrix Origin
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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/toml"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestCompileIcebergScanWithInjectedPlanner(t *testing.T) {
	testCompile := NewMockCompile(t)
	enableProtectedIcebergCNToCNForTest(t, testCompile)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           42,
			SchemaID:             7,
			PartitionSpecIDs:     []int{0},
			MetadataLocationHash: "meta-hash",
			ManifestListHash:     "manifest-hash",
			RefName:              "main",
			PlanningMode:         "client-side",
		},
		DataTasks: []api.DataFileTask{
			{
				DataFile: api.DataFile{
					FilePath:        "warehouse/iceberg/orders/part-0.parquet",
					FileFormat:      "parquet",
					FileSizeInBytes: 200,
					RecordCount:     20,
					SpecID:          0,
				},
				CredentialScope: "scope-0",
			},
			{
				DataFile: api.DataFile{
					FilePath:        "warehouse/iceberg/orders/part-1.parquet",
					FileFormat:      "parquet",
					FileSizeInBytes: 100,
					RecordCount:     10,
					SpecID:          0,
				},
				CredentialScope: "scope-1",
			},
		},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Required: true, Projected: true, ParquetFieldID: 1},
		},
		Profile: api.PlanningProfile{
			MetadataBytes:         10,
			ManifestListBytes:     20,
			ManifestBytes:         30,
			ManifestsSelected:     2,
			ManifestsPruned:       1,
			DataFilesSelected:     2,
			DataFilesPruned:       3,
			DataFileBytesSelected: 300,
			DataFileBytesPruned:   400,
			PlanningCacheHits:     4,
			PlanningCacheMiss:     5,
		},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		Stats:    &plan.Stats{Outcnt: 99, TableCnt: 99},
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{
				CatalogId:         7,
				Namespace:         "sales.orders",
				Table:             "orders",
				Ref:               "audit_branch",
				ProjectedFieldIds: []int32{1},
			},
			TbColToDataCol: map[string]int32{},
		},
	}

	scopes, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Equal(t, 1, planner.calls, "multi-CN scan planning must run once on the coordinator")
	require.Equal(t, uint64(7), planner.req.Catalog.CatalogID)
	require.Equal(t, api.Namespace{"sales", "orders"}, planner.req.Namespace)
	require.Equal(t, "orders", planner.req.Table)
	require.Equal(t, "audit_branch", planner.req.Ref)
	require.Equal(t, "audit_branch", planner.req.Snapshot.RefName)
	require.Equal(t, []int{1}, planner.req.ProjectionIDs)
	require.Empty(t, planner.req.PrunePredicates)
	require.Len(t, scopes, 2)
	require.Equal(t, int32(2), node.Stats.BlockNum)
	require.Equal(t, float64(300), node.Stats.Cost)
	require.Equal(t, float64(99), node.Stats.Outcnt)
	require.Equal(t, float64(99), node.Stats.TableCnt)

	seen := make(map[string]bool)
	planningStatsScopes := 0
	for _, scope := range scopes {
		require.NoError(t, checkScopeWithExpectedList(scope, []vm.OpType{vm.External}))
		ext := scope.RootOp.(*external.External)
		require.Equal(t, int32(plan.ExternType_ICEBERG_TB), ext.Es.Extern.ExternType)
		require.NotNil(t, ext.Es.IcebergSnapshot)
		require.Equal(t, int64(42), ext.Es.IcebergSnapshot.SnapshotId)
		require.Len(t, ext.Es.IcebergColumns, 1)
		require.Len(t, ext.Es.IcebergDataTasks, len(ext.Es.FileList))
		if !ext.Es.IcebergPlanningStats.Empty() {
			planningStatsScopes++
			require.Equal(t, int64(10), ext.Es.IcebergPlanningStats.IcebergMetadataBytes)
			require.Equal(t, int64(20), ext.Es.IcebergPlanningStats.IcebergManifestListBytes)
			require.Equal(t, int64(30), ext.Es.IcebergPlanningStats.IcebergManifestBytes)
			require.Equal(t, int64(2), ext.Es.IcebergPlanningStats.IcebergManifestsSelected)
			require.Equal(t, int64(1), ext.Es.IcebergPlanningStats.IcebergManifestsPruned)
			require.Equal(t, int64(2), ext.Es.IcebergPlanningStats.IcebergDataFilesSelected)
			require.Equal(t, int64(3), ext.Es.IcebergPlanningStats.IcebergDataFilesPruned)
			require.Equal(t, int64(300), ext.Es.IcebergPlanningStats.IcebergDataFileBytesSelected)
			require.Equal(t, int64(400), ext.Es.IcebergPlanningStats.IcebergDataFileBytesPruned)
			require.Equal(t, int64(4), ext.Es.IcebergPlanningStats.IcebergPlanningCacheHits)
			require.Equal(t, int64(5), ext.Es.IcebergPlanningStats.IcebergPlanningCacheMiss)
		}
		for i, task := range ext.Es.IcebergDataTasks {
			require.Equal(t, task.FilePath, ext.Es.FileList[i])
			require.Equal(t, task.FileSize, ext.Es.FileSize[i])
			seen[task.FilePath] = true
		}
	}
	require.Equal(t, 1, planningStatsScopes)
	require.Equal(t, map[string]bool{
		"warehouse/iceberg/orders/part-0.parquet": true,
		"warehouse/iceberg/orders/part-1.parquet": true,
	}, seen)
}

func TestCompileIcebergScanBlocksRemoteCredentialFanoutWithoutProtectedTransport(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}, {Addr: "cn2:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_MULTICN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	testCompile.icebergScanPlanner = &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{
			{
				DataFile:        api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 200, RecordCount: 20},
				CredentialScope: "scope-0",
			},
			{
				DataFile:        api.DataFile{FilePath: "warehouse/iceberg/orders/part-1.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10},
				CredentialScope: "scope-1",
			},
		},
	}}

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}
	_, err := testCompile.compileIcebergScan(node, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrRemoteSigningDenied))
	require.Contains(t, err.Error(), "protected CN-to-CN transport")
}

func TestCompileIcebergScanUsesRuntimeRegisteredPlanner(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	restoreIcebergPlannerRuntimeForTest(t, planner)

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}
	scopes, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	require.Equal(t, "orders", planner.req.Table)
}

func TestCompileIcebergScanPlansOnEveryCompile(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plans: []*api.IcebergScanPlan{
		{
			DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
		},
		{
			DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-1.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
		},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}
	first, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	second, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Equal(t, 2, planner.calls)
	require.Equal(t, "warehouse/iceberg/orders/part-0.parquet", first[0].RootOp.(*external.External).Es.FileList[0])
	require.Equal(t, "warehouse/iceberg/orders/part-1.parquet", second[0].RootOp.(*external.External).Es.FileList[0])
}

func TestCompileIcebergScanPassesAccessContextToPlanner(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}
	access := icebergScanAccessContext{
		catalog: model.Catalog{
			AccountID:      42,
			CatalogID:      7,
			Name:           "prod",
			Type:           "rest",
			URI:            "https://catalog.example.com/iceberg",
			Warehouse:      "warehouse_a",
			AuthMode:       model.AuthModeCredential,
			TokenSecretRef: "secret://iceberg/prod",
		},
		externalPrincipal: "ksa-analytics",
		residencyPolicies: []model.ResidencyPolicy{{
			ScopeType:         model.ResidencyScopeCluster,
			CatalogID:         7,
			AllowedCatalogURI: "https://catalog.example.com/iceberg",
			AllowedEndpoint:   "s3.me-central-1.amazonaws.com",
			AllowedRegion:     "me-central-1",
			AllowedBucket:     "warehouse",
			PolicyState:       model.ResidencyPolicyEnabled,
		}},
	}

	_, err := testCompile.compileIcebergScanWithAccess(node, true, access)
	require.NoError(t, err)
	require.Equal(t, access.catalog, planner.req.Catalog)
	require.Equal(t, "ksa-analytics", planner.req.ExternalPrincipal)
	require.Equal(t, api.Namespace{"sales"}, planner.req.Namespace)
	require.Equal(t, "orders", planner.req.Table)
	require.Len(t, planner.req.ResidencyPolicies, 1)
	require.NotNil(t, planner.req.CatalogValidator)
	require.NotNil(t, planner.req.ObjectResidencyValidator)
	require.NoError(t, planner.req.CatalogValidator(context.Background(), api.CatalogRequest{Catalog: access.catalog}))
	require.NoError(t, planner.req.ObjectResidencyValidator(context.Background(), api.ObjectResidencyRequest{
		AccountID: 42,
		CatalogID: 7,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "me-central-1",
		Bucket:    "warehouse",
	}))
	err = planner.req.ObjectResidencyValidator(context.Background(), api.ObjectResidencyRequest{
		AccountID: 42,
		CatalogID: 7,
		Endpoint:  "s3.me-central-1.amazonaws.com",
		Region:    "us-east-1",
		Bucket:    "warehouse",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrResidencyDenied))
}

func TestCompileIcebergScanRejectsInvalidRuntimePlanner(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	restoreIcebergPlannerRuntimeForTest(t, "not-a-planner")
	node := &plan.Node{ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB), IcebergScan: &plan.IcebergScan{}}}

	_, err := testCompile.compileIcebergScan(node, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

func TestIcebergRemoteFanoutPolicyAllowsObjectRefWithRemoteSigning(t *testing.T) {
	testCompile := NewMockCompile(t)
	setIcebergConfigForTest(t, testCompile, func(params *config.IcebergParameters) {
		params.EnableRemoteSigning = true
	})
	testCompile.addr = "cn1:6001"

	err := testCompile.validateIcebergRemoteFanoutPolicy(nil, icebergExternalScanRuntime{
		objectIORef: "remote-signing-object-ref",
		dataTasks:   []*pipeline.IcebergDataFileTask{{FilePath: "warehouse/iceberg/orders/part-0.parquet"}},
	}, []icebergDataFileScopeShard{
		{node: engine.Node{Addr: "cn1:6001"}},
		{node: engine.Node{Addr: "cn2:6001"}},
	})
	require.NoError(t, err)
}

func TestIcebergRemoteFanoutPolicyBlocksObjectRefWhenRemoteSigningDisabled(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.addr = "cn1:6001"

	err := testCompile.validateIcebergRemoteFanoutPolicy(nil, icebergExternalScanRuntime{
		objectIORef: "remote-signing-object-ref",
		dataTasks:   []*pipeline.IcebergDataFileTask{{FilePath: "warehouse/iceberg/orders/part-0.parquet"}},
	}, []icebergDataFileScopeShard{
		{node: engine.Node{Addr: "cn1:6001"}},
		{node: engine.Node{Addr: "cn2:6001"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrRemoteSigningDenied))
	require.Contains(t, err.Error(), "remote signing")
}

func TestIcebergRemoteFanoutPolicyBlocksCredentialScopeEvenWithRemoteSigning(t *testing.T) {
	testCompile := NewMockCompile(t)
	setIcebergConfigForTest(t, testCompile, func(params *config.IcebergParameters) {
		params.EnableRemoteSigning = true
	})
	testCompile.addr = "cn1:6001"

	err := testCompile.validateIcebergRemoteFanoutPolicy(nil, icebergExternalScanRuntime{
		objectIORef: "remote-signing-object-ref",
		dataTasks: []*pipeline.IcebergDataFileTask{{
			FilePath:        "warehouse/iceberg/orders/part-0.parquet",
			CredentialScope: "vended-scope",
		}},
	}, []icebergDataFileScopeShard{
		{node: engine.Node{Addr: "cn1:6001"}},
		{node: engine.Node{Addr: "cn2:6001"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrRemoteSigningDenied))
	require.Contains(t, err.Error(), "remote credential fanout")
}

func TestCompileIcebergScanPassesPlanningTimeoutFromParameterUnit(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}

	var params config.FrontendParameters
	params.SetDefaultValues()
	params.Iceberg.PlanningTimeout = toml.Duration{Duration: 7 * time.Second}
	pu := config.NewParameterUnit(&params, nil, nil, nil)
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	testCompile.proc.Ctx = ctx
	testCompile.proc.ReplaceTopCtx(ctx)

	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}
	_, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Equal(t, "7s", planner.req.PlanningTimeout)
}

func enableProtectedIcebergCNToCNForTest(t *testing.T, c *Compile) {
	t.Helper()
	setIcebergConfigForTest(t, c, func(params *config.IcebergParameters) {
		params.ProtectedCNToCN = true
	})
}

func setIcebergConfigForTest(t *testing.T, c *Compile, mutate func(*config.IcebergParameters)) {
	t.Helper()
	var params config.FrontendParameters
	params.SetDefaultValues()
	if mutate != nil {
		mutate(&params.Iceberg)
	}
	pu := config.NewParameterUnit(&params, nil, nil, nil)
	ctx := context.WithValue(context.Background(), config.ParameterUnitKey, pu)
	c.proc.Ctx = ctx
	c.proc.ReplaceTopCtx(ctx)
}

func restoreIcebergPlannerRuntimeForTest(t *testing.T, value any) {
	t.Helper()
	rt := moruntime.ServiceRuntime("")
	old, hadOld := rt.GetGlobalVariables(IcebergScanPlannerRuntimeKey)
	rt.SetGlobalVariables(IcebergScanPlannerRuntimeKey, value)
	t.Cleanup(func() {
		if hadOld {
			rt.SetGlobalVariables(IcebergScanPlannerRuntimeKey, old)
		} else {
			rt.SetGlobalVariables(IcebergScanPlannerRuntimeKey, nil)
		}
	})
}

func TestCompileIcebergScanExtractsSafePrunePredicates(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}, {Name: "name"}}},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{
				CatalogId:         7,
				Namespace:         "sales",
				Table:             "orders",
				ProjectedFieldIds: []int32{11, 12},
			},
			TbColToDataCol: map[string]int32{},
		},
		FilterList: []*plan.Expr{
			icebergTestCmp(">", icebergTestCol(0), icebergTestI64(100)),
			icebergTestCmp("<", icebergTestI64(200), icebergTestCol(0)),
			icebergTestCmp("=", icebergTestCol(1), icebergTestString("alice")),
			icebergTestCmp(">", icebergTestCol(0), icebergTestTimestamp(1767225600000000)),
		},
	}

	_, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Equal(t, []api.PrunePredicate{
		{FieldID: 11, Op: api.PruneOpGT, Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 100}},
		{FieldID: 11, Op: api.PruneOpGT, Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 200}},
		{FieldID: 12, Op: api.PruneOpEQ, Literal: api.PruneLiteral{Kind: api.TypeString, String: "alice"}},
		{FieldID: 11, Op: api.PruneOpGT, Literal: api.PruneLiteral{Kind: api.TypeTimestampTZ, Int64: 1767225600000000, Normalized: true}},
	}, planner.req.PrunePredicates)
	require.NotEmpty(t, planner.req.ResidualSQL)
	require.Contains(t, planner.req.ResidualSQL, "filter_digest:")
	require.NotEmpty(t, node.ExternScan.IcebergScan.FilterDigest)
	require.Len(t, node.FilterList, 4, "metadata pruning hints must not remove residual SQL filters")
	require.True(t, planner.req.EnableRowGroupPlanning)
}

func TestCompileIcebergScanReplansWithFieldIDsFromColumnMapping(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	firstPlan := &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{
			{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-old.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 2}},
			{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-new.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 2}},
		},
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 4, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 4},
		},
	}
	secondPlan := *firstPlan
	secondPlan.DataTasks = []api.DataFileTask{
		{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-new.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 2}},
	}
	planner := &fakeCompileIcebergPlanner{plans: []*api.IcebergScanPlan{firstPlan, &secondPlan}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}, {Name: "amount"}}},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
		FilterList: []*plan.Expr{
			icebergTestCmp(">=", icebergTestCol(0), icebergTestI64(3)),
		},
	}

	_, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Equal(t, 2, planner.calls)
	require.Empty(t, planner.reqs[0].PrunePredicates)
	require.Empty(t, planner.reqs[0].ProjectionIDs)
	require.Equal(t, []int{1, 4}, planner.reqs[1].ProjectionIDs)
	require.Equal(t, []api.PrunePredicate{
		{FieldID: 1, Op: api.PruneOpGTE, Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 3}},
	}, planner.reqs[1].PrunePredicates)
	require.True(t, planner.reqs[1].EnableRowGroupPlanning)
	require.Equal(t, []int32{1, 4}, node.ExternScan.IcebergScan.ProjectedFieldIds)
	require.Equal(t, int32(1), node.Stats.BlockNum)
}

func TestCompileIcebergScanSkipsPrunePredicatesWhenFieldIDMappingIsAmbiguous(t *testing.T) {
	node := &plan.Node{
		TableDef:   &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}, {Name: "name"}}},
		ExternScan: &plan.ExternScan{IcebergScan: &plan.IcebergScan{ProjectedFieldIds: []int32{11}}},
		FilterList: []*plan.Expr{
			icebergTestCmp(">", icebergTestCol(0), icebergTestI64(100)),
		},
	}
	require.Empty(t, icebergPrunePredicatesFromNode(node))
}

func TestCompileIcebergScanPrunePredicateExtractionBoundaries(t *testing.T) {
	mismatched := &plan.Node{
		TableDef:   &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}, {Name: "name"}}},
		ExternScan: &plan.ExternScan{IcebergScan: &plan.IcebergScan{ProjectedFieldIds: []int32{11}}},
		FilterList: []*plan.Expr{
			icebergTestCmp(">", icebergTestCol(0), icebergTestI64(100)),
		},
	}
	require.Empty(t, icebergPrunePredicatesFromNode(mismatched), "field ids must align one-for-one with table columns")
	require.Len(t, mismatched.FilterList, 1, "prune hints must not consume residual filters")

	flipped := &plan.Node{
		TableDef:   &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}, {Name: "name"}}},
		ExternScan: &plan.ExternScan{IcebergScan: &plan.IcebergScan{ProjectedFieldIds: []int32{11, 12}}},
		FilterList: []*plan.Expr{
			icebergTestCmp("<=", icebergTestI64(5), icebergTestCol(1)),
			icebergTestCmp("=", icebergTestCol(0), &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timeval{Timeval: 123}}}}),
		},
	}
	require.Equal(t, []api.PrunePredicate{
		{FieldID: 12, Op: api.PruneOpGTE, Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 5}},
	}, icebergPrunePredicatesFromNode(flipped))
	require.Len(t, flipped.FilterList, 2, "compile pruning metadata must leave original SQL filters as residual")
}

func TestCompileIcebergScanSkipsRowGroupPlanningWithoutPrunePredicates(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "id"}}},
		ExternScan: &plan.ExternScan{
			Type: int32(plan.ExternType_ICEBERG_TB),
			IcebergScan: &plan.IcebergScan{
				CatalogId:         7,
				Namespace:         "sales",
				Table:             "orders",
				ProjectedFieldIds: []int32{11},
			},
			TbColToDataCol: map[string]int32{},
		},
	}

	_, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Empty(t, planner.req.PrunePredicates)
	require.False(t, planner.req.EnableRowGroupPlanning)
}

func TestIcebergScanPlanRequestEnablesMergeOnRead(t *testing.T) {
	req := icebergScanPlanRequest(&plan.IcebergScan{
		Namespace: "sales",
		Table:     "orders",
		ReadMode:  model.ReadModeMergeOnRead,
	})
	require.True(t, req.EnableDeleteApply)
}

func TestIcebergTimestampLiteralNormalizationUsesUTCInstant(t *testing.T) {
	ksa := time.FixedZone("UTC+3", 3*int(time.Hour/time.Second))
	ts := types.FromClockZone(ksa, 2026, 1, 1, 0, 0, 0, 0)
	lit, ok := icebergPruneLiteralFromPlanLiteral(&plan.Literal{
		Value: &plan.Literal_Timestampval{Timestampval: int64(ts)},
	})
	require.True(t, ok)
	require.Equal(t, api.TypeTimestampTZ, lit.Kind)
	require.True(t, lit.Normalized)
	require.Equal(t, time.Date(2025, 12, 31, 21, 0, 0, 0, time.UTC).UnixMicro(), lit.Int64)
}

func TestCompileIcebergScanWiresDeleteTasksToExternalRuntime(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.cnList = engine.Nodes{{Addr: "cn1:6001", Mcpu: 1}}
	testCompile.addr = "cn1:6001"
	testCompile.execType = plan2.ExecTypeAP_ONECN
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	testCompile.icebergScanPlanner = &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
		DeleteTasks: []api.DeleteFileTask{{
			DataFile: api.DataFile{Content: api.DataFileContentPositionDelete, FilePath: "warehouse/iceberg/orders/delete-pos.parquet", FileFormat: "parquet"},
		}},
	}}

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders", ReadMode: model.ReadModeMergeOnRead},
			TbColToDataCol: map[string]int32{},
		},
	}
	scopes, err := testCompile.compileIcebergScan(node, true)
	require.NoError(t, err)
	require.Len(t, scopes, 1)
	ext, ok := scopes[0].RootOp.(*external.External)
	require.True(t, ok)
	require.Len(t, ext.Es.IcebergDeleteTasks, 1)
	require.True(t, ext.Es.NeedRowOrdinal)
}

func TestCompileIcebergScanRequiresPlanner(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	node := &plan.Node{ExternScan: &plan.ExternScan{Type: int32(plan.ExternType_ICEBERG_TB), IcebergScan: &plan.IcebergScan{}}}
	_, err := testCompile.compileIcebergScan(node, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Iceberg scan planner is not configured")
}

func TestCompileIcebergScanPropagatesStatementCancel(t *testing.T) {
	testCompile := NewMockCompile(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testCompile.proc.Ctx = ctx
	testCompile.proc.ReplaceTopCtx(ctx)
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{waitForCancel: true}
	testCompile.icebergScanPlanner = planner

	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}

	_, err := testCompile.compileIcebergScan(node, true)
	require.Error(t, err)
	require.True(t, planner.sawCanceled)
	require.Contains(t, err.Error(), string(api.ErrPlanningTimeout))
}

func TestPrepareIcebergExternalScanDoesNotPlanOrPrecheck(t *testing.T) {
	testCompile := NewMockCompile(t)
	testCompile.isPrepare = true
	testCompile.anal = &AnalyzeModule{qry: &plan.Query{}}
	planner := &fakeCompileIcebergPlanner{plan: &api.IcebergScanPlan{
		DataTasks: []api.DataFileTask{{DataFile: api.DataFile{FilePath: "warehouse/iceberg/orders/part-0.parquet", FileFormat: "parquet", FileSizeInBytes: 100, RecordCount: 10}}},
	}}
	testCompile.icebergScanPlanner = planner
	node := &plan.Node{
		TableDef: &plan.TableDef{},
		ExternScan: &plan.ExternScan{
			Type:           int32(plan.ExternType_ICEBERG_TB),
			IcebergScan:    &plan.IcebergScan{CatalogId: 7, Namespace: "sales", Table: "orders"},
			TbColToDataCol: map[string]int32{},
		},
	}

	_, err := testCompile.compileExternScan(node)
	require.ErrorIs(t, err, cantCompileForPrepareErr)
	require.Empty(t, planner.req.Table, "PREPARE must not perform Iceberg catalog/metadata planning")
}

type fakeCompileIcebergPlanner struct {
	req           api.ScanPlanRequest
	reqs          []api.ScanPlanRequest
	plan          *api.IcebergScanPlan
	plans         []*api.IcebergScanPlan
	err           error
	waitForCancel bool
	sawCanceled   bool
	calls         int
}

func (p *fakeCompileIcebergPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	p.calls++
	p.req = req
	p.reqs = append(p.reqs, req)
	if p.waitForCancel {
		<-ctx.Done()
		p.sawCanceled = true
		return nil, api.WrapError(api.ErrPlanningTimeout, "Iceberg scan planning context was cancelled", nil, ctx.Err())
	}
	if p.err != nil {
		return nil, p.err
	}
	if len(p.plans) > 0 {
		idx := p.calls - 1
		if idx >= len(p.plans) {
			idx = len(p.plans) - 1
		}
		return p.plans[idx], nil
	}
	return p.plan, nil
}

func icebergTestCmp(op string, left, right *plan.Expr) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: op},
		Args: []*plan.Expr{left, right},
	}}}
}

func icebergTestCol(pos int32) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
}

func icebergTestI64(value int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}}
}

func icebergTestString(value string) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}}}
}

func icebergTestTimestamp(value int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: int64(types.UnixMicroToTimestamp(value))}}}}
}
