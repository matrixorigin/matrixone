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

package embed

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/maintenance"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/icebergwrite"
	sqlicompile "github.com/matrixorigin/matrixone/pkg/sql/compile"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"

	"github.com/parquet-go/parquet-go"
)

const (
	embeddedIcebergSnapshotOld     int64 = 101
	embeddedIcebergSnapshotCurrent int64 = 202
	embeddedIcebergTimestampCutoff       = int64(1767312000000) // 2026-01-02T00:00:00Z in ms.
)

func TestIcebergSQLEngineEmbeddedReadPruningExplainAndTimeTravel(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)
		readTable := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_read", "orders", model.ReadModeAppendOnly)

		beforeFullScan := fixture.planner.callCount()
		require.Equal(t, int64(4), queryInt64(t, tenantSQL, fmt.Sprintf("select count(*) from %s", readTable)))
		require.Equal(t, beforeFullScan+1, fixture.planner.callCount(), "3-CN read must plan once on the coordinator")
		require.Equal(t, [][]int64{{3, 30}, {4, 40}}, queryIntRows(t, tenantSQL,
			fmt.Sprintf("select id, amount from %s where id >= 3 order by id", readTable)))

		mustExecSQL(t, tenantSQL, fmt.Sprintf("create table %s.keep_bucket (bucket bigint)", fixture.databaseName))
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s.keep_bucket values (2)", fixture.databaseName))
		require.Equal(t, int64(2), queryInt64(t, tenantSQL,
			fmt.Sprintf("select count(*) from %s o join %s.keep_bucket k on o.bucket = k.bucket", readTable, fixture.databaseName)))
		require.Equal(t, [][]int64{{1, 2, 30}, {2, 2, 70}}, queryIntRows(t, tenantSQL,
			fmt.Sprintf("select bucket, count(*), sum(amount) from %s group by bucket order by bucket", readTable)))

		beforePrune := fixture.planner.callCount()
		require.Equal(t, int64(70), queryInt64(t, tenantSQL, fmt.Sprintf("select sum(amount) from %s where id >= 3", readTable)))
		pruneReq, prunePlan := fixture.planner.lastCallAfter(beforePrune)
		require.Equal(t, []int{1, 4}, pruneReq.ProjectionIDs)
		require.Equal(t, []api.PrunePredicate{{
			FieldID: 1,
			Op:      api.PruneOpGTE,
			Literal: api.PruneLiteral{Kind: api.TypeLong, Int64: 3},
		}}, pruneReq.PrunePredicates)
		require.True(t, pruneReq.EnableRowGroupPlanning)
		require.Contains(t, pruneReq.ResidualSQL, "filter_digest:")
		require.Equal(t, 1, prunePlan.Profile.DataFilesPruned)
		require.Equal(t, 1, prunePlan.Profile.DataFilesSelected)
		require.Greater(t, prunePlan.Profile.DataFileBytesSelected, int64(0))
		require.Equal(t, "embedded-fixture", prunePlan.Profile.PlanningMode)

		beforeSnapshot := fixture.planner.callCount()
		require.Equal(t, int64(2), queryInt64(t, tenantSQL,
			fmt.Sprintf("select count(*) from %s for iceberg snapshot %d", readTable, embeddedIcebergSnapshotOld)))
		snapshotReq, snapshotPlan := fixture.planner.lastCallAfter(beforeSnapshot)
		require.True(t, snapshotReq.Snapshot.HasSnapshotID)
		require.Equal(t, embeddedIcebergSnapshotOld, snapshotReq.Snapshot.SnapshotID)
		require.Equal(t, embeddedIcebergSnapshotOld, snapshotPlan.Snapshot.SnapshotID)
		mustExecSQL(t, tenantSQL, "set time_zone = '+03:00'")
		require.Equal(t, int64(2), queryInt64(t, tenantSQL,
			fmt.Sprintf("select count(*) from %s for iceberg timestamp as of timestamp '2026-01-01 12:00:00'", readTable)))

		err := tenantSQL.Exec(fmt.Sprintf("select count(*) from %s {timestamp = '2026-01-01 00:00:00'} for iceberg snapshot 101", readTable))
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "iceberg")

		mustExecSQL(t, tenantSQL, fmt.Sprintf("create table %s.native_orders (id bigint)", fixture.databaseName))
		err = tenantSQL.Exec(fmt.Sprintf("select * from %s.native_orders for iceberg snapshot 101", fixture.databaseName))
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "iceberg")
	})
}

func TestIcebergSQLEngineEmbeddedSecurityErrors(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)

		missingPrincipal := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_no_principal", "orders", model.ReadModeAppendOnly)
		installEmbeddedIcebergAccessRows(t, c, fixture, false, true)
		err := tenantSQL.Exec(fmt.Sprintf("select count(*) from %s", missingPrincipal))
		require.Error(t, err)
		require.Contains(t, err.Error(), string(api.ErrPrincipalNotMapped))

		installEmbeddedIcebergAccessRows(t, c, fixture, true, false)
		err = tenantSQL.Exec(fmt.Sprintf("select count(*) from %s", missingPrincipal))
		require.Error(t, err)
		require.Contains(t, err.Error(), string(api.ErrResidencyDenied))

		installEmbeddedIcebergAccessRows(t, c, fixture, true, true)
		for _, tc := range []struct {
			remoteTable string
			code        api.ErrorCode
		}{
			{remoteTable: "orders_unauthorized", code: api.ErrAuthUnauthorized},
			{remoteTable: "orders_forbidden", code: api.ErrAuthForbidden},
			{remoteTable: "orders_missing", code: api.ErrTableNotFound},
			{remoteTable: "orders_unavailable", code: api.ErrCatalogUnavailable},
		} {
			table := createEmbeddedIcebergTable(t, tenantSQL, fixture, tc.remoteTable, tc.remoteTable, model.ReadModeAppendOnly)
			err = tenantSQL.Exec(fmt.Sprintf("select count(*) from %s", table))
			require.Error(t, err)
			require.Contains(t, err.Error(), string(tc.code))
			require.NotContains(t, err.Error(), "secret://")
			require.NotContains(t, err.Error(), "s3://warehouse")
		}
	})
}

func TestIcebergSQLEngineEmbeddedCredentialSecurityAndRedaction(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)
		vendedBuilds := fixture.UseVendedObjectIO(t, time.Date(2026, 6, 30, 0, 0, 0, 0, time.UTC))
		readTable := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_vended_security", "orders", model.ReadModeAppendOnly)

		beforeRead := fixture.planner.callCount()
		require.Equal(t, int64(4), queryInt64(t, tenantSQL, fmt.Sprintf("select count(*) from %s", readTable)))
		require.Greater(t, vendedBuilds.Load(), int32(0), "scan must resolve data files through the vended credential provider")
		_, plan := fixture.planner.lastCallAfter(beforeRead)
		assertEmbeddedIcebergNoCredentialLeak(t, queryText(t, tenantSQL, fmt.Sprintf("show create table %s", readTable)))
		assertEmbeddedIcebergNoCredentialLeak(t, plan.Snapshot.MetadataLocationHash)
		assertEmbeddedIcebergNoCredentialLeak(t, plan.Snapshot.ManifestListHash)
		assertEmbeddedIcebergNoCredentialLeak(t, plan.ObjectIORef)
		require.NotEmpty(t, plan.Snapshot.MetadataLocationHash)
		require.NotEmpty(t, plan.Snapshot.ManifestListHash)
		require.NotContains(t, plan.Snapshot.MetadataLocationHash, "warehouse")
		require.NotContains(t, plan.Snapshot.ManifestListHash, "warehouse")

		installEmbeddedIcebergAccessRows(t, c, fixture, true, false)
		err := tenantSQL.Exec(fmt.Sprintf("select count(*) from %s", readTable))
		require.Error(t, err)
		require.Contains(t, err.Error(), string(api.ErrResidencyDenied))
		assertEmbeddedIcebergNoCredentialLeak(t, err.Error())
	})
}

func TestIcebergSQLEngineEmbeddedDeleteApply(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)

		appendOnly := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_append_delete", "orders_mor_append_only", model.ReadModeAppendOnly)
		err := tenantSQL.Exec(fmt.Sprintf("select count(*) from %s", appendOnly))
		require.Error(t, err)
		require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))

		mergeOnRead := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_mor", "orders_mor_merge", model.ReadModeMergeOnRead)
		beforeMergeOnRead := fixture.planner.callCount()
		require.Equal(t, [][]int64{{3, 30}, {4, 40}}, queryIntRows(t, tenantSQL,
			fmt.Sprintf("select id, amount from %s order by id", mergeOnRead)))
		_, mergePlan := fixture.planner.lastCallAfter(beforeMergeOnRead)
		require.Equal(t, beforeMergeOnRead+1, fixture.planner.callCount(), "3-CN delete apply scan must plan once on the coordinator")
		require.Equal(t, 2, mergePlan.Profile.DeleteFilesSelected)
	})
}

func TestIcebergSQLEngineEmbeddedImportNativePinsSnapshot(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)
		readTable := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_import_source", "orders", model.ReadModeAppendOnly)
		targetTable := fixture.databaseName + ".orders_imported_native"

		beforeImport := fixture.planner.callCount()
		mustExecSQL(t, tenantSQL, fmt.Sprintf(
			"create table %s as select id, hidden_key, bucket, amount from %s for iceberg snapshot %d",
			targetTable, readTable, embeddedIcebergSnapshotOld,
		))
		importReq, importPlan := fixture.planner.lastCallAfter(beforeImport)
		require.True(t, importReq.Snapshot.HasSnapshotID)
		require.Equal(t, embeddedIcebergSnapshotOld, importReq.Snapshot.SnapshotID)
		require.Equal(t, embeddedIcebergSnapshotOld, importPlan.Snapshot.SnapshotID)
		require.Equal(t, 1, importPlan.Profile.DataFilesSelected)
		require.Greater(t, importPlan.Profile.DataFileBytesSelected, int64(0))

		sourcePinned := queryIntRows(t, tenantSQL,
			fmt.Sprintf("select id, hidden_key, bucket, amount from %s for iceberg snapshot %d order by id", readTable, embeddedIcebergSnapshotOld))
		imported := queryIntRows(t, tenantSQL, fmt.Sprintf("select id, hidden_key, bucket, amount from %s order by id", targetTable))
		require.Equal(t, sourcePinned, imported)
		require.Equal(t, [][]int64{{1, 10, 1, 10}, {2, 20, 1, 20}}, imported)
		require.Equal(t, int64(4), queryInt64(t, tenantSQL, fmt.Sprintf("select count(*) from %s", readTable)))
		require.Equal(t, int64(2), queryInt64(t, tenantSQL, fmt.Sprintf("select count(*) from %s", targetTable)))
		require.Equal(t, int64(3), queryInt64(t, tenantSQL, fmt.Sprintf("select sum(id) from %s", targetTable)))
		require.Equal(t, int64(30), queryInt64(t, tenantSQL, fmt.Sprintf("select sum(amount) from %s", targetTable)))
	})
}

func TestIcebergSQLEngineEmbeddedBranchRefReadAndAppendWriteIntent(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)
		writeRecorder := &embeddedIcebergWriteRecorder{}
		installEmbeddedIcebergWriteCoordinator(t, c, writeRecorder)

		readTable := createEmbeddedIcebergTable(t, tenantSQL, fixture, "orders_ref_read", "orders", model.ReadModeAppendOnly)
		beforeRefRead := fixture.planner.callCount()
		require.Equal(t, int64(4), queryInt64(t, tenantSQL,
			fmt.Sprintf("select count(*) from %s for iceberg ref audit_branch", readTable)))
		refReq, refPlan := fixture.planner.lastCallAfter(beforeRefRead)
		require.Equal(t, "audit_branch", refReq.Ref)
		require.Equal(t, "audit_branch", refReq.Snapshot.RefName)
		require.Equal(t, "audit_branch", refPlan.Snapshot.RefName)

		mustExecSQL(t, tenantSQL, fmt.Sprintf("create table %s.ref_stage (id bigint, hidden_key bigint, bucket bigint, amount bigint)", fixture.databaseName))
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s.ref_stage values (21, 210, 2, 2100)", fixture.databaseName))

		appendBranchTable := createEmbeddedIcebergTableWithRefModes(t, tenantSQL, fixture,
			"orders_branch_append", "orders", "branch:publish", model.ReadModeAppendOnly, model.WriteModeAppendOnly)
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s select id, hidden_key, bucket, amount from %s.ref_stage", appendBranchTable, fixture.databaseName))
		appendCall := writeRecorder.lastOperation(t, icebergwrite.OperationAppend)
		require.Equal(t, "branch:publish", appendCall.Request.DefaultRef)
		require.Equal(t, []int{1}, appendCall.AppendRows)
		require.Equal(t, 1, appendCall.CommitCalls)
	})
}

func TestIcebergSQLEngineEmbeddedWriteDMLAndMaintenanceSQL(t *testing.T) {
	RunBaseClusterTests(func(c Cluster) {
		rootDB := openIcebergRootTestDB(t, c)
		defer rootDB.Close()
		tenantSQL := openIcebergTenantTestSQL(t, c, rootDB)

		fixture := newEmbeddedIcebergFixture(t, c)
		defer fixture.Close()
		installEmbeddedIcebergPlanner(t, c, fixture.planner)
		setupEmbeddedIcebergCatalog(t, c, tenantSQL, fixture)

		writeRecorder := &embeddedIcebergWriteRecorder{}
		installEmbeddedIcebergWriteCoordinator(t, c, writeRecorder)
		maintenanceRecorder := &embeddedIcebergMaintenanceRecorder{}
		installEmbeddedIcebergMaintenanceExecutor(t, c, maintenanceRecorder)
		maintenanceDB := openIcebergTenantFrontendDB(t, c, tenantSQL.accountName)
		defer maintenanceDB.Close()

		mustExecSQL(t, tenantSQL, fmt.Sprintf("create table %s.write_stage (id bigint, hidden_key bigint, bucket bigint, amount bigint)", fixture.databaseName))
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s.write_stage values (10, 100, 1, 1000), (11, 110, 1, 1100)", fixture.databaseName))

		appendTable := createEmbeddedIcebergTableWithModes(t, tenantSQL, fixture, "orders_write_append", "orders", model.ReadModeAppendOnly, model.WriteModeAppendOnly)
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s select id, hidden_key, bucket, amount from %s.write_stage", appendTable, fixture.databaseName))
		appendCall := writeRecorder.lastOperation(t, icebergwrite.OperationAppend)
		require.Equal(t, model.WriteModeAppendOnly, appendCall.Request.WriteMode)
		require.Equal(t, []int{2}, appendCall.AppendRows)
		require.Equal(t, 1, appendCall.CommitCalls)

		dmlTable := createEmbeddedIcebergTableWithModes(t, tenantSQL, fixture, "orders_write_dml", "orders_write_dml", model.ReadModeMergeOnRead, model.WriteModeMergeOnRead)
		mustExecSQL(t, tenantSQL, fmt.Sprintf("delete from %s where id = 1", dmlTable))
		deleteCall := writeRecorder.lastOperation(t, icebergwrite.OperationDelete)
		require.Equal(t, []int{1}, deleteCall.AppendRows)
		require.Equal(t, embeddedIcebergSnapshotCurrent, deleteCall.Request.DMLScan.BaseSnapshotID)
		require.Len(t, deleteCall.Request.DMLScan.DataFiles, 2)

		mustExecSQL(t, tenantSQL, fmt.Sprintf("update %s set amount = amount + 100 where id = 2", dmlTable))
		updateCall := writeRecorder.lastOperation(t, icebergwrite.OperationUpdate)
		require.Equal(t, []int{1}, updateCall.AppendRows)
		require.Equal(t, embeddedIcebergSnapshotCurrent, updateCall.Request.DMLScan.BaseSnapshotID)

		mustExecSQL(t, tenantSQL, fmt.Sprintf("create table %s.merge_stage (id bigint, hidden_key bigint, bucket bigint, amount bigint)", fixture.databaseName))
		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert into %s.merge_stage values (3, 300, 2, 3000), (99, 990, 9, 9900)", fixture.databaseName))
		mustExecSQL(t, tenantSQL, fmt.Sprintf(
			"merge into %s as t using %s.merge_stage as s on t.id = s.id when matched then update set hidden_key = s.hidden_key, bucket = s.bucket, amount = s.amount when not matched then insert (id, hidden_key, bucket, amount) values (s.id, s.hidden_key, s.bucket, s.amount)",
			dmlTable, fixture.databaseName,
		))
		mergeCall := writeRecorder.lastOperation(t, icebergwrite.OperationMerge)
		require.Equal(t, 2, mergeCall.totalAppendRows())
		require.Equal(t, embeddedIcebergSnapshotCurrent, mergeCall.Request.DMLScan.BaseSnapshotID)

		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert overwrite %s select id, hidden_key, bucket, amount from %s.merge_stage where id = 99", dmlTable, fixture.databaseName))
		overwriteCall := writeRecorder.lastOperation(t, icebergwrite.OperationOverwrite)
		require.Equal(t, []int{1}, overwriteCall.AppendRows)
		require.Equal(t, "", overwriteCall.Request.DMLScan.OverwriteScope)
		require.Len(t, overwriteCall.Request.DMLScan.DataFiles, 2)

		mustExecSQL(t, tenantSQL, fmt.Sprintf("insert overwrite %s partition(bucket = 2) select id, hidden_key, bucket, amount from %s.merge_stage where id = 99", dmlTable, fixture.databaseName))
		partitionOverwriteCall := writeRecorder.lastOperation(t, icebergwrite.OperationOverwrite)
		require.Equal(t, "partition", partitionOverwriteCall.Request.DMLScan.OverwriteScope)
		require.Equal(t, int64(2), partitionOverwriteCall.Request.DMLScan.OverwritePartition["bucket"])

		err := tenantSQL.Exec(fmt.Sprintf("insert overwrite %s partition(p0) select id, hidden_key, bucket, amount from %s.merge_stage", dmlTable, fixture.databaseName))
		require.Error(t, err)
		require.Contains(t, err.Error(), "PARTITION name syntax")

		for _, stmt := range []string{
			"call iceberg_rewrite_data_files('" + fixture.catalogName + ".sales.orders', 'ref=main,target_file_size=1048576')",
			"call iceberg_rewrite_manifests('" + fixture.catalogName + ".sales.orders', 'ref=main')",
			"call iceberg_expire_snapshots('" + fixture.catalogName + ".sales.orders', 'older_than=2026-01-04 00:00:00,retain_last=1')",
		} {
			mustExec(t, maintenanceDB, stmt)
		}
		maintenanceRecorder.requireOperations(t,
			maintenance.OperationRewriteDataFiles,
			maintenance.OperationRewriteManifests,
			maintenance.OperationExpireSnapshots,
		)
	})
}

type embeddedIcebergFixture struct {
	catalogName  string
	databaseName string
	endpoint     string
	catalogURI   string
	catalogID    uint64
	accountID    uint64
	roleID       uint64
	userID       uint64
	objectRef    string
	objectFS     fileservice.ETLFileService
	fileSizes    map[string]int64
	planner      *embeddedIcebergPlanner
}

func newEmbeddedIcebergFixture(t *testing.T, c Cluster) *embeddedIcebergFixture {
	t.Helper()
	suffix := time.Now().UnixNano()
	fs, err := fileservice.NewMemoryFS(fmt.Sprintf("iceberg-embed-%d", suffix), fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)

	fixture := &embeddedIcebergFixture{
		catalogName:  fmt.Sprintf("icecat_l2_%d", suffix),
		databaseName: fmt.Sprintf("iceberg_l2_%d", suffix),
		endpoint:     fmt.Sprintf("s3-%d.me-central-1.amazonaws.com", suffix),
		catalogURI:   fmt.Sprintf("https://catalog.example/v1/%d", suffix),
		objectFS:     fs,
		fileSizes:    make(map[string]int64),
	}
	writeEmbeddedIcebergObject(t, fs, "warehouse/embed/orders/part-old.parquet", embeddedIcebergDataParquet(t, []embeddedIcebergDataRow{
		{ID: 1, HiddenKey: 10, Bucket: 1, Amount: 10},
		{ID: 2, HiddenKey: 20, Bucket: 1, Amount: 20},
	}))
	writeEmbeddedIcebergObject(t, fs, "warehouse/embed/orders/part-new.parquet", embeddedIcebergDataParquet(t, []embeddedIcebergDataRow{
		{ID: 3, HiddenKey: 30, Bucket: 2, Amount: 30},
		{ID: 4, HiddenKey: 40, Bucket: 2, Amount: 40},
	}))
	writeEmbeddedIcebergObject(t, fs, "warehouse/embed/orders/delete-pos.parquet", embeddedIcebergPositionDeleteParquet(t,
		"s3://warehouse/embed/orders/part-old.parquet", 0))
	writeEmbeddedIcebergObject(t, fs, "warehouse/embed/orders/delete-eq.parquet", embeddedIcebergEqualityDeleteParquet(t, 20))
	for _, path := range []string{
		"warehouse/embed/orders/part-old.parquet",
		"warehouse/embed/orders/part-new.parquet",
		"warehouse/embed/orders/delete-pos.parquet",
		"warehouse/embed/orders/delete-eq.parquet",
	} {
		stat, err := fs.StatFile(context.Background(), path)
		require.NoError(t, err)
		fixture.fileSizes["s3://"+path] = stat.Size
	}

	ref, err := icebergio.RegisterObjectIOProvider(context.Background(), icebergio.ScopedProvider{FileService: fs}, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       uint32(fixture.accountID),
			CatalogID:       fixture.catalogID,
			StorageLocation: strings.TrimPrefix(strings.TrimSpace(location), "s3://"),
			Endpoint:        fixture.endpoint,
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "embedded-principal",
		}
	}, time.Hour)
	require.NoError(t, err)
	fixture.objectRef = ref
	fixture.planner = &embeddedIcebergPlanner{fixture: fixture}
	return fixture
}

func (f *embeddedIcebergFixture) UseVendedObjectIO(t *testing.T, now time.Time) *atomic.Int32 {
	t.Helper()
	if f == nil || f.objectFS == nil {
		t.Fatalf("embedded Iceberg fixture has no object file service")
	}
	if f.objectRef != "" {
		icebergio.ReleaseObjectIORef(f.objectRef)
		f.objectRef = ""
	}
	var builds atomic.Int32
	provider := icebergio.VendedCredentialProvider{
		Credentials: []api.StorageCredential{{
			Prefix: "s3://warehouse/embed",
			Config: map[string]string{
				"s3.access-key-id":     "AKIA_EMBEDDED_TEST",
				"s3.secret-access-key": "raw-secret-for-redaction-test",
				"s3.session-token":     "session-token-for-redaction-test",
			},
			ExpiresAt: now.Add(10 * time.Minute),
		}},
		Now:    func() time.Time { return now },
		MinTTL: time.Minute,
		BuildFileService: func(ctx context.Context, scope icebergio.ObjectScope, credential api.StorageCredential) (fileservice.ETLFileService, string, error) {
			builds.Add(1)
			if credential.Config["s3.access-key-id"] != "AKIA_EMBEDDED_TEST" {
				return nil, "", fmt.Errorf("unexpected vended credential")
			}
			return f.objectFS, strings.TrimPrefix(scope.StorageLocation, "s3://"), nil
		},
	}
	ref, err := icebergio.RegisterObjectIOProvider(context.Background(), provider, func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{
			AccountID:       uint32(f.accountID),
			CatalogID:       f.catalogID,
			StorageLocation: strings.TrimSpace(location),
			Endpoint:        f.endpoint,
			Region:          "me-central-1",
			Bucket:          "warehouse",
			Principal:       "embedded-principal",
		}
	}, time.Hour)
	require.NoError(t, err)
	f.objectRef = ref
	return &builds
}

func (f *embeddedIcebergFixture) Close() {
	if f != nil && f.objectRef != "" {
		icebergio.ReleaseObjectIORef(f.objectRef)
	}
}

type embeddedSQLSession struct {
	sqlExec     executor.SQLExecutor
	accountName string
	accountID   uint32
	userID      uint32
	roleID      uint32
	database    string
	timeZone    *time.Location
}

func newEmbeddedSQLSession(t *testing.T, c Cluster, accountName string, accountID, userID, roleID uint32) *embeddedSQLSession {
	t.Helper()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	return &embeddedSQLSession{
		sqlExec:     cn0.(*operator).reset.svc.(cnservice.Service).GetSQLExecutor(),
		accountName: accountName,
		accountID:   accountID,
		userID:      userID,
		roleID:      roleID,
		timeZone:    time.Local,
	}
}

func (s *embeddedSQLSession) Exec(stmt string) error {
	if strings.EqualFold(strings.TrimSpace(stmt), "set time_zone = '+03:00'") {
		s.timeZone = time.FixedZone("+03:00", 3*60*60)
		return nil
	}
	result, err := s.execResult(stmt)
	if err == nil {
		result.Close()
	}
	return err
}

func (s *embeddedSQLSession) execResult(stmt string) (executor.Result, error) {
	stmtOpt := executor.StatementOption{}.
		WithAccountID(s.accountID).
		WithUserID(s.userID).
		WithRoleID(s.roleID).
		WithDisableLog()
	opts := executor.Options{}.
		WithAccountID(s.accountID).
		WithDatabase(s.database).
		WithTimeZone(s.timeZone).
		WithStatementOption(stmtOpt)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.sqlExec.Exec(ctx, stmt, opts)
}

func setupEmbeddedIcebergCatalog(t *testing.T, c Cluster, db *embeddedSQLSession, fixture *embeddedIcebergFixture) {
	t.Helper()
	mustExecSQL(t, db, fmt.Sprintf("create database %s", fixture.databaseName))
	fixture.accountID = uint64(queryInt64(t, db, "select current_account_id()"))
	fixture.roleID = uint64(queryInt64(t, db, "select current_role_id()"))
	fixture.userID = uint64(queryInt64(t, db, "select current_user_id()"))
	fixture.catalogID = 1
	mustExecSQL(t, db, sqliceberg.InsertCatalogSQL(model.Catalog{
		AccountID:      uint32(fixture.accountID),
		CatalogID:      fixture.catalogID,
		Name:           fixture.catalogName,
		Type:           "rest",
		URI:            fixture.catalogURI,
		Warehouse:      "s3://warehouse/embed",
		AuthMode:       model.AuthModeNone,
		TokenSecretRef: "secret://catalog/token",
		Version:        1,
	}))
	installEmbeddedIcebergAccessRows(t, c, fixture, true, true)
}

func installEmbeddedIcebergAccessRows(t *testing.T, c Cluster, fixture *embeddedIcebergFixture, principal, residency bool) {
	t.Helper()
	execEmbeddedIcebergSystemSQL(t, c, fixture, fmt.Sprintf("delete from mo_catalog.mo_iceberg_principal_map where catalog_id = %d", fixture.catalogID))
	execEmbeddedIcebergSystemSQL(t, c, fixture, fmt.Sprintf("delete from mo_catalog.mo_iceberg_residency_policy where allowed_endpoint = '%s'", fixture.endpoint))
	if principal {
		execEmbeddedIcebergSystemSQL(t, c, fixture, fmt.Sprintf(
			"insert into mo_catalog.mo_iceberg_principal_map(account_id,catalog_id,mo_role_id,mo_user_id,external_principal,scope_json,created_by,version) values (%d,%d,%d,%d,'embedded-principal','{}',%d,1)",
			fixture.accountID, fixture.catalogID, fixture.roleID, fixture.userID, fixture.userID))
	}
	if residency {
		execEmbeddedIcebergSystemSQL(t, c, fixture, fmt.Sprintf(
			"insert into mo_catalog.mo_iceberg_residency_policy(scope_type,account_id,catalog_id,allowed_catalog_uri,allowed_endpoint,allowed_region,allowed_bucket,policy_state,created_by,version) values ('cluster',0,0,'%s','%s','*','*','enabled',%d,1)",
			fixture.catalogURI, fixture.endpoint, fixture.userID))
	}
}

func execEmbeddedIcebergSystemSQL(t *testing.T, c Cluster, fixture *embeddedIcebergFixture, stmt string) {
	t.Helper()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	sqlExec := cn0.(*operator).reset.svc.(cnservice.Service).GetSQLExecutor()
	stmtOpt := executor.StatementOption{}.
		WithAccountID(uint32(fixture.accountID)).
		WithUserID(uint32(fixture.userID)).
		WithRoleID(uint32(fixture.roleID)).
		WithDisableLog()
	opts := executor.Options{}.
		WithAccountID(uint32(fixture.accountID)).
		WithDatabase("mo_catalog").
		WithStatementOption(stmtOpt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := sqlExec.Exec(ctx, stmt, opts)
	if err == nil {
		result.Close()
	}
	require.NoError(t, err, stmt)
}

func createEmbeddedIcebergTable(t *testing.T, db *embeddedSQLSession, fixture *embeddedIcebergFixture, moTable, remoteTable, readMode string) string {
	return createEmbeddedIcebergTableWithModes(t, db, fixture, moTable, remoteTable, readMode, model.WriteModeReadOnly)
}

func createEmbeddedIcebergTableWithModes(t *testing.T, db *embeddedSQLSession, fixture *embeddedIcebergFixture, moTable, remoteTable, readMode, writeMode string) string {
	return createEmbeddedIcebergTableWithRefModes(t, db, fixture, moTable, remoteTable, "main", readMode, writeMode)
}

func createEmbeddedIcebergTableWithRefModes(t *testing.T, db *embeddedSQLSession, fixture *embeddedIcebergFixture, moTable, remoteTable, ref, readMode, writeMode string) string {
	t.Helper()
	mustExecSQL(t, db, fmt.Sprintf(
		"create external table %s.%s (id bigint, hidden_key bigint, bucket bigint, amount bigint) engine = iceberg with ('catalog'='%s','namespace'='sales','table'='%s','ref'='%s','read_mode'='%s','write_mode'='%s')",
		fixture.databaseName, moTable, fixture.catalogName, remoteTable, ref, readMode, writeMode,
	))
	return fixture.databaseName + "." + moTable
}

func openIcebergTenantTestSQL(t *testing.T, c Cluster, rootDB *sql.DB) *embeddedSQLSession {
	t.Helper()
	accountName := fmt.Sprintf("iceacc_%d", time.Now().UnixNano())
	mustExec(t, rootDB, fmt.Sprintf("create account %s admin_name 'admin' identified by '111'", accountName))
	t.Cleanup(func() {
		cleanupDB := openIcebergRootTestDB(t, c)
		defer cleanupDB.Close()
		_, _ = cleanupDB.Exec(fmt.Sprintf("drop account if exists %s", accountName))
	})
	var accountID uint32
	require.NoError(t, rootDB.QueryRow(
		fmt.Sprintf("select account_id from mo_catalog.mo_account where account_name = '%s'", accountName),
	).Scan(&accountID))
	require.NotZero(t, accountID)
	session := newEmbeddedSQLSession(t, c, accountName, accountID, 2, 2)
	require.Equal(t, int64(accountID), queryInt64(t, session, "select current_account_id()"))
	require.Equal(t, int64(2), queryInt64(t, session, "select current_user_id()"))
	require.Equal(t, int64(2), queryInt64(t, session, "select current_role_id()"))
	return session
}

func openIcebergTenantFrontendDB(t *testing.T, c Cluster, accountName string) *sql.DB {
	t.Helper()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("%s#admin#moadmin:111@tcp(127.0.0.1:%d)/", accountName, cn0.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func openIcebergRootTestDB(t *testing.T, c Cluster) *sql.DB {
	t.Helper()
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	dsn := fmt.Sprintf("sys#root#moadmin:111@tcp(127.0.0.1:%d)/", cn0.GetServiceConfig().CN.Frontend.Port)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	return db
}

func installEmbeddedIcebergPlanner(t *testing.T, c Cluster, planner api.ScanPlanner) {
	t.Helper()
	type restore struct {
		serviceID string
		hadValue  bool
		value     any
		pu        *config.ParameterUnit
		protected bool
	}
	restores := make([]restore, 0)
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		rt := moruntime.ServiceRuntime(svc.ServiceID())
		if rt == nil {
			return true
		}
		old, hadOld := rt.GetGlobalVariables(sqlicompile.IcebergScanPlannerRuntimeKey)
		rt.SetGlobalVariables(sqlicompile.IcebergScanPlannerRuntimeKey, planner)
		r := restore{serviceID: svc.ServiceID(), hadValue: hadOld, value: old}
		if puValue, ok := rt.GetGlobalVariables("parameter-unit"); ok {
			if pu, ok := puValue.(*config.ParameterUnit); ok && pu != nil && pu.SV != nil {
				r.pu = pu
				r.protected = pu.SV.Iceberg.ProtectedCNToCN
				pu.SV.Iceberg.ProtectedCNToCN = true
				pu.SV.Iceberg.Enable = true
			}
		}
		restores = append(restores, r)
		return true
	})
	t.Cleanup(func() {
		for _, r := range restores {
			if rt := moruntime.ServiceRuntime(r.serviceID); rt != nil {
				if r.hadValue {
					rt.SetGlobalVariables(sqlicompile.IcebergScanPlannerRuntimeKey, r.value)
				} else {
					rt.SetGlobalVariables(sqlicompile.IcebergScanPlannerRuntimeKey, nil)
				}
			}
			if r.pu != nil && r.pu.SV != nil {
				r.pu.SV.Iceberg.ProtectedCNToCN = r.protected
			}
		}
	})
}

func installEmbeddedIcebergWriteCoordinator(t *testing.T, c Cluster, factory icebergwrite.CoordinatorFactory) {
	t.Helper()
	installEmbeddedRuntimeVariable(t, c, sqlicompile.IcebergAppendCoordinatorFactoryRuntimeKey, factory)
}

func installEmbeddedIcebergMaintenanceExecutor(t *testing.T, c Cluster, executor frontend.IcebergMaintenanceCallExecutor) {
	t.Helper()
	installEmbeddedRuntimeVariable(t, c, frontend.IcebergMaintenanceCallExecutorRuntimeKey, executor)
}

func installEmbeddedRuntimeVariable(t *testing.T, c Cluster, key string, value any) {
	t.Helper()
	type restore struct {
		serviceID string
		hadValue  bool
		value     any
	}
	seen := make(map[string]struct{})
	restores := make([]restore, 0)
	install := func(serviceID string) {
		if _, ok := seen[serviceID]; ok {
			return
		}
		seen[serviceID] = struct{}{}
		rt := moruntime.ServiceRuntime(serviceID)
		if rt == nil {
			return
		}
		old, hadOld := rt.GetGlobalVariables(key)
		rt.SetGlobalVariables(key, value)
		restores = append(restores, restore{serviceID: serviceID, hadValue: hadOld, value: old})
	}
	install("")
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_CN {
			install(svc.ServiceID())
		}
		return true
	})
	t.Cleanup(func() {
		for _, r := range restores {
			if rt := moruntime.ServiceRuntime(r.serviceID); rt != nil {
				if r.hadValue {
					rt.SetGlobalVariables(key, r.value)
				} else {
					rt.SetGlobalVariables(key, nil)
				}
			}
		}
	})
}

type embeddedIcebergPlanner struct {
	mu      sync.Mutex
	fixture *embeddedIcebergFixture
	calls   []embeddedIcebergPlannerCall
}

type embeddedIcebergPlannerCall struct {
	req  api.ScanPlanRequest
	plan api.IcebergScanPlan
}

func (p *embeddedIcebergPlanner) PlanScan(ctx context.Context, req api.ScanPlanRequest) (*api.IcebergScanPlan, error) {
	switch req.Table {
	case "orders_unauthorized":
		return nil, api.NewError(api.ErrAuthUnauthorized, "embedded catalog unauthorized", nil)
	case "orders_forbidden":
		return nil, api.NewError(api.ErrAuthForbidden, "embedded catalog forbidden", nil)
	case "orders_missing":
		return nil, api.NewError(api.ErrTableNotFound, "embedded table missing", nil)
	case "orders_unavailable":
		return nil, api.NewError(api.ErrCatalogUnavailable, "embedded catalog unavailable", nil)
	}
	if embeddedIcebergTableHasDeletes(req.Table) && !req.EnableDeleteApply {
		return nil, api.NewError(api.ErrUnsupportedFeature, "delete files require merge_on_read read mode", map[string]string{"table": req.Table})
	}

	tasks := p.dataTasksForRequest(req)
	plan := api.IcebergScanPlan{
		Snapshot: api.SnapshotPlan{
			SnapshotID:           p.snapshotID(req),
			SchemaID:             7,
			PartitionSpecIDs:     []int{1},
			MetadataLocationHash: api.PathHash("s3://warehouse/embed/orders/metadata/v2.json"),
			ManifestListHash:     api.PathHash("s3://warehouse/embed/orders/metadata/snap-202.avro"),
			RefName:              firstNonEmptyString(req.Ref, req.Snapshot.RefName, "main"),
			PlanningMode:         "embedded-fixture",
		},
		DataTasks:   tasks,
		DeleteTasks: p.deleteTasksForRequest(req),
		ColumnMapping: []api.IcebergColumnMapping{
			{FieldID: 1, ColumnName: "id", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 1},
			{FieldID: 2, ColumnName: "hidden_key", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 2, Hidden: embeddedIcebergTableHasDeletes(req.Table)},
			{FieldID: 3, ColumnName: "bucket", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 3},
			{FieldID: 4, ColumnName: "amount", MOType: api.MOType{Name: "BIGINT"}, Projected: true, ParquetFieldID: 4},
			{FieldID: -1001, ColumnName: api.DMLDataFilePathColumnName, MOType: api.MOType{Name: "TEXT", Width: types.MaxVarcharLen}, Projected: true, Hidden: true},
			{FieldID: -1002, ColumnName: api.DMLRowOrdinalColumnName, MOType: api.MOType{Name: "BIGINT"}, Projected: true, Hidden: true},
		},
		ResidualFilter: api.ResidualFilter{AlwaysTrue: true},
		Profile: api.PlanningProfile{
			ManifestsSelected:     1,
			DataFilesSelected:     len(tasks),
			DataFilesPruned:       2 - len(tasks),
			DataFileBytesSelected: p.totalTaskBytes(tasks),
			PlanningMode:          "embedded-fixture",
			DeleteFilesSelected:   len(p.deleteTasksForRequest(req)),
		},
		ObjectIORef: p.fixture.objectRef,
	}
	p.mu.Lock()
	p.calls = append(p.calls, embeddedIcebergPlannerCall{req: req, plan: plan})
	p.mu.Unlock()
	return &plan, nil
}

func (p *embeddedIcebergPlanner) dataTasksForRequest(req api.ScanPlanRequest) []api.DataFileTask {
	old := p.dataTask("s3://warehouse/embed/orders/part-old.parquet", 2)
	current := p.dataTask("s3://warehouse/embed/orders/part-new.parquet", 2)
	if p.snapshotID(req) == embeddedIcebergSnapshotOld {
		return []api.DataFileTask{old}
	}
	if prunesToNewFile(req.PrunePredicates) {
		return []api.DataFileTask{current}
	}
	return []api.DataFileTask{old, current}
}

func (p *embeddedIcebergPlanner) dataTask(path string, rows int64) api.DataFileTask {
	return api.DataFileTask{DataFile: api.DataFile{
		Content:         api.DataFileContentData,
		FilePath:        path,
		FileFormat:      "parquet",
		FileSizeInBytes: p.fixture.fileSizes[path],
		RecordCount:     rows,
		SpecID:          1,
		SequenceNumber:  1,
	}}
}

func (p *embeddedIcebergPlanner) deleteTasksForRequest(req api.ScanPlanRequest) []api.DeleteFileTask {
	if !embeddedIcebergTableHasDeletes(req.Table) || !req.EnableDeleteApply {
		return nil
	}
	return []api.DeleteFileTask{{
		DataFile: api.DataFile{
			Content:         api.DataFileContentPositionDelete,
			FilePath:        "s3://warehouse/embed/orders/delete-pos.parquet",
			FileFormat:      "parquet",
			FileSizeInBytes: p.fixture.fileSizes["s3://warehouse/embed/orders/delete-pos.parquet"],
			RecordCount:     1,
			SpecID:          1,
			SequenceNumber:  2,
		},
		AppliesToPath:  "s3://warehouse/embed/orders/part-old.parquet",
		SequenceNumber: 2,
	}, {
		DataFile: api.DataFile{
			Content:         api.DataFileContentEqualityDelete,
			FilePath:        "s3://warehouse/embed/orders/delete-eq.parquet",
			FileFormat:      "parquet",
			FileSizeInBytes: p.fixture.fileSizes["s3://warehouse/embed/orders/delete-eq.parquet"],
			RecordCount:     1,
			SpecID:          1,
			SequenceNumber:  2,
			EqualityIDs:     []int{2},
		},
		SequenceNumber: 2,
	}}
}

func embeddedIcebergTableHasDeletes(table string) bool {
	return strings.HasPrefix(strings.TrimSpace(table), "orders_mor")
}

func (p *embeddedIcebergPlanner) snapshotID(req api.ScanPlanRequest) int64 {
	if req.Snapshot.HasSnapshotID && req.Snapshot.SnapshotID == embeddedIcebergSnapshotOld {
		return embeddedIcebergSnapshotOld
	}
	if req.Snapshot.HasTimestampMS && req.Snapshot.TimestampMS < embeddedIcebergTimestampCutoff {
		return embeddedIcebergSnapshotOld
	}
	return embeddedIcebergSnapshotCurrent
}

func (p *embeddedIcebergPlanner) totalTaskBytes(tasks []api.DataFileTask) int64 {
	var total int64
	for _, task := range tasks {
		total += task.DataFile.FileSizeInBytes
	}
	return total
}

func (p *embeddedIcebergPlanner) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

func (p *embeddedIcebergPlanner) lastCallAfter(before int) (api.ScanPlanRequest, api.IcebergScanPlan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.calls) <= before {
		return api.ScanPlanRequest{}, api.IcebergScanPlan{}
	}
	call := p.calls[len(p.calls)-1]
	return call.req, call.plan
}

func prunesToNewFile(predicates []api.PrunePredicate) bool {
	for _, pred := range predicates {
		if pred.FieldID != 1 || pred.Literal.Kind != api.TypeLong {
			continue
		}
		switch pred.Op {
		case api.PruneOpGT, api.PruneOpGTE:
			if pred.Literal.Int64 >= 3 {
				return true
			}
		}
	}
	return false
}

type embeddedIcebergDataRow struct {
	ID        int64
	HiddenKey int64
	Bucket    int64
	Amount    int64
}

func embeddedIcebergDataParquet(t *testing.T, rows []embeddedIcebergDataRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	type parquetRow struct {
		ID        int64 `parquet:"id,id(1)"`
		HiddenKey int64 `parquet:"hidden_key,id(2)"`
		Bucket    int64 `parquet:"bucket,id(3)"`
		Amount    int64 `parquet:"amount,id(4)"`
	}
	writer := parquet.NewGenericWriter[parquetRow](&buf)
	parquetRows := make([]parquetRow, len(rows))
	for idx, row := range rows {
		parquetRows[idx] = parquetRow{
			ID:        row.ID,
			HiddenKey: row.HiddenKey,
			Bucket:    row.Bucket,
			Amount:    row.Amount,
		}
	}
	_, err := writer.Write(parquetRows)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func embeddedIcebergPositionDeleteParquet(t *testing.T, dataFile string, pos int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	type positionDeleteRow struct {
		FilePath string `parquet:"file_path"`
		Pos      int64  `parquet:"pos"`
	}
	writer := parquet.NewGenericWriter[positionDeleteRow](&buf)
	_, err := writer.Write([]positionDeleteRow{{FilePath: dataFile, Pos: pos}})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func embeddedIcebergEqualityDeleteParquet(t *testing.T, hiddenKey int64) []byte {
	t.Helper()
	var buf bytes.Buffer
	schema := parquet.NewSchema("delete", parquet.Group{
		"hidden_key": parquet.FieldID(parquet.Leaf(parquet.Int64Type), 2),
	})
	writer := parquet.NewWriter(&buf, schema)
	_, err := writer.WriteRows([]parquet.Row{
		{parquet.Int64Value(hiddenKey).Level(0, 0, 0)},
	})
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

func writeEmbeddedIcebergObject(t *testing.T, fs fileservice.ETLFileService, path string, data []byte) {
	t.Helper()
	require.NoError(t, fs.Write(context.Background(), fileservice.IOVector{
		FilePath: path,
		Entries: []fileservice.IOEntry{{
			Offset: 0,
			Size:   int64(len(data)),
			Data:   append([]byte(nil), data...),
		}},
	}))
}

func mustExecSQL(t *testing.T, db *embeddedSQLSession, stmt string) {
	t.Helper()
	require.NoError(t, db.Exec(stmt), stmt)
}

func queryInt64(t *testing.T, db *embeddedSQLSession, query string) int64 {
	t.Helper()
	rows := queryIntRows(t, db, query)
	require.Len(t, rows, 1, query)
	require.Len(t, rows[0], 1, query)
	return rows[0][0]
}

func queryIntRows(t *testing.T, db *embeddedSQLSession, query string) [][]int64 {
	t.Helper()
	result, err := db.execResult(query)
	require.NoError(t, err, query)
	defer result.Close()
	out := make([][]int64, 0)
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for row := 0; row < rows; row++ {
			values := make([]int64, len(cols))
			for colIdx, col := range cols {
				values[colIdx] = fixedInt64At(t, col, row)
			}
			out = append(out, values)
		}
		return true
	})
	return out
}

func fixedInt64At(t *testing.T, vec *vector.Vector, row int) int64 {
	t.Helper()
	switch vec.GetType().Oid {
	case types.T_int8:
		return int64(executor.GetFixedRows[int8](vec)[row])
	case types.T_int16:
		return int64(executor.GetFixedRows[int16](vec)[row])
	case types.T_int32:
		return int64(executor.GetFixedRows[int32](vec)[row])
	case types.T_int64:
		return executor.GetFixedRows[int64](vec)[row]
	case types.T_uint8:
		return int64(executor.GetFixedRows[uint8](vec)[row])
	case types.T_uint16:
		return int64(executor.GetFixedRows[uint16](vec)[row])
	case types.T_uint32:
		return int64(executor.GetFixedRows[uint32](vec)[row])
	case types.T_uint64:
		return int64(executor.GetFixedRows[uint64](vec)[row])
	default:
		t.Fatalf("expected integer vector, got %s", vec.GetType().String())
		return 0
	}
}

func queryText(t *testing.T, db *embeddedSQLSession, query string) string {
	t.Helper()
	result, err := db.execResult(query)
	require.NoError(t, err, query)
	defer result.Close()
	var b strings.Builder
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for row := 0; row < rows; row++ {
			for _, col := range cols {
				if col.GetNulls().Contains(uint64(row)) {
					continue
				}
				values := executor.GetStringRows(col)
				b.WriteString(values[row])
				b.WriteByte('\n')
			}
		}
		return true
	})
	return b.String()
}

func assertEmbeddedIcebergNoCredentialLeak(t *testing.T, text string) {
	t.Helper()
	for _, forbidden := range []string{
		"AKIA_EMBEDDED_TEST",
		"raw-secret-for-redaction-test",
		"session-token-for-redaction-test",
		"secret://catalog/token",
		"s3://warehouse/embed",
		"warehouse/embed/orders",
	} {
		require.NotContains(t, text, forbidden)
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

type embeddedIcebergWriteRecorder struct {
	mu    sync.Mutex
	calls []embeddedIcebergWriteCall
}

type embeddedIcebergWriteCall struct {
	Request     icebergwrite.AppendRequest
	AppendRows  []int
	CommitCalls int
	AbortCalls  int
}

func (c embeddedIcebergWriteCall) totalAppendRows() int {
	total := 0
	for _, rows := range c.AppendRows {
		total += rows
	}
	return total
}

func (r *embeddedIcebergWriteRecorder) NewCoordinator(ctx context.Context, req icebergwrite.AppendRequest) (icebergwrite.Coordinator, error) {
	return &embeddedIcebergRecordingCoordinator{recorder: r}, nil
}

func (r *embeddedIcebergWriteRecorder) recordBegin(req icebergwrite.AppendRequest) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := req
	copied.DMLScan.DataFiles = append([]api.DataFile(nil), req.DMLScan.DataFiles...)
	if req.DMLScan.OverwritePartition != nil {
		copied.DMLScan.OverwritePartition = make(map[string]any, len(req.DMLScan.OverwritePartition))
		for key, value := range req.DMLScan.OverwritePartition {
			copied.DMLScan.OverwritePartition[key] = value
		}
	}
	r.calls = append(r.calls, embeddedIcebergWriteCall{Request: copied})
	return len(r.calls) - 1
}

func (r *embeddedIcebergWriteRecorder) recordAppend(callIdx int, rows int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if callIdx >= 0 && callIdx < len(r.calls) {
		r.calls[callIdx].AppendRows = append(r.calls[callIdx].AppendRows, rows)
	}
}

func (r *embeddedIcebergWriteRecorder) recordCommit(callIdx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if callIdx >= 0 && callIdx < len(r.calls) {
		r.calls[callIdx].CommitCalls++
	}
}

func (r *embeddedIcebergWriteRecorder) recordAbort(callIdx int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if callIdx >= 0 && callIdx < len(r.calls) {
		r.calls[callIdx].AbortCalls++
	}
}

func (r *embeddedIcebergWriteRecorder) lastOperation(t *testing.T, operation string) embeddedIcebergWriteCall {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for idx := len(r.calls) - 1; idx >= 0; idx-- {
		if r.calls[idx].Request.Operation == operation {
			return r.calls[idx]
		}
	}
	t.Fatalf("expected Iceberg write operation %s, got %d calls", operation, len(r.calls))
	return embeddedIcebergWriteCall{}
}

type embeddedIcebergRecordingCoordinator struct {
	recorder *embeddedIcebergWriteRecorder
	callIdx  int
}

func (c *embeddedIcebergRecordingCoordinator) Begin(ctx context.Context, req icebergwrite.AppendRequest) error {
	c.callIdx = c.recorder.recordBegin(req)
	return nil
}

func (c *embeddedIcebergRecordingCoordinator) Append(ctx context.Context, bat *batch.Batch) error {
	c.recorder.recordAppend(c.callIdx, bat.RowCount())
	return nil
}

func (c *embeddedIcebergRecordingCoordinator) AppendWithProcess(proc *process.Process, bat *batch.Batch) error {
	c.recorder.recordAppend(c.callIdx, bat.RowCount())
	return nil
}

func (c *embeddedIcebergRecordingCoordinator) Commit(ctx context.Context) error {
	c.recorder.recordCommit(c.callIdx)
	return nil
}

func (c *embeddedIcebergRecordingCoordinator) Abort(ctx context.Context, cause error) error {
	c.recorder.recordAbort(c.callIdx)
	return nil
}

type embeddedIcebergMaintenanceRecorder struct {
	mu    sync.Mutex
	calls []embeddedIcebergMaintenanceCall
}

type embeddedIcebergMaintenanceCall struct {
	Operation maintenance.Operation
	Target    string
	Options   map[string]string
}

func (r *embeddedIcebergMaintenanceRecorder) ExecuteIcebergMaintenanceCall(ctx context.Context, ses frontend.FeSession, call frontend.IcebergBuiltinProcedureCall) ([]frontend.ExecResult, error) {
	options := make(map[string]string, len(call.Parsed.Options))
	for key, value := range call.Parsed.Options {
		options[key] = value
	}
	r.mu.Lock()
	r.calls = append(r.calls, embeddedIcebergMaintenanceCall{
		Operation: call.Parsed.Operation,
		Target:    call.Parsed.Target,
		Options:   options,
	})
	r.mu.Unlock()
	return nil, nil
}

func (r *embeddedIcebergMaintenanceRecorder) requireOperations(t *testing.T, operations ...maintenance.Operation) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Len(t, r.calls, len(operations))
	for idx, operation := range operations {
		require.Equal(t, operation, r.calls[idx].Operation)
		require.Equal(t, "main", firstNonEmptyString(r.calls[idx].Options["ref"], "main"))
	}
}

var _ icebergwrite.CoordinatorFactory = (*embeddedIcebergWriteRecorder)(nil)
var _ icebergwrite.Coordinator = (*embeddedIcebergRecordingCoordinator)(nil)
var _ icebergwrite.ProcessAwareCoordinator = (*embeddedIcebergRecordingCoordinator)(nil)
var _ frontend.IcebergMaintenanceCallExecutor = (*embeddedIcebergMaintenanceRecorder)(nil)
