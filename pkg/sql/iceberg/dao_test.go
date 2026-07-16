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

package iceberg

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/dml"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergwrite "github.com/matrixorigin/matrixone/pkg/iceberg/write"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
)

type fakeExec struct {
	sqls []string
	row  RowScanner
	rows RowsScanner
}

type zeroAffectedExec struct {
	fakeExec
}

func (f *zeroAffectedExec) Exec(ctx context.Context, sql string) (uint64, error) {
	f.sqls = append(f.sqls, sql)
	return 0, nil
}

func TestDAOOptimisticUpdateRejectsVersionConflict(t *testing.T) {
	exec := &zeroAffectedExec{}
	err := NewDAO(exec).UpdateOrphanFileCleanupStatus(context.Background(), 7, "job-1", "path-hash", OrphanCleanupStatusDeleted, 3)
	if err == nil || !strings.Contains(err.Error(), "ICEBERG_COMMIT_CONFLICT") {
		t.Fatalf("expected optimistic conflict, got %v", err)
	}
}

func (f *fakeExec) Exec(ctx context.Context, sql string) (uint64, error) {
	f.sqls = append(f.sqls, sql)
	return 1, nil
}

func (f *fakeExec) QueryRow(ctx context.Context, sql string) RowScanner {
	f.sqls = append(f.sqls, sql)
	if f.row != nil {
		return f.row
	}
	return fakeRow{}
}

func (f *fakeExec) Query(ctx context.Context, sql string) (RowsScanner, error) {
	f.sqls = append(f.sqls, sql)
	if f.rows != nil {
		return f.rows, nil
	}
	return fakeRows{}, nil
}

type fakeRow struct{}

func (fakeRow) Scan(dest ...any) error {
	return nil
}

type staticRow struct {
	values []any
}

func (r staticRow) Scan(dest ...any) error {
	for i := range dest {
		assignScanValue(dest[i], r.values[i])
	}
	return nil
}

type fakeRows struct{}

func (fakeRows) Close() error {
	return nil
}

func (fakeRows) Next() bool {
	return false
}

func (fakeRows) Scan(dest ...any) error {
	return nil
}

func (fakeRows) Err() error {
	return nil
}

type staticRows struct {
	rows [][]any
	idx  int
}

func (r *staticRows) Close() error {
	return nil
}

func (r *staticRows) Next() bool {
	return r.idx < len(r.rows)
}

func (r *staticRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i := range dest {
		assignScanValue(dest[i], row[i])
	}
	return nil
}

func (r *staticRows) Err() error {
	return nil
}

func assignScanValue(dest any, value any) {
	switch ptr := dest.(type) {
	case *uint32:
		*ptr = value.(uint32)
	case *uint64:
		*ptr = value.(uint64)
	case *string:
		*ptr = value.(string)
	case *time.Time:
		*ptr = value.(time.Time)
	}
}

func TestP0SystemTableDDLs(t *testing.T) {
	if len(P0SystemTableDDLs) != 5 {
		t.Fatalf("expected 5 P0 Iceberg system tables, got %d", len(P0SystemTableDDLs))
	}
	for _, def := range P0SystemTableDDLs {
		if !strings.Contains(def.DDL, "primary key") {
			t.Fatalf("%s DDL must define primary key", def.Name)
		}
	}
}

func TestP1WriteSystemTableDDLs(t *testing.T) {
	if len(P1WriteSystemTableDDLs) != 2 {
		t.Fatalf("expected 2 P1 Iceberg write system tables, got %d", len(P1WriteSystemTableDDLs))
	}
	for _, def := range P1WriteSystemTableDDLs {
		if _, err := mysql.ParseOne(context.Background(), def.DDL, 1); err != nil {
			t.Fatalf("%s DDL should parse: %v\n%s", def.Name, err, def.DDL)
		}
		lower := strings.ToLower(def.DDL)
		for _, want := range []string{"account_id", "created_at", "updated_at", "version", "primary key"} {
			if !strings.Contains(lower, want) {
				t.Fatalf("%s DDL missing %q: %s", def.Name, want, def.DDL)
			}
		}
		if !strings.Contains(lower, "key idx_iceberg_") {
			t.Fatalf("%s DDL should define query index: %s", def.Name, def.DDL)
		}
	}
}

func TestP2MaintenanceSystemTableDDLs(t *testing.T) {
	if len(P2MaintenanceSystemTableDDLs) != 1 {
		t.Fatalf("expected 1 P2 Iceberg maintenance system table, got %d", len(P2MaintenanceSystemTableDDLs))
	}
	for _, def := range P2MaintenanceSystemTableDDLs {
		if _, err := mysql.ParseOne(context.Background(), def.DDL, 1); err != nil {
			t.Fatalf("%s DDL should parse: %v\n%s", def.Name, err, def.DDL)
		}
		lower := strings.ToLower(def.DDL)
		for _, want := range []string{"account_id", "created_at", "updated_at", "version", "primary key", "key idx_iceberg_maintenance_account_status"} {
			if !strings.Contains(lower, want) {
				t.Fatalf("%s DDL missing %q: %s", def.Name, want, def.DDL)
			}
		}
	}
}

func TestDeferredMappingUpdateValidationAndApply(t *testing.T) {
	ctx := context.Background()
	valid := DeferredMappingUpdate{
		AccountID:                1,
		DatabaseID:               2,
		TableID:                  3,
		LastSnapshotID:           "123",
		LastMetadataLocationHash: "hash",
		ExpectedVersion:          4,
	}
	if err := ValidateDeferredMappingUpdate(ctx, valid); err != nil {
		t.Fatalf("valid deferred update should pass: %v", err)
	}
	sql := BuildDeferredMappingUpdateSQL(valid)
	for _, want := range []string{
		"update mo_catalog.mo_iceberg_tables",
		"last_snapshot_id = '123'",
		"last_metadata_location_hash = 'hash'",
		"account_id = 1",
		"db_id = 2",
		"table_id = 3",
		"version = 4",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("deferred update SQL missing %q: %s", want, sql)
		}
	}

	exec := &fakeExec{}
	if err := NewDAO(exec).ApplyDeferredMappingUpdate(ctx, valid); err != nil {
		t.Fatalf("apply deferred update should execute: %v", err)
	}
	if len(exec.sqls) != 1 || exec.sqls[0] != sql {
		t.Fatalf("unexpected executed SQL: %#v", exec.sqls)
	}

	if err := (&DAO{}).ApplyDeferredMappingUpdate(ctx, valid); err == nil {
		t.Fatalf("nil executor should be rejected")
	}
	for _, bad := range []DeferredMappingUpdate{
		{DatabaseID: 2, TableID: 3, LastSnapshotID: "123", LastMetadataLocationHash: "hash", ExpectedVersion: 4},
		{AccountID: 1, TableID: 3, LastSnapshotID: "123", LastMetadataLocationHash: "hash", ExpectedVersion: 4},
		{AccountID: 1, DatabaseID: 2, LastSnapshotID: "123", LastMetadataLocationHash: "hash", ExpectedVersion: 4},
		{AccountID: 1, DatabaseID: 2, TableID: 3, LastMetadataLocationHash: "hash", ExpectedVersion: 4},
		{AccountID: 1, DatabaseID: 2, TableID: 3, LastSnapshotID: "123", ExpectedVersion: 4},
		{AccountID: 1, DatabaseID: 2, TableID: 3, LastSnapshotID: "123", LastMetadataLocationHash: "hash"},
	} {
		if err := ValidateDeferredMappingUpdate(ctx, bad); err == nil {
			t.Fatalf("invalid deferred update should be rejected: %+v", bad)
		}
	}
}

func TestDAOInsertCatalogSQL(t *testing.T) {
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.InsertCatalog(context.Background(), model.Catalog{
		AccountID: 1,
		CatalogID: 2,
		Name:      "ksa",
		Type:      "rest",
		URI:       "https://catalog.example",
		AuthMode:  model.AuthModeCredential,
	})
	if err != nil {
		t.Fatalf("insert catalog: %v", err)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "mo_iceberg_catalogs") {
		t.Fatalf("unexpected sqls: %#v", exec.sqls)
	}
}

func TestInsertCatalogSQLUsesStorageAllocatorWhenIDIsUnset(t *testing.T) {
	sql := InsertCatalogSQL(model.Catalog{
		AccountID: 1,
		Name:      "ksa",
		Type:      "rest",
		URI:       "https://catalog.example",
	})
	require.NotContains(t, sql, "catalog_id")
	require.Contains(t, sql, "insert into mo_catalog.mo_iceberg_catalogs(account_id,name")
	require.Contains(t, CatalogsDDL, "catalog_id bigint unsigned not null auto_increment")
	require.Contains(t, CatalogsDDL, "primary key(account_id, catalog_id)")
}

func TestQuoteSQLStringEscapesBackslashAndQuote(t *testing.T) {
	sql := InsertCatalogSQL(model.Catalog{
		AccountID: 1,
		CatalogID: 2,
		Name:      `prod\' ; drop table mo_catalog.mo_user; --`,
		Type:      "rest",
		URI:       `https://catalog.example/path\'x`,
	})
	if strings.Contains(sql, `prod\' ;`) || strings.Contains(sql, `path\'x`) {
		t.Fatalf("backslash quote sequence was not escaped: %s", sql)
	}
	if !strings.Contains(sql, `prod\\'' ; drop table mo_catalog.mo_user; --`) {
		t.Fatalf("expected backslash and quote escaping in catalog name: %s", sql)
	}
	if !strings.Contains(sql, `path\\''x`) {
		t.Fatalf("expected backslash and quote escaping in uri: %s", sql)
	}
}

func TestListResidencyPoliciesAllowsClusterAccountZero(t *testing.T) {
	exec := &fakeExec{}
	dao := NewDAO(exec)
	_, err := dao.ListResidencyPolicies(context.Background(), 0, 7)
	if err != nil {
		t.Fatalf("list residency policies with account 0: %v", err)
	}
	if len(exec.sqls) != 1 {
		t.Fatalf("expected one query, got %#v", exec.sqls)
	}
	for _, want := range []string{"mo_iceberg_residency_policy", "account_id = 0", "catalog_id = 7"} {
		if !strings.Contains(exec.sqls[0], want) {
			t.Fatalf("residency policy query missing %q: %s", want, exec.sqls[0])
		}
	}
}

func TestDeferredUpdateSQL(t *testing.T) {
	sql := BuildDeferredMappingUpdateSQL(DeferredMappingUpdate{
		AccountID:                1,
		DatabaseID:               2,
		TableID:                  3,
		LastSnapshotID:           "44",
		LastMetadataLocationHash: "abc",
		ExpectedVersion:          7,
	})
	for _, want := range []string{"last_snapshot_id = '44'", "last_metadata_location_hash = 'abc'", "version = version + 1", "version = 7"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("deferred update sql missing %q: %s", want, sql)
		}
	}
}

func TestOptimisticTableMappingSQL(t *testing.T) {
	sql := UpdateTableMappingOptimisticSQL(model.TableMapping{
		AccountID:            1,
		DatabaseID:           2,
		TableID:              3,
		CatalogID:            4,
		Namespace:            "db",
		TableName:            "orders",
		LastSnapshotID:       "55",
		CapabilitiesJSON:     `{"read":true}`,
		WriterOwnerAccountID: 9,
	}, 8)
	for _, want := range []string{"mo_iceberg_tables", "version = version + 1", "version = 8", "namespace = 'db'", "table_name = 'orders'"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("optimistic update sql missing %q: %s", want, sql)
		}
	}
}

func TestRefCacheRefreshDAO(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 34, 56, 789000000, time.FixedZone("KSA", 3*60*60))
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.RefreshRefCache(context.Background(), []model.RefCache{
		{
			AccountID:  1,
			CatalogID:  2,
			Namespace:  `gold\'`,
			TableName:  "orders",
			RefName:    "main",
			RefType:    "branch",
			SnapshotID: "101",
			LastSeenAt: now,
		},
		{
			AccountID:  1,
			CatalogID:  2,
			Namespace:  `gold\'`,
			TableName:  "orders",
			RefName:    "release",
			RefType:    "tag",
			SnapshotID: "100",
			Source:     "nessie",
			Version:    7,
		},
	})
	if err != nil {
		t.Fatalf("refresh ref cache: %v", err)
	}
	if len(exec.sqls) != 3 {
		t.Fatalf("expected delete plus two inserts, got %#v", exec.sqls)
	}
	if !strings.Contains(exec.sqls[0], "delete from mo_catalog.mo_iceberg_refs") ||
		!strings.Contains(exec.sqls[0], `namespace = 'gold\\'''`) ||
		!strings.Contains(exec.sqls[0], "table_name = 'orders'") {
		t.Fatalf("unexpected ref cache delete SQL: %s", exec.sqls[0])
	}
	for _, want := range []string{"insert into mo_catalog.mo_iceberg_refs", "'main'", "'branch'", "'101'", "'catalog'", "'2026-06-18 09:34:56.789000'", ",1)"} {
		if !strings.Contains(exec.sqls[1], want) {
			t.Fatalf("ref cache insert SQL missing %q: %s", want, exec.sqls[1])
		}
	}
	for _, want := range []string{"'release'", "'tag'", "'100'", "'nessie'", "utc_timestamp", ",7)"} {
		if !strings.Contains(exec.sqls[2], want) {
			t.Fatalf("ref cache second insert SQL missing %q: %s", want, exec.sqls[2])
		}
	}

	sql := GetRefCacheSQL(1, 2, "db", "orders", "main")
	if !strings.Contains(sql, "select") || !strings.Contains(sql, "mo_iceberg_refs") {
		t.Fatalf("unexpected ref cache SQL: %s", sql)
	}
}

func TestRefCacheRefreshValidatesBeforeDelete(t *testing.T) {
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.RefreshRefCache(context.Background(), []model.RefCache{
		{
			AccountID:  1,
			CatalogID:  2,
			Namespace:  "gold",
			TableName:  "orders",
			RefName:    "main",
			RefType:    "branch",
			SnapshotID: "101",
		},
		{
			AccountID:  1,
			CatalogID:  2,
			Namespace:  "gold",
			TableName:  "customers",
			RefName:    "main",
			RefType:    "branch",
			SnapshotID: "201",
		},
	})
	if err == nil {
		t.Fatalf("expected mismatched table refresh to fail")
	}
	if len(exec.sqls) != 0 {
		t.Fatalf("refresh should validate before deleting cached refs, got %#v", exec.sqls)
	}
}

func TestListRefCacheStatusMarksStaleness(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	exec := &fakeExec{
		rows: &staticRows{rows: [][]any{
			{uint32(1), uint64(2), "gold", "orders", "main", "branch", "101", now.Add(-30 * time.Second), "catalog", uint64(1)},
			{uint32(1), uint64(2), "gold", "orders", "release", "tag", "100", now.Add(-10 * time.Minute), "catalog", uint64(1)},
		}},
	}
	dao := NewDAO(exec)
	refs, err := dao.ListRefCacheStatus(context.Background(), 1, 2, "gold", "orders", 5*time.Minute, now)
	if err != nil {
		t.Fatalf("list ref cache status: %v", err)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "order by ref_type,ref_name") || !strings.Contains(exec.sqls[0], "last_seen_at") {
		t.Fatalf("unexpected ref cache list SQL: %#v", exec.sqls)
	}
	if len(refs) != 2 {
		t.Fatalf("expected two refs, got %+v", refs)
	}
	if refs[0].RefName != "main" || refs[0].Stale || refs[0].Age != 30*time.Second {
		t.Fatalf("unexpected fresh ref status: %+v", refs[0])
	}
	if refs[1].RefName != "release" || !refs[1].Stale || refs[1].Age != 10*time.Minute {
		t.Fatalf("unexpected stale ref status: %+v", refs[1])
	}
}

func TestMOIRefCacheAPIListStatus(t *testing.T) {
	now := time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)
	exec := &fakeExec{
		rows: &staticRows{rows: [][]any{
			{uint32(1), uint64(2), "gold", "orders", "main", "branch", "101", now.Add(-30 * time.Second), "catalog", uint64(1)},
			{uint32(1), uint64(2), "gold", "orders", "release", "tag", "100", now.Add(-10 * time.Minute), "nessie", uint64(3)},
		}},
	}
	api := MOIRefCacheAPI{
		Lister: NewDAO(exec),
		Now:    func() time.Time { return now },
	}
	resp, err := api.ListRefCacheStatus(context.Background(), MOIRefCacheStatusRequest{
		AccountID:  1,
		CatalogID:  2,
		Namespace:  "gold",
		TableName:  "orders",
		StaleAfter: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("list MOI ref cache status: %v", err)
	}
	if resp.AccountID != 1 || resp.CatalogID != 2 || resp.Namespace != "gold" || resp.TableName != "orders" {
		t.Fatalf("unexpected response header: %+v", resp)
	}
	if len(resp.Refs) != 2 {
		t.Fatalf("expected two refs, got %+v", resp.Refs)
	}
	if resp.Refs[0].DisplayCaption != "branch:main" || resp.Refs[0].Status != "fresh" || resp.Refs[0].AgeSeconds != 30 || resp.Refs[0].LastSeenAt != "2026-06-18T11:59:30Z" {
		t.Fatalf("unexpected fresh MOI ref: %+v", resp.Refs[0])
	}
	if resp.Refs[1].DisplayCaption != "tag:release" || resp.Refs[1].Status != "stale" || !resp.Refs[1].Stale || resp.Refs[1].Source != "nessie" || resp.Refs[1].Version != 3 {
		t.Fatalf("unexpected stale MOI ref: %+v", resp.Refs[1])
	}
}

func TestPublishJobAuditSQL(t *testing.T) {
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.InsertPublishJob(context.Background(), model.PublishJob{
		AccountID:       1,
		JobID:           "job-1",
		SourceDB:        "silver",
		SourceTable:     "orders",
		TargetCatalogID: 7,
		TargetNamespace: "gold",
		TargetTable:     "orders",
		SourceBatch:     "batch-42",
		RowCount:        10,
		FileCount:       2,
		Status:          "pending",
		Version:         3,
	})
	if err != nil {
		t.Fatalf("insert publish job: %v", err)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "mo_iceberg_publish_jobs") || !strings.Contains(exec.sqls[0], "'job-1'") || !strings.Contains(exec.sqls[0], "'pending'") {
		t.Fatalf("unexpected publish job insert SQL: %#v", exec.sqls)
	}

	sql := UpdatePublishJobStatusSQL(1, "job-1", "committed", "", 3)
	for _, want := range []string{"status = 'committed'", "error_category = null", "version = version + 1", "version = 3"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("publish job status SQL missing %q: %s", want, sql)
		}
	}
}

func TestMaintenanceJobAuditSQL(t *testing.T) {
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.InsertMaintenanceJob(context.Background(), model.MaintenanceJob{
		AccountID:          0,
		JobID:              "maint-1",
		CatalogID:          7,
		Namespace:          "gold",
		TableName:          "orders",
		Operation:          "rewrite_manifests",
		SnapshotBefore:     "100",
		RewrittenFileCount: 2,
		RemovedFileCount:   1,
		Status:             "pending",
		Version:            4,
	})
	if err != nil {
		t.Fatalf("insert maintenance job: %v", err)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "mo_iceberg_maintenance_jobs") || !strings.Contains(exec.sqls[0], "values ('maint-1',0,7") || !strings.Contains(exec.sqls[0], "'main'") {
		t.Fatalf("unexpected maintenance job insert SQL: %#v", exec.sqls)
	}

	if err := dao.UpdateMaintenanceJobStatus(context.Background(), 0, "maint-1", "committed", "", "101", 5, 3, 4); err != nil {
		t.Fatalf("update maintenance job for system account: %v", err)
	}
	if len(exec.sqls) != 2 {
		t.Fatalf("expected insert and update SQL, got %#v", exec.sqls)
	}
	sql := exec.sqls[1]
	for _, want := range []string{"status = 'committed'", "error_category = null", "snapshot_after = '101'", "rewritten_file_count = 5", "removed_file_count = 3", "version = version + 1", "account_id = 0", "version = 4"} {
		if !strings.Contains(sql, want) {
			t.Fatalf("maintenance job status SQL missing %q: %s", want, sql)
		}
	}
}

func TestOrphanFileCleanupSQL(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 6000, time.FixedZone("KSA", 3*60*60))
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := dao.InsertOrphanFile(context.Background(), model.OrphanFile{
		AccountID:         0,
		JobID:             "job-1",
		CatalogID:         7,
		Namespace:         "gold",
		TableName:         "orders",
		TableLocationHash: "table-hash",
		FilePath:          "s3://warehouse/gold/orders/data/part-1.parquet",
		FilePathHash:      "file-hash",
		FilePathRedacted:  "<redacted:path:file-hash>",
		WrittenAt:         now,
		ExpireAt:          now.Add(time.Hour),
		CleanupStatus:     "pending",
		Version:           5,
	})
	if err != nil {
		t.Fatalf("insert orphan file: %v", err)
	}
	if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], "mo_iceberg_orphan_files") || !strings.Contains(exec.sqls[0], "values (0,'job-1',7") || !strings.Contains(exec.sqls[0], "'gold'") || !strings.Contains(exec.sqls[0], "'file-hash'") || strings.Contains(exec.sqls[0], "KSA") {
		t.Fatalf("unexpected orphan file insert SQL: %#v", exec.sqls)
	}

	listSQL := ListOrphanCleanupCandidatesSQL(0, 0)
	for _, want := range []string{"namespace", "table_name", "file_path", "account_id = 0", "cleanup_status = 'pending'", "expire_at <= utc_timestamp", "order by expire_at asc", "limit 100"} {
		if !strings.Contains(listSQL, want) {
			t.Fatalf("orphan cleanup list SQL missing %q: %s", want, listSQL)
		}
	}

	if err := dao.UpdateOrphanFileCleanupStatus(context.Background(), 0, "job-1", "file-hash", "deleted", 5); err != nil {
		t.Fatalf("update orphan cleanup status for system account: %v", err)
	}
	if len(exec.sqls) != 2 {
		t.Fatalf("expected insert and update SQL, got %#v", exec.sqls)
	}
	updateSQL := exec.sqls[1]
	for _, want := range []string{"cleanup_status = 'deleted'", "account_id = 0", "file_path_hash = 'file-hash'", "version = version + 1", "version = 5"} {
		if !strings.Contains(updateSQL, want) {
			t.Fatalf("orphan cleanup update SQL missing %q: %s", want, updateSQL)
		}
	}
}

func TestDAOReadMethodsScanRows(t *testing.T) {
	now := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	t.Run("catalog by name", func(t *testing.T) {
		exec := &fakeExec{row: staticRow{values: []any{
			uint32(1), uint64(7), "cat", "rest", "https://catalog.example", "s3://warehouse", model.AuthModeNone, "secret://cat/token", `{"commit":true}`, uint64(3),
		}}}
		got, err := NewDAO(exec).GetCatalogByName(context.Background(), 1, "cat")
		if err != nil {
			t.Fatalf("GetCatalogByName failed: %v", err)
		}
		if got.CatalogID != 7 || got.TokenSecretRef != "secret://cat/token" || !strings.Contains(exec.sqls[0], "name = 'cat'") {
			t.Fatalf("unexpected catalog lookup: %+v sql=%s", got, exec.sqls[0])
		}
	})
	t.Run("catalog by id", func(t *testing.T) {
		exec := &fakeExec{row: staticRow{values: []any{
			uint32(1), uint64(8), "cat2", "rest", "https://catalog2.example", "", model.AuthModeCredential, "", "", uint64(4),
		}}}
		got, err := NewDAO(exec).GetCatalogByID(context.Background(), 1, 8)
		if err != nil {
			t.Fatalf("GetCatalogByID failed: %v", err)
		}
		if got.Name != "cat2" || !strings.Contains(exec.sqls[0], "catalog_id = 8") {
			t.Fatalf("unexpected catalog by id: %+v sql=%s", got, exec.sqls[0])
		}
	})
	t.Run("table mapping", func(t *testing.T) {
		exec := &fakeExec{row: staticRow{values: []any{
			uint32(1), uint64(2), uint64(3), uint64(7), "gold", "orders", "main", model.ReadModeAppendOnly, model.WriteModeMergeOnRead, uint32(1), `{"delete":true}`, "101", "hash", uint64(5),
		}}}
		got, err := NewDAO(exec).GetTableMapping(context.Background(), 1, 2, 3)
		if err != nil {
			t.Fatalf("GetTableMapping failed: %v", err)
		}
		if got.WriteMode != model.WriteModeMergeOnRead || got.WriterOwnerAccountID != 1 || !strings.Contains(exec.sqls[0], "db_id = 2") {
			t.Fatalf("unexpected table mapping: %+v sql=%s", got, exec.sqls[0])
		}
	})
	t.Run("ref cache", func(t *testing.T) {
		exec := &fakeExec{row: staticRow{values: []any{
			uint32(1), uint64(7), "gold", "orders", "main", "branch", "101", now, "catalog", uint64(2),
		}}}
		got, err := NewDAO(exec).GetRefCache(context.Background(), 1, 7, "gold", "orders", "main")
		if err != nil {
			t.Fatalf("GetRefCache failed: %v", err)
		}
		if got.RefName != "main" || got.LastSeenAt != now || !strings.Contains(exec.sqls[0], "ref_name = 'main'") {
			t.Fatalf("unexpected ref cache: %+v sql=%s", got, exec.sqls[0])
		}
	})
}

func TestDAOListMethodsScanRows(t *testing.T) {
	now := time.Date(2026, 7, 6, 10, 0, 0, 0, time.UTC)
	t.Run("principal maps", func(t *testing.T) {
		exec := &fakeExec{rows: &staticRows{rows: [][]any{
			{uint32(1), uint64(7), uint64(11), uint64(0), "role-principal", `{"scope":"role"}`, uint64(1)},
			{uint32(1), uint64(7), uint64(0), uint64(22), "user-principal", "", uint64(2)},
		}}}
		got, err := NewDAO(exec).ListPrincipalMaps(context.Background(), 1, 7)
		if err != nil {
			t.Fatalf("ListPrincipalMaps failed: %v", err)
		}
		if len(got) != 2 || got[0].ExternalPrincipal != "role-principal" || got[1].MOUserID != 22 {
			t.Fatalf("unexpected principal maps: %+v", got)
		}
	})
	t.Run("principal maps allow sys account id", func(t *testing.T) {
		exec := &fakeExec{rows: &staticRows{rows: [][]any{
			{uint32(0), uint64(7), uint64(0), uint64(0), "sys-principal", `{}`, uint64(1)},
		}}}
		got, err := NewDAO(exec).ListPrincipalMaps(context.Background(), 0, 7)
		if err != nil {
			t.Fatalf("ListPrincipalMaps for sys account failed: %v", err)
		}
		if len(got) != 1 || got[0].AccountID != 0 || got[0].ExternalPrincipal != "sys-principal" {
			t.Fatalf("unexpected sys principal maps: %+v", got)
		}
		if !strings.Contains(exec.sqls[0], "account_id = 0") {
			t.Fatalf("sys account lookup should query account_id 0: %s", exec.sqls[0])
		}
	})
	t.Run("residency policies", func(t *testing.T) {
		exec := &fakeExec{rows: &staticRows{rows: [][]any{
			{model.ResidencyScopeCluster, uint32(0), uint64(7), "http://catalog", "s3.local", "us-east-1", "*", model.ResidencyPolicyEnabled, uint64(1)},
			{model.ResidencyScopeAccount, uint32(1), uint64(7), "http://catalog", "s3.local", "us-east-1", "warehouse", model.ResidencyPolicyAudit, uint64(2)},
		}}}
		got, err := NewDAO(exec).ListResidencyPolicies(context.Background(), 1, 7)
		if err != nil {
			t.Fatalf("ListResidencyPolicies failed: %v", err)
		}
		if len(got) != 2 || got[0].AccountID != 0 || got[1].PolicyState != model.ResidencyPolicyAudit {
			t.Fatalf("unexpected residency policies: %+v", got)
		}
	})
	t.Run("orphan candidates", func(t *testing.T) {
		exec := &fakeExec{rows: &staticRows{rows: [][]any{
			{uint32(1), "job-1", uint64(7), "gold", "orders", "table-hash", "s3://warehouse/data.parquet", "file-hash", "<redacted:path:file-hash>", "pending", uint64(3)},
		}}}
		got, err := NewDAO(exec).ListOrphanCleanupCandidates(context.Background(), 1, 25)
		if err != nil {
			t.Fatalf("ListOrphanCleanupCandidates failed: %v", err)
		}
		if len(got) != 1 || got[0].FilePathHash != "file-hash" || !strings.Contains(exec.sqls[0], "limit 25") {
			t.Fatalf("unexpected orphan candidates: %+v sql=%s", got, exec.sqls[0])
		}
	})
	_ = now
}

func TestDAOWriteMethodsValidateAndExecute(t *testing.T) {
	ctx := context.Background()
	catalog := model.Catalog{AccountID: 1, CatalogID: 7, Name: "cat", Type: "rest", URI: "https://catalog.example", Version: 2}
	mapping := model.TableMapping{AccountID: 1, DatabaseID: 2, TableID: 3, CatalogID: 7, Namespace: "gold", TableName: "orders", Version: 1}
	principal := model.PrincipalMap{AccountID: 1, CatalogID: 7, MORoleID: 11, ExternalPrincipal: "role-principal"}
	policy := model.ResidencyPolicy{ScopeType: model.ResidencyScopeCluster, AccountID: 0, CatalogID: 7, AllowedCatalogURI: "http://catalog", AllowedEndpoint: "s3.local", AllowedRegion: "us-east-1", AllowedBucket: "*", PolicyState: model.ResidencyPolicyEnabled}

	tests := []struct {
		name string
		run  func(*DAO) error
		want string
	}{
		{name: "update catalog", run: func(d *DAO) error { return d.UpdateCatalog(ctx, catalog) }, want: "update mo_catalog.mo_iceberg_catalogs"},
		{name: "delete catalog", run: func(d *DAO) error { return d.DeleteCatalog(ctx, 1, 7) }, want: "delete from mo_catalog.mo_iceberg_catalogs"},
		{name: "insert principal", run: func(d *DAO) error { return d.InsertPrincipalMap(ctx, principal) }, want: "insert into mo_catalog.mo_iceberg_principal_map"},
		{name: "insert residency", run: func(d *DAO) error { return d.InsertResidencyPolicy(ctx, policy) }, want: "insert into mo_catalog.mo_iceberg_residency_policy"},
		{name: "insert residency with privilege", run: func(d *DAO) error { return d.InsertResidencyPolicyWithPrivilege(ctx, 0, true, policy) }, want: "insert into mo_catalog.mo_iceberg_residency_policy"},
		{name: "insert table mapping", run: func(d *DAO) error { return d.InsertTableMapping(ctx, mapping) }, want: "insert into mo_catalog.mo_iceberg_tables"},
		{name: "update table mapping", run: func(d *DAO) error { return d.UpdateTableMappingOptimistic(ctx, mapping, 1) }, want: "update mo_catalog.mo_iceberg_tables"},
		{name: "update publish job", run: func(d *DAO) error { return d.UpdatePublishJobStatus(ctx, 1, "job", "committed", "", 1) }, want: "update mo_catalog.mo_iceberg_publish_jobs"},
		{name: "update orphan", run: func(d *DAO) error { return d.UpdateOrphanFileCleanupStatus(ctx, 1, "job", "hash", "deleted", 1) }, want: "update mo_catalog.mo_iceberg_orphan_files"},
		{name: "update maintenance", run: func(d *DAO) error {
			return d.UpdateMaintenanceJobStatus(ctx, 1, "job", "committed", "", "101", 2, 1, 1)
		}, want: "update mo_catalog.mo_iceberg_maintenance_jobs"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &fakeExec{}
			if err := tt.run(NewDAO(exec)); err != nil {
				t.Fatalf("method failed: %v", err)
			}
			if len(exec.sqls) != 1 || !strings.Contains(exec.sqls[0], tt.want) {
				t.Fatalf("unexpected SQL for %s: %#v", tt.name, exec.sqls)
			}
		})
	}

	invalids := []func(*DAO) error{
		func(d *DAO) error { return d.DeleteCatalog(ctx, 0, 7) },
		func(d *DAO) error { _, err := d.GetCatalogByName(ctx, 1, ""); return err },
		func(d *DAO) error { _, err := d.GetCatalogByID(ctx, 1, 0); return err },
		func(d *DAO) error { _, err := d.GetTableMapping(ctx, 1, 0, 3); return err },
		func(d *DAO) error { _, err := d.GetRefCache(ctx, 1, 7, "", "orders", "main"); return err },
		func(d *DAO) error { _, err := d.ListPrincipalMaps(ctx, 1, 0); return err },
		func(d *DAO) error { _, err := d.ListResidencyPolicies(ctx, 1, 0); return err },
		func(d *DAO) error { return d.UpdatePublishJobStatus(ctx, 1, "", "committed", "", 1) },
		func(d *DAO) error { return d.UpdateOrphanFileCleanupStatus(ctx, 1, "job", "", "deleted", 1) },
		func(d *DAO) error { return d.UpdateMaintenanceJobStatus(ctx, 1, "job", "", "", "", 0, 0, 1) },
	}
	for i, fn := range invalids {
		exec := &fakeExec{}
		if err := fn(NewDAO(exec)); err == nil {
			t.Fatalf("invalid case %d unexpectedly succeeded", i)
		}
		if len(exec.sqls) != 0 {
			t.Fatalf("invalid case %d should not execute SQL: %#v", i, exec.sqls)
		}
	}
}

func TestDAOAdditionalSQLBuilders(t *testing.T) {
	checks := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "update catalog",
			sql:  UpdateCatalogSQL(model.Catalog{AccountID: 1, CatalogID: 7, Name: "cat", Type: "rest", URI: "https://catalog", Version: 3}),
			want: []string{"update mo_catalog.mo_iceberg_catalogs", "version = version + 1", "version = 3"},
		},
		{
			name: "principal map insert",
			sql:  InsertPrincipalMapSQL(model.PrincipalMap{AccountID: 1, CatalogID: 7, MOUserID: 9, ExternalPrincipal: "user", Version: 2}),
			want: []string{"insert into mo_catalog.mo_iceberg_principal_map", "'user'", ",2)"},
		},
		{
			name: "principal map list",
			sql:  ListPrincipalMapsSQL(1, 7),
			want: []string{"select account_id,catalog_id,mo_role_id,mo_user_id", "catalog_id = 7"},
		},
		{
			name: "residency insert",
			sql:  InsertResidencyPolicySQL(model.ResidencyPolicy{ScopeType: model.ResidencyScopeAccount, AccountID: 1, CatalogID: 7, AllowedCatalogURI: "http://catalog", AllowedEndpoint: "s3.local", AllowedRegion: "*", AllowedBucket: "*"}),
			want: []string{"insert into mo_catalog.mo_iceberg_residency_policy", "'enabled'"},
		},
		{
			name: "table mapping insert",
			sql:  InsertTableMappingSQL(model.TableMapping{AccountID: 1, DatabaseID: 2, TableID: 3, CatalogID: 7, Namespace: "gold", TableName: "orders"}),
			want: []string{"insert into mo_catalog.mo_iceberg_tables", "'main'", "'append_only'", "'read_only'"},
		},
		{
			name: "catalog by name",
			sql:  GetCatalogByNameSQL(1, "cat"),
			want: []string{"from mo_catalog.mo_iceberg_catalogs", "name = 'cat'"},
		},
		{
			name: "catalog by id",
			sql:  GetCatalogByIDSQL(1, 7),
			want: []string{"from mo_catalog.mo_iceberg_catalogs", "catalog_id = 7"},
		},
		{
			name: "table mapping get",
			sql:  GetTableMappingSQL(1, 2, 3),
			want: []string{"from mo_catalog.mo_iceberg_tables", "db_id = 2", "table_id = 3"},
		},
		{
			name: "table mapping delete",
			sql:  DeleteTableMappingSQL(1, 2, 3),
			want: []string{"delete from mo_catalog.mo_iceberg_tables", "db_id = 2", "table_id = 3"},
		},
	}
	for _, tt := range checks {
		t.Run(tt.name, func(t *testing.T) {
			for _, want := range tt.want {
				if !strings.Contains(tt.sql, want) {
					t.Fatalf("%s SQL missing %q: %s", tt.name, want, tt.sql)
				}
			}
		})
	}
}

func TestOrphanCleanupStoreMapsDAOState(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	storeDAO := &fakeOrphanCleanupStoreDAO{
		files: []model.OrphanFile{{
			AccountID:         1,
			JobID:             "job-1",
			CatalogID:         7,
			Namespace:         "gold",
			TableName:         "orders",
			TableLocationHash: "table-hash",
			FilePath:          "s3://warehouse/gold/orders/data/part-1.parquet",
			FilePathHash:      "file-hash",
			FilePathRedacted:  "<redacted:path:file-hash>",
			WrittenAt:         now.Add(-time.Hour),
			ExpireAt:          now.Add(-time.Minute),
			CleanupStatus:     "pending",
			Version:           3,
		}},
	}
	store := OrphanCleanupStore{DAO: storeDAO, AccountID: 1}

	candidates, err := store.ListExpiredOrphans(context.Background(), now, 10)
	if err != nil {
		t.Fatalf("list orphan candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].FilePath == "" || candidates[0].Namespace != "gold" || candidates[0].Version != 3 {
		t.Fatalf("unexpected candidates: %+v", candidates)
	}
	if err := store.MarkOrphanDeleted(context.Background(), candidates[0]); err != nil {
		t.Fatalf("mark orphan deleted: %v", err)
	}
	if storeDAO.status != OrphanCleanupStatusDeleted || storeDAO.expectedVersion != 3 {
		t.Fatalf("unexpected delete status update: %+v", storeDAO)
	}
	if err := store.MarkOrphanFailed(context.Background(), candidates[0], "ICEBERG_ORPHAN_CLEANUP_FAILED"); err != nil {
		t.Fatalf("mark orphan failed: %v", err)
	}
	if storeDAO.status != OrphanCleanupStatusFailed {
		t.Fatalf("unexpected failed status update: %+v", storeDAO)
	}
}

func TestWriteWorkflowDAOAdapters(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	exec := &fakeExec{}
	dao := NewDAO(exec)
	err := PublishAuditRecorder{DAO: dao}.RecordPublish(context.Background(), icebergwrite.PublishAudit{
		AccountID:            1,
		JobID:                "job-1",
		TargetCatalogID:      7,
		TargetNamespace:      "gold",
		TargetTable:          "orders",
		SourceDB:             "silver",
		SourceTable:          "orders_src",
		SourceBatch:          "batch-42",
		SnapshotID:           200,
		MetadataLocationHash: "metadata-hash",
		CommitID:             "commit-1",
		RowCount:             12,
		FileCount:            3,
		Status:               "committed",
	})
	if err != nil {
		t.Fatalf("record publish audit: %v", err)
	}
	err = OrphanFileRecorder{DAO: dao}.RecordOrphans(context.Background(), []icebergwrite.OrphanCandidate{{
		AccountID:         1,
		JobID:             "job-1",
		CatalogID:         7,
		Namespace:         "gold",
		TableName:         "orders",
		TableLocationHash: api.PathHash("s3://warehouse/gold/orders"),
		FilePath:          "s3://warehouse/gold/orders/data/part-1.parquet",
		FilePathHash:      "file-hash",
		FilePathRedacted:  "<redacted:path:file-hash>",
		WrittenAt:         now,
		ExpireAt:          now.Add(time.Hour),
		CleanupStatus:     "pending",
	}})
	if err != nil {
		t.Fatalf("record orphan file: %v", err)
	}
	if len(exec.sqls) != 2 {
		t.Fatalf("expected two adapter SQL statements, got %#v", exec.sqls)
	}
	for _, want := range []string{"mo_iceberg_publish_jobs", "'job-1'", "'committed'", "'commit-1'", "'200'"} {
		if !strings.Contains(exec.sqls[0], want) {
			t.Fatalf("publish adapter SQL missing %q: %s", want, exec.sqls[0])
		}
	}
	for _, want := range []string{"mo_iceberg_orphan_files", "'file-hash'", "'pending'"} {
		if !strings.Contains(exec.sqls[1], want) {
			t.Fatalf("orphan adapter SQL missing %q: %s", want, exec.sqls[1])
		}
	}
}

func TestNewDMLCommitWorkflowWiresSQLAdapters(t *testing.T) {
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	store := &fakeDMLWorkflowStore{}
	writer := &fakeSQLManifestWriter{}
	committer := &fakeDMLWorkflowCommitter{
		result: &api.CommitResult{
			SnapshotID:           202,
			CommitID:             "commit-dml",
			MetadataLocationHash: "metadata-hash",
			Verified:             true,
		},
	}
	cfg := api.DefaultConfig()
	cfg.Write.OrphanTTL = 2 * time.Hour
	workflow := NewDMLCommitWorkflow(store, DMLCommitWorkflowOptions{
		Config:         cfg,
		ManifestWriter: writer,
		Committer:      committer,
		CommitVerifier: &fakeSQLDMLVerifier{verified: &api.CommitResult{SnapshotID: 303, CommitID: "verified-dml"}},
		Now:            func() time.Time { return now },
	})
	stream := dml.ActionStream{
		Operation: dml.OperationOverwrite,
		Base: dml.CommitBase{
			Namespace: api.Namespace{"gold"},
			Table:     "orders",
			TargetRef: "main",
			CatalogCapabilities: api.CatalogCapabilities{
				MetricsReport: true,
			},
			BaseSnapshotID: 101,
			IdempotencyKey: "stmt-dml",
			StatementID:    "stmt-dml",
		},
		Actions: []dml.Action{{
			Kind: dml.ActionAppendData,
			File: api.DataFile{
				FilePath:        "s3://warehouse/gold/orders/data/part-1.parquet",
				FileFormat:      "parquet",
				RecordCount:     12,
				FileSizeInBytes: 128,
				SpecID:          0,
			},
		}},
		Profile: dml.Profile{
			Operation:      dml.OperationOverwrite,
			AddedDataFiles: 1,
			MatchedRows:    12,
		},
	}
	result, err := workflow.CommitDML(context.Background(), dml.CommitWorkflowRequest{
		Catalog: api.CatalogRequest{
			Catalog: model.Catalog{
				AccountID: 7,
				CatalogID: 42,
				Name:      "ksa_gold",
			},
		},
		Stream:           stream,
		FormatVersion:    2,
		Schema:           api.Schema{SchemaID: 1, Fields: []api.SchemaField{{ID: 1, Name: "id", Type: api.IcebergType{Kind: api.TypeLong}}}},
		PartitionSpecs:   []api.PartitionSpec{{SpecID: 0}},
		SnapshotID:       202,
		SequenceNumber:   11,
		DataManifestPath: "s3://warehouse/gold/orders/metadata/data-202.avro",
		ManifestListPath: "s3://warehouse/gold/orders/metadata/snap-202.avro",
		TableLocation:    "s3://warehouse/gold/orders",
	})
	if err != nil {
		t.Fatalf("commit DML workflow: %v", err)
	}
	if result.SnapshotID != 303 || result.CommitID != "verified-dml" || !result.Verified {
		t.Fatalf("unexpected commit result: %+v", result)
	}
	if len(writer.paths) != 2 || writer.paths[0] != "s3://warehouse/gold/orders/metadata/data-202.avro" || writer.paths[1] != "s3://warehouse/gold/orders/metadata/snap-202.avro" {
		t.Fatalf("unexpected manifest writes: %#v", writer.paths)
	}
	if len(committer.requests) != 1 || committer.requests[0].IdempotencyKey != "stmt-dml" || committer.requests[0].TargetRef != "main" {
		t.Fatalf("unexpected commit requests: %+v", committer.requests)
	}
	if len(store.jobs) != 1 || store.jobs[0].JobID != "stmt-dml" || store.jobs[0].CommitID != "verified-dml" || store.jobs[0].RowCount != 12 {
		t.Fatalf("unexpected audit jobs: %+v", store.jobs)
	}
	if len(committer.metrics) != 1 || committer.metrics[0].StatementID != "stmt-dml" || committer.metrics[0].Rows != 12 {
		t.Fatalf("expected committer metrics reporter to be reused, got %+v", committer.metrics)
	}
}

type fakeOrphanCleanupStoreDAO struct {
	files           []model.OrphanFile
	accountID       uint32
	limit           int
	status          string
	expectedVersion uint64
}

func (d *fakeOrphanCleanupStoreDAO) ListOrphanCleanupCandidates(ctx context.Context, accountID uint32, limit int) ([]model.OrphanFile, error) {
	d.accountID = accountID
	d.limit = limit
	return append([]model.OrphanFile(nil), d.files...), nil
}

func (d *fakeOrphanCleanupStoreDAO) UpdateOrphanFileCleanupStatus(ctx context.Context, accountID uint32, jobID, filePathHash, cleanupStatus string, expectedVersion uint64) error {
	d.accountID = accountID
	d.status = cleanupStatus
	d.expectedVersion = expectedVersion
	return nil
}

type fakeDMLWorkflowStore struct {
	jobs    []model.PublishJob
	orphans []model.OrphanFile
}

func (s *fakeDMLWorkflowStore) InsertPublishJob(ctx context.Context, job model.PublishJob) error {
	s.jobs = append(s.jobs, job)
	return nil
}

func (s *fakeDMLWorkflowStore) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	s.orphans = append(s.orphans, file)
	return nil
}

type fakeSQLManifestWriter struct {
	paths []string
}

func (w *fakeSQLManifestWriter) WriteManifestObject(ctx context.Context, location string, payload []byte) error {
	if len(payload) == 0 {
		return api.NewError(api.ErrObjectIO, "empty manifest payload", nil)
	}
	w.paths = append(w.paths, location)
	return nil
}

type fakeDMLWorkflowCommitter struct {
	requests []api.CommitRequest
	metrics  []api.MetricsReportRequest
	result   *api.CommitResult
}

func (c *fakeDMLWorkflowCommitter) CommitTable(ctx context.Context, req api.CommitRequest) (*api.CommitResult, error) {
	c.requests = append(c.requests, req)
	if c.result != nil {
		return c.result, nil
	}
	return &api.CommitResult{SnapshotID: 1, Verified: true}, nil
}

func (c *fakeDMLWorkflowCommitter) ReportMetrics(ctx context.Context, req api.MetricsReportRequest) error {
	c.metrics = append(c.metrics, req)
	return nil
}

type recordingSQLOrphanRecorder struct {
	candidates []icebergwrite.OrphanCandidate
	err        error
}

func (r *recordingSQLOrphanRecorder) RecordOrphans(ctx context.Context, candidates []icebergwrite.OrphanCandidate) error {
	r.candidates = append(r.candidates, candidates...)
	return r.err
}

type fakeSQLDMLVerifier struct {
	verified *api.CommitResult
}

func (v *fakeSQLDMLVerifier) VerifyDMLCommit(ctx context.Context, req dml.CommitWorkflowRequest, materialized *dml.ManifestMaterializeResult, result *api.CommitResult) (*api.CommitResult, bool, error) {
	if v.verified == nil {
		return nil, false, nil
	}
	return v.verified, true, nil
}
