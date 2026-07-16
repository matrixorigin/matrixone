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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

type RowScanner interface {
	Scan(dest ...any) error
}

type RowsScanner interface {
	Close() error
	Next() bool
	Scan(dest ...any) error
	Err() error
}

type SQLExecutor interface {
	Exec(ctx context.Context, sql string) (uint64, error)
	QueryRow(ctx context.Context, sql string) RowScanner
	Query(ctx context.Context, sql string) (RowsScanner, error)
}

type DAO struct {
	exec SQLExecutor
}

type RefCacheStatus struct {
	model.RefCache
	Age   time.Duration
	Stale bool
}

func NewDAO(exec SQLExecutor) *DAO {
	return &DAO{exec: exec}
}

func (d *DAO) InsertCatalog(ctx context.Context, catalog model.Catalog) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateCatalog(ctx, catalog); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertCatalogSQL(catalog))
}

func (d *DAO) UpdateCatalog(ctx context.Context, catalog model.Catalog) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateCatalog(ctx, catalog); err != nil {
		return err
	}
	return d.execOptimistic(ctx, "catalog update", UpdateCatalogSQL(catalog))
}

func (d *DAO) DeleteCatalog(ctx context.Context, accountID uint32, catalogID uint64) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if accountID == 0 || catalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg catalog delete requires account_id and catalog_id")
	}
	return d.execSQL(ctx, fmt.Sprintf(
		"delete from mo_catalog.%s where account_id = %d and catalog_id = %d",
		TableCatalogs, accountID, catalogID,
	))
}

func (d *DAO) GetCatalogByName(ctx context.Context, accountID uint32, name string) (model.Catalog, error) {
	if d.exec == nil {
		return model.Catalog{}, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if trimNonEmpty(name) == "" {
		return model.Catalog{}, moerr.NewInvalidInput(ctx, "iceberg catalog lookup requires name")
	}
	var c model.Catalog
	err := d.exec.QueryRow(ctx, GetCatalogByNameSQL(accountID, name)).Scan(
		&c.AccountID,
		&c.CatalogID,
		&c.Name,
		&c.Type,
		&c.URI,
		&c.Warehouse,
		&c.AuthMode,
		&c.TokenSecretRef,
		&c.CapabilitiesJSON,
		&c.Version,
	)
	return c, err
}

func (d *DAO) GetCatalogByID(ctx context.Context, accountID uint32, catalogID uint64) (model.Catalog, error) {
	if d.exec == nil {
		return model.Catalog{}, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if accountID == 0 || catalogID == 0 {
		return model.Catalog{}, moerr.NewInvalidInput(ctx, "iceberg catalog lookup requires account_id and catalog_id")
	}
	var c model.Catalog
	err := d.exec.QueryRow(ctx, GetCatalogByIDSQL(accountID, catalogID)).Scan(
		&c.AccountID,
		&c.CatalogID,
		&c.Name,
		&c.Type,
		&c.URI,
		&c.Warehouse,
		&c.AuthMode,
		&c.TokenSecretRef,
		&c.CapabilitiesJSON,
		&c.Version,
	)
	return c, err
}

func (d *DAO) InsertPrincipalMap(ctx context.Context, mapping model.PrincipalMap) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidatePrincipalMap(ctx, mapping); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertPrincipalMapSQL(mapping))
}

func (d *DAO) ListPrincipalMaps(ctx context.Context, accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
	if d.exec == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if catalogID == 0 {
		return nil, moerr.NewInvalidInput(ctx, "iceberg principal map lookup requires catalog_id")
	}
	rows, err := d.exec.Query(ctx, ListPrincipalMapsSQL(accountID, catalogID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	mappings := make([]model.PrincipalMap, 0)
	for rows.Next() {
		var mapping model.PrincipalMap
		if err := rows.Scan(&mapping.AccountID, &mapping.CatalogID, &mapping.MORoleID, &mapping.MOUserID, &mapping.ExternalPrincipal, &mapping.ScopeJSON, &mapping.Version); err != nil {
			return nil, err
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mappings, nil
}

func (d *DAO) InsertResidencyPolicy(ctx context.Context, policy model.ResidencyPolicy) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateResidencyPolicy(ctx, policy); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertResidencyPolicySQL(policy))
}

func (d *DAO) InsertResidencyPolicyWithPrivilege(ctx context.Context, actorAccountID uint32, isClusterAdmin bool, policy model.ResidencyPolicy) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateResidencyPolicyPrivilege(ctx, actorAccountID, isClusterAdmin, policy); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertResidencyPolicySQL(policy))
}

func (d *DAO) ListResidencyPolicies(ctx context.Context, accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
	if d.exec == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if catalogID == 0 {
		return nil, moerr.NewInvalidInput(ctx, "iceberg residency lookup requires catalog_id")
	}
	rows, err := d.exec.Query(ctx, ListResidencyPoliciesSQL(accountID, catalogID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	policies := make([]model.ResidencyPolicy, 0)
	for rows.Next() {
		var policy model.ResidencyPolicy
		if err := rows.Scan(&policy.ScopeType, &policy.AccountID, &policy.CatalogID, &policy.AllowedCatalogURI, &policy.AllowedEndpoint, &policy.AllowedRegion, &policy.AllowedBucket, &policy.PolicyState, &policy.Version); err != nil {
			return nil, err
		}
		policies = append(policies, policy)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return policies, nil
}

func (d *DAO) InsertTableMapping(ctx context.Context, mapping model.TableMapping) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateTableMapping(ctx, mapping); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertTableMappingSQL(mapping))
}

func (d *DAO) GetTableMapping(ctx context.Context, accountID uint32, databaseID uint64, tableID uint64) (model.TableMapping, error) {
	if d.exec == nil {
		return model.TableMapping{}, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if databaseID == 0 || tableID == 0 {
		return model.TableMapping{}, moerr.NewInvalidInput(ctx, "iceberg table mapping lookup requires db_id and table_id")
	}
	var mapping model.TableMapping
	err := d.exec.QueryRow(ctx, GetTableMappingSQL(accountID, databaseID, tableID)).Scan(
		&mapping.AccountID,
		&mapping.DatabaseID,
		&mapping.TableID,
		&mapping.CatalogID,
		&mapping.Namespace,
		&mapping.TableName,
		&mapping.DefaultRef,
		&mapping.ReadMode,
		&mapping.WriteMode,
		&mapping.WriterOwnerAccountID,
		&mapping.CapabilitiesJSON,
		&mapping.LastSnapshotID,
		&mapping.LastMetadataLocationHash,
		&mapping.Version,
	)
	return mapping, err
}

func (d *DAO) UpdateTableMappingOptimistic(ctx context.Context, mapping model.TableMapping, expectedVersion uint64) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateTableMapping(ctx, mapping); err != nil {
		return err
	}
	if expectedVersion == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg table mapping optimistic update requires expected version")
	}
	return d.execOptimistic(ctx, "table mapping update", UpdateTableMappingOptimisticSQL(mapping, expectedVersion))
}

func (d *DAO) GetRefCache(ctx context.Context, accountID uint32, catalogID uint64, namespace, tableName, refName string) (model.RefCache, error) {
	if d.exec == nil {
		return model.RefCache{}, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if accountID == 0 || catalogID == 0 || trimNonEmpty(namespace) == "" || trimNonEmpty(tableName) == "" || trimNonEmpty(refName) == "" {
		return model.RefCache{}, moerr.NewInvalidInput(ctx, "iceberg ref cache lookup requires account_id, catalog_id, namespace, table_name, and ref_name")
	}
	var ref model.RefCache
	err := d.exec.QueryRow(ctx, GetRefCacheSQL(accountID, catalogID, namespace, tableName, refName)).Scan(
		&ref.AccountID,
		&ref.CatalogID,
		&ref.Namespace,
		&ref.TableName,
		&ref.RefName,
		&ref.RefType,
		&ref.SnapshotID,
		&ref.LastSeenAt,
		&ref.Source,
		&ref.Version,
	)
	return ref, err
}

func (d *DAO) ListRefCacheStatus(ctx context.Context, accountID uint32, catalogID uint64, namespace, tableName string, staleAfter time.Duration, now time.Time) ([]RefCacheStatus, error) {
	if d.exec == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if accountID == 0 || catalogID == 0 || trimNonEmpty(namespace) == "" || trimNonEmpty(tableName) == "" {
		return nil, moerr.NewInvalidInput(ctx, "iceberg ref cache list requires account_id, catalog_id, namespace, and table_name")
	}
	rows, err := d.exec.Query(ctx, ListRefCacheSQL(accountID, catalogID, namespace, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	refs := make([]RefCacheStatus, 0)
	for rows.Next() {
		var ref model.RefCache
		if err := rows.Scan(
			&ref.AccountID,
			&ref.CatalogID,
			&ref.Namespace,
			&ref.TableName,
			&ref.RefName,
			&ref.RefType,
			&ref.SnapshotID,
			&ref.LastSeenAt,
			&ref.Source,
			&ref.Version,
		); err != nil {
			return nil, err
		}
		status := RefCacheStatus{RefCache: ref}
		if !ref.LastSeenAt.IsZero() {
			status.Age = now.Sub(ref.LastSeenAt.UTC())
			if status.Age < 0 {
				status.Age = 0
			}
			status.Stale = staleAfter > 0 && status.Age > staleAfter
		} else {
			status.Stale = staleAfter > 0
		}
		refs = append(refs, status)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return refs, nil
}

func (d *DAO) RefreshRefCache(ctx context.Context, refs []model.RefCache) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if len(refs) == 0 {
		return nil
	}
	normalized, err := normalizeRefCacheRefresh(ctx, refs)
	if err != nil {
		return err
	}
	first := normalized[0]
	if err := d.execSQL(ctx, DeleteRefCacheSQL(first.AccountID, first.CatalogID, first.Namespace, first.TableName)); err != nil {
		return err
	}
	for _, ref := range normalized {
		if err := d.execSQL(ctx, InsertRefCacheSQL(ref)); err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) InsertPublishJob(ctx context.Context, job model.PublishJob) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidatePublishJob(ctx, job); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertPublishJobSQL(job))
}

func (d *DAO) UpdatePublishJobStatus(ctx context.Context, accountID uint32, jobID, status, errorCategory string, expectedVersion uint64) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if trimNonEmpty(jobID) == "" || trimNonEmpty(status) == "" || expectedVersion == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg publish job status update requires account_id, job_id, status, and expected version")
	}
	return d.execOptimistic(ctx, "publish job status update", UpdatePublishJobStatusSQL(accountID, jobID, status, errorCategory, expectedVersion))
}

func (d *DAO) InsertOrphanFile(ctx context.Context, file model.OrphanFile) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateOrphanFile(ctx, file); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertOrphanFileSQL(file))
}

func (d *DAO) ListOrphanCleanupCandidates(ctx context.Context, accountID uint32, limit int) ([]model.OrphanFile, error) {
	if d.exec == nil {
		return nil, moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	rows, err := d.exec.Query(ctx, ListOrphanCleanupCandidatesSQL(accountID, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := make([]model.OrphanFile, 0)
	for rows.Next() {
		var file model.OrphanFile
		if err := rows.Scan(&file.AccountID, &file.JobID, &file.CatalogID, &file.Namespace, &file.TableName, &file.TableLocationHash, &file.FilePath, &file.FilePathHash, &file.FilePathRedacted, &file.CleanupStatus, &file.Version); err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return files, nil
}

func (d *DAO) UpdateOrphanFileCleanupStatus(ctx context.Context, accountID uint32, jobID, filePathHash, cleanupStatus string, expectedVersion uint64) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if trimNonEmpty(jobID) == "" || trimNonEmpty(filePathHash) == "" || trimNonEmpty(cleanupStatus) == "" || expectedVersion == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg orphan cleanup status update requires job_id, file_path_hash, cleanup_status, and expected version")
	}
	return d.execOptimistic(ctx, "orphan cleanup status update", UpdateOrphanFileCleanupStatusSQL(accountID, jobID, filePathHash, cleanupStatus, expectedVersion))
}

func (d *DAO) InsertMaintenanceJob(ctx context.Context, job model.MaintenanceJob) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if err := ValidateMaintenanceJob(ctx, job); err != nil {
		return err
	}
	return d.execSQL(ctx, InsertMaintenanceJobSQL(job))
}

func (d *DAO) UpdateMaintenanceJobStatus(ctx context.Context, accountID uint32, jobID, status, errorCategory string, snapshotAfter string, rewrittenFileCount, removedFileCount uint64, expectedVersion uint64) error {
	if d.exec == nil {
		return moerr.NewInvalidInput(ctx, "iceberg DAO executor is nil")
	}
	if trimNonEmpty(jobID) == "" || trimNonEmpty(status) == "" || expectedVersion == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg maintenance job status update requires job_id, status, and expected version")
	}
	return d.execOptimistic(ctx, "maintenance job status update", UpdateMaintenanceJobStatusSQL(accountID, jobID, status, errorCategory, snapshotAfter, rewrittenFileCount, removedFileCount, expectedVersion))
}

func (d *DAO) execSQL(ctx context.Context, sql string) error {
	_, err := d.exec.Exec(ctx, sql)
	return err
}

func (d *DAO) execOptimistic(ctx context.Context, operation, sql string) error {
	affected, err := d.exec.Exec(ctx, sql)
	if err != nil {
		return err
	}
	if affected != 1 {
		return api.ToMOErr(ctx, api.NewError(api.ErrCommitConflict, "Iceberg optimistic update did not match the expected version", map[string]string{
			"operation":     operation,
			"affected_rows": fmt.Sprintf("%d", affected),
		}))
	}
	return nil
}

func ValidateCatalog(ctx context.Context, catalog model.Catalog) error {
	if catalog.AccountID == 0 || catalog.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg catalog requires account_id and catalog_id")
	}
	if trimNonEmpty(catalog.Name) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg catalog name is required")
	}
	if trimNonEmpty(catalog.Type) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg catalog type is required")
	}
	if trimNonEmpty(catalog.URI) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg catalog uri is required")
	}
	return nil
}

func ValidateTableMapping(ctx context.Context, mapping model.TableMapping) error {
	if mapping.AccountID == 0 || mapping.DatabaseID == 0 || mapping.TableID == 0 || mapping.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg table mapping requires account_id, db_id, table_id, and catalog_id")
	}
	if trimNonEmpty(mapping.Namespace) == "" || trimNonEmpty(mapping.TableName) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg table mapping requires namespace and table_name")
	}
	return nil
}

func ValidateRefCache(ctx context.Context, ref model.RefCache) error {
	if ref.AccountID == 0 || ref.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg ref cache requires account_id and catalog_id")
	}
	if trimNonEmpty(ref.Namespace) == "" || trimNonEmpty(ref.TableName) == "" || trimNonEmpty(ref.RefName) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg ref cache requires namespace, table_name, and ref_name")
	}
	if trimNonEmpty(ref.RefType) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg ref cache requires ref_type")
	}
	if trimNonEmpty(ref.SnapshotID) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg ref cache requires snapshot_id")
	}
	return nil
}

func normalizeRefCacheRefresh(ctx context.Context, refs []model.RefCache) ([]model.RefCache, error) {
	out := make([]model.RefCache, len(refs))
	for i, ref := range refs {
		if ref.Source == "" {
			ref.Source = "catalog"
		}
		if ref.Version == 0 {
			ref.Version = 1
		}
		if err := ValidateRefCache(ctx, ref); err != nil {
			return nil, err
		}
		if i > 0 {
			first := out[0]
			if ref.AccountID != first.AccountID ||
				ref.CatalogID != first.CatalogID ||
				ref.Namespace != first.Namespace ||
				ref.TableName != first.TableName {
				return nil, moerr.NewInvalidInput(ctx, "iceberg ref cache refresh requires refs from one table")
			}
		}
		out[i] = ref
	}
	return out, nil
}

func ValidatePublishJob(ctx context.Context, job model.PublishJob) error {
	if job.AccountID == 0 || trimNonEmpty(job.JobID) == "" || job.TargetCatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg publish job requires account_id, job_id, and target_catalog_id")
	}
	if trimNonEmpty(job.TargetNamespace) == "" || trimNonEmpty(job.TargetTable) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg publish job requires target namespace and table")
	}
	if trimNonEmpty(job.Status) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg publish job requires status")
	}
	return nil
}

func ValidateOrphanFile(ctx context.Context, file model.OrphanFile) error {
	if trimNonEmpty(file.JobID) == "" || file.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg orphan file requires job_id and catalog_id")
	}
	if trimNonEmpty(file.Namespace) == "" || trimNonEmpty(file.TableName) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg orphan file requires namespace and table_name")
	}
	if trimNonEmpty(file.TableLocationHash) == "" || trimNonEmpty(file.FilePath) == "" || trimNonEmpty(file.FilePathHash) == "" || trimNonEmpty(file.FilePathRedacted) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg orphan file requires table_location_hash, file_path, file_path_hash, and file_path_redacted")
	}
	if file.WrittenAt.IsZero() || file.ExpireAt.IsZero() {
		return moerr.NewInvalidInput(ctx, "iceberg orphan file requires written_at and expire_at")
	}
	if trimNonEmpty(file.CleanupStatus) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg orphan file requires cleanup_status")
	}
	return nil
}

func ValidateMaintenanceJob(ctx context.Context, job model.MaintenanceJob) error {
	if trimNonEmpty(job.JobID) == "" || job.CatalogID == 0 {
		return moerr.NewInvalidInput(ctx, "iceberg maintenance job requires job_id and catalog_id")
	}
	if trimNonEmpty(job.Namespace) == "" || trimNonEmpty(job.TableName) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg maintenance job requires namespace and table_name")
	}
	if trimNonEmpty(job.Operation) == "" || trimNonEmpty(job.Status) == "" {
		return moerr.NewInvalidInput(ctx, "iceberg maintenance job requires operation and status")
	}
	return nil
}

func InsertCatalogSQL(c model.Catalog) string {
	if c.CatalogID == 0 {
		// SQL-facing CREATE lets the table's auto-increment allocator choose a
		// race-free id. Client-side MAX(id)+1 races across concurrent CNs; callers
		// that import a stable account-scoped id may still use the explicit form.
		return fmt.Sprintf(
			"insert into mo_catalog.%s(account_id,name,type,uri,warehouse,auth_mode,token_secret_ref,capabilities_json,version) values (%d,%s,%s,%s,%s,%s,%s,%s,%d)",
			TableCatalogs,
			c.AccountID,
			quoteSQLString(c.Name),
			quoteSQLString(c.Type),
			quoteSQLString(c.URI),
			nullOrSQLString(c.Warehouse),
			quoteSQLString(defaultString(c.AuthMode, model.AuthModeNone)),
			nullOrSQLString(c.TokenSecretRef),
			nullOrSQLString(c.CapabilitiesJSON),
			defaultVersion(c.Version),
		)
	}
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,catalog_id,name,type,uri,warehouse,auth_mode,token_secret_ref,capabilities_json,version) values (%d,%d,%s,%s,%s,%s,%s,%s,%s,%d)",
		TableCatalogs,
		c.AccountID,
		c.CatalogID,
		quoteSQLString(c.Name),
		quoteSQLString(c.Type),
		quoteSQLString(c.URI),
		nullOrSQLString(c.Warehouse),
		quoteSQLString(defaultString(c.AuthMode, model.AuthModeNone)),
		nullOrSQLString(c.TokenSecretRef),
		nullOrSQLString(c.CapabilitiesJSON),
		defaultVersion(c.Version),
	)
}

func UpdateCatalogSQL(c model.Catalog) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set name = %s, type = %s, uri = %s, warehouse = %s, auth_mode = %s, token_secret_ref = %s, capabilities_json = %s, updated_at = utc_timestamp, version = version + 1 where account_id = %d and catalog_id = %d and version = %d",
		TableCatalogs,
		quoteSQLString(c.Name),
		quoteSQLString(c.Type),
		quoteSQLString(c.URI),
		nullOrSQLString(c.Warehouse),
		quoteSQLString(defaultString(c.AuthMode, model.AuthModeNone)),
		nullOrSQLString(c.TokenSecretRef),
		nullOrSQLString(c.CapabilitiesJSON),
		c.AccountID,
		c.CatalogID,
		defaultVersion(c.Version),
	)
}

func InsertPrincipalMapSQL(m model.PrincipalMap) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,catalog_id,mo_role_id,mo_user_id,external_principal,scope_json,created_by,version) values (%d,%d,%d,%d,%s,%s,%d,%d)",
		TablePrincipalMap,
		m.AccountID,
		m.CatalogID,
		m.MORoleID,
		m.MOUserID,
		quoteSQLString(m.ExternalPrincipal),
		nullOrSQLString(m.ScopeJSON),
		m.CreatedBy,
		defaultVersion(m.Version),
	)
}

func ListPrincipalMapsSQL(accountID uint32, catalogID uint64) string {
	return fmt.Sprintf(
		"select account_id,catalog_id,mo_role_id,mo_user_id,external_principal,scope_json,version from mo_catalog.%s where account_id = %d and catalog_id = %d",
		TablePrincipalMap,
		accountID,
		catalogID,
	)
}

func InsertResidencyPolicySQL(p model.ResidencyPolicy) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(scope_type,account_id,catalog_id,allowed_catalog_uri,allowed_endpoint,allowed_region,allowed_bucket,policy_state,created_by,version) values (%s,%d,%d,%s,%s,%s,%s,%s,%d,%d)",
		TableResidencyPolicy,
		quoteSQLString(p.ScopeType),
		p.AccountID,
		p.CatalogID,
		quoteSQLString(p.AllowedCatalogURI),
		quoteSQLString(p.AllowedEndpoint),
		quoteSQLString(p.AllowedRegion),
		quoteSQLString(p.AllowedBucket),
		quoteSQLString(defaultString(p.PolicyState, model.ResidencyPolicyEnabled)),
		p.CreatedBy,
		defaultVersion(p.Version),
	)
}

func ListResidencyPoliciesSQL(accountID uint32, catalogID uint64) string {
	return fmt.Sprintf(
		"select scope_type,account_id,catalog_id,allowed_catalog_uri,allowed_endpoint,allowed_region,allowed_bucket,policy_state,version from mo_catalog.%s where (scope_type = 'cluster' or account_id = %d) and (catalog_id = 0 or catalog_id = %d)",
		TableResidencyPolicy,
		accountID,
		catalogID,
	)
}

func InsertTableMappingSQL(m model.TableMapping) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,db_id,table_id,catalog_id,namespace,table_name,default_ref,read_mode,write_mode,writer_owner_account_id,capabilities_json,last_snapshot_id,last_metadata_location_hash,version) values (%d,%d,%d,%d,%s,%s,%s,%s,%s,%d,%s,%s,%s,%d)",
		TableTables,
		m.AccountID,
		m.DatabaseID,
		m.TableID,
		m.CatalogID,
		quoteSQLString(m.Namespace),
		quoteSQLString(m.TableName),
		quoteSQLString(defaultString(m.DefaultRef, model.DefaultRefMain)),
		quoteSQLString(defaultString(m.ReadMode, model.ReadModeAppendOnly)),
		quoteSQLString(defaultString(m.WriteMode, model.WriteModeReadOnly)),
		m.WriterOwnerAccountID,
		nullOrSQLString(m.CapabilitiesJSON),
		nullOrSQLString(m.LastSnapshotID),
		nullOrSQLString(m.LastMetadataLocationHash),
		defaultVersion(m.Version),
	)
}

func InsertPublishJobSQL(j model.PublishJob) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(job_id,account_id,source_db,source_table,target_catalog_id,target_namespace,target_table,source_batch,watermark_start,watermark_end,business_window,snapshot_id,commit_id,row_count,file_count,status,error_category,version) values (%s,%d,%s,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%d)",
		TablePublishJobs,
		quoteSQLString(j.JobID),
		j.AccountID,
		nullOrSQLString(j.SourceDB),
		nullOrSQLString(j.SourceTable),
		j.TargetCatalogID,
		quoteSQLString(j.TargetNamespace),
		quoteSQLString(j.TargetTable),
		nullOrSQLString(j.SourceBatch),
		nullOrSQLString(j.WatermarkStart),
		nullOrSQLString(j.WatermarkEnd),
		nullOrSQLString(j.BusinessWindow),
		nullOrSQLString(j.SnapshotID),
		nullOrSQLString(j.CommitID),
		j.RowCount,
		j.FileCount,
		quoteSQLString(j.Status),
		nullOrSQLString(j.ErrorCategory),
		defaultVersion(j.Version),
	)
}

func UpdatePublishJobStatusSQL(accountID uint32, jobID, status, errorCategory string, expectedVersion uint64) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set status = %s, error_category = %s, status_updated_at = utc_timestamp, updated_at = utc_timestamp, version = version + 1 where account_id = %d and job_id = %s and version = %d",
		TablePublishJobs,
		quoteSQLString(status),
		nullOrSQLString(errorCategory),
		accountID,
		quoteSQLString(jobID),
		expectedVersion,
	)
}

func InsertOrphanFileSQL(f model.OrphanFile) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,job_id,catalog_id,namespace,table_name,table_location_hash,file_path,file_path_hash,file_path_redacted,written_at,expire_at,cleanup_status,version) values (%d,%s,%d,%s,%s,%s,%s,%s,%s,%s,%s,%s,%d)",
		TableOrphanFiles,
		f.AccountID,
		quoteSQLString(f.JobID),
		f.CatalogID,
		quoteSQLString(f.Namespace),
		quoteSQLString(f.TableName),
		quoteSQLString(f.TableLocationHash),
		quoteSQLString(f.FilePath),
		quoteSQLString(f.FilePathHash),
		quoteSQLString(f.FilePathRedacted),
		quoteUTCTimestamp(f.WrittenAt),
		quoteUTCTimestamp(f.ExpireAt),
		quoteSQLString(f.CleanupStatus),
		defaultVersion(f.Version),
	)
}

func ListOrphanCleanupCandidatesSQL(accountID uint32, limit int) string {
	if limit <= 0 {
		limit = 100
	}
	return fmt.Sprintf(
		"select account_id,job_id,catalog_id,namespace,table_name,table_location_hash,file_path,file_path_hash,file_path_redacted,cleanup_status,version from mo_catalog.%s where account_id = %d and cleanup_status = 'pending' and expire_at <= utc_timestamp order by expire_at asc limit %d",
		TableOrphanFiles,
		accountID,
		limit,
	)
}

func UpdateOrphanFileCleanupStatusSQL(accountID uint32, jobID, filePathHash, cleanupStatus string, expectedVersion uint64) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set cleanup_status = %s, updated_at = utc_timestamp, version = version + 1 where account_id = %d and job_id = %s and file_path_hash = %s and version = %d",
		TableOrphanFiles,
		quoteSQLString(cleanupStatus),
		accountID,
		quoteSQLString(jobID),
		quoteSQLString(filePathHash),
		expectedVersion,
	)
}

func InsertMaintenanceJobSQL(j model.MaintenanceJob) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(job_id,account_id,catalog_id,namespace,table_name,operation,target_ref,snapshot_before,snapshot_after,rewritten_file_count,removed_file_count,status,error_category,version) values (%s,%d,%d,%s,%s,%s,%s,%s,%s,%d,%d,%s,%s,%d)",
		TableMaintenanceJobs,
		quoteSQLString(j.JobID),
		j.AccountID,
		j.CatalogID,
		quoteSQLString(j.Namespace),
		quoteSQLString(j.TableName),
		quoteSQLString(j.Operation),
		quoteSQLString(defaultString(j.TargetRef, model.DefaultRefMain)),
		nullOrSQLString(j.SnapshotBefore),
		nullOrSQLString(j.SnapshotAfter),
		j.RewrittenFileCount,
		j.RemovedFileCount,
		quoteSQLString(j.Status),
		nullOrSQLString(j.ErrorCategory),
		defaultVersion(j.Version),
	)
}

func InsertRefCacheSQL(r model.RefCache) string {
	return fmt.Sprintf(
		"insert into mo_catalog.%s(account_id,catalog_id,namespace,table_name,ref_name,ref_type,snapshot_id,last_seen_at,source,version) values (%d,%d,%s,%s,%s,%s,%s,%s,%s,%d)",
		TableRefs,
		r.AccountID,
		r.CatalogID,
		quoteSQLString(r.Namespace),
		quoteSQLString(r.TableName),
		quoteSQLString(r.RefName),
		quoteSQLString(r.RefType),
		quoteSQLString(r.SnapshotID),
		timestampOrUTCNow(r.LastSeenAt),
		quoteSQLString(defaultString(r.Source, "catalog")),
		defaultVersion(r.Version),
	)
}

func DeleteRefCacheSQL(accountID uint32, catalogID uint64, namespace, tableName string) string {
	return fmt.Sprintf(
		"delete from mo_catalog.%s where account_id = %d and catalog_id = %d and namespace = %s and table_name = %s",
		TableRefs,
		accountID,
		catalogID,
		quoteSQLString(namespace),
		quoteSQLString(tableName),
	)
}

func UpdateMaintenanceJobStatusSQL(accountID uint32, jobID, status, errorCategory string, snapshotAfter string, rewrittenFileCount, removedFileCount uint64, expectedVersion uint64) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set status = %s, error_category = %s, snapshot_after = %s, rewritten_file_count = %d, removed_file_count = %d, status_updated_at = utc_timestamp, updated_at = utc_timestamp, version = version + 1 where account_id = %d and job_id = %s and version = %d",
		TableMaintenanceJobs,
		quoteSQLString(status),
		nullOrSQLString(errorCategory),
		nullOrSQLString(snapshotAfter),
		rewrittenFileCount,
		removedFileCount,
		accountID,
		quoteSQLString(jobID),
		expectedVersion,
	)
}

func UpdateTableMappingOptimisticSQL(m model.TableMapping, expectedVersion uint64) string {
	return fmt.Sprintf(
		"update mo_catalog.%s set catalog_id = %d, namespace = %s, table_name = %s, default_ref = %s, read_mode = %s, write_mode = %s, writer_owner_account_id = %d, capabilities_json = %s, last_snapshot_id = %s, last_metadata_location_hash = %s, updated_at = utc_timestamp, version = version + 1 where account_id = %d and db_id = %d and table_id = %d and version = %d",
		TableTables,
		m.CatalogID,
		quoteSQLString(m.Namespace),
		quoteSQLString(m.TableName),
		quoteSQLString(defaultString(m.DefaultRef, model.DefaultRefMain)),
		quoteSQLString(defaultString(m.ReadMode, model.ReadModeAppendOnly)),
		quoteSQLString(defaultString(m.WriteMode, model.WriteModeReadOnly)),
		m.WriterOwnerAccountID,
		nullOrSQLString(m.CapabilitiesJSON),
		nullOrSQLString(m.LastSnapshotID),
		nullOrSQLString(m.LastMetadataLocationHash),
		m.AccountID,
		m.DatabaseID,
		m.TableID,
		expectedVersion,
	)
}

func GetCatalogByNameSQL(accountID uint32, name string) string {
	return fmt.Sprintf(
		"select account_id,catalog_id,name,type,uri,warehouse,auth_mode,token_secret_ref,capabilities_json,version from mo_catalog.%s where account_id = %d and name = %s",
		TableCatalogs,
		accountID,
		quoteSQLString(name),
	)
}

func GetCatalogByIDSQL(accountID uint32, catalogID uint64) string {
	return fmt.Sprintf(
		"select account_id,catalog_id,name,type,uri,warehouse,auth_mode,token_secret_ref,capabilities_json,version from mo_catalog.%s where account_id = %d and catalog_id = %d",
		TableCatalogs,
		accountID,
		catalogID,
	)
}

func GetTableMappingSQL(accountID uint32, databaseID uint64, tableID uint64) string {
	return fmt.Sprintf(
		"select account_id,db_id,table_id,catalog_id,namespace,table_name,default_ref,read_mode,write_mode,writer_owner_account_id,capabilities_json,last_snapshot_id,last_metadata_location_hash,version from mo_catalog.%s where account_id = %d and db_id = %d and table_id = %d",
		TableTables,
		accountID,
		databaseID,
		tableID,
	)
}

func DeleteTableMappingSQL(accountID uint32, databaseID uint64, tableID uint64) string {
	return fmt.Sprintf(
		"delete from mo_catalog.%s where account_id = %d and db_id = %d and table_id = %d",
		TableTables,
		accountID,
		databaseID,
		tableID,
	)
}

func GetRefCacheSQL(accountID uint32, catalogID uint64, namespace, tableName, refName string) string {
	return fmt.Sprintf(
		"select account_id,catalog_id,namespace,table_name,ref_name,ref_type,snapshot_id,last_seen_at,source,version from mo_catalog.%s where account_id = %d and catalog_id = %d and namespace = %s and table_name = %s and ref_name = %s",
		TableRefs,
		accountID,
		catalogID,
		quoteSQLString(namespace),
		quoteSQLString(tableName),
		quoteSQLString(refName),
	)
}

func ListRefCacheSQL(accountID uint32, catalogID uint64, namespace, tableName string) string {
	return fmt.Sprintf(
		"select account_id,catalog_id,namespace,table_name,ref_name,ref_type,snapshot_id,last_seen_at,source,version from mo_catalog.%s where account_id = %d and catalog_id = %d and namespace = %s and table_name = %s order by ref_type,ref_name",
		TableRefs,
		accountID,
		catalogID,
		quoteSQLString(namespace),
		quoteSQLString(tableName),
	)
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func defaultVersion(version uint64) uint64 {
	if version == 0 {
		return 1
	}
	return version
}
