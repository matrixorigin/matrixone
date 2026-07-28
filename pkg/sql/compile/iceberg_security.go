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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

type icebergCatalogAccessInfo struct {
	catalog model.Catalog
	table   model.TableMapping
}

type icebergScanAccessContext struct {
	catalog           model.Catalog
	table             model.TableMapping
	externalPrincipal string
	principalMap      model.PrincipalMap
	residencyPolicies []model.ResidencyPolicy
}

func (c *Compile) checkIcebergScanAccess(node *plan.Node) (icebergScanAccessContext, error) {
	ctx := c.icebergSecurityContext()
	if node == nil || node.TableDef == nil {
		return icebergScanAccessContext{}, moerr.NewInvalidInput(ctx, "iceberg scan access check requires table definition")
	}
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	dbID, tableID, err := icebergScanObjectIDs(ctx, node)
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	info, err := c.loadIcebergCatalogAccessInfo(accountID, dbID, tableID)
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	principalMaps, err := c.loadIcebergPrincipalMaps(accountID, info.catalog.CatalogID)
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	residencyPolicies, err := c.loadIcebergResidencyPolicies(accountID, info.catalog.CatalogID)
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	decision, err := sqliceberg.CheckCatalogAccess(ctx, principalMaps, residencyPolicies, sqliceberg.CatalogAccessRequest{
		AccountID:  accountID,
		CatalogID:  info.catalog.CatalogID,
		RoleID:     uint64(defines.GetRoleId(ctx)),
		UserID:     uint64(defines.GetUserId(ctx)),
		CatalogURI: info.catalog.URI,
	})
	if err != nil {
		return icebergScanAccessContext{}, err
	}
	return icebergScanAccessContext{
		catalog:           info.catalog,
		table:             info.table,
		externalPrincipal: decision.ExternalPrincipal,
		principalMap:      decision.PrincipalMap,
		residencyPolicies: residencyPolicies,
	}, nil
}

func (c *Compile) icebergSecurityContext() context.Context {
	if c.proc != nil {
		if c.proc.Ctx != nil {
			return c.proc.Ctx
		}
		if c.proc.GetTopContext() != nil {
			return c.proc.GetTopContext()
		}
	}
	return context.Background()
}

func (c *Compile) icebergConfig(ctx context.Context) (api.Config, bool, error) {
	pu := icebergParameterUnitFromContext(ctx)
	if pu == nil && c != nil && c.proc != nil {
		if rt := moruntime.ServiceRuntime(c.proc.GetService()); rt != nil {
			if value, ok := rt.GetGlobalVariables("parameter-unit"); ok {
				pu, _ = value.(*config.ParameterUnit)
			}
		}
	}
	if pu == nil || pu.SV == nil {
		return api.Config{}, false, nil
	}
	cfg, err := api.NewConfigFromParameters(ctx, pu.SV.Iceberg)
	if err != nil {
		return api.Config{}, true, err
	}
	return cfg, true, nil
}

func (c *Compile) validateIcebergRemoteFanoutPolicy(
	ctx context.Context,
	runtime icebergExternalScanRuntime,
	shards []icebergDataFileScopeShard,
) error {
	if ctx == nil {
		ctx = c.icebergSecurityContext()
	}
	if !icebergFanoutIncludesRemoteCN(shards, c.addr) {
		return nil
	}
	credentialScopes := icebergRuntimeCredentialScopeCount(runtime)
	hasObjectRef := runtime.objectIORef != ""
	message := "Iceberg remote scan fanout is disabled until ObjectIO provider handoff is implemented"
	if credentialScopes > 0 {
		message = "Iceberg remote credential fanout is disabled until ObjectIO provider handoff is implemented"
	} else if hasObjectRef {
		message = "Iceberg remote object IO reference fanout is disabled until ObjectIO provider handoff is implemented"
	}
	return api.ToMOErr(ctx, api.NewError(api.ErrRemoteSigningDenied, message, map[string]string{
		"has_object_io_ref": fmt.Sprintf("%t", hasObjectRef),
		"credential_scopes": fmt.Sprintf("%d", credentialScopes),
		"data_tasks":        fmt.Sprintf("%d", len(runtime.dataTasks)),
		"remote_cns":        fmt.Sprintf("%d", icebergRemoteCNCount(shards, c.addr)),
	}))
}

func icebergRuntimeCredentialScopeCount(runtime icebergExternalScanRuntime) int {
	count := 0
	for _, task := range runtime.dataTasks {
		if task != nil && task.CredentialScope != "" {
			count++
		}
	}
	for _, task := range runtime.deleteTasks {
		if task != nil && task.CredentialScope != "" {
			count++
		}
	}
	return count
}

func icebergFanoutIncludesRemoteCN(shards []icebergDataFileScopeShard, localAddr string) bool {
	return icebergRemoteCNCount(shards, localAddr) > 0
}

func icebergRemoteCNCount(shards []icebergDataFileScopeShard, localAddr string) int {
	seen := make(map[string]struct{})
	for _, shard := range shards {
		addr := shard.node.Addr
		if addr == "" || addr == localAddr {
			continue
		}
		seen[addr] = struct{}{}
	}
	return len(seen)
}

func icebergScanObjectIDs(ctx context.Context, node *plan.Node) (uint64, uint64, error) {
	dbID := node.TableDef.GetDbId()
	tableID := node.TableDef.GetTblId()
	if dbID == 0 && node.ObjRef != nil && node.ObjRef.Db > 0 {
		dbID = uint64(node.ObjRef.Db)
	}
	if tableID == 0 && node.ObjRef != nil && node.ObjRef.Obj > 0 {
		tableID = uint64(node.ObjRef.Obj)
	}
	if dbID == 0 || tableID == 0 {
		return 0, 0, moerr.NewInvalidInput(ctx, "iceberg scan access check requires db_id and table_id")
	}
	return dbID, tableID, nil
}

func (c *Compile) loadIcebergCatalogAccessInfo(accountID uint32, dbID, tableID uint64) (icebergCatalogAccessInfo, error) {
	sql := fmt.Sprintf(
		"select t.account_id, t.db_id, t.table_id, t.catalog_id, t.namespace, t.table_name, t.default_ref, t.read_mode, t.write_mode, t.writer_owner_account_id, coalesce(cast(t.capabilities_json as char), ''), coalesce(t.last_snapshot_id, ''), coalesce(t.last_metadata_location_hash, ''), t.version, c.name, c.type, c.uri, coalesce(c.warehouse, ''), c.auth_mode, coalesce(c.token_secret_ref, ''), coalesce(cast(c.capabilities_json as char), ''), c.version from mo_catalog.%s t join mo_catalog.%s c on c.account_id = t.account_id and c.catalog_id = t.catalog_id where t.account_id = %d and t.db_id = %d and t.table_id = %d",
		sqliceberg.TableTables,
		sqliceberg.TableCatalogs,
		accountID,
		dbID,
		tableID,
	)
	res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return icebergCatalogAccessInfo{}, err
	}
	defer res.Close()
	var info icebergCatalogAccessInfo
	rowsSeen := 0
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows == 0 {
			return true
		}
		tableAccountIDs := executor.GetFixedRows[uint32](cols[0])
		dbIDs := executor.GetFixedRows[uint64](cols[1])
		tableIDs := executor.GetFixedRows[uint64](cols[2])
		catalogIDs := executor.GetFixedRows[uint64](cols[3])
		namespaces := executor.GetStringRows(cols[4])
		tableNames := executor.GetStringRows(cols[5])
		defaultRefs := executor.GetStringRows(cols[6])
		readModes := executor.GetStringRows(cols[7])
		writeModes := executor.GetStringRows(cols[8])
		writerOwnerAccountIDs := executor.GetFixedRows[uint32](cols[9])
		tableCapabilities := executor.GetStringRows(cols[10])
		lastSnapshotIDs := executor.GetStringRows(cols[11])
		lastMetadataLocationHashes := executor.GetStringRows(cols[12])
		tableVersions := executor.GetFixedRows[uint64](cols[13])
		catalogNames := executor.GetStringRows(cols[14])
		catalogTypes := executor.GetStringRows(cols[15])
		catalogURIs := executor.GetStringRows(cols[16])
		warehouses := executor.GetStringRows(cols[17])
		authModes := executor.GetStringRows(cols[18])
		tokenSecretRefs := executor.GetStringRows(cols[19])
		catalogCapabilities := executor.GetStringRows(cols[20])
		catalogVersions := executor.GetFixedRows[uint64](cols[21])
		for i := 0; i < rows; i++ {
			info.table = model.TableMapping{
				AccountID:                tableAccountIDs[i],
				DatabaseID:               dbIDs[i],
				TableID:                  tableIDs[i],
				CatalogID:                catalogIDs[i],
				Namespace:                namespaces[i],
				TableName:                tableNames[i],
				DefaultRef:               defaultRefs[i],
				ReadMode:                 readModes[i],
				WriteMode:                writeModes[i],
				WriterOwnerAccountID:     writerOwnerAccountIDs[i],
				CapabilitiesJSON:         tableCapabilities[i],
				LastSnapshotID:           lastSnapshotIDs[i],
				LastMetadataLocationHash: lastMetadataLocationHashes[i],
				Version:                  tableVersions[i],
			}
			info.catalog = model.Catalog{
				AccountID:        tableAccountIDs[i],
				CatalogID:        catalogIDs[i],
				Name:             catalogNames[i],
				Type:             catalogTypes[i],
				URI:              catalogURIs[i],
				Warehouse:        warehouses[i],
				AuthMode:         authModes[i],
				TokenSecretRef:   tokenSecretRefs[i],
				CapabilitiesJSON: catalogCapabilities[i],
				Version:          catalogVersions[i],
			}
			rowsSeen++
			if rowsSeen > 1 {
				return false
			}
		}
		return true
	})
	if rowsSeen == 0 {
		return icebergCatalogAccessInfo{}, api.ToMOErr(c.icebergSecurityContext(), api.NewError(api.ErrTableNotFound, "Iceberg table mapping is missing", map[string]string{
			"account_id": fmt.Sprintf("%d", accountID),
			"db_id":      fmt.Sprintf("%d", dbID),
			"table_id":   fmt.Sprintf("%d", tableID),
		}))
	}
	if rowsSeen > 1 {
		return icebergCatalogAccessInfo{}, moerr.NewInvalidState(c.icebergSecurityContext(), "duplicate iceberg table mappings for db_id/table_id")
	}
	return info, nil
}

func (c *Compile) loadIcebergPrincipalMaps(accountID uint32, catalogID uint64) ([]model.PrincipalMap, error) {
	sql := fmt.Sprintf(
		"select mo_role_id, mo_user_id, external_principal, coalesce(cast(scope_json as char), ''), version from mo_catalog.%s where account_id = %d and catalog_id = %d",
		sqliceberg.TablePrincipalMap,
		accountID,
		catalogID,
	)
	res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return nil, err
	}
	defer res.Close()
	mappings := make([]model.PrincipalMap, 0)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		roleIDs := executor.GetFixedRows[uint64](cols[0])
		userIDs := executor.GetFixedRows[uint64](cols[1])
		externalPrincipals := executor.GetStringRows(cols[2])
		scopeJSONs := executor.GetStringRows(cols[3])
		versions := executor.GetFixedRows[uint64](cols[4])
		for i := 0; i < rows; i++ {
			mappings = append(mappings, model.PrincipalMap{
				AccountID:         accountID,
				CatalogID:         catalogID,
				MORoleID:          roleIDs[i],
				MOUserID:          userIDs[i],
				ExternalPrincipal: externalPrincipals[i],
				ScopeJSON:         scopeJSONs[i],
				Version:           versions[i],
			})
		}
		return true
	})
	return mappings, nil
}

func (c *Compile) loadIcebergResidencyPolicies(accountID uint32, catalogID uint64) ([]model.ResidencyPolicy, error) {
	sql := fmt.Sprintf(
		"select scope_type, account_id, catalog_id, allowed_catalog_uri, allowed_endpoint, allowed_region, allowed_bucket, coalesce(policy_state, 'enabled'), version from mo_catalog.%s where (scope_type = 'cluster' or account_id = %d) and (catalog_id = 0 or catalog_id = %d)",
		sqliceberg.TableResidencyPolicy,
		accountID,
		catalogID,
	)
	res, err := c.runSqlWithResultAndOptions(sql, NoAccountId, executor.StatementOption{}.WithDisableLog())
	if err != nil {
		return nil, err
	}
	defer res.Close()
	policies := make([]model.ResidencyPolicy, 0)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		scopeTypes := executor.GetStringRows(cols[0])
		accountIDs := executor.GetFixedRows[uint32](cols[1])
		catalogIDs := executor.GetFixedRows[uint64](cols[2])
		allowedCatalogURIs := executor.GetStringRows(cols[3])
		allowedEndpoints := executor.GetStringRows(cols[4])
		allowedRegions := executor.GetStringRows(cols[5])
		allowedBuckets := executor.GetStringRows(cols[6])
		policyStates := executor.GetStringRows(cols[7])
		versions := executor.GetFixedRows[uint64](cols[8])
		for i := 0; i < rows; i++ {
			policies = append(policies, model.ResidencyPolicy{
				ScopeType:         scopeTypes[i],
				AccountID:         accountIDs[i],
				CatalogID:         catalogIDs[i],
				AllowedCatalogURI: allowedCatalogURIs[i],
				AllowedEndpoint:   allowedEndpoints[i],
				AllowedRegion:     allowedRegions[i],
				AllowedBucket:     allowedBuckets[i],
				PolicyState:       policyStates[i],
				Version:           versions[i],
			})
		}
		return true
	})
	return policies, nil
}
