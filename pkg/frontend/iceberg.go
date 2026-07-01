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

package frontend

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	sqliceberg "github.com/matrixorigin/matrixone/pkg/sql/iceberg"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func handleCreateIcebergCatalog(ctx context.Context, ses *Session, stmt *tree.CreateIcebergCatalog) error {
	opts, err := icebergOptionsToMap(ctx, stmt.Options)
	if err != nil {
		return err
	}
	catalogType := firstIcebergOption(opts, "type")
	if catalogType == "" {
		catalogType = "rest"
	}
	uri := opts["uri"]
	if strings.TrimSpace(uri) == "" {
		return moerr.NewInvalidInput(ctx, "CREATE ICEBERG CATALOG requires uri option")
	}
	accountID := ses.GetAccountId()

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	catalogName := string(stmt.Name)
	existingID, err := queryIcebergCatalogID(ctx, bh, accountID, catalogName)
	if err != nil {
		return err
	}
	if existingID != 0 {
		if stmt.IfNotExists {
			return nil
		}
		return moerr.NewInvalidInputf(ctx, "iceberg catalog %s already exists", catalogName)
	}
	nextID, err := nextIcebergCatalogID(ctx, bh, accountID)
	if err != nil {
		return err
	}
	authMode := firstIcebergOption(opts, "auth_mode")
	if authMode == "" {
		authMode = firstIcebergOption(opts, "auth")
	}
	if authMode == "" {
		authMode = model.AuthModeNone
	}
	tokenSecretRef := firstIcebergOption(opts, "token_secret_ref")
	if tokenSecretRef == "" {
		tokenSecretRef = firstIcebergOption(opts, "token_secret")
	}
	if err := validateIcebergSecretRef(ctx, tokenSecretRef); err != nil {
		return err
	}

	return bh.Exec(ctx, sqliceberg.InsertCatalogSQL(model.Catalog{
		AccountID:        accountID,
		CatalogID:        nextID,
		Name:             catalogName,
		Type:             catalogType,
		URI:              uri,
		Warehouse:        opts["warehouse"],
		AuthMode:         authMode,
		TokenSecretRef:   tokenSecretRef,
		CapabilitiesJSON: opts["capabilities_json"],
		Version:          1,
	}))
}

func handleAlterIcebergCatalog(ctx context.Context, ses *Session, stmt *tree.AlterIcebergCatalog) error {
	opts, err := icebergOptionsToMap(ctx, stmt.Options)
	if err != nil {
		return err
	}
	if len(opts) == 0 {
		return moerr.NewInvalidInput(ctx, "ALTER ICEBERG CATALOG requires at least one option")
	}
	setters := make([]string, 0, len(opts)+2)
	for _, key := range []string{"type", "uri", "warehouse", "auth_mode", "token_secret_ref", "capabilities_json"} {
		if value, ok := opts[key]; ok {
			if key == "token_secret_ref" {
				if err := validateIcebergSecretRef(ctx, value); err != nil {
					return err
				}
			}
			setters = append(setters, fmt.Sprintf("%s = %s", key, quoteIcebergSQLString(value)))
		}
	}
	if auth, ok := opts["auth"]; ok {
		setters = append(setters, fmt.Sprintf("auth_mode = %s", quoteIcebergSQLString(auth)))
	}
	if token, ok := opts["token_secret"]; ok {
		if err := validateIcebergSecretRef(ctx, token); err != nil {
			return err
		}
		setters = append(setters, fmt.Sprintf("token_secret_ref = %s", quoteIcebergSQLString(token)))
	}
	if len(setters) == 0 {
		return moerr.NewInvalidInput(ctx, "ALTER ICEBERG CATALOG option is not supported")
	}
	setters = append(setters, "updated_at = utc_timestamp", "version = version + 1")
	sort.Strings(setters)

	accountID := ses.GetAccountId()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	if catalogID, err := queryIcebergCatalogID(ctx, bh, accountID, string(stmt.Name)); err != nil {
		return err
	} else if catalogID == 0 {
		return moerr.NewInvalidInputf(ctx, "iceberg catalog %s does not exist", string(stmt.Name))
	}
	return bh.Exec(ctx, fmt.Sprintf(
		"update mo_catalog.%s set %s where account_id = %d and name = %s",
		sqliceberg.TableCatalogs,
		strings.Join(setters, ", "),
		accountID,
		quoteIcebergSQLString(string(stmt.Name)),
	))
}

func handleDropIcebergCatalog(ctx context.Context, ses *Session, stmt *tree.DropIcebergCatalog) error {
	accountID := ses.GetAccountId()
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	catalogName := string(stmt.Name)
	catalogID, err := queryIcebergCatalogID(ctx, bh, accountID, catalogName)
	if err != nil {
		return err
	}
	if catalogID == 0 {
		if stmt.IfExists {
			return nil
		}
		return moerr.NewInvalidInputf(ctx, "iceberg catalog %s does not exist", catalogName)
	}
	inUse, err := icebergCatalogHasMappings(ctx, bh, accountID, catalogID)
	if err != nil {
		return err
	}
	if inUse {
		return moerr.NewInvalidInputf(ctx, "iceberg catalog %s is still used by table mappings", catalogName)
	}
	return bh.Exec(ctx, fmt.Sprintf(
		"delete from mo_catalog.%s where account_id = %d and catalog_id = %d",
		sqliceberg.TableCatalogs,
		accountID,
		catalogID,
	))
}

func handleShowIcebergCatalogs(ctx context.Context, ses *Session, _ *tree.ShowIcebergCatalogs) error {
	sql := fmt.Sprintf(
		"select name,type,uri,warehouse,auth_mode,version from mo_catalog.%s where account_id = %d order by name",
		sqliceberg.TableCatalogs,
		ses.GetAccountId(),
	)
	return showIcebergQuery(ctx, ses, sql, []icebergShowColumn{
		{name: "name", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "type", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "uri", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "warehouse", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "auth_mode", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "version", typ: defines.MYSQL_TYPE_LONGLONG},
	})
}

func handleShowIcebergNamespaces(ctx context.Context, ses *Session, stmt *tree.ShowIcebergNamespaces) error {
	accountID := ses.GetAccountId()
	filter := ""
	if stmt.Catalog != "" {
		filter = fmt.Sprintf(" and c.name = %s", quoteIcebergSQLString(string(stmt.Catalog)))
	}
	sql := fmt.Sprintf(
		"select distinct c.name,t.namespace from mo_catalog.%s t join mo_catalog.%s c on t.account_id = c.account_id and t.catalog_id = c.catalog_id where t.account_id = %d%s order by c.name,t.namespace",
		sqliceberg.TableTables,
		sqliceberg.TableCatalogs,
		accountID,
		filter,
	)
	return showIcebergQuery(ctx, ses, sql, []icebergShowColumn{
		{name: "catalog", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "namespace", typ: defines.MYSQL_TYPE_VARCHAR},
	})
}

func handleShowIcebergTables(ctx context.Context, ses *Session, stmt *tree.ShowIcebergTables) error {
	accountID := ses.GetAccountId()
	filters := make([]string, 0, 2)
	if stmt.Catalog != "" {
		filters = append(filters, fmt.Sprintf("c.name = %s", quoteIcebergSQLString(string(stmt.Catalog))))
	}
	if stmt.Namespace != "" {
		filters = append(filters, fmt.Sprintf("t.namespace = %s", quoteIcebergSQLString(stmt.Namespace)))
	}
	where := ""
	if len(filters) > 0 {
		where = " and " + strings.Join(filters, " and ")
	}
	sql := fmt.Sprintf(
		"select c.name,t.namespace,t.table_name,t.default_ref,t.read_mode,t.write_mode from mo_catalog.%s t join mo_catalog.%s c on t.account_id = c.account_id and t.catalog_id = c.catalog_id where t.account_id = %d%s order by c.name,t.namespace,t.table_name",
		sqliceberg.TableTables,
		sqliceberg.TableCatalogs,
		accountID,
		where,
	)
	return showIcebergQuery(ctx, ses, sql, []icebergShowColumn{
		{name: "catalog", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "namespace", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "table", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "ref", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "read_mode", typ: defines.MYSQL_TYPE_VARCHAR},
		{name: "write_mode", typ: defines.MYSQL_TYPE_VARCHAR},
	})
}

type icebergShowColumn struct {
	name string
	typ  defines.MysqlType
}

func showIcebergQuery(ctx context.Context, ses *Session, sql string, cols []icebergShowColumn) error {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	results, err := ExeSqlInBgSes(ctx, bh, sql)
	if err != nil {
		return err
	}
	mrs := ses.GetMysqlResultSet()
	for _, colSpec := range cols {
		col := new(MysqlColumn)
		col.SetName(colSpec.name)
		col.SetColumnType(colSpec.typ)
		mrs.AddColumn(col)
	}
	if len(results) > 0 {
		result := results[0]
		for rowIdx := uint64(0); rowIdx < result.GetRowCount(); rowIdx++ {
			row := make([]interface{}, len(cols))
			for colIdx := range cols {
				if isNull, err := result.ColumnIsNull(ctx, rowIdx, uint64(colIdx)); err != nil {
					return err
				} else if isNull {
					row[colIdx] = nil
					continue
				}
				switch cols[colIdx].typ {
				case defines.MYSQL_TYPE_LONGLONG:
					value, err := result.GetUint64(ctx, rowIdx, uint64(colIdx))
					if err != nil {
						return err
					}
					row[colIdx] = value
				default:
					value, err := result.GetString(ctx, rowIdx, uint64(colIdx))
					if err != nil {
						return err
					}
					row[colIdx] = value
				}
			}
			mrs.AddRow(row)
		}
	}
	return trySaveQueryResult(ctx, ses, mrs)
}

func icebergOptionsToMap(ctx context.Context, opts tree.IcebergOptions) (map[string]string, error) {
	values := make(map[string]string, len(opts))
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(string(opt.Key)))
		value := strings.TrimSpace(opt.Val)
		if key == "" || value == "" {
			return nil, moerr.NewInvalidInput(ctx, "iceberg catalog option key and value cannot be empty")
		}
		if _, exists := values[key]; exists {
			return nil, moerr.NewInvalidInputf(ctx, "duplicate iceberg catalog option %s", key)
		}
		values[key] = value
	}
	return values, nil
}

func firstIcebergOption(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(values[key]); value != "" {
			return value
		}
	}
	return ""
}

func validateIcebergSecretRef(ctx context.Context, value string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(value)), "secret://") {
		return nil
	}
	return moerr.NewInvalidInput(ctx, "Iceberg token_secret must be a secret:// reference; inline secrets are not allowed")
}

func queryIcebergCatalogID(ctx context.Context, bh BackgroundExec, accountID uint32, catalogName string) (uint64, error) {
	sql := sqliceberg.GetCatalogByNameSQL(accountID, catalogName)
	results, err := ExeSqlInBgSes(ctx, bh, sql)
	if err != nil {
		return 0, err
	}
	if !execResultArrayHasData(results) {
		return 0, nil
	}
	return results[0].GetUint64(ctx, 0, 1)
}

func nextIcebergCatalogID(ctx context.Context, bh BackgroundExec, accountID uint32) (uint64, error) {
	sql := fmt.Sprintf(
		"select coalesce(max(catalog_id), 0) + 1 from mo_catalog.%s where account_id = %d",
		sqliceberg.TableCatalogs,
		accountID,
	)
	results, err := ExeSqlInBgSes(ctx, bh, sql)
	if err != nil {
		return 0, err
	}
	if !execResultArrayHasData(results) {
		return 1, nil
	}
	return results[0].GetUint64(ctx, 0, 0)
}

func icebergCatalogHasMappings(ctx context.Context, bh BackgroundExec, accountID uint32, catalogID uint64) (bool, error) {
	sql := fmt.Sprintf(
		"select count(*) from mo_catalog.%s where account_id = %d and catalog_id = %d",
		sqliceberg.TableTables,
		accountID,
		catalogID,
	)
	results, err := ExeSqlInBgSes(ctx, bh, sql)
	if err != nil {
		return false, err
	}
	if !execResultArrayHasData(results) {
		return false, nil
	}
	count, err := results[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func quoteIcebergSQLString(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}
