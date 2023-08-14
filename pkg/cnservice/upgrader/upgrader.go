// Copyright 2021 - 2023 Matrix Origin
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

package upgrader

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"strconv"
	"strings"
)

var registeredTable = []*table.Table{motrace.SingleRowLogTable}

type Upgrader struct {
	IEFactory func() ie.InternalExecutor
}

func ParseDataTypeToColType(dataType string) (table.ColType, error) {
	switch {
	case strings.Contains(strings.ToLower(dataType), "datetime"):
		return table.TDatetime, nil
	case strings.Contains(strings.ToLower(dataType), "bigint"):
		if strings.Contains(strings.ToLower(dataType), "unsigned") {
			return table.TUint64, nil
		}
		return table.TInt64, nil
	case strings.Contains(strings.ToLower(dataType), "double"):
		return table.TFloat64, nil
	case strings.Contains(strings.ToLower(dataType), "json"):
		return table.TJson, nil
	case strings.Contains(strings.ToLower(dataType), "text"):
		return table.TText, nil
	case strings.Contains(strings.ToLower(dataType), "varchar"):
		return table.TVarchar, nil
	case strings.Contains(strings.ToLower(dataType), "bytes"):
		return table.TBytes, nil
	case strings.Contains(strings.ToLower(dataType), "uuid"):
		return table.TUuid, nil
	default:
		return table.TSkip, moerr.NewInternalError(context.Background(), "unknown data type")
	}
}

func (u *Upgrader) GetCurrentSchema(ctx context.Context, exec ie.InternalExecutor, database, tbl string) (*table.Table, error) {
	// Query information_schema.columns to get column info
	query := fmt.Sprintf("SELECT COLUMN_NAME, DATA_TYPE FROM `information_schema`.columns WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", database, tbl)

	// Execute the query
	result := exec.Query(ctx, query, ie.NewOptsBuilder().Finish())

	// Check for errors
	if err := result.Error(); err != nil {
		return nil, err
	}

	cnt := result.RowCount()
	if cnt == 0 {
		return nil, nil
	}

	// Build a list of table.Columns based on the query result
	cols := []table.Column{}
	errors := []error{}
	for i := uint64(0); i < result.RowCount(); i++ {
		name, err := result.StringValueByName(ctx, i, "column_name")
		if err != nil {
			errors = append(errors, err)
			continue
		}
		dataType, err := result.StringValueByName(ctx, i, "data_type")
		if err != nil {
			errors = append(errors, err)
			continue
		}
		colType, err := ParseDataTypeToColType(dataType)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		cols = append(cols, table.Column{Name: name, ColType: colType})
	}

	// If errors occurred, return them
	if len(errors) > 0 {
		return nil, moerr.NewInternalError(ctx, "can not get the schema")
	}

	// Construct and return the table
	return &table.Table{
		Database: database,
		Table:    tbl,
		Columns:  cols,
	}, nil
}

func (u *Upgrader) GenerateDiff(currentSchema *table.Table, expectedSchema *table.Table) (table.SchemaDiff, error) {
	// Create maps for easy comparison
	currentColumns := make(map[string]table.Column)
	expectedColumns := make(map[string]table.Column)
	for _, column := range currentSchema.Columns {
		currentColumns[column.Name] = column
	}
	for _, column := range expectedSchema.Columns {
		expectedColumns[column.Name] = column
	}

	// Find added columns
	addedColumns := []table.Column{}
	for columnName, column := range expectedColumns {
		if _, exists := currentColumns[columnName]; !exists {
			addedColumns = append(addedColumns, column)
		}
	}

	// If there are differences, create a SchemaDiff with table and database information
	if len(addedColumns) > 0 {
		return table.SchemaDiff{
			AddedColumns: addedColumns,
			TableName:    expectedSchema.Table,
			DatabaseName: expectedSchema.Database,
		}, nil
	}

	// Todo: handle removed and modified columns

	// If no differences, return an empty SchemaDiff and nil error
	return table.SchemaDiff{}, nil
}
func (u *Upgrader) GenerateUpgradeSQL(diff table.SchemaDiff) (string, error) {
	if len(diff.AddedColumns) == 0 {
		return "", moerr.NewInternalError(context.Background(), "no added columns in schema diff")
	}

	// Get database and table name from the schema diff
	databaseName := diff.DatabaseName
	tableName := diff.TableName

	// Initialize the commands slice with the beginning of a transaction
	commands := []string{"BEGIN;"}

	// Generate the ALTER TABLE command for each added column
	for _, column := range diff.AddedColumns {
		command := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN `%s` %s", databaseName, tableName, column.Name, column.ColType.String(column.Scale))

		// If there's a default value, include it
		if column.Default != "" {
			command += fmt.Sprintf(" DEFAULT %s;", column.Default)
		} else {
			command += ";"
		}

		commands = append(commands, command)
	}

	// Add the end of the transaction to the commands
	commands = append(commands, "COMMIT;")

	// Join all commands into a single string
	return strings.Join(commands, "\n"), nil
}

func (u *Upgrader) Upgrade(ctx context.Context) error {
	allTenants, err := u.GetAllTenantInfo(ctx)
	if err != nil {
		return err
	}

	if err = u.UpgradeNewTableColumn(ctx); err != nil {
		return err
	}

	if err = u.UpgradeNewTable(ctx, allTenants); err != nil {
		return err
	}

	if err = u.UpgradeNewView(ctx, allTenants); err != nil {
		return err
	}
	return nil
}

// Upgrade the newly added columns in the system table
func (u *Upgrader) UpgradeNewTableColumn(ctx context.Context) error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	for _, tbl := range registeredTable {
		currentSchema, err := u.GetCurrentSchema(ctx, exec, tbl.Database, tbl.Table)
		if err != nil {
			return err
		}

		diff, err := u.GenerateDiff(currentSchema, tbl)
		if err != nil {
			return err
		} else if len(diff.AddedColumns) == 0 {
			continue
		}

		upgradeSQL, err := u.GenerateUpgradeSQL(diff)
		if err != nil {
			return err
		}

		// Execute upgrade SQL
		if err = exec.Exec(ctx, upgradeSQL, ie.NewOptsBuilder().Finish()); err != nil {
			return err
		}
	}
	return nil
}

// Upgrade system tables, add system tables
func (u *Upgrader) UpgradeNewTable(ctx context.Context, tenants []*frontend.TenantInfo) error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	for _, tbl := range needUpgradNewTable {
		if tbl.Account == table.AccountAll {
			for _, tenant := range tenants {
				if err := u.upgradeFunc(ctx, tbl, false, tenant, exec); err != nil {
					return err
				}
			}
		} else {
			if err := u.upgradeFunc(ctx, tbl, false, &frontend.TenantInfo{
				Tenant:        frontend.GetDefaultTenant(),
				TenantID:      catalog.System_Account,
				User:          "internal",
				UserID:        frontend.GetUserRootId(),
				DefaultRoleID: frontend.GetDefaultRoleId(),
				DefaultRole:   frontend.GetDefaultRole(),
			}, exec); err != nil {
				return err
			}
		}

	}
	return nil
}

// Upgrade system tables, add system views
func (u *Upgrader) UpgradeNewView(ctx context.Context, tenants []*frontend.TenantInfo) error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	for _, tbl := range needUpgradNewView {
		if tbl.Account == table.AccountAll {
			for _, tenant := range tenants {
				if err := u.upgradeFunc(ctx, tbl, true, tenant, exec); err != nil {
					return err
				}
			}
		} else {
			if err := u.upgradeFunc(ctx, tbl, true, &frontend.TenantInfo{
				Tenant:        frontend.GetDefaultTenant(),
				TenantID:      catalog.System_Account,
				User:          frontend.GetUserRoot(),
				UserID:        frontend.GetUserRootId(),
				DefaultRoleID: frontend.GetDefaultRoleId(),
				DefaultRole:   frontend.GetDefaultRole(),
			}, exec); err != nil {
				return err
			}
		}
	}
	return nil
}

// Check if the table exists
func (u *Upgrader) CheckSchemaIsExist(ctx context.Context, exec ie.InternalExecutor, opts *ie.OptsBuilder, database, tbl string) (bool, error) {
	// Query mo_catalog.mo_tables to get table info
	query := fmt.Sprintf("select rel_id, relname from `mo_catalog`.`mo_tables` where reldatabase = '%s' and relname = '%s'", database, tbl)

	// Execute the query
	result := exec.Query(ctx, query, opts.Finish())

	// Check for errors
	if err := result.Error(); err != nil {
		return false, err
	}

	cnt := result.RowCount()
	if cnt == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

// GetAllTenantInfo Obtain all tenant information in the system
func (u *Upgrader) GetAllTenantInfo(ctx context.Context) ([]*frontend.TenantInfo, error) {
	exec := u.IEFactory()
	if exec == nil {
		return nil, moerr.NewInternalError(ctx, "can not get the Internal Executor")
	}

	tenantInfos := make([]*frontend.TenantInfo, 0)

	// Query information_schema.columns to get column info
	query := "SELECT ACCOUNT_ID, ACCOUNT_NAME, VERSION FROM `mo_catalog`.`mo_account`"
	// Execute the query
	sysAcc := &frontend.TenantInfo{
		Tenant:        frontend.GetUserRoot(),
		TenantID:      frontend.GetSysTenantId(),
		User:          frontend.GetUserRoot(),
		UserID:        frontend.GetUserRootId(),
		DefaultRoleID: frontend.GetDefaultRoleId(),
		DefaultRole:   frontend.GetDefaultRole(),
	}
	ctx = attachAccount(ctx, sysAcc)
	result := exec.Query(ctx, query, makeOptions(sysAcc).Finish())

	errors := []error{}
	for i := uint64(0); i < result.RowCount(); i++ {
		tenantName, err := result.StringValueByName(ctx, i, "account_name")
		if err != nil {
			errors = append(errors, err)
			continue
		}

		accountIdStr, err := result.StringValueByName(ctx, i, "account_id")
		if err != nil {
			errors = append(errors, err)
			continue
		}

		accountId, err := strconv.Atoi(accountIdStr)
		if err != nil {
			errors = append(errors, err)
			continue
		}

		if uint32(accountId) == catalog.System_Account {
			tenantInfos = append(tenantInfos, &frontend.TenantInfo{
				Tenant:        tenantName,
				TenantID:      uint32(accountId),
				User:          frontend.GetUserRoot(),
				UserID:        frontend.GetUserRootId(),
				DefaultRoleID: frontend.GetDefaultRoleId(),
				DefaultRole:   frontend.GetDefaultRole(),
			})
		} else {
			tenantInfos = append(tenantInfos, &frontend.TenantInfo{
				Tenant:        tenantName,
				TenantID:      uint32(accountId),
				User:          "internal",
				UserID:        frontend.GetAdminUserId(),
				DefaultRoleID: frontend.GetAccountAdminRoleId(),
				DefaultRole:   frontend.GetAccountAdminRole(),
			})
		}

	}

	// If errors occurred, return them
	if len(errors) > 0 {
		return nil, moerr.NewInternalError(ctx, "can not get the schema")
	}
	return tenantInfos, nil
}

func (u *Upgrader) upgradeFunc(ctx context.Context, tbl *table.Table, isView bool, tenant *frontend.TenantInfo, exec ie.InternalExecutor) error {
	// Switch Tenants
	ctx = attachAccount(ctx, tenant)
	opts := makeOptions(tenant)
	isExist, err := u.CheckSchemaIsExist(ctx, exec, opts, tbl.Database, tbl.Table)
	if err != nil {
		return err
	}
	if isExist {
		return nil
	}
	sql := tbl.CreateTableSql
	if isView {
		sql = tbl.CreateViewSql
	}
	// Execute upgrade SQL
	if err = exec.Exec(ctx, sql, opts.Finish()); err != nil {
		return err
	}
	return nil
}

func attachAccount(ctx context.Context, tenant *frontend.TenantInfo) context.Context {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, tenant.GetTenantID())
	ctx = context.WithValue(ctx, defines.UserIDKey{}, tenant.GetUserID())
	ctx = context.WithValue(ctx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())
	return ctx
}

func makeOptions(tenant *frontend.TenantInfo) *ie.OptsBuilder {
	return ie.NewOptsBuilder().AccountId(tenant.GetTenantID()).UserId(tenant.GetUserID()).DefaultRoleId(tenant.GetDefaultRoleID())
}
