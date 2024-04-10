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
	liberrors "errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

var registeredTable = []*table.Table{motrace.SingleRowLogTable}

type Upgrader struct {
	IEFactory func() ie.InternalExecutor
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
		errors = append(errors, moerr.NewInternalError(ctx, "can not get the schema"))
		return nil, liberrors.Join(errors...)
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
	sysAcc := &frontend.TenantInfo{
		Tenant:        frontend.GetUserRoot(),
		TenantID:      frontend.GetSysTenantId(),
		User:          frontend.GetUserRoot(),
		UserID:        frontend.GetUserRootId(),
		DefaultRoleID: frontend.GetDefaultRoleId(),
		DefaultRole:   frontend.GetDefaultRole(),
	}
	ctx = attachAccount(ctx, sysAcc)
	allTenants, err := u.GetAllTenantInfo(ctx)
	if err != nil {
		return err
	}

	errors := []error{}

	if errs := u.UpgradeExistingView(ctx, allTenants); len(errs) > 0 {
		logutil.Errorf("upgrade existing system view failed")
		errors = append(errors, errs...)
	}

	if errs := u.UpgradeNewViewColumn(ctx); len(errs) > 0 {
		logutil.Errorf("upgrade new view column failed")
		errors = append(errors, errs...)
	}

	if errs := u.UpgradeMoIndexesSchema(ctx, allTenants); len(errs) > 0 {
		logutil.Errorf("upgrade mo_indexes failed")
		errors = append(errors, errs...)
	}

	if errs := u.UpgradeNewTableColumn(ctx); len(errs) > 0 {
		logutil.Errorf("upgrade new table column failed")
		errors = append(errors, errs...)
	}

	if errs := u.UpgradeNewTable(ctx, allTenants); len(errs) > 0 {
		logutil.Errorf("upgrade new table failed")
		errors = append(errors, errs...)
	}

	if errs := u.UpgradeNewView(ctx, allTenants); len(errs) > 0 {
		logutil.Errorf("upgrade new system view failed")
		errors = append(errors, errs...)
	}

	if len(errors) > 0 {
		//panic("Upgrade failed during system startup! " + convErrsToFormatMsg(errors))
		return moerr.NewInternalError(ctx, "Upgrade failed during system startup! "+convErrsToFormatMsg(errors))
	}

	return nil
}

// UpgradeNewViewColumn the newly added columns in the system table
func (u *Upgrader) UpgradeNewViewColumn(ctx context.Context) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	errors := []error{}
	for _, tbl := range registeredViews {
		currentSchema, err := u.GetCurrentSchema(ctx, exec, tbl.Database, tbl.Table)
		if err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		}

		//if there is no view, skip
		if currentSchema == nil {
			continue
		}

		diff, err := u.GenerateDiff(currentSchema, tbl)
		if err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		} else if len(diff.AddedColumns) == 0 {
			continue
		}

		if err = exec.ExecTxn(ctx, []string{tbl.CreateTableSql, tbl.CreateViewSql}, ie.NewOptsBuilder().Finish()); err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
		}
	}

	if len(errors) > 0 {
		return errors
	}
	return nil
}

// UpgradeNewTableColumn the newly added columns in the system table
func (u *Upgrader) UpgradeNewTableColumn(ctx context.Context) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	errors := []error{}
	for _, tbl := range registeredTable {
		currentSchema, err := u.GetCurrentSchema(ctx, exec, tbl.Database, tbl.Table)
		if err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		}

		if currentSchema == nil {
			continue
		}

		diff, err := u.GenerateDiff(currentSchema, tbl)
		if err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		} else if len(diff.AddedColumns) == 0 {
			continue
		}

		upgradeSQL, err := u.GenerateUpgradeSQL(diff)
		if err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		}

		// Execute upgrade SQL
		if err = exec.Exec(ctx, upgradeSQL, ie.NewOptsBuilder().Finish()); err != nil {
			errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			continue
			//return err
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// UpgradeNewTable system tables, add system tables
func (u *Upgrader) UpgradeNewTable(ctx context.Context, tenants []*frontend.TenantInfo) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	errors := []error{}
	for _, tbl := range needUpgradeNewTable {
		if tbl.Account == table.AccountAll {
			for _, tenant := range tenants {
				if err := u.upgradeFunc(ctx, tbl, false, tenant, exec); err != nil {
					errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, tenant.Tenant, tenant.TenantID, err.Error()))
					continue
					//return err
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
				errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			}
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// UpgradeNewView system tables, add system views
func (u *Upgrader) UpgradeNewView(ctx context.Context, tenants []*frontend.TenantInfo) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	errors := []error{}
	for _, tbl := range needUpgradeNewView {
		if tbl.Account == table.AccountAll {
			for _, tenant := range tenants {
				if err := u.upgradeFunc(ctx, tbl, true, tenant, exec); err != nil {
					errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, tenant.Tenant, tenant.TenantID, err.Error()))
					continue
					//return err
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
				errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			}
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// UpgradeMoIndexesSchema system tables, add system views
// Order of Execution:
// 1. boostrap.go builds mo_catalog.mo_indexes table
// 2. sysview.InitSchema builds INFORMATION_SCHEMA.columns table that will have columns in mo_indexes table
// 3. UpgradeMoIndexesSchema adds 3 new columns to mo_indexes table
// NOTE: mo_indexes is a multi-tenant table, so we need to upgrade (add column) for all tenants.
func (u *Upgrader) UpgradeMoIndexesSchema(ctx context.Context, tenants []*frontend.TenantInfo) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	ifEmpty := func(sql string, opts *ie.OptsBuilder) (bool, error) {
		result := exec.Query(ctx, sql, opts.Finish())

		if err := result.Error(); err != nil {
			return false, moerr.NewUpgrateError(ctx, "mo_catalog", "mo_indexes", frontend.GetDefaultTenant(), catalog.System_Account, err.Error())
		}

		return result.RowCount() == 0, nil

	}

	execTxn := func(sqls []string, opts *ie.OptsBuilder) error {

		stmt := []string{"begin;"}
		stmt = append(stmt, sqls...)
		stmt = append(stmt, "commit;")

		upgradeSQL := strings.Join(stmt, "\n")

		err := exec.Exec(ctx, upgradeSQL, opts.Finish())
		if err != nil {
			return err
		}

		return nil
	}

	var errors []error
	for _, tenant := range tenants {
		ctx = attachAccount(ctx, tenant)
		opts := makeOptions(tenant)

		ifEmptyStmt, thenStmt := conditionalUpgradeV1SQLs(int(tenant.TenantID))
		for i := 0; i < len(ifEmptyStmt); i++ {
			if columnNotPresent, err1 := ifEmpty(ifEmptyStmt[i], opts); err1 != nil {
				errors = append(errors, moerr.NewUpgrateError(ctx, "mo_catalog", "mo_indexes", frontend.GetDefaultTenant(), catalog.System_Account, err1.Error()))
			} else if columnNotPresent {
				err2 := execTxn(thenStmt[i], opts)
				if err2 != nil {
					errors = append(errors, moerr.NewUpgrateError(ctx, "mo_catalog", "mo_indexes", frontend.GetDefaultTenant(), catalog.System_Account, err2.Error()))
				}
			}
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil

}

// UpgradeExistingView: Modify the definition of existing system views
func (u *Upgrader) UpgradeExistingView(ctx context.Context, tenants []*frontend.TenantInfo) []error {
	exec := u.IEFactory()
	if exec == nil {
		return nil
	}

	errors := []error{}
	for _, tbl := range needUpgradeExistingView {
		if tbl.Account == table.AccountAll {
			for _, tenant := range tenants {
				if err := u.upgradeWithPreDropFunc(ctx, tbl, tenant, exec); err != nil {
					//make upgrade error with databaseName, table/View Name, tenantID:TenantName, error message
					errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, tenant.Tenant, tenant.TenantID, err.Error()))
					continue
				}
			}
		} else {
			if err := u.upgradeWithPreDropFunc(ctx, tbl, &frontend.TenantInfo{
				Tenant:        frontend.GetDefaultTenant(),
				TenantID:      catalog.System_Account,
				User:          frontend.GetUserRoot(),
				UserID:        frontend.GetUserRootId(),
				DefaultRoleID: frontend.GetDefaultRoleId(),
				DefaultRole:   frontend.GetDefaultRole(),
			}, exec); err != nil {
				errors = append(errors, moerr.NewUpgrateError(ctx, tbl.Database, tbl.Table, frontend.GetDefaultTenant(), catalog.System_Account, err.Error()))
			}
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

// CheckSchemaIsExist Check if the table exists
func (u *Upgrader) CheckSchemaIsExist(ctx context.Context, exec ie.InternalExecutor, opts *ie.OptsBuilder, database, tbl string) (bool, error) {
	// Query mo_catalog.mo_tables to get table info
	query := fmt.Sprintf("select rel_id, relname from `mo_catalog`.`mo_tables` where account_id = current_account_id() and reldatabase = '%s' and relname = '%s'", database, tbl)

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

// CheckViewDefineIsValid Check if the definition of the view is the latest version and valid
func (u *Upgrader) CheckViewDefineIsValid(ctx context.Context, exec ie.InternalExecutor, opts *ie.OptsBuilder, database, view, newViewDef string) (bool, error) {
	// Query mo_catalog.mo_tables to get table info
	dbName := strings.ToLower(database)
	viewName := strings.ToLower(view)
	query := fmt.Sprintf("select rel_createsql from mo_catalog.mo_tables where account_id = current_account_id() and relkind = 'v' and reldatabase = '%s' and relname = '%s'", dbName, viewName)

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
		curViewDef, err := result.StringValueByName(ctx, 0, "rel_createsql")
		if err != nil {
			return false, moerr.NewInternalError(ctx, "can not get the view define")
		}
		return curViewDef == newViewDef, nil
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
		errors = append(errors, moerr.NewInternalError(ctx, "can not get the schema"))
		return nil, liberrors.Join(errors...)
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

// upgradeWithPreDropFunc Pre deletion operation is required before upgrading
func (u *Upgrader) upgradeWithPreDropFunc(ctx context.Context, tbl *table.Table, tenant *frontend.TenantInfo, exec ie.InternalExecutor) error {
	// Switch Tenants
	ctx = attachAccount(ctx, tenant)
	opts := makeOptions(tenant)

	isValid, err := u.CheckViewDefineIsValid(ctx, exec, opts, tbl.Database, tbl.Table, tbl.CreateViewSql)
	if err != nil {
		return err
	}

	if !isValid {
		/*
			stmt := []string{
				"begin;",
				appendSemicolon(fmt.Sprintf("drop view if exists `%s`.`%s`", tbl.Database, tbl.Table)), //drop existing view definitions
				appendSemicolon(tbl.CreateViewSql), //recreate view
				"commit;",
			}
			// alter view
			upgradeSQL := strings.Join(stmt, "\n")

			// Execute upgrade SQL
			if err = exec.Exec(ctx, upgradeSQL, ie.NewOptsBuilder().Finish()); err != nil {
				return err
			}
		*/
		// Delete existing view definitions
		if err = exec.Exec(ctx, fmt.Sprintf("drop view if exists `%s`.`%s`", tbl.Database, tbl.Table), opts.Finish()); err != nil {
			return err
		}

		// Execute upgrade SQL
		if err := exec.Exec(ctx, tbl.CreateViewSql, opts.Finish()); err != nil {
			return err
		}

	}
	return nil
}
