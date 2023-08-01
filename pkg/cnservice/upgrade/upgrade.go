// Copyright 2021 - 2022 Matrix Origin
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

package upgrade

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

var registeredTable = []*table.Table{motrace.SingleStatementTable, motrace.SingleRowLogTable}

type Upgrader struct {
	IEFactory func() ie.InternalExecutor
}

func ParseDataTypeToColType(dataType string) table.ColType {
	switch dataType {
	case "datetime":
		return table.TDatetime
	case "bigint":
		if strings.Contains(dataType, "unsigned") {
			return table.TUint64
		}
		return table.TInt64
	case "double":
		return table.TFloat64
	case "json":
		return table.TJson
	case "text":
		return table.TText
	case "varchar":
		return table.TVarchar
	case "bytes":
		return table.TBytes
	case "uuid":
		return table.TUuid
	default:
		panic("Unknown data type: " + dataType)
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

	// Build a list of table.Columns based on the query result
	cols := []table.Column{}
	errors := []error{}
	for i := uint64(0); i < result.RowCount(); i++ {
		name, err := result.StringValueByName(ctx, i, "COLUMN_NAME")
		if err != nil {
			errors = append(errors, err)
			continue
		}
		dataType, err := result.StringValueByName(ctx, i, "DATA_TYPE")
		if err != nil {
			errors = append(errors, err)
			continue
		}
		cols = append(cols, table.Column{Name: name, ColType: ParseDataTypeToColType(dataType)})
	}

	// If errors occurred, return them
	if len(errors) > 0 {
		return nil, moerr.NewInternalError(ctx, "can not get the schema", errors)
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
		return "", moerr.NewInternalError(nil, "no added columns in schema diff", nil)
	}

	// Get database and table name from the schema diff
	databaseName := diff.DatabaseName
	tableName := diff.TableName

	// Generate the ALTER TABLE command
	command := fmt.Sprintf("ALTER TABLE `%s`.`%s` ", databaseName, tableName)

	for i, column := range diff.AddedColumns {
		if i > 0 {
			command += ", "
		}

		command += fmt.Sprintf("ADD COLUMN `%s` %s", column.Name, column.ColType.String(column.Scale))

		// If there's a default value, include it
		if column.Default != "" {
			command += fmt.Sprintf(" DEFAULT %s", column.Default)
		}
	}

	return command, nil
}

func (u *Upgrader) Upgrade(ctx context.Context) error {
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
		}

		upgradeSQL, err := u.GenerateUpgradeSQL(diff)
		if err != nil {
			return err
		}

		// Execute upgrade SQL
		if err := exec.Exec(ctx, upgradeSQL, ie.NewOptsBuilder().Finish()); err != nil {
			return moerr.NewInternalError(ctx, "failed to upgrade", []error{err})
		}
	}

	return nil
}
