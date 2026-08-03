// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package v4_0_6

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

var Handler *versionHandle

func init() {
	Handler = &versionHandle{metadata: versions.Version{
		Version:           "4.0.6",
		MinUpgradeVersion: "4.0.5",
		UpgradeCluster:    versions.Yes,
		UpgradeTenant:     versions.Yes,
		VersionOffset:     uint32(len(tenantUpgEntries) + len(clusterUpgEntries)),
	}}
}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	if err := upgradeLegacyForeignKeyMetadata(ctx, tenantID, txn); err != nil {
		return err
	}

	for _, entry := range tenantUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(tenantID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("tenant upgrade entry execute error",
				zap.Error(err), zap.Int32("tenantId", tenantID), zap.String("version", v.metadata.Version), zap.String("upgrade entry", entry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade entry complete",
			zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.metadata.Version))
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade success", zap.Int32("tenantId", tenantID), zap.String("toVersion", v.metadata.Version))
	return nil
}

const legacyForeignKeyTableDefinitionsSQL = "SELECT fk.db_name, fk.table_name, " +
	"fk.constraint_name, fk.column_name, fk.refer_db_name, fk.refer_table_name, fk.refer_column_name, fk.on_delete, fk.on_update " +
	"FROM mo_catalog.mo_foreign_keys fk " +
	"WHERE fk.constraint_id = 0 " +
	"ORDER BY fk.db_name, fk.table_name, fk.constraint_name, fk.column_name"

type legacyForeignKeyTableDefinition struct {
	database    string
	table       string
	foreignKeys []legacyForeignKeyCatalogRow
}

// legacyForeignKeyCatalogRow is the authoritative catalog source for the
// legacy foreign keys whose constraint_id has not yet been populated.
type legacyForeignKeyCatalogRow struct {
	constraintName  string
	columnName      string
	referDBName     string
	referTableName  string
	referColumnName string
	onDelete        string
	onUpdate        string
}

type legacyForeignKeyCatalogConstraint struct {
	name string
	rows []legacyForeignKeyCatalogRow
}

// upgradeLegacyForeignKeyMetadata restores zero-valued legacy FK ordinals. The
// catalog identifies the rows and actions to migrate, and SHOW CREATE TABLE
// supplies the current foreign-key declaration name and column order. In
// particular, do not use mo_tables.rel_createsql: it is a CREATE-time snapshot
// and omits later ALTERs.
func upgradeLegacyForeignKeyMetadata(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	definitions, err := getLegacyForeignKeyTableDefinitions(tenantID, txn)
	if err != nil {
		return err
	}
	if len(definitions) == 0 {
		return nil
	}
	for _, definition := range definitions {
		createSQL, err := getLegacyForeignKeyShowCreateSQL(tenantID, txn, definition)
		if err != nil {
			return err
		}
		updates, err := legacyForeignKeyMetadataUpdates(definition, createSQL)
		if err != nil {
			return err
		}
		for _, sql := range updates {
			res, err := txn.Exec(sql, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	return nil
}

func getLegacyForeignKeyTableDefinitions(tenantID int32, txn executor.TxnExecutor) ([]legacyForeignKeyTableDefinition, error) {
	res, err := txn.Exec(legacyForeignKeyTableDefinitionsSQL, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	definitions := make([]legacyForeignKeyTableDefinition, 0)
	definitionByTable := make(map[string]int)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			database := cols[0].GetStringAt(i)
			table := cols[1].GetStringAt(i)
			key := database + "\x00" + table
			definitionIndex, ok := definitionByTable[key]
			if !ok {
				definitionIndex = len(definitions)
				definitionByTable[key] = definitionIndex
				definitions = append(definitions, legacyForeignKeyTableDefinition{
					database: database,
					table:    table,
				})
			}
			definitions[definitionIndex].foreignKeys = append(definitions[definitionIndex].foreignKeys, legacyForeignKeyCatalogRow{
				constraintName:  cols[2].GetStringAt(i),
				columnName:      cols[3].GetStringAt(i),
				referDBName:     cols[4].GetStringAt(i),
				referTableName:  cols[5].GetStringAt(i),
				referColumnName: cols[6].GetStringAt(i),
				onDelete:        cols[7].GetStringAt(i),
				onUpdate:        cols[8].GetStringAt(i),
			})
		}
		return true
	})
	return definitions, nil
}

func getLegacyForeignKeyShowCreateSQL(tenantID int32, txn executor.TxnExecutor, definition legacyForeignKeyTableDefinition) (string, error) {
	res, err := txn.Exec(
		fmt.Sprintf("SHOW CREATE TABLE %s", sqlquote.QualifiedIdent(definition.database, definition.table)),
		executor.StatementOption{}.WithAccountID(uint32(tenantID)),
	)
	if err != nil {
		return "", err
	}
	defer res.Close()

	var createSQL string
	rowCount := 0
	validResult := true
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) < 2 {
			validResult = false
			return false
		}
		for i := 0; i < rows; i++ {
			rowCount++
			if rowCount != 1 {
				validResult = false
				return false
			}
			createSQL = cols[1].GetStringAt(i)
		}
		return true
	})
	if !validResult || rowCount != 1 || createSQL == "" {
		return "", moerr.NewInternalErrorNoCtxf("SHOW CREATE TABLE for legacy foreign-key table %s.%s returned an invalid result", definition.database, definition.table)
	}
	return createSQL, nil
}

func legacyForeignKeyMetadataUpdates(definition legacyForeignKeyTableDefinition, createSQL string) ([]string, error) {
	statements, err := mysql.Parse(context.Background(), createSQL, 1)
	if err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("parse legacy foreign-key table %s.%s: %v", definition.database, definition.table, err)
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()

	if len(statements) != 1 {
		return nil, moerr.NewInternalErrorNoCtxf("legacy foreign-key table %s.%s has %d statements", definition.database, definition.table, len(statements))
	}
	createTable, ok := statements[0].(*tree.CreateTable)
	if !ok {
		return nil, moerr.NewInternalErrorNoCtxf("legacy foreign-key table %s.%s does not have a CREATE TABLE definition", definition.database, definition.table)
	}

	constraints := legacyForeignKeyCatalogConstraints(definition.foreignKeys)
	matchedConstraints := make(map[string]bool)
	updates := make([]string, 0)
	for _, tableDef := range createTable.Defs {
		foreignKey, ok := tableDef.(*tree.ForeignKey)
		if !ok {
			continue
		}
		if foreignKey.Refer == nil {
			return nil, moerr.NewInternalErrorNoCtxf("legacy foreign key in %s.%s has incomplete persisted definition", definition.database, definition.table)
		}

		// Match the current SHOW CREATE TABLE declaration to the catalog by both
		// name and complete child/reference column set. The latter prevents a
		// dropped and re-added constraint that reused its name from being matched
		// to the old definition.
		if foreignKey.ConstraintSymbol != "" {
			for _, constraint := range constraints {
				if constraint.name == foreignKey.ConstraintSymbol &&
					sameLegacyForeignKeyColumns(definition.database, foreignKey, constraint.rows) {
					onDelete, onUpdate, err := legacyForeignKeyCatalogConstraintActions(constraint)
					if err != nil {
						return nil, err
					}
					matchedConstraints[constraint.name] = true
					updates = appendLegacyForeignKeyASTUpdates(updates, definition, constraint.name, foreignKey, onDelete, onUpdate)
					break
				}
			}
			continue
		}

		// An unnamed FK is assigned a generated name in the catalog. Match it to
		// the catalog rows by its complete child/reference column set.
		// If that signature is ambiguous, leave every candidate to the catalog
		// fallback below rather than assigning one AST action to another FK.
		matchedIndex := -1
		for i, constraint := range constraints {
			if matchedConstraints[constraint.name] || !sameLegacyForeignKeyColumns(definition.database, foreignKey, constraint.rows) {
				continue
			}
			if matchedIndex >= 0 {
				matchedIndex = -1
				break
			}
			matchedIndex = i
		}
		if matchedIndex >= 0 {
			constraint := constraints[matchedIndex]
			onDelete, onUpdate, err := legacyForeignKeyCatalogConstraintActions(constraint)
			if err != nil {
				return nil, err
			}
			matchedConstraints[constraint.name] = true
			updates = appendLegacyForeignKeyASTUpdates(updates, definition, constraint.name, foreignKey, onDelete, onUpdate)
		}
	}

	// A one-column FK needs no declaration-order recovery. For a composite FK,
	// constraint_id is the only stored order and legacy rows have it zero; never
	// fabricate an ordinal by sorting catalog column names when SHOW CREATE TABLE
	// cannot be reconciled.
	for _, constraint := range constraints {
		if matchedConstraints[constraint.name] {
			continue
		}
		if len(constraint.rows) != 1 {
			return nil, moerr.NewInternalErrorNoCtxf(
				"cannot reconcile column order for legacy composite foreign key %s in %s.%s",
				constraint.name, definition.database, definition.table,
			)
		}
		onDelete, onUpdate, err := legacyForeignKeyCatalogConstraintActions(constraint)
		if err != nil {
			return nil, err
		}
		row := constraint.rows[0]
		updates = append(updates, legacyForeignKeyUpdateSQL(
			definition,
			constraint.name,
			row.columnName,
			1,
			onDelete,
			onUpdate,
		))
	}
	return updates, nil
}

func legacyForeignKeyCatalogConstraints(rows []legacyForeignKeyCatalogRow) []legacyForeignKeyCatalogConstraint {
	byName := make(map[string][]legacyForeignKeyCatalogRow)
	for _, row := range rows {
		byName[row.constraintName] = append(byName[row.constraintName], row)
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)

	constraints := make([]legacyForeignKeyCatalogConstraint, 0, len(names))
	for _, name := range names {
		constraintRows := byName[name]
		sort.Slice(constraintRows, func(i, j int) bool {
			return constraintRows[i].columnName < constraintRows[j].columnName
		})
		constraints = append(constraints, legacyForeignKeyCatalogConstraint{name: name, rows: constraintRows})
	}
	return constraints
}

func sameLegacyForeignKeyColumns(database string, foreignKey *tree.ForeignKey, rows []legacyForeignKeyCatalogRow) bool {
	if len(foreignKey.KeyParts) != len(rows) || foreignKey.Refer == nil || len(foreignKey.Refer.KeyParts) != len(rows) {
		return false
	}
	referDatabase := database
	if foreignKey.Refer.TableName != nil && foreignKey.Refer.TableName.SchemaName != "" {
		referDatabase = string(foreignKey.Refer.TableName.SchemaName)
	}
	referTable := ""
	if foreignKey.Refer.TableName != nil {
		referTable = string(foreignKey.Refer.TableName.ObjectName)
	}
	columns := make([]string, len(foreignKey.KeyParts))
	for i, keyPart := range foreignKey.KeyParts {
		columns[i] = keyPart.ColName.ColName() + "\x00" + foreignKey.Refer.KeyParts[i].ColName.ColName()
	}
	sort.Strings(columns)
	catalogColumns := make([]string, len(rows))
	for i, row := range rows {
		catalogReferDatabase := row.referDBName
		if catalogReferDatabase == "" {
			catalogReferDatabase = database
		}
		if catalogReferDatabase != referDatabase || row.referTableName != referTable {
			return false
		}
		catalogColumns[i] = row.columnName + "\x00" + row.referColumnName
	}
	sort.Strings(catalogColumns)
	return strings.Join(columns, "\x00") == strings.Join(catalogColumns, "\x00")
}

func appendLegacyForeignKeyASTUpdates(
	updates []string,
	definition legacyForeignKeyTableDefinition,
	constraintName string,
	foreignKey *tree.ForeignKey,
	onDelete, onUpdate string,
) []string {
	for ordinal, keyPart := range foreignKey.KeyParts {
		updates = append(updates, legacyForeignKeyUpdateSQL(
			definition,
			constraintName,
			keyPart.ColName.ColName(),
			ordinal+1,
			onDelete,
			onUpdate,
		))
	}
	return updates
}

func legacyForeignKeyCatalogConstraintActions(constraint legacyForeignKeyCatalogConstraint) (string, string, error) {
	if len(constraint.rows) == 0 {
		return "", "", moerr.NewInternalErrorNoCtxf("legacy foreign key %s has no catalog rows", constraint.name)
	}
	onDelete := legacyCatalogReferenceActionName(constraint.rows[0].onDelete)
	onUpdate := legacyCatalogReferenceActionName(constraint.rows[0].onUpdate)
	for _, row := range constraint.rows[1:] {
		if legacyCatalogReferenceActionName(row.onDelete) != onDelete || legacyCatalogReferenceActionName(row.onUpdate) != onUpdate {
			return "", "", moerr.NewInternalErrorNoCtxf("legacy foreign key %s has inconsistent catalog actions", constraint.name)
		}
	}
	return onDelete, onUpdate, nil
}

func legacyForeignKeyUpdateSQL(
	definition legacyForeignKeyTableDefinition,
	constraintName, columnName string,
	ordinal int,
	onDelete, onUpdate string,
) string {
	return fmt.Sprintf(
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = %d, on_delete = %s, on_update = %s "+
			"WHERE constraint_id = 0 AND db_name = %s AND table_name = %s AND constraint_name = %s AND column_name = %s",
		ordinal,
		sqlquote.String(onDelete),
		sqlquote.String(onUpdate),
		sqlquote.String(definition.database),
		sqlquote.String(definition.table),
		sqlquote.String(constraintName),
		sqlquote.String(columnName),
	)
}

// Legacy rows with constraint_id = 0 were written before omitted foreign-key
// actions were represented as NO_ACTION. SHOW CREATE TABLE renders an omitted
// action as RESTRICT, so it cannot recover that distinction either. Preserve
// the established legacy migration policy: normalize RESTRICT to NO_ACTION.
func legacyCatalogReferenceActionName(action string) string {
	if strings.EqualFold(strings.TrimSpace(action), "RESTRICT") || strings.TrimSpace(action) == "" {
		return "NO_ACTION"
	}
	return action
}

func (v *versionHandle) HandleClusterUpgrade(_ context.Context, txn executor.TxnExecutor) error {
	for _, entry := range clusterUpgEntries {
		start := time.Now()
		if err := entry.Upgrade(txn, uint32(txn.Txn().TxnOptions().AccountID)); err != nil {
			getLogger(txn.Txn().TxnOptions().CN).Error("cluster upgrade entry execute error",
				zap.Error(err), zap.String("version", v.metadata.Version), zap.String("upgrade entry", entry.String()))
			return err
		}
		getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade entry complete",
			zap.String("upgrade entry", entry.String()), zap.Int64("time cost(ms)", time.Since(start).Milliseconds()), zap.String("toVersion", v.metadata.Version))
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("cluster upgrade success", zap.String("toVersion", v.metadata.Version))
	return nil
}

func (v *versionHandle) HandleCreateFrameworkDeps(executor.TxnExecutor) error {
	return moerr.NewInternalErrorNoCtxf("Only v1.2.0 can initialize upgrade framework, current version is:%s", v.metadata.Version)
}
