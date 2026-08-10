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

// The legacy invisible-index reconciliation is a dynamic per-tenant metadata
// migration rather than a static UpgradeEntry. Count it explicitly so binaries
// that already completed an earlier v4.0.6 offset still execute it.
const legacyInvisibleIndexUpgradeOffset uint32 = 1

func init() {
	Handler = &versionHandle{metadata: versions.Version{
		Version:           "4.0.6",
		MinUpgradeVersion: "4.0.5",
		UpgradeCluster:    versions.Yes,
		UpgradeTenant:     versions.Yes,
		VersionOffset:     uint32(len(tenantUpgEntries)+len(clusterUpgEntries)) + legacyInvisibleIndexUpgradeOffset,
	}}
}

type versionHandle struct{ metadata versions.Version }

func (v *versionHandle) Metadata() versions.Version { return v.metadata }

func (v *versionHandle) Prepare(_ context.Context, txn executor.TxnExecutor, _ bool) error {
	txn.Use(catalog.MO_CATALOG)
	return nil
}

func (v *versionHandle) HandleTenantUpgrade(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
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
	if err := upgradeLegacyForeignKeyMetadata(ctx, tenantID, txn); err != nil {
		return err
	}
	if err := upgradeLegacyInvisibleIndexMetadata(tenantID, txn); err != nil {
		return err
	}
	getLogger(txn.Txn().TxnOptions().CN).Info("tenant upgrade success", zap.Int32("tenantId", tenantID), zap.String("toVersion", v.metadata.Version))
	return nil
}

type legacyInvisibleIndexDefinition struct {
	database string
	table    string
	index    string
}

func legacyInvisibleIndexDefinitionsQuery(tenantID int32) string {
	return fmt.Sprintf(
		"SELECT DISTINCT tbl.reldatabase, tbl.relname, idx.name "+
			"FROM mo_catalog.mo_indexes idx "+
			"JOIN mo_catalog.mo_tables tbl ON tbl.rel_id = idx.table_id AND tbl.reldatabase_id = idx.database_id "+
			"WHERE tbl.account_id = %d AND idx.is_visible = 0 AND idx.hidden = 0 AND idx.type <> 'PRIMARY' "+
			"ORDER BY tbl.reldatabase, tbl.relname, idx.name",
		tenantID,
	)
}

// upgradeLegacyInvisibleIndexMetadata uses mo_indexes as the compatibility
// source of truth for old IndexDefs. Before visibility_set existed, a default
// visible index and an explicitly invisible index both serialized as
// visible=false, so the table constraint alone cannot distinguish them.
// Replaying the public ALTER path records visibility_set in the constraint and
// keeps mo_indexes plus every logical component of a multi-table index aligned.
func upgradeLegacyInvisibleIndexMetadata(tenantID int32, txn executor.TxnExecutor) error {
	definitions, err := getLegacyInvisibleIndexDefinitions(tenantID, txn)
	if err != nil {
		return err
	}

	for _, definition := range definitions {
		statement := fmt.Sprintf(
			"ALTER TABLE %s ALTER INDEX %s INVISIBLE",
			sqlquote.QualifiedIdent(definition.database, definition.table),
			sqlquote.Ident(definition.index),
		)
		res, err := txn.Exec(statement, versions.UpgradeStatementOption(uint32(tenantID)))
		if err != nil {
			return fmt.Errorf("migrate invisible index %s on %s.%s: %w", definition.index, definition.database, definition.table, err)
		}
		res.Close()
	}
	return nil
}

func getLegacyInvisibleIndexDefinitions(tenantID int32, txn executor.TxnExecutor) ([]legacyInvisibleIndexDefinition, error) {
	res, err := txn.Exec(
		legacyInvisibleIndexDefinitionsQuery(tenantID),
		executor.StatementOption{}.WithAccountID(uint32(tenantID)),
	)
	if err != nil {
		return nil, fmt.Errorf("list legacy invisible indexes for tenant %d: %w", tenantID, err)
	}
	defer res.Close()

	definitions := make([]legacyInvisibleIndexDefinition, 0)
	seen := make(map[string]struct{})
	validResult := true
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) < 3 {
			validResult = false
			return false
		}
		for i := 0; i < rows; i++ {
			definition := legacyInvisibleIndexDefinition{
				database: cols[0].GetStringAt(i),
				table:    cols[1].GetStringAt(i),
				index:    cols[2].GetStringAt(i),
			}
			if definition.database == "" || definition.table == "" || definition.index == "" {
				validResult = false
				return false
			}
			key := definition.database + "\x00" + definition.table + "\x00" + definition.index
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			definitions = append(definitions, definition)
		}
		return true
	})
	if !validResult {
		return nil, moerr.NewInternalErrorNoCtxf("invalid legacy invisible-index catalog result for tenant %d", tenantID)
	}
	return definitions, nil
}

const legacyForeignKeyTableDefinitionsSQL = "SELECT fk.db_name, fk.table_name, " +
	"fk.constraint_name, fk.column_name, fk.refer_db_name, fk.refer_table_name, fk.refer_column_name, fk.on_delete, fk.on_update " +
	"FROM mo_catalog.mo_foreign_keys fk " +
	"WHERE fk.constraint_id = 0 " +
	"ORDER BY fk.db_name, fk.table_name, fk.constraint_name, fk.column_name"

// legacyForeignKeyReferencedIndexDefinitionsSQL intentionally has a broader
// predicate than legacyForeignKeyTableDefinitionsSQL. Older upgrades may have
// assigned constraint_id while leaving the referenced key absent, so every FK
// whose new metadata column is empty must be reconciled with current catalog
// state.
const legacyForeignKeyReferencedIndexDefinitionsSQL = "SELECT fk.db_name, fk.table_name, " +
	"fk.constraint_name, fk.column_name, fk.refer_db_name, fk.refer_table_name, fk.refer_column_name, fk.on_delete, fk.on_update " +
	"FROM mo_catalog.mo_foreign_keys fk " +
	"WHERE fk.referenced_index_name = '' " +
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

type legacyReferenceAction struct {
	name   string
	origin string
}

type legacyForeignKeyActions struct {
	onDelete legacyReferenceAction
	onUpdate legacyReferenceAction
}

// upgradeLegacyForeignKeyMetadata restores zero-valued legacy FK ordinals. The
// catalog identifies the current constraints and SHOW CREATE TABLE supplies
// their current names and column order. rel_createsql is consulted only as a
// historical syntax record for a named CREATE-time FK that still reconciles
// with the catalog; it is never treated as the current table definition.
func upgradeLegacyForeignKeyMetadata(ctx context.Context, tenantID int32, txn executor.TxnExecutor) error {
	ordinalDefinitions, err := getLegacyForeignKeyTableDefinitions(legacyForeignKeyTableDefinitionsSQL, tenantID, txn)
	if err != nil {
		return err
	}
	referencedIndexDefinitions, err := getLegacyForeignKeyTableDefinitions(legacyForeignKeyReferencedIndexDefinitionsSQL, tenantID, txn)
	if err != nil {
		return err
	}

	showCreateByTable := make(map[string]string)
	getShowCreate := func(definition legacyForeignKeyTableDefinition) (string, error) {
		key := definition.database + "\x00" + definition.table
		if createSQL, ok := showCreateByTable[key]; ok {
			return createSQL, nil
		}
		createSQL, err := getLegacyForeignKeyShowCreateSQL(tenantID, txn, definition)
		if err != nil {
			return "", err
		}
		showCreateByTable[key] = createSQL
		return createSQL, nil
	}
	historicalCreateByTable := make(map[string]string)
	getHistoricalCreate := func(definition legacyForeignKeyTableDefinition) (string, error) {
		key := definition.database + "\x00" + definition.table
		if createSQL, ok := historicalCreateByTable[key]; ok {
			return createSQL, nil
		}
		createSQL, err := getLegacyForeignKeyHistoricalCreateSQL(tenantID, txn, definition)
		if err != nil {
			return "", err
		}
		historicalCreateByTable[key] = createSQL
		return createSQL, nil
	}

	// constraint_id and action origins have a legacy zero-value sentinel, so
	// only those rows need the ordinal/action migration.
	for _, definition := range ordinalDefinitions {
		createSQL, err := getShowCreate(definition)
		if err != nil {
			return err
		}
		historicalCreateSQL, err := getHistoricalCreate(definition)
		if err != nil {
			return err
		}
		updates, err := legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(definition, createSQL, historicalCreateSQL)
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

	// referenced_index_name is independent of the legacy ordinal sentinel.
	// Backfill it for both zero-ordinal rows and already-numbered historical
	// rows, without reinterpreting their action metadata.
	for _, definition := range referencedIndexDefinitions {
		createSQL, err := getShowCreate(definition)
		if err != nil {
			return err
		}
		referencedKeys, err := legacyForeignKeyReferencedKeys(definition, createSQL)
		if err != nil {
			return err
		}
		for constraintName, key := range referencedKeys {
			indexName, err := getLegacyForeignKeyReferencedIndexName(tenantID, txn, key)
			if err != nil {
				return err
			}
			if indexName == "" {
				continue
			}
			res, err := txn.Exec(legacyForeignKeyReferencedIndexUpdateSQL(definition, constraintName, indexName), executor.StatementOption{}.WithAccountID(uint32(tenantID)))
			if err != nil {
				return err
			}
			res.Close()
		}
	}
	return nil
}

type legacyForeignKeyReferencedKey struct {
	database string
	table    string
	columns  []string
}

// legacyForeignKeyReferencedKeys reconciles the current SHOW CREATE output
// with catalog constraints. It never reads rel_createsql: ALTER-added FKs are
// deliberately matched by their current child/reference column pairs.
func legacyForeignKeyReferencedKeys(definition legacyForeignKeyTableDefinition, createSQL string) (map[string]legacyForeignKeyReferencedKey, error) {
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
	matched := make(map[string]bool)
	keys := make(map[string]legacyForeignKeyReferencedKey)
	for _, tableDef := range createTable.Defs {
		foreignKey, ok := tableDef.(*tree.ForeignKey)
		if !ok || foreignKey.Refer == nil {
			continue
		}
		matchedIndex := -1
		for i, constraint := range constraints {
			if matched[constraint.name] || !sameLegacyForeignKeyColumns(definition.database, foreignKey, constraint.rows) {
				continue
			}
			if foreignKey.ConstraintSymbol != "" && foreignKey.ConstraintSymbol != constraint.name {
				continue
			}
			if matchedIndex >= 0 {
				matchedIndex = -1
				break
			}
			matchedIndex = i
		}
		if matchedIndex < 0 {
			continue
		}
		constraint := constraints[matchedIndex]
		matched[constraint.name] = true
		database := definition.database
		if foreignKey.Refer.TableName.SchemaName != "" {
			database = string(foreignKey.Refer.TableName.SchemaName)
		}
		columns := make([]string, len(foreignKey.Refer.KeyParts))
		for i, keyPart := range foreignKey.Refer.KeyParts {
			columns[i] = keyPart.ColName.ColName()
		}
		keys[constraint.name] = legacyForeignKeyReferencedKey{
			database: database,
			table:    string(foreignKey.Refer.TableName.ObjectName),
			columns:  columns,
		}
	}
	for _, constraint := range constraints {
		if matched[constraint.name] || len(constraint.rows) != 1 {
			continue
		}
		row := constraint.rows[0]
		database := row.referDBName
		if database == "" {
			database = definition.database
		}
		keys[constraint.name] = legacyForeignKeyReferencedKey{database: database, table: row.referTableName, columns: []string{row.referColumnName}}
	}
	return keys, nil
}

func getLegacyForeignKeyReferencedIndexName(tenantID int32, txn executor.TxnExecutor, key legacyForeignKeyReferencedKey) (string, error) {
	query := fmt.Sprintf(
		"SELECT idx.name, idx.type, idx.ordinal_position, idx.column_name FROM mo_catalog.mo_indexes idx "+
			"JOIN mo_catalog.mo_tables tbl ON idx.table_id = tbl.rel_id "+
			"WHERE tbl.account_id = %d AND tbl.reldatabase = %s AND tbl.relname = %s AND (idx.type = 'PRIMARY' OR idx.type = 'UNIQUE') "+
			"ORDER BY CASE WHEN idx.type = 'PRIMARY' THEN 0 ELSE 1 END, idx.name, idx.ordinal_position",
		tenantID, sqlquote.String(key.database), sqlquote.String(key.table),
	)
	res, err := txn.Exec(query, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
	if err != nil {
		return "", err
	}
	defer res.Close()
	type candidate struct {
		name    string
		primary bool
		columns []string
	}
	candidates := make(map[string]*candidate)
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows; i++ {
			name := cols[0].GetStringAt(i)
			candidateKey := cols[1].GetStringAt(i) + "\x00" + name
			entry := candidates[candidateKey]
			if entry == nil {
				entry = &candidate{name: name, primary: strings.EqualFold(cols[1].GetStringAt(i), "PRIMARY")}
				candidates[candidateKey] = entry
			}
			entry.columns = append(entry.columns, cols[3].GetStringAt(i))
		}
		return true
	})
	ordered := make([]*candidate, 0, len(candidates))
	for _, candidate := range candidates {
		ordered = append(ordered, candidate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].primary != ordered[j].primary {
			return ordered[i].primary
		}
		return ordered[i].name < ordered[j].name
	})
	for _, candidate := range ordered {
		if len(candidate.columns) < len(key.columns) {
			continue
		}
		matched := true
		for i, column := range key.columns {
			if candidate.columns[i] != column {
				matched = false
				break
			}
		}
		if matched {
			return candidate.name, nil
		}
	}
	// Legacy MatrixOne accepted some FK shapes that do not name an ordered
	// leading prefix of a PRIMARY/UNIQUE key. Leave the value empty rather than
	// inventing a wrong UNIQUE_CONSTRAINT_NAME; new FK creation rejects them.
	return "", nil
}

func legacyForeignKeyReferencedIndexUpdateSQL(definition legacyForeignKeyTableDefinition, constraintName, indexName string) string {
	return fmt.Sprintf(
		"UPDATE mo_catalog.mo_foreign_keys SET referenced_index_name = %s WHERE db_name = %s AND table_name = %s AND constraint_name = %s",
		sqlquote.String(indexName), sqlquote.String(definition.database), sqlquote.String(definition.table), sqlquote.String(constraintName),
	)
}

func getLegacyForeignKeyTableDefinitions(query string, tenantID int32, txn executor.TxnExecutor) ([]legacyForeignKeyTableDefinition, error) {
	res, err := txn.Exec(query, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
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

func getLegacyForeignKeyHistoricalCreateSQL(tenantID int32, txn executor.TxnExecutor, definition legacyForeignKeyTableDefinition) (string, error) {
	query := fmt.Sprintf(
		"SELECT rel_createsql FROM mo_catalog.mo_tables WHERE account_id = %d AND reldatabase = %s AND relname = %s",
		tenantID, sqlquote.String(definition.database), sqlquote.String(definition.table),
	)
	res, err := txn.Exec(query, executor.StatementOption{}.WithAccountID(uint32(tenantID)))
	if err != nil {
		return "", err
	}
	defer res.Close()

	var createSQL string
	rowCount := 0
	validResult := true
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if len(cols) < 1 {
			validResult = false
			return false
		}
		for i := 0; i < rows; i++ {
			rowCount++
			if rowCount != 1 {
				validResult = false
				return false
			}
			createSQL = cols[0].GetStringAt(i)
		}
		return true
	})
	if !validResult || rowCount > 1 {
		return "", moerr.NewInternalErrorNoCtxf("historical CREATE definition for legacy foreign-key table %s.%s returned an invalid result", definition.database, definition.table)
	}
	return createSQL, nil
}

func legacyForeignKeyMetadataUpdates(definition legacyForeignKeyTableDefinition, createSQL string) ([]string, error) {
	return legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(definition, createSQL, "")
}

func legacyForeignKeyMetadataUpdatesWithHistoricalDefinition(
	definition legacyForeignKeyTableDefinition,
	createSQL string,
	historicalCreateSQL string,
) ([]string, error) {
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
	historicalActions := legacyForeignKeyHistoricalActions(definition, constraints, historicalCreateSQL)
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
					actions, err := legacyForeignKeyCatalogConstraintActions(constraint)
					if err != nil {
						return nil, err
					}
					if historical, ok := historicalActions[constraint.name]; ok {
						actions = historical
					}
					matchedConstraints[constraint.name] = true
					updates = appendLegacyForeignKeyASTUpdates(updates, definition, constraint.name, foreignKey, actions)
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
			actions, err := legacyForeignKeyCatalogConstraintActions(constraint)
			if err != nil {
				return nil, err
			}
			if historical, ok := historicalActions[constraint.name]; ok {
				actions = historical
			}
			matchedConstraints[constraint.name] = true
			updates = appendLegacyForeignKeyASTUpdates(updates, definition, constraint.name, foreignKey, actions)
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
		actions, err := legacyForeignKeyCatalogConstraintActions(constraint)
		if err != nil {
			return nil, err
		}
		if historical, ok := historicalActions[constraint.name]; ok {
			actions = historical
		}
		row := constraint.rows[0]
		updates = append(updates, legacyForeignKeyUpdateSQL(
			definition,
			constraint.name,
			row.columnName,
			1,
			actions,
		))
	}
	return updates, nil
}

// legacyForeignKeyHistoricalActions extracts action syntax only for named
// CREATE-time constraints that still match the authoritative catalog by name,
// child columns, referenced table, and referenced columns. Invalid, stale, or
// unnamed historical definitions are ignored so ALTER-added and generated-name
// constraints continue through the catalog/SHOW CREATE fallback.
func legacyForeignKeyHistoricalActions(
	definition legacyForeignKeyTableDefinition,
	constraints []legacyForeignKeyCatalogConstraint,
	createSQL string,
) map[string]legacyForeignKeyActions {
	if strings.TrimSpace(createSQL) == "" {
		return nil
	}
	statements, err := mysql.Parse(context.Background(), createSQL, 1)
	if err != nil {
		return nil
	}
	defer func() {
		for _, statement := range statements {
			statement.Free()
		}
	}()
	if len(statements) != 1 {
		return nil
	}
	createTable, ok := statements[0].(*tree.CreateTable)
	if !ok {
		return nil
	}

	actionsByConstraint := make(map[string]legacyForeignKeyActions)
	for _, tableDef := range createTable.Defs {
		foreignKey, ok := tableDef.(*tree.ForeignKey)
		if !ok || foreignKey.Refer == nil || foreignKey.ConstraintSymbol == "" {
			continue
		}
		for _, constraint := range constraints {
			if constraint.name != foreignKey.ConstraintSymbol ||
				!sameLegacyForeignKeyColumns(definition.database, foreignKey, constraint.rows) {
				continue
			}
			catalogActions, err := legacyForeignKeyCatalogConstraintActions(constraint)
			if err != nil {
				continue
			}
			historicalActions := legacyForeignKeyASTActions(foreignKey)
			if legacyHistoricalActionMatchesCatalog(historicalActions.onDelete, catalogActions.onDelete) &&
				legacyHistoricalActionMatchesCatalog(historicalActions.onUpdate, catalogActions.onUpdate) {
				actionsByConstraint[constraint.name] = historicalActions
			}
			break
		}
	}
	return actionsByConstraint
}

func legacyForeignKeyASTActions(foreignKey *tree.ForeignKey) legacyForeignKeyActions {
	return legacyForeignKeyActions{
		onDelete: legacyASTReferenceAction(foreignKey.Refer.OnDelete),
		onUpdate: legacyASTReferenceAction(foreignKey.Refer.OnUpdate),
	}
}

func legacyASTReferenceAction(action tree.ReferenceOptionType) legacyReferenceAction {
	if action == tree.REFERENCE_OPTION_INVALID {
		return legacyReferenceAction{name: "NO_ACTION", origin: "ACTION_ORIGIN_DEFAULT"}
	}
	return legacyReferenceAction{
		name:   strings.ToUpper(strings.ReplaceAll(action.ToString(), " ", "_")),
		origin: "ACTION_ORIGIN_EXPLICIT",
	}
}

func legacyHistoricalActionMatchesCatalog(historical, catalog legacyReferenceAction) bool {
	if historical.origin == "ACTION_ORIGIN_DEFAULT" {
		// Legacy execution stored an omitted action as RESTRICT. Some partially
		// migrated catalogs may already contain the metadata spelling NO_ACTION.
		return catalog.name == "RESTRICT" || catalog.name == "NO_ACTION"
	}
	return historical.name == catalog.name
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
	actions legacyForeignKeyActions,
) []string {
	for ordinal, keyPart := range foreignKey.KeyParts {
		updates = append(updates, legacyForeignKeyUpdateSQL(
			definition,
			constraintName,
			keyPart.ColName.ColName(),
			ordinal+1,
			actions,
		))
	}
	return updates
}

func legacyForeignKeyCatalogConstraintActions(constraint legacyForeignKeyCatalogConstraint) (legacyForeignKeyActions, error) {
	if len(constraint.rows) == 0 {
		return legacyForeignKeyActions{}, moerr.NewInternalErrorNoCtxf("legacy foreign key %s has no catalog rows", constraint.name)
	}
	onDelete := legacyCatalogReferenceAction(constraint.rows[0].onDelete)
	onUpdate := legacyCatalogReferenceAction(constraint.rows[0].onUpdate)
	for _, row := range constraint.rows[1:] {
		if legacyCatalogReferenceAction(row.onDelete).name != onDelete.name || legacyCatalogReferenceAction(row.onUpdate).name != onUpdate.name {
			return legacyForeignKeyActions{}, moerr.NewInternalErrorNoCtxf("legacy foreign key %s has inconsistent catalog actions", constraint.name)
		}
	}
	return legacyForeignKeyActions{onDelete: onDelete, onUpdate: onUpdate}, nil
}

func legacyForeignKeyUpdateSQL(
	definition legacyForeignKeyTableDefinition,
	constraintName, columnName string,
	ordinal int,
	actions legacyForeignKeyActions,
) string {
	return fmt.Sprintf(
		"UPDATE mo_catalog.mo_foreign_keys SET constraint_id = %d, on_delete = %s, on_update = %s, on_delete_origin = %s, on_update_origin = %s "+
			"WHERE constraint_id = 0 AND db_name = %s AND table_name = %s AND constraint_name = %s AND column_name = %s",
		ordinal,
		sqlquote.String(actions.onDelete.name),
		sqlquote.String(actions.onUpdate.name),
		sqlquote.String(actions.onDelete.origin),
		sqlquote.String(actions.onUpdate.origin),
		sqlquote.String(definition.database),
		sqlquote.String(definition.table),
		sqlquote.String(constraintName),
		sqlquote.String(columnName),
	)
}

// The catalog is the authoritative fallback for ALTER-added, unnamed, stale,
// or otherwise unreconciled historical definitions. Preserve its action. Only
// RESTRICT and NO_ACTION can be confused with an omitted action, so mark those
// origins as ambiguous; all other actions necessarily came from explicit SQL.
func legacyCatalogReferenceAction(action string) legacyReferenceAction {
	action = strings.TrimSpace(action)
	if action == "" {
		action = "NO_ACTION"
	}
	action = strings.ToUpper(strings.ReplaceAll(action, " ", "_"))
	origin := "ACTION_ORIGIN_EXPLICIT"
	if action == "RESTRICT" || action == "NO_ACTION" {
		origin = "ACTION_ORIGIN_LEGACY_AMBIGUOUS"
	}
	return legacyReferenceAction{name: action, origin: origin}
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
