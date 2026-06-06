// Copyright 2021 Matrix Origin
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

package checkpointtool

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

const (
	// Meta columns in LogicalTableView (object, block, row)
	logicalViewMetaCols = 3

	// System table IDs
	moTablesID  = uint64(catalog.MO_TABLES_ID)
	moColumnsID = uint64(catalog.MO_COLUMNS_ID)
)

// TableColumn describes one column in a user table schema.
type TableColumn struct {
	Name             string // SQL column name
	SQLType          string // SQL type string (e.g. "BIGINT", "VARCHAR(100)")
	Position         int    // SQL ordinal position
	PhysicalPosition int    // physical/object column position
}

// TableSchema holds the decoded schema for one user table.
type TableSchema struct {
	TableName    string
	DatabaseName string
	Columns      []TableColumn // sorted by Position
	CreateSQL    string        // raw CREATE TABLE from mo_tables.rel_createsql
}

// ReadTableSchema reads the schema for a user table by reading mo_tables and mo_columns
// from the checkpoint.
//
// It composes the logical view at snapshotTS, reads system table data for mo_tables (table 2)
// and mo_columns (table 3), and extracts the schema for the given user tableID.
//
// IMPORTANT: LogicalTableView includes hidden columns (e.g. __mo_fake_pk_col) in its data
// rows, so we cannot use catalog index constants directly. Instead, column positions are
// resolved by name from the view headers.
//
// If the schema cannot be resolved from checkpoint catalog metadata, the returned schema
// will not contain visible columns. Callers that need an exact user-facing schema must
// treat that as an error instead of falling back to raw physical object columns.
func (r *CheckpointReader) ReadTableSchema(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
	dataView *LogicalTableView,
) *TableSchema {
	_ = dataView
	schema := &TableSchema{TableName: fmt.Sprintf("%d", tableID)}

	// Try to read mo_tables from the checkpoint (table 2)
	moTablesView, err := r.getTableLogicalView(ctx, moTablesID, snapshotTS)
	if err != nil {
		// Don't log - expected for system tables or when mo_tables not in checkpoint
	} else if moTablesView != nil {
		// Resolve column positions by name (handles hidden column offset)
		relIDCol := moTablesView.columnDataIndex("rel_id")
		if relIDCol < 0 {
			relIDCol = moTablesPhysRelID
		}
		for _, row := range moTablesView.Rows {
			dataRow := row[logicalViewMetaCols:]
			if relIDCol < 0 || relIDCol >= len(dataRow) {
				continue
			}
			if dataRow[relIDCol] == fmt.Sprintf("%d", tableID) {
				schema = buildSchemaFromMoTablesRow(moTablesView, row)
				break
			}
		}
	}

	// Try to read mo_columns from the checkpoint (table 3)
	if schema.TableName == fmt.Sprintf("%d", tableID) {
		// Couldn't get table name from mo_tables; still try for columns
	}
	moColumnsView, err := r.getTableLogicalView(ctx, moColumnsID, snapshotTS)
	if err == nil && moColumnsView != nil {
		cols := buildColumnsFromMoColumnsRows(moColumnsView, tableID)
		if len(cols) > 0 {
			schema.Columns = cols
		}
	}

	return schema
}

// getTableLogicalView composes the checkpoint view at snapshotTS and builds a logical
// table view for the given tableID by aggregating data/tomb entries across all
// relevant checkpoint entries (GCKP + ICKPs).
func (r *CheckpointReader) getTableLogicalView(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) (*LogicalTableView, error) {
	composed, err := r.ComposeAt(snapshotTS)
	if err != nil {
		return nil, err
	}

	tbl, ok := composed.Tables[tableID]
	if !ok || (len(tbl.DataRanges) == 0 && len(tbl.TombRanges) == 0) {
		return nil, moerr.NewInternalErrorf(ctx, "table %d not found in checkpoint at ts %s", tableID, snapshotTS.ToString())
	}

	// Collect all entries that contain this table (base + incrementals)
	type entryRef struct {
		entry *EntryInfo
	}
	var entryRefs []entryRef
	if composed.BaseEntry != nil {
		entryRefs = append(entryRefs, entryRef{entry: composed.BaseEntry})
	}
	for _, incr := range composed.Incrementals {
		entryRefs = append(entryRefs, entryRef{entry: incr})
	}

	// Aggregate data/tomb entries across all checkpoint entries
	var allData, allTomb []*ObjectEntryInfo
	for _, ref := range entryRefs {
		e := r.entries[ref.entry.Index]
		dataEntries, tombEntries, err := r.GetObjectEntries(e, tableID)
		if err != nil {
			continue // skip entries that don't have this table
		}
		allData = append(allData, dataEntries...)
		allTomb = append(allTomb, tombEntries...)
	}

	if len(allData) == 0 {
		return nil, moerr.NewInternalErrorf(ctx, "no data entries for table %d", tableID)
	}

	return r.BuildLogicalTableView(ctx, snapshotTS, allData, allTomb)
}

// DumpTableCSV reads a user table's logical view and writes it as CSV to w.
//
// It first resolves the table schema from mo_tables/mo_columns, then writes the
// CSV with proper column headers and data rows.
func (r *CheckpointReader) DumpTableCSV(
	ctx context.Context,
	w io.Writer,
	tableID uint64,
	snapshotTS types.TS,
	dataEntries, tombEntries []*ObjectEntryInfo,
) error {
	view, err := r.BuildLogicalTableView(ctx, snapshotTS, dataEntries, tombEntries)
	if err != nil {
		return err
	}

	schema := r.ReadTableSchema(ctx, tableID, snapshotTS, view)
	if len(schema.Columns) == 0 {
		return moerr.NewInternalErrorf(
			ctx,
			"cannot resolve visible columns for table %d from checkpoint metadata; mo_columns data is unavailable or incomplete",
			tableID,
		)
	}
	return WriteCSV(w, schema, view)
}

// DumpTableCSVComposed dumps a table to CSV by composing the full checkpoint view
// at snapshotTS (aggregating across GCKP + all ICKPs). This is equivalent to calling
// DumpTableCSV with entries from ComposeAt.
func (r *CheckpointReader) DumpTableCSVComposed(
	ctx context.Context,
	w io.Writer,
	tableID uint64,
	snapshotTS types.TS,
) error {
	view, err := r.getTableLogicalView(ctx, tableID, snapshotTS)
	if err != nil {
		return err
	}
	schema := r.ReadTableSchema(ctx, tableID, snapshotTS, view)
	if len(schema.Columns) == 0 {
		return moerr.NewInternalErrorf(
			ctx,
			"cannot resolve visible columns for table %d from checkpoint metadata; mo_columns data is unavailable or incomplete",
			tableID,
		)
	}
	return WriteCSV(w, schema, view)
}

// WriteCSV writes a LogicalTableView with the given schema as CSV to w.
func WriteCSV(w io.Writer, schema *TableSchema, view *LogicalTableView) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	// Write header comments (DDL + metadata)
	if schema.CreateSQL != "" {
		if _, err := fmt.Fprintf(w, "-- %s\n", schema.CreateSQL); err != nil {
			return err
		}
	}
	if schema.DatabaseName != "" {
		if _, err := fmt.Fprintf(w, "-- Database: %s\n", schema.DatabaseName); err != nil {
			return err
		}
	}
	if schema.TableName != "" {
		if _, err := fmt.Fprintf(w, "-- Table: %s\n", schema.TableName); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "-- Visible rows: %d (deleted: %d, physical: %d)\n",
		view.VisibleRows, view.DeletedRows, view.PhysicalRows); err != nil {
		return err
	}

	// Merge schema column names into headers
	merged := MergeLogicalViewWithSchema(view, schema)
	if err := cw.Write(merged.Headers); err != nil {
		return err
	}

	// Write data rows (skip the 3 meta columns)
	for _, row := range merged.Rows {
		if err := cw.Write(row); err != nil {
			return err
		}
	}

	cw.Flush()
	return cw.Error()
}

// MergeLogicalViewWithSchema replaces col_N headers with real column names from the schema
// and filters data rows to only include visible (non-hidden) columns by their physical position.
// Returns a new LogicalTableView with merged headers and filtered data rows.
func MergeLogicalViewWithSchema(view *LogicalTableView, schema *TableSchema) *LogicalTableView {
	dataWidth := len(view.Headers) - logicalViewMetaCols
	if dataWidth < 0 {
		dataWidth = 0
	}

	// Without schema columns we cannot safely distinguish visible columns from hidden ones.
	if len(schema.Columns) == 0 {
		return &LogicalTableView{
			Headers:      nil,
			Rows:         nil,
			VisibleRows:  view.VisibleRows,
			DeletedRows:  view.DeletedRows,
			PhysicalRows: view.PhysicalRows,
		}
	}

	// Build visible column headers and their physical data positions
	newHeaders := make([]string, 0, len(schema.Columns))
	colMap := make([]int, 0, len(schema.Columns)) // visibleIdx → physical data column index

	for _, col := range schema.Columns {
		newHeaders = append(newHeaders, col.Name)
		physicalPos := col.PhysicalPosition
		if physicalPos < 0 {
			physicalPos = col.Position
		}
		colMap = append(colMap, physicalPos)
	}

	// Extract data rows: pick only visible columns by their physical position
	newRows := make([][]string, len(view.Rows))
	for i, row := range view.Rows {
		newRow := make([]string, len(colMap))
		for j, pos := range colMap {
			dataIdx := logicalViewMetaCols + pos
			if dataIdx < len(row) {
				newRow[j] = row[dataIdx]
			}
		}
		newRows[i] = newRow
	}

	return &LogicalTableView{
		Headers:      newHeaders,
		Rows:         newRows,
		VisibleRows:  view.VisibleRows,
		DeletedRows:  view.DeletedRows,
		PhysicalRows: view.PhysicalRows,
	}
}

// buildSchemaFromMoTablesRow extracts a TableSchema from a mo_tables data row.
// Uses column name lookup from view headers to handle hidden column offsets.
// mo_tables columns: rel_id, relname, reldatabase, reldatabase_id, ..., rel_createsql
func buildSchemaFromMoTablesRow(view *LogicalTableView, fullRow []string) *TableSchema {
	schema := &TableSchema{}
	dataRow := fullRow[logicalViewMetaCols:]

	relNameIdx := view.columnDataIndex("relname")
	if relNameIdx < 0 {
		relNameIdx = moTablesPhysRelName
	}
	if relNameIdx < len(dataRow) {
		schema.TableName = dataRow[relNameIdx]
	}

	relDBIdx := view.columnDataIndex("reldatabase")
	if relDBIdx < 0 {
		relDBIdx = moTablesPhysRelDatabase
	}
	if relDBIdx < len(dataRow) {
		schema.DatabaseName = dataRow[relDBIdx]
	}

	createSQLIdx := view.columnDataIndex("rel_createsql")
	if createSQLIdx < 0 {
		createSQLIdx = moTablesPhysRelCreateSQL
	}
	if createSQLIdx < len(dataRow) {
		schema.CreateSQL = dataRow[createSQLIdx]
	}
	return schema
}

// buildColumnsFromMoColumnsRows builds a sorted column list from mo_columns data rows
// filtered for a specific tableID and excluding hidden columns.
// Uses column name lookup from view headers to handle hidden column offsets.
// mo_columns columns: att_relname_id, att_relname, attname, atttyp, attnum, ..., att_is_hidden
func buildColumnsFromMoColumnsRows(view *LogicalTableView, tableID uint64) []TableColumn {
	tableIDStr := fmt.Sprintf("%d", tableID)

	// Resolve column positions by name with hardcoded fallback for col_N headers
	relnameIDCol := view.columnDataIndex("att_relname_id")
	if relnameIDCol < 0 {
		relnameIDCol = moColsPhysRelNameID
	}
	nameCol := view.columnDataIndex("attname")
	if nameCol < 0 {
		nameCol = moColsPhysAttName
	}
	typCol := view.columnDataIndex("atttyp")
	if typCol < 0 {
		typCol = moColsPhysAttTyp
	}
	numCol := view.columnDataIndex("attnum")
	if numCol < 0 {
		numCol = moColsPhysAttNum
	}
	hiddenCol := view.columnDataIndex("att_is_hidden")
	if hiddenCol < 0 {
		hiddenCol = moColsPhysIsHidden
	}
	seqNumCol := view.columnDataIndex("att_seqnum")
	if seqNumCol < 0 {
		seqNumCol = moColsPhysSeqNum
	}

	var cols []TableColumn
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewMetaCols:]

		// Skip hidden columns
		if hiddenCol >= 0 && hiddenCol < len(row) {
			if row[hiddenCol] == "1" || row[hiddenCol] == "true" {
				continue
			}
		}

		// Filter by table ID
		if relnameIDCol < 0 || relnameIDCol >= len(row) {
			continue
		}
		if row[relnameIDCol] != tableIDStr {
			continue
		}

		pos := len(cols)
		if numCol >= 0 && numCol < len(row) {
			if n, err := strconv.Atoi(row[numCol]); err == nil {
				pos = n
			}
		}

		physicalPos := pos - 1
		if seqNumCol >= 0 && seqNumCol < len(row) {
			if n, err := strconv.Atoi(row[seqNumCol]); err == nil {
				physicalPos = n
			}
		}

		col := TableColumn{
			Position:         pos,
			PhysicalPosition: physicalPos,
		}
		if nameCol >= 0 && nameCol < len(row) {
			col.Name = row[nameCol]
		}
		if typCol >= 0 && typCol < len(row) {
			col.SQLType = row[typCol]
		}

		cols = append(cols, col)
	}

	// Sort by position
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Position < cols[j].Position
	})

	return cols
}

// getDataColumnsFromView extracts only the data columns (skipping object/block/row meta columns).
// ShowCreateTable returns the CREATE TABLE DDL for a given tableID by reading
// the checkpoint's mo_tables and mo_columns system tables (GCKP + following ICKPs).
//
// Priority:
//  1. mo_tables.rel_createsql — the original CREATE TABLE SQL (most accurate)
//  2. Reconstructed from mo_columns (attname, atttyp, attnum, att_is_hidden)
//  3. Hardcoded built-in table schemas (for core system tables like mo_tables, mo_columns, etc.)
//
// NOTE:
// We intentionally do not synthesize a CREATE TABLE from raw physical object column
// types when mo_tables/mo_columns metadata is unavailable. Physical object columns may
// include hidden system columns, and without mo_columns we cannot safely distinguish
// visible columns from hidden ones.
//
// hardcoded physical column positions for mo_tables (after stripping meta cols).
// Layout: [rel_id(0), relname(1), reldatabase(2), reldatabase_id(3), ...]
// These are used as fallback when the LogicalTableView has col_N headers.
const (
	moTablesPhysRelID        = 0
	moTablesPhysRelName      = 1
	moTablesPhysRelDatabase  = 2
	moTablesPhysRelCreateSQL = 7

	// mo_columns hardcoded physical positions (after meta cols).
	moColsPhysRelNameID = 4
	moColsPhysAttName   = 6
	moColsPhysAttTyp    = 7
	moColsPhysAttNum    = 8
	moColsPhysIsHidden  = 18
	moColsPhysSeqNum    = 22
)

func (r *CheckpointReader) ShowCreateTable(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) (string, error) {
	// 1. Try mo_tables.rel_createsql
	moTablesView, err := r.getTableLogicalView(ctx, moTablesID, snapshotTS)
	if err == nil && moTablesView != nil {
		relIDCol := moTablesView.columnDataIndex("rel_id")
		if relIDCol < 0 {
			relIDCol = moTablesPhysRelID
		}
		createSQLCol := moTablesView.columnDataIndex("rel_createsql")
		if createSQLCol < 0 {
			createSQLCol = moTablesPhysRelCreateSQL
		}
		for _, fullRow := range moTablesView.Rows {
			row := fullRow[logicalViewMetaCols:]
			if relIDCol < len(row) && row[relIDCol] == fmt.Sprintf("%d", tableID) {
				if createSQLCol < len(row) && row[createSQLCol] != "" {
					return row[createSQLCol], nil
				}
				break
			}
		}
	}

	// 2. Reconstruct from mo_columns
	moColumnsView, err := r.getTableLogicalView(ctx, moColumnsID, snapshotTS)
	if err == nil && moColumnsView != nil {
		if ddl := buildCreateTableFromMoColumns(moColumnsView, tableID); ddl != "" {
			return ddl, nil
		}
	}

	// 3. Hardcoded built-in table schemas
	if ddl := hardcodedCreateTable(tableID); ddl != "" {
		return ddl, nil
	}

	return "", moerr.NewInternalErrorf(
		ctx,
		"cannot resolve exact schema for table %d from checkpoint metadata; mo_tables/mo_columns data is unavailable or incomplete",
		tableID,
	)
}

// getTableName tries to get the table name for a tableID from the LogicalTableView's
// mo_tables data if available, falling back to the table ID as string.
func getTableName(view *LogicalTableView, tableID uint64) string {
	// view here is the table's own data view, not mo_tables.
	// We can't get the name from it, so use table ID
	return fmt.Sprintf("%d", tableID)
}

// buildCreateTableFromMoColumns reconstructs a CREATE TABLE DDL from mo_columns data.
func buildCreateTableFromMoColumns(view *LogicalTableView, tableID uint64) string {
	tableIDStr := fmt.Sprintf("%d", tableID)
	relnameIDCol := view.columnDataIndex("att_relname_id")
	if relnameIDCol < 0 {
		relnameIDCol = moColsPhysRelNameID
	}
	nameCol := view.columnDataIndex("attname")
	if nameCol < 0 {
		nameCol = moColsPhysAttName
	}
	typCol := view.columnDataIndex("atttyp")
	if typCol < 0 {
		typCol = moColsPhysAttTyp
	}
	numCol := view.columnDataIndex("attnum")
	if numCol < 0 {
		numCol = moColsPhysAttNum
	}
	hiddenCol := view.columnDataIndex("att_is_hidden")
	if hiddenCol < 0 {
		hiddenCol = moColsPhysIsHidden
	}

	type colInfo struct {
		name     string
		sqlType  string
		position int
	}
	var cols []colInfo

	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewMetaCols:]
		if hiddenCol >= 0 && hiddenCol < len(row) {
			if row[hiddenCol] == "1" || row[hiddenCol] == "true" {
				continue
			}
		}
		if relnameIDCol < 0 || relnameIDCol >= len(row) || row[relnameIDCol] != tableIDStr {
			continue
		}
		pos := len(cols)
		if numCol >= 0 && numCol < len(row) {
			if n, err := strconv.Atoi(row[numCol]); err == nil {
				pos = n
			}
		}
		c := colInfo{position: pos}
		if nameCol >= 0 && nameCol < len(row) {
			c.name = row[nameCol]
		}
		if typCol >= 0 && typCol < len(row) {
			c.sqlType = row[typCol]
		}
		cols = append(cols, c)
	}

	if len(cols) == 0 {
		return ""
	}

	sort.Slice(cols, func(i, j int) bool { return cols[i].position < cols[j].position })

	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(fmt.Sprintf("`%d`", tableID))
	sb.WriteString(" (\n")
	for i, c := range cols {
		sb.WriteString("  `")
		if c.name != "" {
			sb.WriteString(c.name)
		} else {
			sb.WriteString(fmt.Sprintf("col_%d", i))
		}
		sb.WriteString("`")
		if c.sqlType != "" {
			sb.WriteString(" ")
			sb.WriteString(c.sqlType)
		}
		if i < len(cols)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString(");")
	return sb.String()
}

// hardcodedCreateTable returns the CREATE TABLE DDL for core built-in system tables.
// These tables' schemas are known at compile time and may not appear in the checkpoint's
// mo_tables/mo_columns (due to minimal deployments).
func hardcodedCreateTable(tableID uint64) string {
	switch tableID {
	case catalog.MO_DATABASE_ID:
		return "CREATE TABLE `mo_database` (\n" +
			"  `dat_id` BIGINT,\n" +
			"  `datname` VARCHAR(5000),\n" +
			"  `dat_catalog_name` VARCHAR(5000),\n" +
			"  `dat_createsql` VARCHAR(5000),\n" +
			"  `owner` INT UNSIGNED,\n" +
			"  `creator` INT UNSIGNED,\n" +
			"  `created_time` TIMESTAMP,\n" +
			"  `account_id` INT UNSIGNED,\n" +
			"  `dat_type` VARCHAR(32),\n" +
			"  `__mo_cpkey_dat` VARCHAR(65535)\n" +
			");"
	case catalog.MO_TABLES_ID:
		return "CREATE TABLE `mo_tables` (\n" +
			"  `rel_id` BIGINT,\n" +
			"  `relname` VARCHAR(5000),\n" +
			"  `reldatabase` VARCHAR(5000),\n" +
			"  `reldatabase_id` BIGINT,\n" +
			"  `relpersistence` VARCHAR(5000),\n" +
			"  `relkind` VARCHAR(5000),\n" +
			"  `rel_comment` VARCHAR(5000),\n" +
			"  `rel_createsql` TEXT,\n" +
			"  `created_time` TIMESTAMP,\n" +
			"  `creator` INT UNSIGNED,\n" +
			"  `owner` INT UNSIGNED,\n" +
			"  `account_id` INT UNSIGNED,\n" +
			"  `partitioned` TINYINT,\n" +
			"  `partition_info` BLOB,\n" +
			"  `viewdef` VARCHAR(5000),\n" +
			"  `constraint` VARCHAR(5000),\n" +
			"  `schema_version` INT UNSIGNED,\n" +
			"  `schema_catalog_version` INT UNSIGNED,\n" +
			"  `extra_info` VARCHAR,\n" +
			"  `__mo_cpkey_rel` VARCHAR(65535),\n" +
			"  `rel_logical_id` BIGINT\n" +
			");"
	case catalog.MO_COLUMNS_ID:
		return "CREATE TABLE `mo_columns` (\n" +
			"  `att_uniq_name` VARCHAR(256),\n" +
			"  `account_id` INT UNSIGNED,\n" +
			"  `att_database_id` BIGINT,\n" +
			"  `att_database` VARCHAR(256),\n" +
			"  `att_relname_id` BIGINT,\n" +
			"  `att_relname` VARCHAR(256),\n" +
			"  `attname` VARCHAR(256),\n" +
			"  `atttyp` VARCHAR(256),\n" +
			"  `attnum` INT,\n" +
			"  `att_length` INT,\n" +
			"  `attnotnull` TINYINT,\n" +
			"  `atthasdef` TINYINT,\n" +
			"  `att_default` VARCHAR(2048),\n" +
			"  `attisdropped` TINYINT,\n" +
			"  `att_constraint_type` CHAR(1),\n" +
			"  `att_is_unsigned` TINYINT,\n" +
			"  `att_is_auto_increment` TINYINT,\n" +
			"  `att_comment` VARCHAR(2048),\n" +
			"  `att_is_hidden` TINYINT,\n" +
			"  `att_has_update` TINYINT,\n" +
			"  `att_update` VARCHAR(2048),\n" +
			"  `att_is_clusterby` TINYINT,\n" +
			"  `att_seqnum` SMALLINT UNSIGNED,\n" +
			"  `att_enum` VARCHAR,\n" +
			"  `__mo_cpkey_col` VARCHAR(65535),\n" +
			"  `attr_has_generated` TINYINT,\n" +
			"  `attr_generated` VARCHAR(2048)\n" +
			");"
	default:
		return ""
	}
}

// columnDataIndex returns the data-column index (0-based, after stripping meta columns)
// for the named column, or -1 if not found.
func (v *LogicalTableView) columnDataIndex(colName string) int {
	for i := logicalViewMetaCols; i < len(v.Headers); i++ {
		if v.Headers[i] == colName {
			return i - logicalViewMetaCols
		}
	}
	return -1
}
