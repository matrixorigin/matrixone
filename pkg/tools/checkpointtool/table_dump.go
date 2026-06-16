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
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/tools/objecttool"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	// Meta columns in LogicalTableView (object, block, row)
	logicalViewMetaCols = 3

	// System table IDs
	moTablesID  = uint64(catalog.MO_TABLES_ID)
	moColumnsID = uint64(catalog.MO_COLUMNS_ID)
)

const (
	csvPipelineQueueCapacity = 2
	csvPipelineReaderMax     = 4
	csvPipelineReportEvery   = 10 * time.Second
	csvPipelineMinFreeMemory = 1 << 30
	csvPipelineFreeRatio     = 10
	csvPipelineMemoryPoll    = 200 * time.Millisecond
	csvPipelineWorkerMemory  = 256 << 20
)

type catalogLayout struct {
	name            string
	moTablesSchema  []string
	moColumnsSchema []string
}

type catalogLayoutMatch struct {
	layout catalogLayout
	offset int
}

var (
	currentCatalogLayout = catalogLayout{
		name:            "current",
		moTablesSchema:  append([]string(nil), catalog.MoTablesSchema...),
		moColumnsSchema: append([]string(nil), catalog.MoColumnsSchema...),
	}
	preCPKLayout = catalogLayout{
		name: "pre-cpk",
		moTablesSchema: catalogSchemaWithout(
			catalog.MoTablesSchema,
			catalog.SystemRelAttr_ExtraInfo,
			catalog.SystemRelAttr_CPKey,
		),
		moColumnsSchema: catalogSchemaWithout(
			catalog.MoColumnsSchema,
			catalog.SystemColAttr_CPKey,
		),
	}
	legacy3CatalogLayout = catalogLayout{
		name:            "3.0-dev",
		moTablesSchema:  append([]string(nil), catalog.MoTablesSchema[:len(catalog.MoTablesSchema)-1]...),
		moColumnsSchema: append([]string(nil), catalog.MoColumnsSchema[:len(catalog.MoColumnsSchema)-2]...),
	}
)

// TableColumn describes one column in a user table schema.
type TableColumn struct {
	Name             string // SQL column name
	SQLType          string // SQL type string (e.g. "BIGINT", "VARCHAR(100)")
	Position         int    // SQL ordinal position
	PhysicalPosition int    // physical/object column position
	Unsigned         bool
	NotNull          bool
	Default          string
	HasDefault       bool
	OnUpdate         string
	Generated        string
	GeneratedStored  bool
	Comment          string
	ConstraintType   string
	AutoIncrement    bool
	ClusterBy        bool
	EnumValues       string
}

type TableUniqueKey struct {
	Name       string
	Columns    []string
	Unique     bool
	Algo       string
	AlgoParams string
	Comment    string
}

// TableSchema holds the decoded schema for one user table.
type TableSchema struct {
	TableName    string
	DatabaseName string
	Columns      []TableColumn // sorted by Position
	CreateSQL    string        // raw CREATE TABLE from mo_tables.rel_createsql
	Comment      string
	UniqueKeys   []TableUniqueKey
	PrimaryKey   []string
	Partition    string
}

type TableCatalogEntry struct {
	TableID      uint64
	AccountID    uint32
	DatabaseID   uint64
	DatabaseName string
	TableName    string
	RelKind      string
}

type TableListOptions struct {
	AccountID    *uint32
	DatabaseID   *uint64
	IncludeViews bool
}

type CSVRowOrder string

const (
	CSVRowOrderStorage CSVRowOrder = "storage"
	CSVRowOrderLexical CSVRowOrder = "lexical"
)

type CSVExportOptions struct {
	IncludeMetadata bool
	IncludeHeader   bool
	RowOrder        CSVRowOrder
}

type exportedCSVRow struct {
	values []string
	nulls  []bool
}

// TableDumpData contains the checkpoint metadata needed to dump one table.
type TableDumpData struct {
	TableID     uint64
	Schema      *TableSchema
	DataEntries []*ObjectEntryInfo
	TombEntries []*ObjectEntryInfo
}

var moIndexesHeaders = []string{
	"id",
	"table_id",
	"database_id",
	"name",
	"type",
	catalog.IndexAlgoName,
	catalog.IndexAlgoTableType,
	catalog.IndexAlgoParams,
	"is_visible",
	"hidden",
	"comment",
	"column_name",
	"ordinal_position",
	"options",
	"index_table_name",
}

var moPartitionMetadataHeaders = []string{
	"table_id",
	"table_name",
	"database_name",
	"partition_method",
	"partition_description",
	"partition_count",
}

var moPartitionTablesHeaders = []string{
	"partition_id",
	"partition_table_name",
	"primary_table_id",
	"partition_name",
	"partition_ordinal_position",
	"partition_expression_str",
	"partition_expression",
}

type indexDDLColumn struct {
	name    string
	ordinal int
}

type indexDDLInfo struct {
	name       string
	indexType  string
	algo       string
	algoParams string
	comment    string
	columns    map[string]indexDDLColumn
}

type CSVExportOption func(*CSVExportOptions)

func defaultCSVExportOptions() CSVExportOptions {
	return CSVExportOptions{
		IncludeMetadata: true,
		IncludeHeader:   true,
		RowOrder:        CSVRowOrderStorage,
	}
}

func WithCSVMetaComments(include bool) CSVExportOption {
	return func(opts *CSVExportOptions) {
		opts.IncludeMetadata = include
	}
}

func WithCSVHeader(include bool) CSVExportOption {
	return func(opts *CSVExportOptions) {
		opts.IncludeHeader = include
	}
}

func WithCSVRowOrder(order CSVRowOrder) CSVExportOption {
	return func(opts *CSVExportOptions) {
		opts.RowOrder = order
	}
}

func ParseCSVRowOrder(s string) (CSVRowOrder, error) {
	switch CSVRowOrder(strings.ToLower(strings.TrimSpace(s))) {
	case "", CSVRowOrderStorage:
		return CSVRowOrderStorage, nil
	case CSVRowOrderLexical:
		return CSVRowOrderLexical, nil
	default:
		return "", fmt.Errorf("unsupported row order %q (supported: %s, %s)", s, CSVRowOrderStorage, CSVRowOrderLexical)
	}
}

func resolveCSVExportOptions(opts []CSVExportOption) CSVExportOptions {
	resolved := defaultCSVExportOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}

type builtinColumnDef struct {
	Name     string
	SQLType  string
	Position int
	Hidden   bool
}

func knownCatalogLayouts() []catalogLayout {
	return []catalogLayout{preCPKLayout, currentCatalogLayout, legacy3CatalogLayout}
}

func schemaForLayout(layout catalogLayout, tableID uint64) []string {
	switch tableID {
	case moTablesID:
		return layout.moTablesSchema
	case moColumnsID:
		return layout.moColumnsSchema
	default:
		return nil
	}
}

func catalogSchemaWithout(schema []string, names ...string) []string {
	excluded := make(map[string]struct{}, len(names))
	for _, name := range names {
		excluded[name] = struct{}{}
	}
	filtered := make([]string, 0, len(schema))
	for _, name := range schema {
		if _, ok := excluded[name]; ok {
			continue
		}
		filtered = append(filtered, name)
	}
	return filtered
}

func builtinColumnsForLayout(layout catalogLayout, tableID uint64) []builtinColumnDef {
	switch tableID {
	case catalog.MO_DATABASE_ID:
		return []builtinColumnDef{
			{Name: "dat_id", SQLType: "BIGINT", Position: 0},
			{Name: "datname", SQLType: "VARCHAR(5000)", Position: 1},
			{Name: "dat_catalog_name", SQLType: "VARCHAR(5000)", Position: 2},
			{Name: "dat_createsql", SQLType: "VARCHAR(5000)", Position: 3},
			{Name: "owner", SQLType: "INT UNSIGNED", Position: 4},
			{Name: "creator", SQLType: "INT UNSIGNED", Position: 5},
			{Name: "created_time", SQLType: "TIMESTAMP", Position: 6},
			{Name: "account_id", SQLType: "INT UNSIGNED", Position: 7},
			{Name: "dat_type", SQLType: "VARCHAR(32)", Position: 8},
			{Name: catalog.SystemDBAttr_CPKey, SQLType: "VARCHAR(65535)", Position: 9, Hidden: true},
		}
	case catalog.MO_TABLES_ID:
		cols := []builtinColumnDef{
			{Name: "rel_id", SQLType: "BIGINT", Position: 0},
			{Name: "relname", SQLType: "VARCHAR(5000)", Position: 1},
			{Name: "reldatabase", SQLType: "VARCHAR(5000)", Position: 2},
			{Name: "reldatabase_id", SQLType: "BIGINT", Position: 3},
			{Name: "relpersistence", SQLType: "VARCHAR(5000)", Position: 4},
			{Name: "relkind", SQLType: "VARCHAR(5000)", Position: 5},
			{Name: "rel_comment", SQLType: "VARCHAR(5000)", Position: 6},
			{Name: "rel_createsql", SQLType: "TEXT", Position: 7},
			{Name: "created_time", SQLType: "TIMESTAMP", Position: 8},
			{Name: "creator", SQLType: "INT UNSIGNED", Position: 9},
			{Name: "owner", SQLType: "INT UNSIGNED", Position: 10},
			{Name: "account_id", SQLType: "INT UNSIGNED", Position: 11},
			{Name: "partitioned", SQLType: "TINYINT", Position: 12},
			{Name: "partition_info", SQLType: "BLOB", Position: 13},
			{Name: "viewdef", SQLType: "VARCHAR(5000)", Position: 14},
			{Name: "constraint", SQLType: "VARCHAR(5000)", Position: 15},
			{Name: "schema_version", SQLType: "INT UNSIGNED", Position: 16},
			{Name: "schema_catalog_version", SQLType: "INT UNSIGNED", Position: 17},
			{Name: "extra_info", SQLType: "VARCHAR", Position: 18, Hidden: true},
			{Name: catalog.SystemRelAttr_CPKey, SQLType: "VARCHAR(65535)", Position: 19, Hidden: true},
		}
		if layout.name != legacy3CatalogLayout.name {
			cols = append(cols, builtinColumnDef{Name: "rel_logical_id", SQLType: "BIGINT", Position: 20})
		}
		return cols
	case catalog.MO_COLUMNS_ID:
		cols := []builtinColumnDef{
			{Name: "att_uniq_name", SQLType: "VARCHAR(256)", Position: 0},
			{Name: "account_id", SQLType: "INT UNSIGNED", Position: 1},
			{Name: "att_database_id", SQLType: "BIGINT", Position: 2},
			{Name: "att_database", SQLType: "VARCHAR(256)", Position: 3},
			{Name: "att_relname_id", SQLType: "BIGINT", Position: 4},
			{Name: "att_relname", SQLType: "VARCHAR(256)", Position: 5},
			{Name: "attname", SQLType: "VARCHAR(256)", Position: 6},
			{Name: "atttyp", SQLType: "VARCHAR(256)", Position: 7},
			{Name: "attnum", SQLType: "INT", Position: 8},
			{Name: "att_length", SQLType: "INT", Position: 9},
			{Name: "attnotnull", SQLType: "TINYINT", Position: 10},
			{Name: "atthasdef", SQLType: "TINYINT", Position: 11},
			{Name: "att_default", SQLType: "VARCHAR(2048)", Position: 12},
			{Name: "attisdropped", SQLType: "TINYINT", Position: 13},
			{Name: "att_constraint_type", SQLType: "CHAR(1)", Position: 14},
			{Name: "att_is_unsigned", SQLType: "TINYINT", Position: 15},
			{Name: "att_is_auto_increment", SQLType: "TINYINT", Position: 16},
			{Name: "att_comment", SQLType: "VARCHAR(2048)", Position: 17},
			{Name: "att_is_hidden", SQLType: "TINYINT", Position: 18},
			{Name: catalog.SystemColAttr_HasUpdate, SQLType: "TINYINT", Position: 19},
			{Name: catalog.SystemColAttr_Update, SQLType: "VARCHAR(2048)", Position: 20},
			{Name: catalog.SystemColAttr_IsClusterBy, SQLType: "TINYINT", Position: 21},
			{Name: catalog.SystemColAttr_Seqnum, SQLType: "SMALLINT UNSIGNED", Position: 22},
			{Name: catalog.SystemColAttr_EnumValues, SQLType: "VARCHAR", Position: 23},
			{Name: catalog.SystemColAttr_CPKey, SQLType: "VARCHAR(65535)", Position: 24, Hidden: true},
		}
		if layout.name != legacy3CatalogLayout.name {
			cols = append(cols,
				builtinColumnDef{Name: "attr_has_generated", SQLType: "TINYINT", Position: 25},
				builtinColumnDef{Name: "attr_generated", SQLType: "VARCHAR(2048)", Position: 26},
			)
		}
		return cols
	default:
		return nil
	}
}

func builtinTableSchemaForLayout(layout catalogLayout, tableID uint64) *TableSchema {
	cols := builtinColumnsForLayout(layout, tableID)
	if len(cols) == 0 {
		return nil
	}

	schema := &TableSchema{
		DatabaseName: "mo_catalog",
	}
	switch tableID {
	case catalog.MO_DATABASE_ID:
		schema.TableName = "mo_database"
	case catalog.MO_TABLES_ID:
		schema.TableName = "mo_tables"
	case catalog.MO_COLUMNS_ID:
		schema.TableName = "mo_columns"
	}
	for _, col := range cols {
		if col.Hidden {
			continue
		}
		schema.Columns = append(schema.Columns, TableColumn{
			Name:             col.Name,
			SQLType:          col.SQLType,
			Position:         col.Position,
			PhysicalPosition: col.Position,
		})
	}
	schema.CreateSQL = renderCreateTableDDL(schema.TableName, schema.Columns)
	return schema
}

func renderCreateTableDDL(tableName string, cols []TableColumn) string {
	return renderCreateTableDDLWithComment(tableName, cols, "")
}

func renderCreateTableDDLWithComment(tableName string, cols []TableColumn, comment string, uniqueKeys ...TableUniqueKey) string {
	return renderCreateTableDDLFull(tableName, cols, comment, "", nil, uniqueKeys...)
}

func renderCreateTableDDLFull(tableName string, cols []TableColumn, comment string, partition string, primaryKey []string, uniqueKeys ...TableUniqueKey) string {
	if tableName == "" || len(cols) == 0 {
		return ""
	}

	var sb strings.Builder
	primaryCols := primaryKeyColumns(cols, primaryKey)
	uniqueKeys = normalizedUniqueKeys(uniqueKeys)
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(quoteDDLIdent(tableName))
	sb.WriteString(" (\n")
	for i, col := range cols {
		sb.WriteString("  ")
		sb.WriteString(quoteDDLIdent(col.Name))
		if col.SQLType != "" {
			sb.WriteString(" ")
			sb.WriteString(col.SQLType)
			if col.Unsigned && !strings.Contains(strings.ToUpper(col.SQLType), "UNSIGNED") {
				sb.WriteString(" UNSIGNED")
			}
		}
		appendColumnDDLAttributes(&sb, col)
		if i < len(cols)-1 || len(primaryCols) > 0 || len(uniqueKeys) > 0 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	if len(primaryCols) > 0 {
		sb.WriteString("  PRIMARY KEY (")
		for i, col := range primaryCols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteDDLIdent(col.Name))
		}
		if len(uniqueKeys) > 0 {
			sb.WriteString("),\n")
		} else {
			sb.WriteString(")\n")
		}
	}
	for i, key := range uniqueKeys {
		if len(key.Columns) == 0 {
			continue
		}
		sb.WriteString("  ")
		if key.Unique {
			sb.WriteString("UNIQUE ")
		}
		sb.WriteString("KEY ")
		sb.WriteString(quoteDDLIdent(key.Name))
		appendIndexAlgorithmDDL(&sb, key.Algo)
		sb.WriteString("(")
		for i, col := range key.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteDDLIdent(col))
		}
		sb.WriteString(")")
		if err := appendIndexTrailingOptionsDDL(&sb, key.AlgoParams, key.Comment); err != nil {
			ckpDebugSchemaf("mo_tables constraint index options skipped index=%s algo=%q params=%q err=%v", key.Name, key.Algo, key.AlgoParams, err)
		}
		if i < len(uniqueKeys)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString(")")
	if comment != "" {
		sb.WriteString(" COMMENT=")
		sb.WriteString(quoteDDLString(comment))
	}
	if partition != "" {
		sb.WriteString(" ")
		sb.WriteString(partition)
	}
	if clusterCols := clusterByColumns(cols); len(clusterCols) > 0 {
		sb.WriteString(" CLUSTER BY (")
		for i, col := range clusterCols {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteDDLIdent(col.Name))
		}
		sb.WriteString(")")
	}
	sb.WriteString(";")
	return sb.String()
}

func appendColumnDDLAttributes(sb *strings.Builder, col TableColumn) {
	if col.Generated != "" {
		if col.NotNull {
			sb.WriteString(" NOT NULL")
		}
		sb.WriteString(" GENERATED ALWAYS AS (")
		sb.WriteString(col.Generated)
		sb.WriteString(")")
		if col.GeneratedStored {
			sb.WriteString(" STORED")
		} else {
			sb.WriteString(" VIRTUAL")
		}
	} else {
		if col.NotNull || col.AutoIncrement {
			sb.WriteString(" NOT NULL")
		}
		if col.AutoIncrement {
			sb.WriteString(" AUTO_INCREMENT")
		}
		if col.HasDefault {
			if strings.TrimSpace(col.Default) == "" {
				sb.WriteString(" DEFAULT NULL")
			} else {
				sb.WriteString(" DEFAULT ")
				sb.WriteString(formatDDLDefault(col.Default))
			}
		}
		if col.OnUpdate != "" {
			sb.WriteString(" ON UPDATE ")
			sb.WriteString(formatDDLDefault(col.OnUpdate))
		}
	}
	if col.Comment != "" {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteDDLString(col.Comment))
	}
}

func primaryKeyColumns(cols []TableColumn, primaryKey []string) []TableColumn {
	if len(primaryKey) == 0 {
		return nil
	}
	byName := make(map[string]TableColumn, len(cols))
	for _, col := range cols {
		byName[strings.ToLower(col.Name)] = col
	}
	primary := make([]TableColumn, 0, 1)
	for i, name := range primaryKey {
		col, ok := byName[strings.ToLower(name)]
		if !ok {
			col = TableColumn{Name: name, Position: i + 1}
		}
		primary = append(primary, col)
	}
	return primary
}

func clusterByColumns(cols []TableColumn) []TableColumn {
	clusterBy := make([]TableColumn, 0, 1)
	for _, col := range cols {
		if col.ClusterBy {
			clusterBy = append(clusterBy, col)
		}
	}
	sort.Slice(clusterBy, func(i, j int) bool {
		return clusterBy[i].Position < clusterBy[j].Position
	})
	return clusterBy
}

func normalizedUniqueKeys(keys []TableUniqueKey) []TableUniqueKey {
	seen := make(map[string]struct{}, len(keys))
	out := make([]TableUniqueKey, 0, len(keys))
	for _, key := range keys {
		cols := make([]string, 0, len(key.Columns))
		for _, col := range key.Columns {
			col = strings.TrimSpace(col)
			if col == "" {
				continue
			}
			cols = append(cols, col)
		}
		if len(cols) == 0 {
			continue
		}
		name := strings.TrimSpace(key.Name)
		if name == "" {
			name = cols[0]
		}
		signature := strconv.FormatBool(key.Unique) + "\x00" + strings.ToLower(name) + "\x00" + strings.ToLower(strings.Join(cols, "\x00"))
		if _, ok := seen[signature]; ok {
			continue
		}
		seen[signature] = struct{}{}
		out = append(out, TableUniqueKey{
			Name:       name,
			Columns:    cols,
			Unique:     key.Unique,
			Algo:       strings.TrimSpace(key.Algo),
			AlgoParams: strings.TrimSpace(key.AlgoParams),
			Comment:    strings.TrimSpace(key.Comment),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return strings.Join(out[i].Columns, "\x00") < strings.Join(out[j].Columns, "\x00")
	})
	return out
}

func formatDDLDefault(defaultExpr string) string {
	defaultExpr = strings.TrimSpace(defaultExpr)
	if defaultExpr == "" {
		return "NULL"
	}
	if len(defaultExpr) >= 2 && defaultExpr[0] == '\'' && defaultExpr[len(defaultExpr)-1] == '\'' {
		return "'" + strings.ReplaceAll(defaultExpr[1:len(defaultExpr)-1], "'", "''") + "'"
	}
	return defaultExpr
}

// RenderCreateTableDDLFromSchema renders a CREATE TABLE statement from resolved
// checkpoint column metadata.
func RenderCreateTableDDLFromSchema(schema *TableSchema) string {
	if schema == nil {
		return ""
	}
	if schema.TableName == "" || len(schema.Columns) == 0 {
		return ""
	}
	if schema.Comment != "" && !isPrintableSQLText(schema.Comment) {
		return ""
	}
	for _, col := range schema.Columns {
		if col.Name == "" || !isPrintableSQLType(col.SQLType) {
			return ""
		}
		if col.Default != "" && !isPrintableDDLExpression(col.Default) {
			return ""
		}
		if col.OnUpdate != "" && !isPrintableDDLExpression(col.OnUpdate) {
			return ""
		}
		if col.Generated != "" && !isPrintableDDLExpression(col.Generated) {
			return ""
		}
		if col.Comment != "" && !isPrintableSQLText(col.Comment) {
			return ""
		}
	}
	return renderCreateTableDDLFull(schema.TableName, schema.Columns, schema.Comment, schema.Partition, schema.PrimaryKey, schema.UniqueKeys...)
}

func inferBuiltinCatalogLayout(
	tableID uint64,
	moTablesView *LogicalTableView,
	moColumnsView *LogicalTableView,
) catalogLayout {
	switch tableID {
	case moTablesID:
		if moTablesView != nil {
			if layout, _, ok := inferCatalogLayout(len(moTablesView.Headers)-logicalViewMetaCols, moTablesID); ok {
				return layout
			}
		}
	case moColumnsID:
		if moColumnsView != nil {
			if layout, _, ok := inferCatalogLayout(len(moColumnsView.Headers)-logicalViewMetaCols, moColumnsID); ok {
				return layout
			}
		}
	case catalog.MO_DATABASE_ID:
		if moTablesView != nil {
			if layout, _, ok := inferCatalogLayout(len(moTablesView.Headers)-logicalViewMetaCols, moTablesID); ok {
				return layout
			}
		}
		if moColumnsView != nil {
			if layout, _, ok := inferCatalogLayout(len(moColumnsView.Headers)-logicalViewMetaCols, moColumnsID); ok {
				return layout
			}
		}
	}
	return currentCatalogLayout
}

func mergeBuiltinSchemaFallback(schema *TableSchema, builtin *TableSchema, tableID uint64) *TableSchema {
	if builtin == nil {
		return schema
	}
	if schema == nil {
		schema = &TableSchema{TableName: fmt.Sprintf("%d", tableID)}
	}
	if schema.TableName == "" || schema.TableName == fmt.Sprintf("%d", tableID) {
		schema.TableName = builtin.TableName
	}
	if schema.DatabaseName == "" {
		schema.DatabaseName = builtin.DatabaseName
	}
	if schema.CreateSQL == "" {
		schema.CreateSQL = builtin.CreateSQL
	}
	if len(schema.Columns) == 0 {
		schema.Columns = builtin.Columns
	}
	return schema
}

func inferCatalogLayout(dataWidth int, tableID uint64) (catalogLayout, int, bool) {
	for _, offset := range []int{0, 1, 2} {
		for _, layout := range knownCatalogLayouts() {
			schema := schemaForLayout(layout, tableID)
			if len(schema) == 0 {
				continue
			}
			if dataWidth == len(schema)+offset {
				return layout, offset, true
			}
		}
	}
	return catalogLayout{}, 0, false
}

func catalogLayoutMatches(dataWidth int, tableID uint64) []catalogLayoutMatch {
	var matches []catalogLayoutMatch
	for _, layout := range knownCatalogLayouts() {
		schema := schemaForLayout(layout, tableID)
		if len(schema) == 0 {
			continue
		}
		for _, offset := range []int{0, 1, 2} {
			if dataWidth >= len(schema)+offset {
				matches = append(matches, catalogLayoutMatch{
					layout: layout,
					offset: offset,
				})
			}
		}
	}
	return matches
}

func fallbackCatalogColIndex(view *LogicalTableView, tableID uint64, colName string) int {
	if idx := view.columnDataIndex(colName); idx >= 0 {
		return idx
	}
	dataOffset := logicalViewDataOffset(view)
	layout, offset, ok := inferCatalogLayout(len(view.Headers)-dataOffset, tableID)
	if !ok {
		return -1
	}
	for i, name := range schemaForLayout(layout, tableID) {
		if name == colName {
			return i + offset
		}
	}
	return -1
}

func catalogColIndexForLayout(layout catalogLayout, tableID uint64, colName string, offset int) int {
	for i, name := range schemaForLayout(layout, tableID) {
		if name == colName {
			return i + offset
		}
	}
	return -1
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
		relIDCol := fallbackCatalogColIndex(moTablesView, moTablesID, "rel_id")
		for _, row := range moTablesView.Rows {
			dataRow := row[logicalViewDataOffset(moTablesView):]
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

	if len(schema.Columns) == 0 {
		layout := inferBuiltinCatalogLayout(tableID, moTablesView, moColumnsView)
		schema = mergeBuiltinSchemaFallback(schema, builtinTableSchemaForLayout(layout, tableID), tableID)
	}
	if schema.Partition == "" {
		if partitionClause := r.readPartitionClause(ctx, tableID, snapshotTS); partitionClause != "" {
			schema.Partition = partitionClause
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
	allData, allTomb, err := r.getTableEntriesAt(ctx, tableID, snapshotTS)
	if err != nil {
		return nil, err
	}
	view, err := r.BuildLogicalTableView(ctx, snapshotTS, allData, allTomb)
	if err != nil {
		return nil, err
	}
	applyCatalogColumnHeaders(view, tableID)
	return view, nil
}

func applyCatalogColumnHeaders(view *LogicalTableView, tableID uint64) {
	if view == nil || len(view.ColSeqNums) == 0 {
		return
	}
	schema := schemaForLayout(currentCatalogLayout, tableID)
	if len(schema) == 0 {
		return
	}
	offset := logicalViewDataOffset(view)
	for dataIdx, seqNum := range view.ColSeqNums {
		headerIdx := offset + dataIdx
		if headerIdx >= len(view.Headers) || int(seqNum) >= len(schema) {
			continue
		}
		view.Headers[headerIdx] = schema[seqNum]
	}
}

func (r *CheckpointReader) getTableEntriesAt(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) ([]*ObjectEntryInfo, []*ObjectEntryInfo, error) {
	composed, err := r.ComposeAt(snapshotTS)
	if err != nil {
		return nil, nil, err
	}

	tbl, ok := composed.Tables[tableID]
	if !ok || (len(tbl.DataRanges) == 0 && len(tbl.TombRanges) == 0) {
		return nil, nil, moerr.NewInternalErrorf(ctx, "table %d not found in checkpoint at ts %s", tableID, snapshotTS.ToString())
	}

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

	var allData, allTomb []*ObjectEntryInfo
	for _, ref := range entryRefs {
		e := r.entries[ref.entry.Index]
		dataEntries, tombEntries, err := r.GetObjectEntries(e, tableID)
		if err != nil {
			continue
		}
		allData = append(allData, dataEntries...)
		allTomb = append(allTomb, tombEntries...)
	}

	if len(allData) == 0 {
		return nil, nil, moerr.NewInternalErrorf(ctx, "no data entries for table %d", tableID)
	}
	return allData, allTomb, nil
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
	opts ...CSVExportOption,
) error {
	schema := r.ReadTableSchema(ctx, tableID, snapshotTS, nil)
	if len(schema.Columns) == 0 {
		return moerr.NewInternalErrorf(
			ctx,
			"cannot resolve visible columns for table %d from checkpoint metadata; mo_columns data is unavailable or incomplete",
			tableID,
		)
	}
	return r.streamTableCSV(ctx, w, schema, snapshotTS, dataEntries, tombEntries, resolveCSVExportOptions(opts))
}

// DumpTableCSVComposed dumps a table to CSV by composing the full checkpoint view
// at snapshotTS (aggregating across GCKP + all ICKPs). This is equivalent to calling
// DumpTableCSV with entries from ComposeAt.
func (r *CheckpointReader) DumpTableCSVComposed(
	ctx context.Context,
	w io.Writer,
	tableID uint64,
	snapshotTS types.TS,
	opts ...CSVExportOption,
) error {
	data, err := r.PrepareTableDumpData(ctx, tableID, snapshotTS)
	if err != nil {
		return err
	}
	return r.DumpPreparedTableCSV(ctx, w, data, snapshotTS, opts...)
}

// PrepareTableDumpData resolves the table schema and object lists once so batch
// dumps can avoid repeatedly composing checkpoint metadata per table worker.
func (r *CheckpointReader) PrepareTableDumpData(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) (*TableDumpData, error) {
	dataEntries, tombEntries, err := r.getTableEntriesAt(ctx, tableID, snapshotTS)
	if err != nil {
		if !isTableDataUnavailable(err) {
			return nil, err
		}
		dataEntries = nil
		tombEntries = nil
	}
	schema := r.ReadTableSchema(ctx, tableID, snapshotTS, nil)
	if len(schema.Columns) == 0 {
		return nil, moerr.NewInternalErrorf(
			ctx,
			"cannot resolve visible columns for table %d from checkpoint metadata; mo_columns data is unavailable or incomplete",
			tableID,
		)
	}
	partitionTableIDs, err := r.readPartitionTableIDs(ctx, snapshotTS, tableID)
	if err != nil {
		return nil, err
	}
	for _, partitionTableID := range partitionTableIDs {
		partitionData, partitionTomb, err := r.getTableEntriesAt(ctx, partitionTableID, snapshotTS)
		if err != nil {
			if !isTableDataUnavailable(err) {
				return nil, err
			}
			ckpDebugSchemaf("partition table data unavailable primary=%d partition=%d err=%v", tableID, partitionTableID, err)
			continue
		}
		dataEntries = append(dataEntries, partitionData...)
		tombEntries = append(tombEntries, partitionTomb...)
	}
	if len(partitionTableIDs) > 0 {
		ckpDebugSchemaf("partition table dump data resolved primary=%d partitions=%v data_entries=%d tomb_entries=%d", tableID, partitionTableIDs, len(dataEntries), len(tombEntries))
	}
	return &TableDumpData{
		TableID:     tableID,
		Schema:      cloneTableSchema(schema),
		DataEntries: dataEntries,
		TombEntries: tombEntries,
	}, nil
}

// PrepareTableDumpDataForTables resolves schemas and object lists for multiple
// tables by composing the checkpoint once and scanning each selected checkpoint
// entry once.
func (r *CheckpointReader) PrepareTableDumpDataForTables(
	ctx context.Context,
	tableIDs []uint64,
	snapshotTS types.TS,
) (map[uint64]*TableDumpData, error) {
	composed, err := r.ComposeAt(snapshotTS)
	if err != nil {
		return nil, err
	}

	tableSet := make(map[uint64]struct{}, len(tableIDs))
	result := make(map[uint64]*TableDumpData, len(tableIDs))
	partitionToPrimary := make(map[uint64]uint64)
	partitionIDsByPrimary := make(map[uint64][]uint64)
	for _, tableID := range tableIDs {
		if _, ok := tableSet[tableID]; ok {
			continue
		}
		schema := r.ReadTableSchema(ctx, tableID, snapshotTS, nil)
		if len(schema.Columns) == 0 {
			return nil, moerr.NewInternalErrorf(
				ctx,
				"cannot resolve visible columns for table %d from checkpoint metadata; mo_columns data is unavailable or incomplete",
				tableID,
			)
		}
		tableSet[tableID] = struct{}{}
		result[tableID] = &TableDumpData{
			TableID: tableID,
			Schema:  cloneTableSchema(schema),
		}
	}
	partitionTableMap, err := r.readPartitionTableIDsForPrimaries(ctx, snapshotTS, tableSet)
	if err != nil {
		return nil, err
	}
	for primaryID, partitionIDs := range partitionTableMap {
		for _, partitionID := range partitionIDs {
			tableSet[partitionID] = struct{}{}
			partitionToPrimary[partitionID] = primaryID
		}
		partitionIDsByPrimary[primaryID] = append(partitionIDsByPrimary[primaryID], partitionIDs...)
	}

	entryRefs := make([]*EntryInfo, 0, len(composed.Incrementals)+1)
	if composed.BaseEntry != nil {
		entryRefs = append(entryRefs, composed.BaseEntry)
	}
	entryRefs = append(entryRefs, composed.Incrementals...)

	for _, ref := range entryRefs {
		e := r.entries[ref.Index]
		dataByTable, tombByTable, err := r.GetObjectEntriesForTables(e, tableSet)
		if err != nil {
			if isDataFileNotFound(err) {
				continue
			}
			return nil, err
		}
		for tableID, entries := range dataByTable {
			targetID := tableID
			if primaryID, ok := partitionToPrimary[tableID]; ok {
				targetID = primaryID
			}
			if result[targetID] == nil {
				continue
			}
			result[targetID].DataEntries = append(result[targetID].DataEntries, entries...)
		}
		for tableID, entries := range tombByTable {
			targetID := tableID
			if primaryID, ok := partitionToPrimary[tableID]; ok {
				targetID = primaryID
			}
			if result[targetID] == nil {
				continue
			}
			result[targetID].TombEntries = append(result[targetID].TombEntries, entries...)
		}
	}

	for tableID, data := range result {
		if data.Schema == nil || len(data.Schema.Columns) == 0 {
			return nil, moerr.NewInternalErrorf(ctx, "cannot resolve visible columns for table %d from checkpoint metadata", tableID)
		}
		if partitionIDs := partitionIDsByPrimary[tableID]; len(partitionIDs) > 0 {
			ckpDebugSchemaf("partition table dump data resolved primary=%d partitions=%v data_entries=%d tomb_entries=%d", tableID, partitionIDs, len(data.DataEntries), len(data.TombEntries))
		}
	}
	return result, nil
}

func isTableDataUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "not found in checkpoint") ||
		strings.Contains(msg, "no data entries for table")
}

// DumpPreparedTableCSV writes a table using metadata previously resolved by
// PrepareTableDumpData.
func (r *CheckpointReader) DumpPreparedTableCSV(
	ctx context.Context,
	w io.Writer,
	data *TableDumpData,
	snapshotTS types.TS,
	opts ...CSVExportOption,
) error {
	if data == nil {
		return moerr.NewInternalError(ctx, "missing prepared table dump data")
	}
	if data.Schema == nil || len(data.Schema.Columns) == 0 {
		return moerr.NewInternalErrorf(ctx, "cannot resolve visible columns for table %d from checkpoint metadata", data.TableID)
	}
	return r.streamTableCSV(ctx, w, data.Schema, snapshotTS, data.DataEntries, data.TombEntries, resolveCSVExportOptions(opts))
}

func cloneTableSchema(schema *TableSchema) *TableSchema {
	if schema == nil {
		return nil
	}
	clone := &TableSchema{
		TableName:    strings.Clone(schema.TableName),
		DatabaseName: strings.Clone(schema.DatabaseName),
		CreateSQL:    strings.Clone(schema.CreateSQL),
		Comment:      strings.Clone(schema.Comment),
		Partition:    strings.Clone(schema.Partition),
	}
	if len(schema.PrimaryKey) > 0 {
		clone.PrimaryKey = make([]string, len(schema.PrimaryKey))
		for i, col := range schema.PrimaryKey {
			clone.PrimaryKey[i] = strings.Clone(col)
		}
	}
	if len(schema.Columns) > 0 {
		clone.Columns = make([]TableColumn, len(schema.Columns))
		for i, col := range schema.Columns {
			clone.Columns[i] = TableColumn{
				Name:             strings.Clone(col.Name),
				SQLType:          strings.Clone(col.SQLType),
				Position:         col.Position,
				PhysicalPosition: col.PhysicalPosition,
				Unsigned:         col.Unsigned,
				NotNull:          col.NotNull,
				Default:          strings.Clone(col.Default),
				HasDefault:       col.HasDefault,
				OnUpdate:         strings.Clone(col.OnUpdate),
				Generated:        strings.Clone(col.Generated),
				GeneratedStored:  col.GeneratedStored,
				Comment:          strings.Clone(col.Comment),
				ConstraintType:   strings.Clone(col.ConstraintType),
				AutoIncrement:    col.AutoIncrement,
				ClusterBy:        col.ClusterBy,
				EnumValues:       strings.Clone(col.EnumValues),
			}
		}
	}
	if len(schema.UniqueKeys) > 0 {
		clone.UniqueKeys = make([]TableUniqueKey, len(schema.UniqueKeys))
		for i, key := range schema.UniqueKeys {
			clone.UniqueKeys[i].Name = strings.Clone(key.Name)
			clone.UniqueKeys[i].Unique = key.Unique
			clone.UniqueKeys[i].Algo = strings.Clone(key.Algo)
			clone.UniqueKeys[i].AlgoParams = strings.Clone(key.AlgoParams)
			clone.UniqueKeys[i].Comment = strings.Clone(key.Comment)
			if len(key.Columns) > 0 {
				clone.UniqueKeys[i].Columns = make([]string, len(key.Columns))
				for j, col := range key.Columns {
					clone.UniqueKeys[i].Columns[j] = strings.Clone(col)
				}
			}
		}
	}
	return clone
}

func (r *CheckpointReader) ListCatalogTables(
	ctx context.Context,
	snapshotTS types.TS,
	opts TableListOptions,
) ([]TableCatalogEntry, error) {
	tables, projectedErr := r.listCatalogTablesFromProjectedMoTables(ctx, snapshotTS)
	moTablesView, err := r.getTableLogicalView(ctx, moTablesID, snapshotTS)
	if err != nil && projectedErr != nil {
		return nil, err
	}
	if projectedErr != nil || len(tables) == 0 {
		tables = nil
	}
	if moTablesView != nil {
		tables = mergeCatalogTableEntries(tables, buildCatalogTablesFromMoTablesRows(moTablesView))
		if schema := r.ReadTableSchema(ctx, moTablesID, snapshotTS, moTablesView); len(schema.Columns) > 0 {
			projected := MergeLogicalViewWithSchema(moTablesView, schema)
			tables = mergeCatalogTableEntries(tables, buildCatalogTablesFromMoTablesRows(projected))
		}
	}
	filtered := make([]TableCatalogEntry, 0, len(tables))
	for _, table := range tables {
		if opts.AccountID != nil && table.AccountID != *opts.AccountID {
			continue
		}
		if opts.DatabaseID != nil && table.DatabaseID != *opts.DatabaseID {
			continue
		}
		if !opts.IncludeViews && table.RelKind != "" && table.RelKind != "r" {
			continue
		}
		filtered = append(filtered, table)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].AccountID != filtered[j].AccountID {
			return filtered[i].AccountID < filtered[j].AccountID
		}
		if filtered[i].DatabaseName != filtered[j].DatabaseName {
			return filtered[i].DatabaseName < filtered[j].DatabaseName
		}
		if filtered[i].TableName != filtered[j].TableName {
			return filtered[i].TableName < filtered[j].TableName
		}
		return filtered[i].TableID < filtered[j].TableID
	})
	return filtered, nil
}

func (r *CheckpointReader) listCatalogTablesFromProjectedMoTables(
	ctx context.Context,
	snapshotTS types.TS,
) ([]TableCatalogEntry, error) {
	var buf bytes.Buffer
	if err := r.DumpTableCSVComposed(ctx, &buf, moTablesID, snapshotTS, WithCSVHeader(true), WithCSVMetaComments(false)); err != nil {
		return nil, err
	}
	reader := csv.NewReader(bytes.NewReader(buf.Bytes()))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	return buildCatalogTablesFromCSVRecords(records[0], records[1:]), nil
}

func buildCatalogTablesFromCSVRecords(header []string, rows [][]string) []TableCatalogEntry {
	index := make(map[string]int, len(header))
	for i, name := range header {
		index[name] = i
	}
	col := func(name string) int {
		if idx, ok := index[name]; ok {
			return idx
		}
		return -1
	}
	return buildCatalogTablesFromMoTablesRowsAt(
		&LogicalTableView{
			Headers: header,
			Rows:    rows,
		},
		col("rel_id"),
		col("relname"),
		col("reldatabase"),
		col("reldatabase_id"),
		col("relkind"),
		col("account_id"),
	)
}

func mergeCatalogTableEntries(base []TableCatalogEntry, extra []TableCatalogEntry) []TableCatalogEntry {
	if len(extra) == 0 {
		return base
	}
	seen := make(map[uint64]int, len(base)+len(extra))
	for i, table := range base {
		seen[table.TableID] = i
	}
	for _, table := range extra {
		if idx, ok := seen[table.TableID]; ok {
			if betterCatalogTableEntry(table, base[idx]) {
				base[idx] = table
			}
			continue
		}
		seen[table.TableID] = len(base)
		base = append(base, table)
	}
	return base
}

func betterCatalogTableEntry(candidate, current TableCatalogEntry) bool {
	if !validCatalogName(current.TableName) && validCatalogName(candidate.TableName) {
		return true
	}
	if !validCatalogName(current.DatabaseName) && validCatalogName(candidate.DatabaseName) {
		return true
	}
	if current.DatabaseID == 0 && candidate.DatabaseID != 0 {
		return true
	}
	if current.RelKind == "" && candidate.RelKind != "" {
		return true
	}
	return false
}

func (r *CheckpointReader) streamTableCSV(
	ctx context.Context,
	w io.Writer,
	schema *TableSchema,
	snapshotTS types.TS,
	dataEntries, tombEntries []*ObjectEntryInfo,
	options CSVExportOptions,
) error {
	if !options.IncludeMetadata && options.RowOrder == CSVRowOrderStorage {
		return r.streamTableCSVPipeline(ctx, w, schema, snapshotTS, dataEntries, tombEntries, options)
	}

	needsBuffer := options.IncludeMetadata || options.RowOrder == CSVRowOrderLexical
	var output io.Writer = w
	var tmpFile *os.File
	if needsBuffer {
		var err error
		tmpFile, err = os.CreateTemp("", "mo-tool-table-dump-*.csv")
		if err != nil {
			return err
		}
		tmpName := tmpFile.Name()
		defer os.Remove(tmpName)
		defer tmpFile.Close()
		output = tmpFile
	}

	header := make([]string, 0, len(schema.Columns))
	columnSeqNums := make([]int, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		header = append(header, col.Name)
		seqNum := col.PhysicalPosition
		if seqNum < 0 {
			seqNum = col.Position
		}
		columnSeqNums = append(columnSeqNums, seqNum)
	}
	physicalPositions := append([]int(nil), columnSeqNums...)
	var projectedTypes []types.Type
	if options.IncludeHeader {
		// header is emitted after we know output mode but before rows
		if err := writeSQLLoadCSVRow(output, nil, header, nil); err != nil {
			return err
		}
	}

	var lexicalRows []exportedCSVRow
	onRow := func(_ string, _ int, _ int, values []string, nulls []bool) error {
		row := projectCSVRow(values, physicalPositions)
		rowNulls := projectCSVNulls(nulls, physicalPositions)
		if options.RowOrder == CSVRowOrderLexical {
			lexicalRows = append(lexicalRows, exportedCSVRow{values: row, nulls: rowNulls})
			return nil
		}
		return writeSQLLoadCSVRow(output, projectedTypes, row, rowNulls)
	}

	stats, err := r.scanLogicalTable(ctx, snapshotTS, dataEntries, tombEntries,
		func(cols []objecttool.ColInfo) error {
			physicalPositions = dataIndexesForSeqNums(cols, columnSeqNums)
			projectedTypes = buildProjectedTypes(cols, physicalPositions)
			return nil
		},
		onRow,
	)
	if err != nil {
		return err
	}
	if options.RowOrder == CSVRowOrderLexical {
		sortCSVRowsLexical(lexicalRows)
		for _, row := range lexicalRows {
			if err := writeSQLLoadCSVRow(output, projectedTypes, row.values, row.nulls); err != nil {
				return err
			}
		}
	}

	if !needsBuffer {
		return nil
	}
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if options.IncludeMetadata {
		if err := writeCSVMetadata(w, schema, stats); err != nil {
			return err
		}
	}
	_, err = io.Copy(w, tmpFile)
	return err
}

type csvPipelineChunk struct {
	objectIdx int
	blockIdx  int
	rows      int
	data      []byte
}

type csvPipelineObjectJob struct {
	objectIdx int
	entry     *ObjectEntryInfo
}

type csvPipelineBlock struct {
	objectIdx          int
	blockIdx           int
	entry              *ObjectEntryInfo
	projectedTypes     []types.Type
	relevantTombstones []objectio.ObjectStats
	bat                *batch.Batch
	release            func()
	commitTSVec        *vector.Vector
	releaseCommitTS    func()
}

func (b *csvPipelineBlock) releaseBlock() {
	if b.releaseCommitTS != nil {
		b.releaseCommitTS()
		b.releaseCommitTS = nil
	}
	if b.release != nil {
		b.release()
		b.release = nil
	}
}

type csvPipelineCounters struct {
	totalObjects     int64
	processedObjects atomic.Int64
	readQueuedBlocks atomic.Int64
	readQueuedRows   atomic.Int64
	readBatches      atomic.Int64
	readRows         atomic.Int64
	processedBatches atomic.Int64
	visibleRows      atomic.Int64
	physicalRows     atomic.Int64
	queuedBatches    atomic.Int64
	queuedBytes      atomic.Int64
	writtenBatches   atomic.Int64
	writtenBytes     atomic.Int64
	memoryWaits      atomic.Int64
	memoryAvailable  atomic.Int64
	memoryFloor      atomic.Int64
	readNanos        atomic.Int64
	processNanos     atomic.Int64
	writeNanos       atomic.Int64
}

type csvPipelineWorkerPlan struct {
	readerWorkers     int
	processorWorkers  int
	readQueueCapacity int
	cpuLimit          int
	memoryLimit       int
	memoryTotal       uint64
	memoryFree        uint64
	memoryFloor       uint64
	memoryPerWork     uint64
}

func (r *CheckpointReader) streamTableCSVPipeline(
	ctx context.Context,
	w io.Writer,
	schema *TableSchema,
	snapshotTS types.TS,
	dataEntries, tombEntries []*ObjectEntryInfo,
	options CSVExportOptions,
) error {
	header := make([]string, 0, len(schema.Columns))
	columnSeqNums := make([]int, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		header = append(header, col.Name)
		seqNum := col.PhysicalPosition
		if seqNum < 0 {
			seqNum = col.Position
		}
		columnSeqNums = append(columnSeqNums, seqNum)
	}
	if options.IncludeHeader {
		if err := writeSQLLoadCSVRow(w, nil, header, nil); err != nil {
			return err
		}
	}

	visibleDataEntries := visibleObjectEntries(dataEntries, snapshotTS)
	visibleTombEntries := visibleObjectEntries(tombEntries, snapshotTS)
	tombstoneStats := dedupeObjectStats(visibleTombEntries)
	counters := &csvPipelineCounters{totalObjects: int64(len(visibleDataEntries))}
	workerPlan := csvPipelineWorkerCount(len(visibleDataEntries))
	counters.memoryFloor.Store(int64(workerPlan.memoryFloor))
	counters.memoryAvailable.Store(int64(workerPlan.memoryFree))

	fmt.Fprintf(os.Stderr,
		"csv pipeline: start objects=%d reader_mode=in_processor processor_workers=%d writer_workers=1 cpu_limit=%d memory_limit=%d memory_total=%d memory_free=%d memory_floor=%d memory_per_worker=%d write_queue_capacity=%d report_every=%s\n",
		len(visibleDataEntries),
		workerPlan.processorWorkers,
		workerPlan.cpuLimit,
		workerPlan.memoryLimit,
		workerPlan.memoryTotal,
		workerPlan.memoryFree,
		workerPlan.memoryFloor,
		workerPlan.memoryPerWork,
		csvPipelineQueueCapacity,
		csvPipelineReportEvery,
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chunks := make(chan csvPipelineChunk, csvPipelineQueueCapacity)
	writerDone := make(chan error, 1)
	reportDone := make(chan struct{})
	defer close(reportDone)
	go reportCSVPipeline(ctx, reportDone, counters)
	go writeCSVChunks(ctx, w, chunks, counters, cancel, writerDone)

	producerErr := r.produceCSVChunks(ctx, chunks, counters, snapshotTS, visibleDataEntries, tombstoneStats, columnSeqNums, workerPlan)
	close(chunks)
	writerErr := <-writerDone
	printCSVPipelineReport("finish", counters)
	if producerErr != nil {
		return producerErr
	}
	return writerErr
}

func (r *CheckpointReader) produceCSVChunks(
	ctx context.Context,
	chunks chan<- csvPipelineChunk,
	counters *csvPipelineCounters,
	snapshotTS types.TS,
	visibleDataEntries []*ObjectEntryInfo,
	tombstoneStats []objectio.ObjectStats,
	columnSeqNums []int,
	workerPlan csvPipelineWorkerPlan,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	objectJobs := make(chan csvPipelineObjectJob, workerPlan.processorWorkers)
	var processorWG sync.WaitGroup
	var errOnce sync.Once
	var workerErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			workerErr = err
			cancel()
		})
	}

	processorWG.Add(workerPlan.processorWorkers)
	for workerID := 0; workerID < workerPlan.processorWorkers; workerID++ {
		go func() {
			defer processorWG.Done()
			for job := range objectJobs {
				if err := ctx.Err(); err != nil {
					setErr(err)
					return
				}
				if err := r.processCSVObjectChunks(ctx, chunks, counters, snapshotTS, tombstoneStats, columnSeqNums, job); err != nil {
					setErr(err)
					return
				}
			}
		}()
	}

	for objectIdx, entry := range visibleDataEntries {
		select {
		case objectJobs <- csvPipelineObjectJob{objectIdx: objectIdx, entry: entry}:
		case <-ctx.Done():
			close(objectJobs)
			processorWG.Wait()
			if workerErr != nil {
				return workerErr
			}
			return ctx.Err()
		}
	}
	close(objectJobs)
	processorWG.Wait()
	if workerErr != nil {
		return workerErr
	}
	return ctx.Err()
}

func (r *CheckpointReader) processCSVObjectChunks(
	ctx context.Context,
	chunks chan<- csvPipelineChunk,
	counters *csvPipelineCounters,
	snapshotTS types.TS,
	tombstoneStats []objectio.ObjectStats,
	columnSeqNums []int,
	job csvPipelineObjectJob,
) error {
	entry := job.entry
	if err := ctx.Err(); err != nil {
		return err
	}
	objName := entry.ObjectStats.ObjectName().String()
	reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
	if err != nil {
		if isDataFileNotFound(err) {
			counters.processedObjects.Add(1)
			return nil
		}
		return err
	}
	defer reader.Close()

	physicalPositions := dataIndexesForSeqNums(reader.Columns(), columnSeqNums)
	projectedTypes := buildProjectedTypes(reader.Columns(), physicalPositions)
	relevantTombstones, err := r.filterTombstonesForObject(ctx, entry.ObjectStats.ObjectName().ObjectId(), tombstoneStats)
	if err != nil {
		return err
	}

	for blockIdx := 0; blockIdx < int(entry.ObjectStats.BlkCnt()); blockIdx++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := waitForCSVMemory(ctx, counters); err != nil {
			return err
		}

		start := time.Now()
		bat, release, err := reader.ReadBlock(ctx, uint32(blockIdx))
		if err != nil {
			return err
		}
		commitTSVec, releaseCommitTS, err := reader.ReadBlockCommitTS(ctx, uint32(blockIdx))
		counters.readNanos.Add(time.Since(start).Nanoseconds())
		if err != nil {
			release()
			return err
		}
		if bat.RowCount() == 0 {
			if releaseCommitTS != nil {
				releaseCommitTS()
			}
			release()
			continue
		}

		block := csvPipelineBlock{
			objectIdx:          job.objectIdx,
			blockIdx:           blockIdx,
			entry:              entry,
			projectedTypes:     projectedTypes,
			relevantTombstones: relevantTombstones,
			bat:                bat,
			release:            release,
			commitTSVec:        commitTSVec,
			releaseCommitTS:    releaseCommitTS,
		}
		counters.readBatches.Add(1)
		counters.readRows.Add(int64(bat.RowCount()))
		chunk, err := r.buildCSVChunkForBlock(ctx, block, snapshotTS, physicalPositions, counters)
		if err != nil {
			return err
		}
		if len(chunk.data) == 0 {
			continue
		}
		counters.queuedBatches.Add(1)
		counters.queuedBytes.Add(int64(len(chunk.data)))
		select {
		case chunks <- chunk:
		case <-ctx.Done():
			counters.queuedBatches.Add(-1)
			counters.queuedBytes.Add(-int64(len(chunk.data)))
			return ctx.Err()
		}
	}

	counters.processedObjects.Add(1)
	return nil
}

func (r *CheckpointReader) readCSVObjectBlocks(
	ctx context.Context,
	blocks chan<- csvPipelineBlock,
	counters *csvPipelineCounters,
	snapshotTS types.TS,
	tombstoneStats []objectio.ObjectStats,
	columnSeqNums []int,
	job csvPipelineObjectJob,
) error {
	entry := job.entry
	if err := ctx.Err(); err != nil {
		return err
	}
	objName := entry.ObjectStats.ObjectName().String()
	reader, err := objecttool.OpenWithFS(ctx, r.fs, objName, objName)
	if err != nil {
		if isDataFileNotFound(err) {
			counters.processedObjects.Add(1)
			return nil
		}
		return err
	}

	physicalPositions := dataIndexesForSeqNums(reader.Columns(), columnSeqNums)
	projectedTypes := buildProjectedTypes(reader.Columns(), physicalPositions)
	relevantTombstones, err := r.filterTombstonesForObject(ctx, entry.ObjectStats.ObjectName().ObjectId(), tombstoneStats)
	if err != nil {
		_ = reader.Close()
		return err
	}

	for blockIdx := 0; blockIdx < int(entry.ObjectStats.BlkCnt()); blockIdx++ {
		if err := ctx.Err(); err != nil {
			_ = reader.Close()
			return err
		}
		if err := waitForCSVMemory(ctx, counters); err != nil {
			_ = reader.Close()
			return err
		}

		start := time.Now()
		bat, release, err := reader.ReadBlock(ctx, uint32(blockIdx))
		if err != nil {
			_ = reader.Close()
			return err
		}
		commitTSVec, releaseCommitTS, err := reader.ReadBlockCommitTS(ctx, uint32(blockIdx))
		counters.readNanos.Add(time.Since(start).Nanoseconds())
		if err != nil {
			release()
			_ = reader.Close()
			return err
		}
		if bat.RowCount() == 0 {
			if releaseCommitTS != nil {
				releaseCommitTS()
			}
			release()
			continue
		}

		block := csvPipelineBlock{
			objectIdx:          job.objectIdx,
			blockIdx:           blockIdx,
			entry:              entry,
			projectedTypes:     projectedTypes,
			relevantTombstones: relevantTombstones,
			bat:                bat,
			release:            release,
			commitTSVec:        commitTSVec,
			releaseCommitTS:    releaseCommitTS,
		}
		counters.readBatches.Add(1)
		counters.readRows.Add(int64(bat.RowCount()))
		counters.readQueuedBlocks.Add(1)
		counters.readQueuedRows.Add(int64(bat.RowCount()))
		select {
		case blocks <- block:
		case <-ctx.Done():
			counters.readQueuedBlocks.Add(-1)
			counters.readQueuedRows.Add(-int64(bat.RowCount()))
			block.releaseBlock()
			_ = reader.Close()
			return ctx.Err()
		}
	}

	if err := reader.Close(); err != nil {
		return err
	}
	counters.processedObjects.Add(1)
	return nil
}

func (r *CheckpointReader) processCSVBlockChunk(
	ctx context.Context,
	chunks chan<- csvPipelineChunk,
	counters *csvPipelineCounters,
	snapshotTS types.TS,
	physicalPositions []int,
	block csvPipelineBlock,
) error {
	if err := ctx.Err(); err != nil {
		block.releaseBlock()
		return err
	}
	chunk, err := r.buildCSVChunkForBlock(ctx, block, snapshotTS, physicalPositions, counters)
	if err != nil {
		return err
	}
	if len(chunk.data) == 0 {
		return nil
	}
	counters.queuedBatches.Add(1)
	counters.queuedBytes.Add(int64(len(chunk.data)))
	select {
	case chunks <- chunk:
		return nil
	case <-ctx.Done():
		counters.queuedBatches.Add(-1)
		counters.queuedBytes.Add(-int64(len(chunk.data)))
		return ctx.Err()
	}
}

func csvPipelineWorkerCount(objects int) csvPipelineWorkerPlan {
	cpuLimit := runtime.GOMAXPROCS(0)
	if cpuLimit < 1 {
		cpuLimit = 1
	}

	total, free, ok := readSystemMemory()
	floor := csvPipelineMemoryFloorFromTotal(total, ok)
	memoryLimit := cpuLimit
	if ok {
		memoryLimit = 1
		if free > floor {
			budget := free - floor
			memoryLimit = int(budget / csvPipelineWorkerMemory)
			if memoryLimit < 1 {
				memoryLimit = 1
			}
		}
	}
	processorWorkers := cpuLimit
	if memoryLimit < processorWorkers {
		processorWorkers = memoryLimit
	}
	if objects > 0 && objects < processorWorkers {
		processorWorkers = objects
	}
	if processorWorkers < 1 {
		processorWorkers = 1
	}

	readerWorkers := csvPipelineReaderMax
	if objects > 0 && objects < readerWorkers {
		readerWorkers = objects
	}
	if processorWorkers < readerWorkers {
		readerWorkers = processorWorkers
	}
	if readerWorkers < 1 {
		readerWorkers = 1
	}
	readQueueCapacity := readerWorkers * 2
	if processorWorkers < readQueueCapacity {
		readQueueCapacity = processorWorkers
	}
	if readQueueCapacity < 1 {
		readQueueCapacity = 1
	}
	return csvPipelineWorkerPlan{
		readerWorkers:     readerWorkers,
		processorWorkers:  processorWorkers,
		readQueueCapacity: readQueueCapacity,
		cpuLimit:          cpuLimit,
		memoryLimit:       memoryLimit,
		memoryTotal:       total,
		memoryFree:        free,
		memoryFloor:       floor,
		memoryPerWork:     csvPipelineWorkerMemory,
	}
}

func csvPipelineMemoryFloorFromTotal(total uint64, ok bool) uint64 {
	if !ok || total == 0 {
		return csvPipelineMinFreeMemory
	}
	floor := total / csvPipelineFreeRatio
	if floor < csvPipelineMinFreeMemory {
		return csvPipelineMinFreeMemory
	}
	return floor
}

func waitForCSVMemory(ctx context.Context, counters *csvPipelineCounters) error {
	floor := uint64(counters.memoryFloor.Load())
	for {
		_, available, ok := readSystemMemory()
		if !ok {
			return nil
		}
		counters.memoryAvailable.Store(int64(available))
		if available >= floor {
			return nil
		}
		counters.memoryWaits.Add(1)
		timer := time.NewTimer(csvPipelineMemoryPoll)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

func readSystemMemory() (total uint64, available uint64, ok bool) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0, false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		bytes := value * 1024
		switch fields[0] {
		case "MemTotal:":
			total = bytes
		case "MemAvailable:":
			available = bytes
		}
	}
	return total, available, total > 0 && available > 0
}

func (r *CheckpointReader) buildCSVChunkForBlock(
	ctx context.Context,
	block csvPipelineBlock,
	snapshotTS types.TS,
	physicalPositions []int,
	counters *csvPipelineCounters,
) (csvPipelineChunk, error) {
	start := time.Now()
	defer func() {
		counters.processNanos.Add(time.Since(start).Nanoseconds())
	}()
	defer block.releaseBlock()

	bat := block.bat
	if bat.RowCount() == 0 {
		return csvPipelineChunk{}, nil
	}
	counters.physicalRows.Add(int64(bat.RowCount()))

	var commitTSs []types.TS
	if block.commitTSVec != nil {
		commitTSs = vector.MustFixedColWithTypeCheck[types.TS](block.commitTSVec)
	}

	deleteMask, err := r.buildDeleteMaskForBlock(ctx, &snapshotTS, block.entry.ObjectStats, uint16(block.blockIdx), block.relevantTombstones)
	if err != nil {
		return csvPipelineChunk{}, err
	}
	if deleteMask.IsValid() {
		defer deleteMask.Release()
	}

	var buf bytes.Buffer
	rows := 0
	for rowIdx := 0; rowIdx < bat.RowCount(); rowIdx++ {
		if commitTSs != nil && rowIdx < len(commitTSs) &&
			!block.commitTSVec.IsNull(uint64(rowIdx)) &&
			commitTSs[rowIdx].GT(&snapshotTS) {
			continue
		}
		if deleteMask.IsValid() && deleteMask.Contains(uint64(rowIdx)) {
			continue
		}
		if err := writeProjectedCSVRowFromVecs(&buf, block.projectedTypes, bat.Vecs, physicalPositions, rowIdx); err != nil {
			return csvPipelineChunk{}, err
		}
		rows++
	}

	if rows == 0 {
		return csvPipelineChunk{}, nil
	}
	counters.processedBatches.Add(1)
	counters.visibleRows.Add(int64(rows))
	return csvPipelineChunk{
		objectIdx: block.objectIdx,
		blockIdx:  block.blockIdx,
		rows:      rows,
		data:      buf.Bytes(),
	}, nil
}

func writeCSVChunks(
	ctx context.Context,
	w io.Writer,
	chunks <-chan csvPipelineChunk,
	counters *csvPipelineCounters,
	cancel context.CancelFunc,
	done chan<- error,
) {
	for chunk := range chunks {
		if err := ctx.Err(); err != nil {
			done <- err
			return
		}
		start := time.Now()
		if _, err := w.Write(chunk.data); err != nil {
			cancel()
			done <- err
			return
		}
		counters.writeNanos.Add(time.Since(start).Nanoseconds())
		counters.queuedBatches.Add(-1)
		counters.queuedBytes.Add(-int64(len(chunk.data)))
		counters.writtenBatches.Add(1)
		counters.writtenBytes.Add(int64(len(chunk.data)))
	}
	done <- nil
}

func reportCSVPipeline(ctx context.Context, done <-chan struct{}, counters *csvPipelineCounters) {
	ticker := time.NewTicker(csvPipelineReportEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			printCSVPipelineReport("progress", counters)
		case <-done:
			return
		case <-ctx.Done():
			return
		}
	}
}

func printCSVPipelineReport(stage string, counters *csvPipelineCounters) {
	fmt.Fprintf(os.Stderr,
		"csv pipeline: %s objects=%d/%d read_batches=%d read_queue_blocks=%d read_queue_rows=%d processed_batches=%d write_queue_batches=%d write_queue_bytes=%d visible_rows=%d physical_rows=%d written_batches=%d written_bytes=%d read_ms=%d process_ms=%d write_ms=%d mem_available=%d mem_floor=%d mem_waits=%d\n",
		stage,
		counters.processedObjects.Load(),
		counters.totalObjects,
		counters.readBatches.Load(),
		counters.readQueuedBlocks.Load(),
		counters.readQueuedRows.Load(),
		counters.processedBatches.Load(),
		counters.queuedBatches.Load(),
		counters.queuedBytes.Load(),
		counters.visibleRows.Load(),
		counters.physicalRows.Load(),
		counters.writtenBatches.Load(),
		counters.writtenBytes.Load(),
		counters.readNanos.Load()/int64(time.Millisecond),
		counters.processNanos.Load()/int64(time.Millisecond),
		counters.writeNanos.Load()/int64(time.Millisecond),
		counters.memoryAvailable.Load(),
		counters.memoryFloor.Load(),
		counters.memoryWaits.Load(),
	)
}

// WriteCSV writes a LogicalTableView with the given schema as CSV to w.
func WriteCSV(w io.Writer, schema *TableSchema, view *LogicalTableView, opts ...CSVExportOption) error {
	options := resolveCSVExportOptions(opts)

	if options.IncludeMetadata {
		if err := writeCSVMetadata(w, schema, logicalTableStats{
			VisibleRows:  view.VisibleRows,
			DeletedRows:  view.DeletedRows,
			PhysicalRows: view.PhysicalRows,
		}); err != nil {
			return err
		}
	}

	// Merge schema column names into headers
	merged := MergeLogicalViewWithSchema(view, schema)
	projectedTypes := make([]types.Type, len(schema.Columns))
	for i, col := range schema.Columns {
		projectedTypes[i] = sqlTypeStringToType(col.SQLType)
	}
	if options.IncludeHeader {
		if err := writeSQLLoadCSVRow(w, nil, merged.Headers, nil); err != nil {
			return err
		}
	}

	rows := merged.Rows
	if options.RowOrder == CSVRowOrderLexical {
		rows = make([][]string, len(merged.Rows))
		for i, row := range merged.Rows {
			rows[i] = append([]string(nil), row...)
		}
		sort.SliceStable(rows, func(i, j int) bool {
			return compareCSVRowsLexical(rows[i], rows[j]) < 0
		})
	}
	for _, row := range rows {
		if err := writeSQLLoadCSVRow(w, projectedTypes, row, nil); err != nil {
			return err
		}
	}
	return nil
}

func projectCSVRow(values []string, physicalPositions []int) []string {
	row := make([]string, len(physicalPositions))
	for i, pos := range physicalPositions {
		if pos >= 0 && pos < len(values) {
			row[i] = values[pos]
		}
	}
	return row
}

func projectCSVNulls(values []bool, physicalPositions []int) []bool {
	row := make([]bool, len(physicalPositions))
	for i, pos := range physicalPositions {
		if pos >= 0 && pos < len(values) {
			row[i] = values[pos]
		}
	}
	return row
}

func sortCSVRowsLexical(rows []exportedCSVRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		return compareCSVRowsLexical(rows[i].values, rows[j].values) < 0
	})
}

func buildProjectedTypes(cols []objecttool.ColInfo, physicalPositions []int) []types.Type {
	projected := make([]types.Type, len(physicalPositions))
	for i, pos := range physicalPositions {
		if pos >= 0 && pos < len(cols) {
			projected[i] = cols[pos].Type
		}
	}
	return projected
}

func dataIndexesForSeqNums(cols []objecttool.ColInfo, seqNums []int) []int {
	indexes := make([]int, len(seqNums))
	for i, seqNum := range seqNums {
		indexes[i] = seqNum
		for dataIdx, col := range cols {
			if int(col.SeqNum) == seqNum {
				indexes[i] = dataIdx
				break
			}
		}
	}
	return indexes
}

func writeSQLLoadCSVRow(w io.Writer, colTypes []types.Type, fields []string, nulls []bool) error {
	for i, field := range fields {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		typ := types.Type{}
		if i < len(colTypes) {
			typ = colTypes[i]
		}
		isNull := i < len(nulls) && nulls[i]
		if err := writeSQLLoadCSVField(w, typ, field, isNull); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func writeProjectedCSVRowFromVecs(
	w *bytes.Buffer,
	colTypes []types.Type,
	vecs []*vector.Vector,
	physicalPositions []int,
	rowIdx int,
) error {
	for i, pos := range physicalPositions {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		typ := types.Type{}
		if i < len(colTypes) {
			typ = colTypes[i]
		}
		if err := writeSQLLoadCSVFieldFromVec(w, typ, vecs, pos, rowIdx); err != nil {
			return err
		}
	}
	w.WriteByte('\n')
	return nil
}

func writeSQLLoadCSVFieldFromVec(
	w *bytes.Buffer,
	typ types.Type,
	vecs []*vector.Vector,
	pos int,
	rowIdx int,
) error {
	if pos < 0 || pos >= len(vecs) || vecs[pos] == nil || vecs[pos].IsNull(uint64(rowIdx)) {
		_, err := io.WriteString(w, `\N`)
		return err
	}
	vec := vecs[pos]
	rowIdx = vectorRowIndex(vec, rowIdx)
	if shouldQuoteSQLLoadType(typ) {
		w.WriteByte('"')
		appendCSVQuotedVecValue(w, vec, rowIdx)
		w.WriteByte('"')
		return nil
	}
	appendCSVUnquotedVecValue(w, typ, vec, rowIdx)
	return nil
}

func vectorRowIndex(vec *vector.Vector, rowIdx int) int {
	if vec.IsConst() {
		return 0
	}
	return rowIdx
}

func appendCSVQuotedVecValue(w *bytes.Buffer, vec *vector.Vector, rowIdx int) {
	switch vec.GetType().Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink, types.T_json:
		appendEscapedSQLLoadBytes(w, vec.GetBytesAt(rowIdx), '"')
	default:
		appendEscapedSQLLoadString(w, vecValueToString(vec, rowIdx), '"')
	}
}

func appendCSVUnquotedVecValue(w *bytes.Buffer, typ types.Type, vec *vector.Vector, rowIdx int) {
	switch vec.GetType().Oid {
	case types.T_bool:
		if vector.MustFixedColWithTypeCheck[bool](vec)[rowIdx] {
			w.WriteString("true")
		} else {
			w.WriteString("false")
		}
	case types.T_int8:
		appendCSVInt(w, int64(vector.MustFixedColWithTypeCheck[int8](vec)[rowIdx]))
	case types.T_int16:
		appendCSVInt(w, int64(vector.MustFixedColWithTypeCheck[int16](vec)[rowIdx]))
	case types.T_int32:
		appendCSVInt(w, int64(vector.MustFixedColWithTypeCheck[int32](vec)[rowIdx]))
	case types.T_int64:
		appendCSVInt(w, vector.MustFixedColWithTypeCheck[int64](vec)[rowIdx])
	case types.T_uint8:
		appendCSVUint(w, uint64(vector.MustFixedColWithTypeCheck[uint8](vec)[rowIdx]))
	case types.T_uint16:
		appendCSVUint(w, uint64(vector.MustFixedColWithTypeCheck[uint16](vec)[rowIdx]))
	case types.T_uint32:
		appendCSVUint(w, uint64(vector.MustFixedColWithTypeCheck[uint32](vec)[rowIdx]))
	case types.T_uint64:
		appendCSVUint(w, vector.MustFixedColWithTypeCheck[uint64](vec)[rowIdx])
	case types.T_float32:
		appendCSVFloat(w, float64(vector.MustFixedColWithTypeCheck[float32](vec)[rowIdx]), 32)
	case types.T_float64:
		appendCSVFloat(w, vector.MustFixedColWithTypeCheck[float64](vec)[rowIdx], 64)
	case types.T_decimal64:
		appendDecimal64(w, vector.MustFixedColWithTypeCheck[types.Decimal64](vec)[rowIdx], vec.GetType().Scale)
	case types.T_decimal128:
		appendDecimal128(w, vector.MustFixedColWithTypeCheck[types.Decimal128](vec)[rowIdx], vec.GetType().Scale)
	case types.T_date:
		appendDate(w, vector.MustFixedColWithTypeCheck[types.Date](vec)[rowIdx])
	default:
		w.WriteString(vecValueToString(vec, rowIdx))
	}
}

func appendEscapedSQLLoadString(w *bytes.Buffer, s string, enclosed byte) {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			w.WriteByte('\\')
			w.WriteByte('\\')
		case enclosed:
			if enclosed != 0 && enclosed != '\\' {
				w.WriteByte(enclosed)
				w.WriteByte(enclosed)
			} else {
				w.WriteByte(s[i])
			}
		default:
			w.WriteByte(s[i])
		}
	}
}

func appendEscapedSQLLoadBytes(w *bytes.Buffer, data []byte, enclosed byte) {
	for _, b := range data {
		switch b {
		case '\\':
			w.WriteByte('\\')
			w.WriteByte('\\')
		case enclosed:
			if enclosed != 0 && enclosed != '\\' {
				w.WriteByte(enclosed)
				w.WriteByte(enclosed)
			} else {
				w.WriteByte(b)
			}
		default:
			w.WriteByte(b)
		}
	}
}

func appendDecimal64(w *bytes.Buffer, value types.Decimal64, scale int32) {
	if value.Sign() {
		w.WriteByte('-')
		value = value.Minus()
	}
	v := uint64(value)
	if scale <= 0 {
		appendCSVUint(w, v)
		return
	}
	pow := uint64(1)
	for i := int32(0); i < scale; i++ {
		pow *= 10
	}
	intPart := v / pow
	fracPart := v % pow
	appendCSVUint(w, intPart)
	w.WriteByte('.')
	var scratch [32]byte
	frac := strconv.AppendUint(scratch[:0], fracPart, 10)
	for i := len(frac); i < int(scale); i++ {
		w.WriteByte('0')
	}
	w.Write(frac)
}

func appendDecimal128(w *bytes.Buffer, value types.Decimal128, scale int32) {
	if value.Sign() {
		w.WriteByte('-')
		value = value.Minus()
	}
	var scratch [80]byte
	i := len(scratch)
	ten := types.Decimal128{B0_63: types.Pow10[1]}
	one := types.Decimal128{B0_63: 1}
	for value.B64_127 != 0 || value.B0_63 != 0 {
		digit, _ := value.Mod128(ten)
		i--
		scratch[i] = byte(digit.B0_63) + '0'
		value, _ = value.Div128(ten)
		if digit.B0_63 >= 5 {
			value, _ = value.Sub128(one)
		}
		scale--
		if scale == 0 {
			i--
			scratch[i] = '.'
		}
	}
	for scale > 0 {
		i--
		scratch[i] = '0'
		scale--
		if scale == 0 {
			i--
			scratch[i] = '.'
		}
	}
	if scale == 0 {
		i--
		scratch[i] = '0'
	}
	w.Write(scratch[i:])
}

func appendDate(w *bytes.Buffer, value types.Date) {
	year, month, day, _ := value.Calendar(true)
	appendZeroPaddedInt(w, int64(year), 4)
	w.WriteByte('-')
	appendZeroPaddedInt(w, int64(month), 2)
	w.WriteByte('-')
	appendZeroPaddedInt(w, int64(day), 2)
}

func appendZeroPaddedInt(w *bytes.Buffer, value int64, width int) {
	var scratch [32]byte
	buf := strconv.AppendInt(scratch[:0], value, 10)
	for i := len(buf); i < width; i++ {
		w.WriteByte('0')
	}
	w.Write(buf)
}

func appendCSVInt(w *bytes.Buffer, value int64) {
	var scratch [32]byte
	w.Write(strconv.AppendInt(scratch[:0], value, 10))
}

func appendCSVUint(w *bytes.Buffer, value uint64) {
	var scratch [32]byte
	w.Write(strconv.AppendUint(scratch[:0], value, 10))
}

func appendCSVFloat(w *bytes.Buffer, value float64, bitSize int) {
	var scratch [32]byte
	w.Write(strconv.AppendFloat(scratch[:0], value, 'g', -1, bitSize))
}

func writeSQLLoadCSVField(w io.Writer, typ types.Type, field string, isNull bool) error {
	if isNull {
		_, err := io.WriteString(w, `\N`)
		return err
	}

	if shouldQuoteSQLLoadType(typ) {
		if _, err := io.WriteString(w, `"`); err != nil {
			return err
		}
		escaped := escapeSQLLoadString(field, '"')
		if _, err := io.WriteString(w, escaped); err != nil {
			return err
		}
		_, err := io.WriteString(w, `"`)
		return err
	}

	_, err := io.WriteString(w, field)
	return err
}

func escapeSQLLoadString(s string, enclosed byte) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	if enclosed != 0 && enclosed != '\\' {
		s = strings.ReplaceAll(s, string(enclosed), string([]byte{enclosed, enclosed}))
	}
	return s
}

func shouldQuoteSQLLoadType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text,
		types.T_binary, types.T_varbinary, types.T_datalink, types.T_json,
		types.T_geometry, types.T_array_float32, types.T_array_float64:
		return true
	default:
		return false
	}
}

func sqlTypeStringToType(sqlType string) types.Type {
	s := strings.ToUpper(strings.TrimSpace(sqlType))
	switch {
	case strings.Contains(s, "CHAR"), strings.Contains(s, "TEXT"), strings.Contains(s, "BLOB"), strings.Contains(s, "DATALINK"):
		return types.T_varchar.ToType()
	case strings.Contains(s, "JSON"):
		return types.T_json.ToType()
	case strings.Contains(s, "GEOMETRY"):
		return types.T_geometry.ToType()
	case strings.Contains(s, "DECIMAL"):
		return types.New(types.T_decimal128, 0, 0)
	case strings.Contains(s, "TIMESTAMP"):
		return types.T_timestamp.ToType()
	case strings.Contains(s, "DATETIME"):
		return types.T_datetime.ToType()
	case strings.HasPrefix(s, "TIME"):
		return types.T_time.ToType()
	case strings.Contains(s, "DATE"):
		return types.T_date.ToType()
	case strings.Contains(s, "DOUBLE"):
		return types.T_float64.ToType()
	case strings.Contains(s, "FLOAT"):
		return types.T_float32.ToType()
	case strings.Contains(s, "BOOL"):
		return types.T_bool.ToType()
	case strings.Contains(s, "BIGINT"):
		return types.T_int64.ToType()
	case strings.Contains(s, "INT"):
		return types.T_int32.ToType()
	default:
		return types.Type{}
	}
}

func compareCSVRowsLexical(left, right []string) int {
	limit := len(left)
	if len(right) < limit {
		limit = len(right)
	}
	for i := 0; i < limit; i++ {
		if cmp := strings.Compare(left[i], right[i]); cmp != 0 {
			return cmp
		}
	}
	switch {
	case len(left) < len(right):
		return -1
	case len(left) > len(right):
		return 1
	default:
		return 0
	}
}

func writeCSVMetadata(w io.Writer, schema *TableSchema, stats logicalTableStats) error {
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
	_, err := fmt.Fprintf(w, "-- Visible rows: %d (deleted: %d, physical: %d)\n",
		stats.VisibleRows, stats.DeletedRows, stats.PhysicalRows)
	return err
}

// MergeLogicalViewWithSchema replaces col_N headers with real column names from the schema
// and filters data rows to only include visible (non-hidden) columns by their physical position.
// Returns a new LogicalTableView with merged headers and filtered data rows.
func MergeLogicalViewWithSchema(view *LogicalTableView, schema *TableSchema) *LogicalTableView {
	dataWidth := len(view.Headers) - logicalViewDataOffset(view)
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
		dataPos := dataIndexForSeqNum(view, col.PhysicalPosition)
		if dataPos < 0 {
			dataPos = col.PhysicalPosition
		}
		if dataPos < 0 {
			dataPos = col.Position
		}
		colMap = append(colMap, dataPos)
	}

	// Extract data rows: pick only visible columns by their physical position
	newRows := make([][]string, len(view.Rows))
	dataOffset := logicalViewDataOffset(view)
	for i, row := range view.Rows {
		newRow := make([]string, len(colMap))
		for j, pos := range colMap {
			dataIdx := dataOffset + pos
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

func dataIndexForSeqNum(view *LogicalTableView, seqNum int) int {
	if view == nil || seqNum < 0 {
		return -1
	}
	for idx, candidate := range view.ColSeqNums {
		if int(candidate) == seqNum {
			return idx
		}
	}
	return -1
}

// buildSchemaFromMoTablesRow extracts a TableSchema from a mo_tables data row.
// Uses column name lookup from view headers to handle hidden column offsets.
// mo_tables columns: rel_id, relname, reldatabase, reldatabase_id, ..., rel_createsql
func buildSchemaFromMoTablesRow(view *LogicalTableView, fullRow []string) *TableSchema {
	schema := &TableSchema{}
	dataRow := fullRow[logicalViewDataOffset(view):]

	relNameIdx := fallbackCatalogColIndex(view, moTablesID, "relname")
	if relNameIdx >= 0 && relNameIdx < len(dataRow) {
		schema.TableName = strings.Clone(dataRow[relNameIdx])
	}

	relDBIdx := fallbackCatalogColIndex(view, moTablesID, "reldatabase")
	if relDBIdx >= 0 && relDBIdx < len(dataRow) {
		schema.DatabaseName = strings.Clone(dataRow[relDBIdx])
	}

	createSQLIdx := fallbackCatalogColIndex(view, moTablesID, "rel_createsql")
	if createSQLIdx >= 0 && createSQLIdx < len(dataRow) {
		if isPrintableCreateTableSQL(dataRow[createSQLIdx]) {
			schema.CreateSQL = strings.Clone(dataRow[createSQLIdx])
		}
	}
	commentIdx := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_Comment)
	if commentIdx >= 0 && commentIdx < len(dataRow) && isPrintableSQLText(dataRow[commentIdx]) {
		schema.Comment = strings.Clone(dataRow[commentIdx])
	}
	constraintIdx := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_Constraint)
	if constraintIdx >= 0 && constraintIdx < len(dataRow) {
		schema.UniqueKeys = decodeUniqueKeysFromMoTablesConstraint(dataRow[constraintIdx])
		schema.PrimaryKey = decodePrimaryKeyFromMoTablesConstraint(dataRow[constraintIdx])
	}
	return schema
}

func findTableNameFromMoTables(view *LogicalTableView, tableID uint64) string {
	tableIDStr := fmt.Sprintf("%d", tableID)
	try := func(relIDCol, relNameCol int) string {
		if relIDCol < 0 || relNameCol < 0 {
			return ""
		}
		for _, fullRow := range view.Rows {
			row := fullRow[logicalViewDataOffset(view):]
			if relIDCol >= len(row) || relNameCol >= len(row) {
				continue
			}
			if row[relIDCol] == tableIDStr && row[relNameCol] != "" {
				return strings.Clone(row[relNameCol])
			}
		}
		return ""
	}

	relIDCol := fallbackCatalogColIndex(view, moTablesID, "rel_id")
	relNameCol := fallbackCatalogColIndex(view, moTablesID, "relname")
	if name := try(relIDCol, relNameCol); name != "" {
		return name
	}

	dataWidth := len(view.Headers) - logicalViewDataOffset(view)
	for _, match := range catalogLayoutMatches(dataWidth, moTablesID) {
		relIDCol = catalogColIndexForLayout(match.layout, moTablesID, "rel_id", match.offset)
		relNameCol = catalogColIndexForLayout(match.layout, moTablesID, "relname", match.offset)
		if name := try(relIDCol, relNameCol); name != "" {
			return name
		}
	}
	return ""
}

func findTableCommentFromMoTables(view *LogicalTableView, tableID uint64) string {
	tableIDStr := fmt.Sprintf("%d", tableID)
	relIDCol := fallbackCatalogColIndex(view, moTablesID, "rel_id")
	commentCol := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_Comment)
	if relIDCol < 0 || commentCol < 0 {
		return ""
	}
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewDataOffset(view):]
		if relIDCol >= len(row) || commentCol >= len(row) {
			continue
		}
		if row[relIDCol] == tableIDStr && isPrintableSQLText(row[commentCol]) {
			return strings.Clone(row[commentCol])
		}
	}
	return ""
}

func findUniqueKeysFromMoTables(view *LogicalTableView, tableID uint64) []TableUniqueKey {
	tableIDStr := fmt.Sprintf("%d", tableID)
	relIDCol := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_ID)
	constraintCol := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_Constraint)
	if relIDCol < 0 || constraintCol < 0 {
		return nil
	}
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewDataOffset(view):]
		if relIDCol >= len(row) || constraintCol >= len(row) {
			continue
		}
		if row[relIDCol] == tableIDStr {
			return decodeUniqueKeysFromMoTablesConstraint(row[constraintCol])
		}
	}
	return nil
}

func findPrimaryKeyFromMoTables(view *LogicalTableView, tableID uint64) []string {
	tableIDStr := fmt.Sprintf("%d", tableID)
	relIDCol := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_ID)
	constraintCol := fallbackCatalogColIndex(view, moTablesID, catalog.SystemRelAttr_Constraint)
	if relIDCol < 0 || constraintCol < 0 {
		return nil
	}
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewDataOffset(view):]
		if relIDCol >= len(row) || constraintCol >= len(row) {
			continue
		}
		if row[relIDCol] == tableIDStr {
			return decodePrimaryKeyFromMoTablesConstraint(row[constraintCol])
		}
	}
	return nil
}

func createTableDDLFromCatalogViews(tableID uint64, moTablesView, moColumnsView *LogicalTableView, partitionMetadataView ...*LogicalTableView) string {
	tableName := ""
	tableComment := ""
	uniqueKeys := []TableUniqueKey(nil)
	primaryKey := []string(nil)
	partition := ""
	if moTablesView != nil {
		tableName = findTableNameFromMoTables(moTablesView, tableID)
		tableComment = findTableCommentFromMoTables(moTablesView, tableID)
		uniqueKeys = findUniqueKeysFromMoTables(moTablesView, tableID)
		primaryKey = findPrimaryKeyFromMoTables(moTablesView, tableID)
	}
	if len(partitionMetadataView) > 0 && partitionMetadataView[0] != nil {
		partition = buildPartitionClauseFromMetadata(partitionMetadataView[0], tableID)
	}
	if moColumnsView != nil {
		if ddl := buildCreateTableFromMoColumnsWithOptions(moColumnsView, tableID, tableName, tableComment, partition, primaryKey, uniqueKeys); ddl != "" {
			return ddl
		}
	}
	return ""
}

func buildCatalogTablesFromMoTablesRows(view *LogicalTableView) []TableCatalogEntry {
	seen := make(map[uint64]struct{})
	merged := make([]TableCatalogEntry, 0)
	try := func(relIDCol, relNameCol, relDBCol, relDBIDCol, relKindCol, accountIDCol int) {
		tables := buildCatalogTablesFromMoTablesRowsAt(
			view,
			relIDCol,
			relNameCol,
			relDBCol,
			relDBIDCol,
			relKindCol,
			accountIDCol,
		)
		for _, table := range tables {
			if _, ok := seen[table.TableID]; ok {
				continue
			}
			seen[table.TableID] = struct{}{}
			merged = append(merged, table)
		}
	}

	try(
		fallbackCatalogColIndex(view, moTablesID, "rel_id"),
		fallbackCatalogColIndex(view, moTablesID, "relname"),
		fallbackCatalogColIndex(view, moTablesID, "reldatabase"),
		fallbackCatalogColIndex(view, moTablesID, "reldatabase_id"),
		fallbackCatalogColIndex(view, moTablesID, "relkind"),
		fallbackCatalogColIndex(view, moTablesID, "account_id"),
	)
	dataWidth := len(view.Headers) - logicalViewDataOffset(view)
	for _, match := range catalogLayoutMatches(dataWidth, moTablesID) {
		try(
			catalogColIndexForLayout(match.layout, moTablesID, "rel_id", match.offset),
			catalogColIndexForLayout(match.layout, moTablesID, "relname", match.offset),
			catalogColIndexForLayout(match.layout, moTablesID, "reldatabase", match.offset),
			catalogColIndexForLayout(match.layout, moTablesID, "reldatabase_id", match.offset),
			catalogColIndexForLayout(match.layout, moTablesID, "relkind", match.offset),
			catalogColIndexForLayout(match.layout, moTablesID, "account_id", match.offset),
		)
	}
	return merged
}

func buildCatalogTablesFromMoTablesRowsAt(
	view *LogicalTableView,
	relIDCol int,
	relNameCol int,
	relDBCol int,
	relDBIDCol int,
	relKindCol int,
	accountIDCol int,
) []TableCatalogEntry {
	if relIDCol < 0 || relNameCol < 0 || relDBCol < 0 {
		return nil
	}

	seen := make(map[uint64]struct{})
	tables := make([]TableCatalogEntry, 0)
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewDataOffset(view):]
		if relIDCol >= len(row) || relNameCol >= len(row) || relDBCol >= len(row) {
			continue
		}
		tableID, err := strconv.ParseUint(row[relIDCol], 10, 64)
		if err != nil || tableID == 0 {
			continue
		}
		if _, ok := seen[tableID]; ok {
			continue
		}
		entry := TableCatalogEntry{
			TableID:      tableID,
			AccountID:    0,
			DatabaseName: strings.Clone(row[relDBCol]),
			TableName:    strings.Clone(row[relNameCol]),
		}
		if !validCatalogName(entry.DatabaseName) || !validCatalogName(entry.TableName) {
			continue
		}
		if relDBIDCol >= 0 && relDBIDCol < len(row) {
			if dbID, err := strconv.ParseUint(row[relDBIDCol], 10, 64); err == nil {
				entry.DatabaseID = dbID
			}
		}
		if relKindCol >= 0 && relKindCol < len(row) {
			entry.RelKind = strings.Clone(row[relKindCol])
			if !validRelKind(entry.RelKind) {
				continue
			}
		}
		if accountIDCol >= 0 && accountIDCol < len(row) {
			if accountID, err := strconv.ParseUint(row[accountIDCol], 10, 32); err == nil {
				entry.AccountID = uint32(accountID)
			}
		}
		seen[tableID] = struct{}{}
		tables = append(tables, entry)
	}
	return tables
}

func validCatalogName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '$':
		default:
			return false
		}
	}
	return true
}

func validRelKind(s string) bool {
	switch s {
	case "", "r", "v", "e", "cluster", "external", "view":
		return true
	default:
		return false
	}
}

// buildColumnsFromMoColumnsRows builds a sorted column list from mo_columns data rows
// filtered for a specific tableID and excluding hidden columns.
// Uses column name lookup from view headers to handle hidden column offsets.
// mo_columns columns: att_relname_id, att_relname, attname, atttyp, attnum, ..., att_is_hidden
func buildColumnsFromMoColumnsRows(view *LogicalTableView, tableID uint64) []TableColumn {
	relnameIDCol := fallbackCatalogColIndex(view, moColumnsID, "att_relname_id")
	nameCol := fallbackCatalogColIndex(view, moColumnsID, "attname")
	typCol := fallbackCatalogColIndex(view, moColumnsID, "atttyp")
	numCol := fallbackCatalogColIndex(view, moColumnsID, "attnum")
	hiddenCol := fallbackCatalogColIndex(view, moColumnsID, "att_is_hidden")
	seqNumCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_Seqnum)

	ckpDebugSchemaf(
		"mo_columns schema resolve start table=%d rows=%d headers=%d data_width=%d fallback_cols relname_id=%d name=%d typ=%d num=%d hidden=%d seqnum=%d",
		tableID,
		len(view.Rows),
		len(view.Headers),
		len(view.Headers)-logicalViewDataOffset(view),
		relnameIDCol,
		nameCol,
		typCol,
		numCol,
		hiddenCol,
		seqNumCol,
	)

	if relnameIDCol >= 0 && nameCol >= 0 && typCol >= 0 && numCol >= 0 {
		if cols := buildColumnsFromMoColumnsRowsAt(view, tableID, relnameIDCol, nameCol, typCol, numCol, hiddenCol, seqNumCol); len(cols) > 0 {
			ckpDebugSchemaf("mo_columns schema resolve success table=%d source=fallback cols=%d", tableID, len(cols))
			return cols
		}
	}

	dataWidth := len(view.Headers) - logicalViewDataOffset(view)
	for _, match := range catalogLayoutMatches(dataWidth, moColumnsID) {
		relnameIDCol = catalogColIndexForLayout(match.layout, moColumnsID, "att_relname_id", match.offset)
		nameCol = catalogColIndexForLayout(match.layout, moColumnsID, "attname", match.offset)
		typCol = catalogColIndexForLayout(match.layout, moColumnsID, "atttyp", match.offset)
		numCol = catalogColIndexForLayout(match.layout, moColumnsID, "attnum", match.offset)
		hiddenCol = catalogColIndexForLayout(match.layout, moColumnsID, "att_is_hidden", match.offset)
		seqNumCol = catalogColIndexForLayout(match.layout, moColumnsID, catalog.SystemColAttr_Seqnum, match.offset)
		ckpDebugSchemaf(
			"mo_columns schema try table=%d layout=%s offset=%d relname_id=%d name=%d typ=%d num=%d hidden=%d seqnum=%d",
			tableID,
			match.layout.name,
			match.offset,
			relnameIDCol,
			nameCol,
			typCol,
			numCol,
			hiddenCol,
			seqNumCol,
		)
		if cols := buildColumnsFromMoColumnsRowsAt(view, tableID, relnameIDCol, nameCol, typCol, numCol, hiddenCol, seqNumCol); len(cols) > 0 {
			ckpDebugSchemaf("mo_columns schema resolve success table=%d source=%s offset=%d cols=%d", tableID, match.layout.name, match.offset, len(cols))
			return cols
		}
	}

	ckpDebugSchemaf("mo_columns schema resolve failed table=%d", tableID)
	return nil
}

func buildColumnsFromMoColumnsRowsAt(
	view *LogicalTableView,
	tableID uint64,
	relnameIDCol int,
	nameCol int,
	typCol int,
	numCol int,
	hiddenCol int,
	seqNumCol int,
) []TableColumn {
	tableIDStr := fmt.Sprintf("%d", tableID)
	var cols []TableColumn
	matchedRows := 0
	typeDecodeFailures := 0
	notNullCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_NullAbility)
	hasDefaultCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_HasExpr)
	defaultCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_DefaultExpr)
	constraintCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_ConstraintType)
	unsignedCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_IsUnsigned)
	autoIncrementCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_IsAutoIncrement)
	commentCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_Comment)
	hasUpdateCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_HasUpdate)
	updateCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_Update)
	clusterByCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_IsClusterBy)
	enumCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_EnumValues)
	hasGeneratedCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_HasGenerated)
	generatedCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_Generated)
	for _, fullRow := range view.Rows {
		row := fullRow[logicalViewDataOffset(view):]

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
		matchedRows++

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
			if sqlType, ok := decodeMoColumnSQLType(row[typCol]); ok {
				col.SQLType = sqlType
			} else {
				typeDecodeFailures++
				ckpDebugSchemaf(
					"mo_columns atttyp decode failed table=%d matched_rows=%d relname_id_col=%d name_col=%d typ_col=%d num_col=%d hidden_col=%d seqnum_col=%d column=%q %s",
					tableID,
					matchedRows,
					relnameIDCol,
					nameCol,
					typCol,
					numCol,
					hiddenCol,
					seqNumCol,
					col.Name,
					debugMoColumnTypeCell(row[typCol], fullRow),
				)
			}
		}
		if notNullCol >= 0 && notNullCol < len(row) {
			col.NotNull = isTruthyCatalogValue(row[notNullCol])
		}
		if hasDefaultCol >= 0 && hasDefaultCol < len(row) {
			col.HasDefault = isTruthyCatalogValue(row[hasDefaultCol])
		}
		if defaultCol >= 0 && defaultCol < len(row) {
			if defaultExpr := decodeMoColumnDefault(row[defaultCol]); defaultExpr != "" {
				col.Default = defaultExpr
			}
		}
		if constraintCol >= 0 && constraintCol < len(row) {
			col.ConstraintType = row[constraintCol]
		}
		if unsignedCol >= 0 && unsignedCol < len(row) {
			col.Unsigned = isTruthyCatalogValue(row[unsignedCol])
		}
		if autoIncrementCol >= 0 && autoIncrementCol < len(row) {
			col.AutoIncrement = isTruthyCatalogValue(row[autoIncrementCol])
		}
		if commentCol >= 0 && commentCol < len(row) {
			col.Comment = row[commentCol]
		}
		if hasUpdateCol >= 0 && hasUpdateCol < len(row) && isTruthyCatalogValue(row[hasUpdateCol]) &&
			updateCol >= 0 && updateCol < len(row) {
			col.OnUpdate = decodeMoColumnOnUpdate(row[updateCol])
		}
		if clusterByCol >= 0 && clusterByCol < len(row) {
			col.ClusterBy = isTruthyCatalogValue(row[clusterByCol])
		}
		if enumCol >= 0 && enumCol < len(row) {
			col.EnumValues = row[enumCol]
		}
		if hasGeneratedCol >= 0 && hasGeneratedCol < len(row) && isTruthyCatalogValue(row[hasGeneratedCol]) &&
			generatedCol >= 0 && generatedCol < len(row) {
			col.Generated, col.GeneratedStored = decodeMoColumnGenerated(row[generatedCol])
		}
		cols = append(cols, col)
	}

	if typeDecodeFailures > 0 {
		ckpDebugSchemaf(
			"mo_columns schema rejected table=%d matched_rows=%d type_decode_failures=%d relname_id_col=%d name_col=%d typ_col=%d num_col=%d hidden_col=%d seqnum_col=%d",
			tableID,
			matchedRows,
			typeDecodeFailures,
			relnameIDCol,
			nameCol,
			typCol,
			numCol,
			hiddenCol,
			seqNumCol,
		)
		return nil
	}

	// Sort by position
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Position < cols[j].Position
	})

	if matchedRows > 0 && len(cols) == 0 {
		ckpDebugSchemaf(
			"mo_columns matched rows but produced no visible columns table=%d matched_rows=%d relname_id_col=%d name_col=%d typ_col=%d num_col=%d hidden_col=%d seqnum_col=%d",
			tableID,
			matchedRows,
			relnameIDCol,
			nameCol,
			typCol,
			numCol,
			hiddenCol,
			seqNumCol,
		)
	}
	return cols
}

// getDataColumnsFromView extracts only the data columns (skipping object/block/row meta columns).
// ShowCreateTable returns the CREATE TABLE DDL for a given tableID by reading
// the checkpoint's mo_tables and mo_columns system tables (GCKP + following ICKPs).
//
// Priority:
//  1. Reconstructed from mo_columns (attname, decoded atttyp, attnum, att_is_hidden)
//  2. Hardcoded built-in table schemas (for core system tables like mo_tables, mo_columns, etc.)
//
// ALTER TABLE updates mo_columns but does not rewrite mo_tables.rel_createsql, so
// mo_columns is the authoritative source for the current visible column set.
//
// NOTE:
// We intentionally do not synthesize a CREATE TABLE from raw physical object column
// types when mo_tables/mo_columns metadata is unavailable. Physical object columns may
// include hidden system columns, and without mo_columns we cannot safely distinguish
// visible columns from hidden ones.
func (r *CheckpointReader) ShowCreateTable(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) (string, error) {
	// Read mo_tables first so the mo_columns path can still render the table name
	// while taking the current column set from mo_columns.
	moTablesView, err := r.getTableLogicalView(ctx, moTablesID, snapshotTS)
	if err != nil {
		moTablesView = nil
	}

	// 1. Reconstruct from mo_columns.
	moColumnsView, err := r.getTableLogicalView(ctx, moColumnsID, snapshotTS)
	if err != nil {
		moColumnsView = nil
	}

	var partitionMetadataView *LogicalTableView
	if view, err := r.getPartitionMetadataView(ctx, snapshotTS); err == nil {
		partitionMetadataView = view
	}

	if ddl := createTableDDLFromCatalogViews(tableID, moTablesView, moColumnsView, partitionMetadataView); ddl != "" {
		return ddl, nil
	}

	// 3. Hardcoded built-in table schemas
	layout := currentCatalogLayout
	switch tableID {
	case moTablesID:
		if moTablesView != nil {
			if inferred, _, ok := inferCatalogLayout(len(moTablesView.Headers)-logicalViewMetaCols, moTablesID); ok {
				layout = inferred
			}
		}
	case moColumnsID:
		if moColumnsView != nil {
			if inferred, _, ok := inferCatalogLayout(len(moColumnsView.Headers)-logicalViewMetaCols, moColumnsID); ok {
				layout = inferred
			}
		}
	}
	if ddl := hardcodedCreateTableForLayout(tableID, layout); ddl != "" {
		return ddl, nil
	}

	return "", moerr.NewInternalErrorf(
		ctx,
		"cannot resolve exact schema for table %d from checkpoint metadata; mo_tables/mo_columns data is unavailable or incomplete",
		tableID,
	)
}

// ShowCreateIndexStatements returns ALTER TABLE statements for secondary indexes
// recorded in mo_catalog.mo_indexes. CREATE TABLE reconstruction from mo_columns
// cannot see these rows, so callers that need complete DDL should merge or apply
// these definitions.
func (r *CheckpointReader) ShowCreateIndexStatements(
	ctx context.Context,
	tableID uint64,
	tableName string,
	snapshotTS types.TS,
) ([]string, error) {
	moIndexesTableID, ok, err := r.findCatalogTableID(ctx, snapshotTS, catalog.MO_INDEXES)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	view, err := r.dumpCatalogTableViewWithHeaders(ctx, moIndexesTableID, snapshotTS, moIndexesHeaders)
	if err != nil {
		return nil, err
	}
	return buildCreateIndexStatementsFromMoIndexes(view, tableID, tableName)
}

func (r *CheckpointReader) getPartitionMetadataView(
	ctx context.Context,
	snapshotTS types.TS,
) (*LogicalTableView, error) {
	partitionMetadataTableID, ok, err := r.findCatalogTableID(ctx, snapshotTS, catalog.MOPartitionMetadata)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return r.dumpCatalogTableViewWithHeaders(ctx, partitionMetadataTableID, snapshotTS, moPartitionMetadataHeaders)
}

func (r *CheckpointReader) getPartitionTablesView(
	ctx context.Context,
	snapshotTS types.TS,
) (*LogicalTableView, error) {
	partitionTablesID, ok, err := r.findCatalogTableID(ctx, snapshotTS, catalog.MOPartitionTables)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return r.dumpCatalogTableViewWithHeaders(ctx, partitionTablesID, snapshotTS, moPartitionTablesHeaders)
}

func (r *CheckpointReader) readPartitionTableIDs(
	ctx context.Context,
	snapshotTS types.TS,
	primaryTableID uint64,
) ([]uint64, error) {
	view, err := r.getPartitionTablesView(ctx, snapshotTS)
	if err != nil || view == nil {
		return nil, err
	}
	primarySet := map[uint64]struct{}{primaryTableID: {}}
	return buildPartitionTableIDMap(view, primarySet)[primaryTableID], nil
}

func (r *CheckpointReader) readPartitionTableIDsForPrimaries(
	ctx context.Context,
	snapshotTS types.TS,
	primaryTableIDs map[uint64]struct{},
) (map[uint64][]uint64, error) {
	if len(primaryTableIDs) == 0 {
		return nil, nil
	}
	view, err := r.getPartitionTablesView(ctx, snapshotTS)
	if err != nil || view == nil {
		return nil, err
	}
	return buildPartitionTableIDMap(view, primaryTableIDs), nil
}

func (r *CheckpointReader) readPartitionClause(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) string {
	view, err := r.getPartitionMetadataView(ctx, snapshotTS)
	if err != nil || view == nil {
		return ""
	}
	return buildPartitionClauseFromMetadata(view, tableID)
}

func (r *CheckpointReader) dumpCatalogTableViewWithHeaders(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
	headers []string,
) (*LogicalTableView, error) {
	view, err := r.getTableLogicalView(ctx, tableID, snapshotTS)
	if err != nil {
		return nil, err
	}
	if view == nil {
		return &LogicalTableView{}, nil
	}
	applyCatalogHeadersBySeqNums(view, headers)
	return view, nil
}

func applyCatalogHeadersBySeqNums(view *LogicalTableView, headers []string) {
	if view == nil {
		return
	}
	dataOffset := logicalViewDataOffset(view)
	if dataOffset == 0 && len(view.Headers) < logicalViewMetaCols {
		dataOffset = logicalViewMetaCols
	}
	fixedHeaders := make([]string, len(view.Headers))
	if len(fixedHeaders) < dataOffset {
		fixedHeaders = make([]string, dataOffset)
	}
	copy(fixedHeaders, logicalTableViewMetaHeaders)
	for i := dataOffset; i < len(fixedHeaders); i++ {
		fixedHeaders[i] = fmt.Sprintf("col_%d", i-dataOffset)
	}

	mapped := 0
	for dataIdx, seqNum := range view.ColSeqNums {
		headerIdx := dataOffset + dataIdx
		if headerIdx >= len(fixedHeaders) || int(seqNum) >= len(headers) {
			continue
		}
		fixedHeaders[headerIdx] = headers[seqNum]
		mapped++
	}
	if mapped == 0 {
		for i, header := range headers {
			headerIdx := dataOffset + i
			if headerIdx >= len(fixedHeaders) {
				fixedHeaders = append(fixedHeaders, header)
				continue
			}
			fixedHeaders[headerIdx] = header
		}
	}
	view.Headers = fixedHeaders
}

func (r *CheckpointReader) dumpCatalogTableView(
	ctx context.Context,
	tableID uint64,
	snapshotTS types.TS,
) (*LogicalTableView, error) {
	var buf bytes.Buffer
	if err := r.DumpTableCSVComposed(ctx, &buf, tableID, snapshotTS, WithCSVHeader(true), WithCSVMetaComments(false)); err != nil {
		return nil, err
	}
	reader := csv.NewReader(bytes.NewReader(buf.Bytes()))
	reader.FieldsPerRecord = -1
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return &LogicalTableView{}, nil
	}
	return &LogicalTableView{
		Headers: records[0],
		Rows:    records[1:],
	}, nil
}

func (r *CheckpointReader) findCatalogTableID(
	ctx context.Context,
	snapshotTS types.TS,
	tableName string,
) (uint64, bool, error) {
	moTablesView, err := r.getTableLogicalView(ctx, moTablesID, snapshotTS)
	if err != nil {
		return 0, false, fmt.Errorf("read mo_tables: %w", err)
	}
	tables := buildCatalogTablesFromMoTablesRows(moTablesView)
	for _, table := range tables {
		if table.TableName != tableName {
			continue
		}
		if table.DatabaseName == "" || table.DatabaseName == catalog.MO_CATALOG {
			return table.TableID, true, nil
		}
	}
	return 0, false, nil
}

func buildCreateIndexStatementsFromMoIndexes(
	view *LogicalTableView,
	tableID uint64,
	tableName string,
) ([]string, error) {
	if view == nil {
		return nil, nil
	}
	tableIDCol := view.columnDataIndex("table_id")
	nameCol := view.columnDataIndex("name")
	typeCol := view.columnDataIndex("type")
	algoCol := view.columnDataIndex(catalog.IndexAlgoName)
	paramsCol := view.columnDataIndex(catalog.IndexAlgoParams)
	commentCol := view.columnDataIndex("comment")
	columnNameCol := view.columnDataIndex("column_name")
	ordinalCol := view.columnDataIndex("ordinal_position")
	hiddenCol := view.columnDataIndex("hidden")
	if tableIDCol < 0 || nameCol < 0 || columnNameCol < 0 {
		ckpDebugSchemaf(
			"mo_indexes headers unavailable table=%d headers=%v table_id_col=%d name_col=%d column_name_col=%d",
			tableID,
			view.Headers,
			tableIDCol,
			nameCol,
			columnNameCol,
		)
		return nil, nil
	}

	byName := make(map[string]*indexDDLInfo)
	tableIDStr := strconv.FormatUint(tableID, 10)
	matchedRows := 0
	hiddenRows := 0
	for _, row := range view.Rows {
		if tableIDCol >= len(row) || row[tableIDCol] != tableIDStr {
			continue
		}
		matchedRows++
		if hiddenCol >= 0 && hiddenCol < len(row) && isTruthyCatalogValue(row[hiddenCol]) {
			hiddenRows++
		}
		if nameCol >= len(row) || columnNameCol >= len(row) {
			continue
		}
		name := row[nameCol]
		colName := row[columnNameCol]
		if name == "" || strings.EqualFold(name, "PRIMARY") || colName == "" || catalog.IsAlias(colName) {
			continue
		}
		info := byName[name]
		if info == nil {
			info = &indexDDLInfo{name: name, columns: make(map[string]indexDDLColumn)}
			byName[name] = info
		}
		if typeCol >= 0 && typeCol < len(row) && info.indexType == "" {
			info.indexType = row[typeCol]
		}
		if algoCol >= 0 && algoCol < len(row) && info.algo == "" {
			info.algo = row[algoCol]
		}
		if paramsCol >= 0 && paramsCol < len(row) && info.algoParams == "" {
			info.algoParams = row[paramsCol]
		}
		if commentCol >= 0 && commentCol < len(row) && info.comment == "" {
			info.comment = row[commentCol]
		}
		ordinal := len(info.columns) + 1
		if ordinalCol >= 0 && ordinalCol < len(row) {
			if parsed, err := strconv.Atoi(row[ordinalCol]); err == nil {
				ordinal = parsed
			}
		}
		if existing, ok := info.columns[colName]; !ok || ordinal < existing.ordinal {
			info.columns[colName] = indexDDLColumn{name: colName, ordinal: ordinal}
		}
	}

	names := make([]string, 0, len(byName))
	for name, info := range byName {
		if len(info.columns) > 0 {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	statements := make([]string, 0, len(names))
	for _, name := range names {
		stmt, err := renderCreateIndexStatement(tableName, byName[name])
		if err != nil {
			return nil, err
		}
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	ckpDebugSchemaf(
		"mo_indexes resolved table=%d rows=%d hidden_rows=%d indexes=%d names=%v statements=%d",
		tableID,
		matchedRows,
		hiddenRows,
		len(byName),
		names,
		len(statements),
	)
	return statements, nil
}

func buildPartitionClauseFromMetadata(view *LogicalTableView, tableID uint64) string {
	if view == nil {
		return ""
	}
	tableIDCol := view.columnDataIndex("table_id")
	methodCol := view.columnDataIndex("partition_method")
	descriptionCol := view.columnDataIndex("partition_description")
	countCol := view.columnDataIndex("partition_count")
	if tableIDCol < 0 || methodCol < 0 || descriptionCol < 0 {
		ckpDebugSchemaf(
			"partition metadata headers unavailable table=%d headers=%v table_id_col=%d method_col=%d description_col=%d count_col=%d",
			tableID,
			view.Headers,
			tableIDCol,
			methodCol,
			descriptionCol,
			countCol,
		)
	}
	tableIDStr := strconv.FormatUint(tableID, 10)
	if clause := buildPartitionClauseFromMetadataAt(view, tableIDStr, tableIDCol, methodCol, descriptionCol, countCol); clause != "" {
		return clause
	}
	if clause := buildPartitionClauseFromMetadataByRowShape(view, tableIDStr); clause != "" {
		return clause
	}
	ckpDebugSchemaf("partition metadata not found table=%d rows=%d headers=%v", tableID, len(view.Rows), view.Headers)
	return ""
}

type partitionTableRef struct {
	id      uint64
	ordinal int
	name    string
}

func buildPartitionTableIDMap(view *LogicalTableView, primaryTableIDs map[uint64]struct{}) map[uint64][]uint64 {
	result := make(map[uint64][]uint64)
	if view == nil || len(primaryTableIDs) == 0 {
		return result
	}
	partitionIDCol := view.columnDataIndex("partition_id")
	partitionNameCol := view.columnDataIndex("partition_table_name")
	primaryIDCol := view.columnDataIndex("primary_table_id")
	ordinalCol := view.columnDataIndex("partition_ordinal_position")
	byPrimary := make(map[uint64][]partitionTableRef)
	if partitionIDCol >= 0 && primaryIDCol >= 0 {
		addPartitionTableRefsAt(view, primaryTableIDs, byPrimary, partitionIDCol, primaryIDCol, partitionNameCol, ordinalCol)
	}
	if len(byPrimary) == 0 {
		addPartitionTableRefsByRowShape(view, primaryTableIDs, byPrimary)
	}
	for primaryID, refs := range byPrimary {
		sort.Slice(refs, func(i, j int) bool {
			if refs[i].ordinal != refs[j].ordinal {
				return refs[i].ordinal < refs[j].ordinal
			}
			if refs[i].name != refs[j].name {
				return refs[i].name < refs[j].name
			}
			return refs[i].id < refs[j].id
		})
		ids := make([]uint64, 0, len(refs))
		seen := make(map[uint64]struct{}, len(refs))
		for _, ref := range refs {
			if ref.id == 0 {
				continue
			}
			if _, ok := seen[ref.id]; ok {
				continue
			}
			seen[ref.id] = struct{}{}
			ids = append(ids, ref.id)
		}
		if len(ids) > 0 {
			result[primaryID] = ids
			ckpDebugSchemaf("partition table ids resolved primary=%d partitions=%v", primaryID, ids)
		}
	}
	return result
}

func addPartitionTableRefsAt(view *LogicalTableView, primaryTableIDs map[uint64]struct{}, out map[uint64][]partitionTableRef, partitionIDCol, primaryIDCol, partitionNameCol, ordinalCol int) {
	dataOffset := logicalViewDataOffset(view)
	for _, row := range view.Rows {
		if len(row) < dataOffset {
			continue
		}
		dataRow := row[dataOffset:]
		if partitionIDCol >= len(dataRow) || primaryIDCol >= len(dataRow) {
			continue
		}
		partitionID, ok := parseUintCell(dataRow[partitionIDCol])
		if !ok {
			continue
		}
		primaryID, ok := parseUintCell(dataRow[primaryIDCol])
		if !ok {
			continue
		}
		if _, wanted := primaryTableIDs[primaryID]; !wanted {
			continue
		}
		out[primaryID] = append(out[primaryID], partitionTableRef{
			id:      partitionID,
			ordinal: parseIntCellDefault(cellAt(dataRow, ordinalCol), len(out[primaryID])),
			name:    cellAt(dataRow, partitionNameCol),
		})
	}
}

func addPartitionTableRefsByRowShape(view *LogicalTableView, primaryTableIDs map[uint64]struct{}, out map[uint64][]partitionTableRef) {
	dataOffset := logicalViewDataOffset(view)
	for _, row := range view.Rows {
		if len(row) < dataOffset {
			continue
		}
		dataRow := row[dataOffset:]
		for primaryIDCol, cell := range dataRow {
			primaryID, ok := parseUintCell(cell)
			if !ok {
				continue
			}
			if _, wanted := primaryTableIDs[primaryID]; !wanted {
				continue
			}
			partitionIDCol := primaryIDCol - 2
			partitionNameCol := primaryIDCol - 1
			ordinalCol := primaryIDCol + 2
			partitionID, ok := parseUintCell(cellAt(dataRow, partitionIDCol))
			if !ok {
				continue
			}
			out[primaryID] = append(out[primaryID], partitionTableRef{
				id:      partitionID,
				ordinal: parseIntCellDefault(cellAt(dataRow, ordinalCol), len(out[primaryID])),
				name:    cellAt(dataRow, partitionNameCol),
			})
		}
	}
}

func buildPartitionClauseFromMetadataAt(view *LogicalTableView, tableIDStr string, tableIDCol, methodCol, descriptionCol, countCol int) string {
	if tableIDCol < 0 || methodCol < 0 || descriptionCol < 0 {
		return ""
	}
	dataOffset := logicalViewDataOffset(view)
	for _, row := range view.Rows {
		if len(row) < dataOffset {
			continue
		}
		dataRow := row[dataOffset:]
		if tableIDCol >= len(dataRow) || dataRow[tableIDCol] != tableIDStr {
			continue
		}
		if methodCol >= len(dataRow) || descriptionCol >= len(dataRow) {
			continue
		}
		return renderPartitionClauseFromMetadataCells(tableIDStr, strings.TrimSpace(dataRow[methodCol]), strings.TrimSpace(dataRow[descriptionCol]), cellAt(dataRow, countCol))
	}
	return ""
}

func buildPartitionClauseFromMetadataByRowShape(view *LogicalTableView, tableIDStr string) string {
	dataOffset := logicalViewDataOffset(view)
	for _, row := range view.Rows {
		if len(row) < dataOffset {
			continue
		}
		dataRow := row[dataOffset:]
		for tableIDCol, cell := range dataRow {
			if cell != tableIDStr {
				continue
			}
			methodCol := tableIDCol + 3
			descriptionCol := tableIDCol + 4
			countCol := tableIDCol + 5
			if descriptionCol >= len(dataRow) {
				continue
			}
			clause := renderPartitionClauseFromMetadataCells(tableIDStr, strings.TrimSpace(cellAt(dataRow, methodCol)), strings.TrimSpace(cellAt(dataRow, descriptionCol)), cellAt(dataRow, countCol))
			if clause != "" {
				ckpDebugSchemaf("partition metadata resolved by row shape table=%s table_id_col=%d clause=%q", tableIDStr, tableIDCol, clause)
				return clause
			}
		}
	}
	return ""
}

func renderPartitionClauseFromMetadataCells(tableIDStr, method, description, countText string) string {
	if description == "" || !isPrintableSQLText(description) {
		return ""
	}
	clause := "partition by " + description
	if isAutoPartitionCountMethod(method) {
		if count, err := strconv.Atoi(strings.TrimSpace(countText)); err == nil && count > 0 {
			clause += fmt.Sprintf(" partitions %d", count)
		}
	}
	ckpDebugSchemaf("partition metadata resolved table=%s method=%q description=%q clause=%q", tableIDStr, method, description, clause)
	return clause
}

func cellAt(row []string, idx int) string {
	if idx < 0 || idx >= len(row) {
		return ""
	}
	return row[idx]
}

func parseUintCell(cell string) (uint64, bool) {
	value, err := strconv.ParseUint(strings.TrimSpace(cell), 10, 64)
	return value, err == nil && value != 0
}

func parseIntCellDefault(cell string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(cell))
	if err != nil {
		return fallback
	}
	return value
}

func isAutoPartitionCountMethod(method string) bool {
	switch strings.ToLower(strings.TrimSpace(method)) {
	case "key", "linearkey", "hash", "linearhash":
		return true
	default:
		return false
	}
}

func renderCreateIndexStatement(tableName string, info *indexDDLInfo) (string, error) {
	if info == nil || tableName == "" || len(info.columns) == 0 {
		return "", nil
	}
	cols := make([]indexDDLColumn, 0, len(info.columns))
	for _, col := range info.columns {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool {
		if cols[i].ordinal != cols[j].ordinal {
			return cols[i].ordinal < cols[j].ordinal
		}
		return cols[i].name < cols[j].name
	})

	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(quoteDDLIdent(tableName))
	sb.WriteString(" ADD ")
	if strings.EqualFold(info.indexType, "UNIQUE") {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("KEY ")
	sb.WriteString(quoteDDLIdent(info.name))
	appendIndexAlgorithmDDL(&sb, info.algo)
	sb.WriteString("(")
	for i, col := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(quoteDDLIdent(col.name))
	}
	sb.WriteString(")")
	if err := appendIndexTrailingOptionsDDL(&sb, info.algoParams, info.comment); err != nil {
		return "", err
	}
	sb.WriteString(";")
	return sb.String(), nil
}

func appendIndexAlgorithmDDL(sb *strings.Builder, algo string) {
	algo = strings.TrimSpace(algo)
	if !catalog.IsNullIndexAlgo(algo) {
		sb.WriteString(" USING ")
		sb.WriteString(algo)
	}
}

func appendIndexTrailingOptionsDDL(sb *strings.Builder, algoParams, comment string) error {
	if strings.TrimSpace(algoParams) != "" {
		params, err := catalog.IndexParamsToStringList(algoParams)
		if err != nil {
			return err
		}
		if strings.TrimSpace(params) != "" {
			sb.WriteString(params)
		}
	}
	if comment != "" {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteDDLString(comment))
	}
	return nil
}

func decodeUniqueKeysFromMoTablesConstraint(raw string) (keys []TableUniqueKey) {
	if raw == "" {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			ckpDebugSchemaf("mo_tables constraint decode panic len=%d hex=%s panic=%v", len(raw), debugHexPrefix(raw, 64), r)
			keys = nil
		}
	}()
	c := &engine.ConstraintDef{}
	if err := c.UnmarshalBinary([]byte(raw)); err != nil {
		ckpDebugSchemaf("mo_tables constraint decode failed len=%d hex=%s err=%v", len(raw), debugHexPrefix(raw, 64), err)
		return nil
	}
	for _, ct := range c.Cts {
		indexDef, ok := ct.(*engine.IndexDef)
		if !ok || indexDef == nil {
			continue
		}
		for _, index := range indexDef.Indexes {
			if index == nil || strings.EqualFold(index.IndexName, "PRIMARY") {
				continue
			}
			cols := make([]string, 0, len(index.Parts))
			for _, part := range index.Parts {
				part = strings.TrimSpace(part)
				if part == "" || catalog.IsAlias(part) {
					continue
				}
				cols = append(cols, part)
			}
			if len(cols) == 0 {
				continue
			}
			name := index.IndexName
			if name == "" {
				name = cols[0]
			}
			keys = append(keys, TableUniqueKey{
				Name:       name,
				Columns:    cols,
				Unique:     index.Unique,
				Algo:       index.IndexAlgo,
				AlgoParams: index.IndexAlgoParams,
				Comment:    index.Comment,
			})
		}
	}
	keys = normalizedUniqueKeys(keys)
	ckpDebugSchemaf("mo_tables constraint indexes decoded count=%d", len(keys))
	return keys
}

func decodePrimaryKeyFromMoTablesConstraint(raw string) (cols []string) {
	if raw == "" {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			ckpDebugSchemaf("mo_tables primary key decode panic len=%d hex=%s panic=%v", len(raw), debugHexPrefix(raw, 64), r)
			cols = nil
		}
	}()
	c := &engine.ConstraintDef{}
	if err := c.UnmarshalBinary([]byte(raw)); err != nil {
		ckpDebugSchemaf("mo_tables primary key decode failed len=%d hex=%s err=%v", len(raw), debugHexPrefix(raw, 64), err)
		return nil
	}
	for _, ct := range c.Cts {
		pkDef, ok := ct.(*engine.PrimaryKeyDef)
		if !ok || pkDef == nil || pkDef.Pkey == nil {
			continue
		}
		pk := pkDef.Pkey
		if catalog.IsFakePkName(pk.PkeyColName) {
			continue
		}
		for _, name := range pk.Names {
			name = strings.TrimSpace(name)
			if name != "" && !catalog.IsAlias(name) && !catalog.IsFakePkName(name) && name != catalog.CPrimaryKeyColName {
				cols = append(cols, name)
			}
		}
		if len(cols) == 0 {
			name := strings.TrimSpace(pk.PkeyColName)
			if name != "" && !catalog.IsAlias(name) && !catalog.IsFakePkName(name) && name != catalog.CPrimaryKeyColName {
				cols = append(cols, name)
			}
		}
		if len(cols) > 0 {
			ckpDebugSchemaf("mo_tables primary key decoded cols=%v", cols)
			return cols
		}
	}
	return nil
}

func quoteDDLIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

func quoteDDLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return "'" + s + "'"
}

func isTruthyCatalogValue(s string) bool {
	return s == "1" || strings.EqualFold(s, "true")
}

// getTableName tries to get the table name for a tableID from the LogicalTableView's
// mo_tables data if available, falling back to the table ID as string.
func getTableName(view *LogicalTableView, tableID uint64) string {
	// view here is the table's own data view, not mo_tables.
	// We can't get the name from it, so use table ID
	return fmt.Sprintf("%d", tableID)
}

// buildCreateTableFromMoColumns reconstructs a CREATE TABLE DDL from mo_columns data.
func buildCreateTableFromMoColumns(view *LogicalTableView, tableID uint64, tableNames ...string) string {
	tableName := fmt.Sprintf("%d", tableID)
	if len(tableNames) > 0 && tableNames[0] != "" {
		tableName = tableNames[0]
	}
	tableComment := ""
	if len(tableNames) > 1 && tableNames[1] != "" {
		tableComment = tableNames[1]
	}
	return buildCreateTableFromMoColumnsWithOptions(view, tableID, tableName, tableComment, "", nil, nil)
}

func buildCreateTableFromMoColumnsWithOptions(
	view *LogicalTableView,
	tableID uint64,
	tableName string,
	tableComment string,
	partition string,
	primaryKey []string,
	uniqueKeys []TableUniqueKey,
) string {
	relnameIDCol := fallbackCatalogColIndex(view, moColumnsID, "att_relname_id")
	nameCol := fallbackCatalogColIndex(view, moColumnsID, "attname")
	typCol := fallbackCatalogColIndex(view, moColumnsID, "atttyp")
	numCol := fallbackCatalogColIndex(view, moColumnsID, "attnum")
	hiddenCol := fallbackCatalogColIndex(view, moColumnsID, "att_is_hidden")

	if relnameIDCol >= 0 && nameCol >= 0 && typCol >= 0 && numCol >= 0 {
		if ddl := buildCreateTableFromMoColumnsAt(view, tableID, tableName, tableComment, partition, primaryKey, uniqueKeys, relnameIDCol, nameCol, typCol, numCol, hiddenCol); ddl != "" {
			return ddl
		}
	}

	dataWidth := len(view.Headers) - logicalViewDataOffset(view)
	for _, match := range catalogLayoutMatches(dataWidth, moColumnsID) {
		relnameIDCol = catalogColIndexForLayout(match.layout, moColumnsID, "att_relname_id", match.offset)
		nameCol = catalogColIndexForLayout(match.layout, moColumnsID, "attname", match.offset)
		typCol = catalogColIndexForLayout(match.layout, moColumnsID, "atttyp", match.offset)
		numCol = catalogColIndexForLayout(match.layout, moColumnsID, "attnum", match.offset)
		hiddenCol = catalogColIndexForLayout(match.layout, moColumnsID, "att_is_hidden", match.offset)
		if ddl := buildCreateTableFromMoColumnsAt(view, tableID, tableName, tableComment, partition, primaryKey, uniqueKeys, relnameIDCol, nameCol, typCol, numCol, hiddenCol); ddl != "" {
			return ddl
		}
	}

	return ""
}

func buildCreateTableFromMoColumnsAt(
	view *LogicalTableView,
	tableID uint64,
	tableName string,
	tableComment string,
	partition string,
	primaryKey []string,
	uniqueKeys []TableUniqueKey,
	relnameIDCol int,
	nameCol int,
	typCol int,
	numCol int,
	hiddenCol int,
) string {
	seqNumCol := fallbackCatalogColIndex(view, moColumnsID, catalog.SystemColAttr_Seqnum)
	cols := buildColumnsFromMoColumnsRowsAt(view, tableID, relnameIDCol, nameCol, typCol, numCol, hiddenCol, seqNumCol)
	if len(cols) == 0 {
		return ""
	}
	return renderCreateTableDDLFull(tableName, cols, tableComment, partition, primaryKey, uniqueKeys...)
}

func isPrintableSQLType(sqlType string) bool {
	sqlType = strings.TrimSpace(sqlType)
	if sqlType == "" {
		return false
	}
	hasLetter := false
	for _, r := range sqlType {
		switch {
		case r >= 'A' && r <= 'Z':
			hasLetter = true
		case r >= 'a' && r <= 'z':
			hasLetter = true
		case r >= '0' && r <= '9':
		case strings.ContainsRune(" ()_,'+-.", r):
		default:
			return false
		}
	}
	return hasLetter
}

func decodeMoColumnSQLType(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	if sqlType, ok := decodeMoColumnEncodedSQLType(raw); ok {
		return sqlType, true
	}
	if isPrintableSQLType(raw) {
		return raw, true
	}
	return "", false
}

func decodeMoColumnDefault(raw string) string {
	if raw == "" {
		return ""
	}
	var def plan.Default
	if err := types.Decode([]byte(raw), &def); err == nil {
		return strings.TrimSpace(def.OriginString)
	}
	if isPrintableDDLExpression(raw) {
		return strings.TrimSpace(raw)
	}
	return ""
}

func decodeMoColumnOnUpdate(raw string) string {
	if raw == "" {
		return ""
	}
	var update plan.OnUpdate
	if err := types.Decode([]byte(raw), &update); err == nil {
		return strings.TrimSpace(update.OriginString)
	}
	if isPrintableDDLExpression(raw) {
		return strings.TrimSpace(raw)
	}
	return ""
}

func decodeMoColumnGenerated(raw string) (string, bool) {
	if raw == "" {
		return "", false
	}
	var generated plan.GeneratedCol
	if err := types.Decode([]byte(raw), &generated); err == nil {
		return strings.TrimSpace(generated.OriginString), generated.IsStored
	}
	if isPrintableDDLExpression(raw) {
		return strings.TrimSpace(raw), false
	}
	return "", false
}

func decodeMoColumnEncodedSQLType(raw string) (string, bool) {
	var typ types.Type
	if len(raw) < typ.ProtoSize() {
		return "", false
	}
	if err := types.Decode([]byte(raw), &typ); err == nil && typ.Oid != types.T_any {
		sqlType := typ.DescString()
		if isPrintableSQLType(sqlType) {
			return sqlType, true
		}
	}
	return "", false
}

func isPrintableDDLExpression(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true
	}
	return isPrintableSQLText(expr)
}

func isPrintableSQLText(s string) bool {
	for _, r := range s {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
		case r >= 0x20 && r <= 0x7e:
		default:
			return false
		}
	}
	return true
}

func ckpDebugSchemaf(format string, args ...any) {
	if os.Getenv("MO_TOOL_CKP_DEBUG_SCHEMA") == "" {
		return
	}
	fmt.Fprintf(os.Stderr, "ckp schema debug: "+format+"\n", args...)
}

func debugHexPrefix(s string, limit int) string {
	if limit > 0 && len(s) > limit {
		s = s[:limit]
	}
	return hex.EncodeToString([]byte(s))
}

func debugMoColumnTypeCell(raw string, fullRow []string) string {
	var typ types.Type
	decodeErr := "<not-run>"
	oid := ""
	width := int32(0)
	scale := int32(0)
	desc := ""
	if len(raw) >= typ.ProtoSize() {
		if err := types.Decode([]byte(raw), &typ); err != nil {
			decodeErr = err.Error()
		} else {
			decodeErr = "<nil>"
			oid = typ.Oid.String()
			width = typ.Width
			scale = typ.Scale
			desc = typ.DescString()
		}
	}
	objectName := ""
	blockIdx := ""
	rowIdx := ""
	if len(fullRow) > 0 {
		objectName = fullRow[0]
	}
	if len(fullRow) > 1 {
		blockIdx = fullRow[1]
	}
	if len(fullRow) > 2 {
		rowIdx = fullRow[2]
	}
	return fmt.Sprintf(
		"raw_len=%d raw_hex=%s decode_err=%s oid=%s width=%d scale=%d desc=%q object=%s block=%s row=%s",
		len(raw),
		debugHexPrefix(raw, 64),
		decodeErr,
		oid,
		width,
		scale,
		desc,
		objectName,
		blockIdx,
		rowIdx,
	)
}

func isPrintableCreateTableSQL(ddl string) bool {
	ddl = strings.TrimSpace(ddl)
	if ddl == "" {
		return false
	}
	for _, r := range ddl {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
		case r >= 0x20 && r <= 0x7e:
		default:
			return false
		}
	}
	upper := strings.ToUpper(ddl)
	return strings.HasPrefix(upper, "CREATE TABLE ") ||
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE ") ||
		strings.HasPrefix(upper, "CREATE CLUSTER TABLE ")
}

// hardcodedCreateTable returns the CREATE TABLE DDL for core built-in system tables.
// These tables' schemas are known at compile time and may not appear in the checkpoint's
// mo_tables/mo_columns (due to minimal deployments).
func hardcodedCreateTableForLayout(tableID uint64, layout catalogLayout) string {
	schema := builtinTableSchemaForLayout(layout, tableID)
	if schema == nil {
		return ""
	}
	return renderCreateTableDDL(schema.TableName, schema.Columns)
}

// columnDataIndex returns the data-column index (0-based, after stripping meta columns)
// for the named column, or -1 if not found.
func (v *LogicalTableView) columnDataIndex(colName string) int {
	dataOffset := logicalViewDataOffset(v)
	for i := dataOffset; i < len(v.Headers); i++ {
		if v.Headers[i] == colName {
			return i - dataOffset
		}
	}
	return -1
}

func logicalViewDataOffset(view *LogicalTableView) int {
	if view == nil || len(view.Headers) < logicalViewMetaCols {
		return 0
	}
	for i, h := range logicalTableViewMetaHeaders {
		if view.Headers[i] != h {
			return 0
		}
	}
	return logicalViewMetaCols
}
