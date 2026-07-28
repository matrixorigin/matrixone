// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type ParseResult struct {
	kind              int // 0: insert, 1: update, 2: delete, 3: select, 4: insert on duplicate update
	dbName            string
	tableName         string
	projectionColumns []string
	updateColumns     []string
	rows              [][]string
	pkFilters         [][]string
}

func trimQuote(value string) string {
	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") {
		return value[1 : len(value)-1]
	}
	return value
}

func IsSelectClause(inputSql string) (ok bool) {
	// no need to extract, just check the sql is valid
	return strings.HasPrefix(strings.ToUpper(inputSql), "SELECT")
}

func IsInsertClause(inputSql string) (ok bool) {
	return strings.HasPrefix(strings.ToUpper(inputSql), "INSERT")
}

func IsUpdateClause(inputSql string) (ok bool) {
	return strings.HasPrefix(strings.ToUpper(inputSql), "UPDATE")
}

func IsInsertOnDuplicateUpdateClause(inputSql string) (ok bool) {
	return strings.Contains(strings.ToUpper(inputSql), "ON DUPLICATE KEY UPDATE")
}

func ParseSelectByPKs(inputSql string) (result ParseResult, err error) {
	// 1. extract the db name and table name
	schemaRe := regexp.MustCompile(`SELECT ([^F]+) FROM ` + "`([^`]+)`" + `\.` + "`([^`]+)`")
	matches := schemaRe.FindStringSubmatch(inputSql)
	if len(matches) != 4 {
		return result, moerr.NewInternalErrorNoCtxf("invalid select by pk sql: %s, matches: %v", inputSql, matches)
	}
	result.kind = 3
	result.dbName = matches[2]
	result.tableName = matches[3]
	result.projectionColumns = strings.Split(strings.TrimSpace(matches[1]), ",")
	for i, column := range result.projectionColumns {
		result.projectionColumns[i] = trimQuote(strings.TrimSpace(column))
	}
	// 2. extract the pk filters
	// Ex.
	//  1. "SELECT col1, col2, col3 FROM db1.t1 WHERE (col1 = 1 AND col2 = 'test')";
	//     result.pkFilters: [[1, "test"]]
	//  2. "SELECT col2, col3 FROM db1.t1 WHERE (col1 = 1 AND col2 = 'test') OR (col1 = 2 AND col2 = 'test2')";
	//     result.pkFilters: [[1, "test"], [2, "test2"]]
	pkFiltersRe := regexp.MustCompile(`WHERE \(([^)]+)\)(?: OR \(([^)]+)\))*`)
	matches = pkFiltersRe.FindStringSubmatch(inputSql)
	if len(matches) < 2 {
		return result, moerr.NewInternalErrorNoCtxf("invalid select by pk sql: %s, matches: %v", inputSql, matches)
	}

	for i := 1; i < len(matches); i++ {
		if matches[i] == "" {
			continue
		}
		pkFilters := strings.Split(strings.TrimSpace(matches[i]), " AND ")
		row := make([]string, 0, len(pkFilters))
		for _, filter := range pkFilters {
			filterParts := strings.Split(strings.TrimSpace(filter), " = ")
			if len(filterParts) == 2 {
				row = append(row, trimQuote(strings.TrimSpace(filterParts[1])))
			}
		}
		result.pkFilters = append(result.pkFilters, row)
	}
	return result, nil
}

// only support update with where by pk
func ParseUpdate(inputSql string) (result ParseResult, err error) {
	// 1. extract the db name and table name
	// Ex.
	//  1. "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE col1 = 1;"
	//  2. "UPDATE `db1`.`t1` SET col3 = '1-1', col4 = 4 WHERE col1 = 1 AND col2 = 'test';"
	schemaRe := regexp.MustCompile(`UPDATE ` + "`([^`]+)`" + `\.` + "`([^`]+)`")
	matches := schemaRe.FindStringSubmatch(inputSql)
	if len(matches) != 3 {
		return result, moerr.NewInternalErrorNoCtxf("invalid update sql: %s, matches: %v", inputSql, matches)
	}
	result.kind = 1
	result.dbName = matches[1]
	result.tableName = matches[2]

	// 2. extract the update columns
	// Ex.
	//  1. "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE col1 = 1 AND col2 = 'test';"
	//     result.updateColumns: ["col3"]
	//  2. "UPDATE `db1`.`t1` SET col3 = '1-1', col4 = 4 WHERE col1 = 1 AND col2 = 'test';"
	//     result.updateColumns: ["col3", "col4"]
	updateColumnsRe := regexp.MustCompile(`SET (.+?) WHERE`)
	matches = updateColumnsRe.FindStringSubmatch(inputSql)
	if len(matches) != 2 {
		return result, moerr.NewInternalErrorNoCtxf("invalid update sql: %s, matches: %v", inputSql, matches)
	}

	// Split the SET clause by comma and extract column names
	setClause := strings.TrimSpace(matches[1])
	columnAssignments := strings.Split(setClause, ",")
	result.updateColumns = make([]string, 0, len(columnAssignments))

	for _, assignment := range columnAssignments {
		// Each assignment is like "col3 = '1-1'" or "col4 = 4"
		parts := strings.Split(strings.TrimSpace(assignment), "=")
		if len(parts) >= 2 {
			columnName := trimQuote(strings.TrimSpace(parts[0]))
			result.updateColumns = append(result.updateColumns, columnName)
		}
	}

	// extract the pk filters
	// Ex.
	//  1. "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE col1 = 1 AND col2 = 'test';"
	//     result.pkFilters: [[1, "test"]]
	//  2. "UPDATE `db1`.`t1` SET col3 = '1-1', col4 = 4 WHERE col1 = 1;
	//     result.pkFilters: [[1]]
	//  3. "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE (col1 = 1 AND col2 = 'test') OR (col1 = 2 AND col2 = 'test2');"
	//     result.pkFilters: [[1, "test"], [2, "test2"]]
	pkFiltersRe := regexp.MustCompile(`WHERE (?:\(([^)]+)\)(?: OR \(([^)]+)\))*|([^;]+))`)
	matches = pkFiltersRe.FindStringSubmatch(inputSql)
	if len(matches) < 2 {
		logutil.Info("invalid update sql", zap.String("sql", inputSql), zap.Any("matches", matches))
		return result, moerr.NewInternalErrorNoCtxf("invalid update sql: %s, matches: %v", inputSql, matches)
	}

	for i := 1; i < len(matches); i++ {
		if matches[i] == "" {
			continue
		}
		pkFilters := strings.Split(strings.TrimSpace(matches[i]), " AND ")
		row := make([]string, 0, len(pkFilters))
		for _, filter := range pkFilters {
			filterParts := strings.Split(strings.TrimSpace(filter), " = ")
			if len(filterParts) == 2 {
				row = append(row, trimQuote(strings.TrimSpace(filterParts[1])))
			}
		}
		result.pkFilters = append(result.pkFilters, row)
	}

	return result, nil
}

func ParseInsertOnDuplicateUpdate(inputSql string) (result ParseResult, err error) {
	// 1. extract the db name and table name
	insertSql := strings.Split(inputSql, "ON DUPLICATE KEY UPDATE")[0]
	if result, err = ParseInsert(insertSql); err != nil {
		return result, err
	}
	result.kind = 4
	// 2. extract the update columns
	// Ex.
	//  1. "INSERT INTO `db1`.`t1` (col1, col2, col3) VALUES ... ON DUPLICATE KEY UPDATE watermark = VALUES(col2);"
	//     result.updateColumns: ["col2"]
	//  2. "INSERT INTO `db1`.`t1` (col1, col2, col3, col4) VALUES ... ON DUPLICATE KEY UPDATE watermark = VALUES(col1,col3);"
	//     result.updateColumns: ["col1", "col3"]
	updateColumnsRe := regexp.MustCompile(`ON DUPLICATE KEY UPDATE ([^=]+) = VALUES\(([^)]+)\)`)
	matches := updateColumnsRe.FindStringSubmatch(inputSql)
	if len(matches) != 3 {
		return result, moerr.NewInternalErrorNoCtxf("invalid insert on duplicate update sql: %s, matches: %v", inputSql, matches)
	}
	result.updateColumns = strings.Split(strings.TrimSpace(matches[2]), ",")
	for i, column := range result.updateColumns {
		result.updateColumns[i] = trimQuote(strings.TrimSpace(column))
	}
	return result, nil
}

func ParseInsert(inputSql string) (result ParseResult, err error) {
	// 1. extract the db name and table name
	schemaRe := regexp.MustCompile(`INSERT INTO ` + "`([^`]+)`" + `\.` + "`([^`]+)`")
	matches := schemaRe.FindStringSubmatch(inputSql)
	if len(matches) != 3 {
		return result, moerr.NewInternalErrorNoCtxf("invalid insert sql: %s, matches: %v", inputSql, matches)
	}
	result.kind = 0
	result.dbName = matches[1]
	result.tableName = matches[2]

	// 2. extract the projection columns
	// Ex.
	//  1. "INSERT INTO `db1`.`t1` (col1, col2, col3) VALUES ..." +
	//  2. "INSERT INTO `db1`.`t1` (col1, col2, col3, col4) VALUES ..." +
	//  3. "INSERT INTO `db1`.`t1` VALUES ..." +
	projectionColumnsRe := regexp.MustCompile(`INSERT INTO ` + "`([^`]+)`" + `\.` + "`([^`]+)`" + `\s*\(([^)]+)\)`)
	matches = projectionColumnsRe.FindStringSubmatch(inputSql)
	if len(matches) == 4 {
		// trim the spaces and split the columns by comma
		columns := strings.Split(strings.TrimSpace(matches[3]), ",")
		result.projectionColumns = make([]string, 0, len(columns))
		for _, column := range columns {
			result.projectionColumns = append(result.projectionColumns, strings.TrimSpace(column))
		}
	} else {
		// No column definition, projectionColumns will be empty
		result.projectionColumns = []string{}
	}

	// 3. extract the values
	// Ex.
	//  1. "INSERT INTO `db1`.`t1` (col1, col2, col3) VALUES (1, 'test', 'db1')"
	//     result.rows: [["1", "test", "db1"]]
	//  2. "INSERT INTO `db1`.`t1` (col1, col2, col3) VALUES (1, 'test', 'db1'), (2, 'test', 'db2'), (3, 'test', 'db3')"
	//     result.rows: [["1", "test", "db1"], ["2", "test", "db2"], ["3", "test", "db3"]]
	//  3. "INSERT INTO `db1`.`t1` VALUES (1, 'test', 'db1'), (2, 'test', 'db2'), (3, 'test', 'db3')"
	//     result.rows: [["1", "test", "db1"], ["2", "test", "db2"], ["3", "test", "db3"]]

	// Find the position of VALUES keyword
	valuesIndex := strings.Index(strings.ToUpper(inputSql), "VALUES")
	if valuesIndex == -1 {
		return result, moerr.NewInternalErrorNoCtxf("VALUES keyword not found in sql: %s", inputSql)
	}

	// Extract the part after VALUES
	valuesPart := inputSql[valuesIndex:]
	valuesRe := regexp.MustCompile(`\(([^)]+)\)`)
	allMatches := valuesRe.FindAllStringSubmatch(valuesPart, -1)
	if len(allMatches) == 0 {
		return result, moerr.NewInternalErrorNoCtxf("no values found in sql: %s", inputSql)
	}

	result.rows = make([][]string, 0, len(allMatches))
	for _, match := range allMatches {
		if len(match) != 2 {
			continue
		}
		values := strings.Split(strings.TrimSpace(match[1]), ",")
		row := make([]string, 0, len(values))
		for _, value := range values {
			row = append(row, trimQuote(strings.TrimSpace(value)))
		}
		result.rows = append(result.rows, row)
	}
	return result, nil
}

type mockSQLExecutor struct {
	sync.RWMutex
	columnNames  map[string][]string
	columnIds    map[string]map[string]int
	tables       map[string][][]string
	pkColumnsMap map[string][]string
	pkIndexMap   map[string]map[string]int
	// support nulls in the future
}

func NewMockSQLExecutor() *mockSQLExecutor {
	return &mockSQLExecutor{
		columnNames:  make(map[string][]string),
		columnIds:    make(map[string]map[string]int),
		tables:       make(map[string][][]string),
		pkColumnsMap: make(map[string][]string),
		pkIndexMap:   make(map[string]map[string]int),
	}
}

func (m *mockSQLExecutor) Exec(
	ctx context.Context,
	sql string,
	pts ie.SessionOverrideOptions,
) error {
	if IsInsertOnDuplicateUpdateClause(sql) {
		return m.executeInsertOnDuplicateUpdate(sql)
	}
	if IsInsertClause(sql) {
		return m.executeInsert(sql)
	}
	return moerr.NewInternalErrorNoCtxf("invalid sql: %s", sql)
}

func (m *mockSQLExecutor) Query(
	ctx context.Context,
	sql string,
	pts ie.SessionOverrideOptions,
) ie.InternalExecResult {
	if !IsSelectClause(sql) {
		return &InternalExecResultForTest{
			err: moerr.NewInternalErrorNoCtxf("invalid sql: %s", sql),
		}
	}
	return m.executeSelect(sql)
}

func (m *mockSQLExecutor) ApplySessionOverride(
	opts ie.SessionOverrideOptions,
) {
}

func (m *mockSQLExecutor) executeSelect(selectSql string) ie.InternalExecResult {
	selectResult, err := ParseSelectByPKs(selectSql)
	if err != nil {
		return &InternalExecResultForTest{
			err: err,
		}
	}
	dbName := selectResult.dbName
	tableName := selectResult.tableName
	key := GenDbTblKey(dbName, tableName)
	rows := make([][]string, 0, len(selectResult.pkFilters))
	for _, pk := range selectResult.pkFilters {
		row, err := m.GetTableDataByPK(
			dbName,
			tableName,
			pk,
		)
		if err != nil {
			return &InternalExecResultForTest{
				err: err,
			}
		}
		if len(row) == 0 {
			continue
		}
		rows = append(rows, row)
	}

	// fetch the columns specified in projection list
	columnIds := make([]int, 0, len(selectResult.projectionColumns))
	columnIdMap := m.columnIds[key]
	for _, column := range selectResult.projectionColumns {
		columnIds = append(columnIds, columnIdMap[column])
	}
	retData := make([][]any, 0, len(rows))
	for _, row := range rows {
		retRow := make([]any, 0, len(columnIds))
		for _, columnId := range columnIds {
			retRow = append(retRow, row[columnId])
		}
		retData = append(retData, retRow)
	}
	logutil.Info(
		"MockSQLExecutor.executeSelect",
		zap.String("db-name", dbName),
		zap.String("table-name", tableName),
		zap.Int("row-count", len(rows)),
		zap.String("projection-columns", strings.Join(selectResult.projectionColumns, ",")),
		zap.Any("rows", rows),
	)
	return &InternalExecResultForTest{
		affectedRows: uint64(len(rows)),
		resultSet: &MysqlResultSetForTest{
			Columns: selectResult.projectionColumns,
			Data:    retData,
		},
		err: nil,
	}
}

func (m *mockSQLExecutor) executeInsert(insertSql string) error {
	insertResult, err := ParseInsert(insertSql)
	if err != nil {
		return err
	}
	dbName := insertResult.dbName
	tableName := insertResult.tableName
	key := GenDbTblKey(dbName, tableName)

	var projectionColumns []string
	if len(insertResult.projectionColumns) == 0 {
		projectionColumns = make([]string, 0, len(m.columnNames[key]))
		projectionColumns = append(projectionColumns, m.columnNames[key]...)
	} else {
		projectionColumns = insertResult.projectionColumns
	}

	err = m.Insert(
		dbName,
		tableName,
		projectionColumns,
		insertResult.rows,
		false,
	)
	logutil.Info(
		"MockSQLExecutor.executeInsert",
		zap.String("db-name", dbName),
		zap.String("table-name", tableName),
		zap.String("projection-columns", strings.Join(projectionColumns, ",")),
		zap.Any("rows", insertResult.rows),
		zap.Error(err),
	)
	return err
}

func (m *mockSQLExecutor) executeInsertOnDuplicateUpdate(insertSql string) error {
	insertResult, err := ParseInsertOnDuplicateUpdate(insertSql)
	if err != nil {
		return err
	}
	dbName := insertResult.dbName
	tableName := insertResult.tableName
	key := GenDbTblKey(dbName, tableName)

	if len(insertResult.projectionColumns) == 0 {
		insertResult.projectionColumns = make([]string, 0, len(m.columnNames[key]))
		insertResult.projectionColumns = append(insertResult.projectionColumns, m.columnNames[key]...)
	}

	err = m.Insert(
		dbName,
		tableName,
		insertResult.projectionColumns,
		insertResult.rows,
		true,
	)
	logutil.Info(
		"MockSQLExecutor.executeInsertOnDuplicateUpdate",
		zap.String("db-name", dbName),
		zap.String("table-name", tableName),
		zap.String("projection-columns", strings.Join(insertResult.projectionColumns, ",")),
		zap.Any("rows", insertResult.rows),
		zap.Error(err),
	)
	return err
}

func (m *mockSQLExecutor) CreateTable(
	dbName string,
	tableName string,
	columns []string,
	pkColumns []string,
) error {
	m.Lock()
	defer m.Unlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; ok {
		return moerr.NewInternalErrorNoCtxf("table %s already exists", key)
	}
	m.columnNames[key] = columns
	m.columnIds[key] = make(map[string]int)
	for i, column := range columns {
		m.columnIds[key][column] = i
	}
	m.tables[key] = make([][]string, 0, 100)
	m.pkColumnsMap[key] = append(m.pkColumnsMap[key], pkColumns...)
	if len(pkColumns) > 0 {
		m.pkIndexMap[key] = make(map[string]int)
	}
	return nil
}

func (m *mockSQLExecutor) RemoveTable(
	dbName string,
	tableName string,
) error {
	m.Lock()
	defer m.Unlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; !ok {
		return moerr.NewInternalErrorNoCtxf("table %s not found", key)
	}
	delete(m.tables, key)
	delete(m.columnNames, key)
	delete(m.columnIds, key)
	delete(m.pkColumnsMap, key)
	delete(m.pkIndexMap, key)
	return nil
}

func (m *mockSQLExecutor) Delete(
	dbName string,
	tableName string,
	pkValues []string,
) error {
	m.Lock()
	defer m.Unlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; !ok {
		return moerr.NewInternalErrorNoCtxf("table %s not found", key)
	}
	pkColumns, hasPK := m.pkColumnsMap[key]
	if !hasPK {
		return moerr.NewInternalErrorNoCtxf("table %s has no primary key", key)
	}
	if len(pkValues) != len(pkColumns) {
		return moerr.NewInternalErrorNoCtxf("pk values length mismatch: %d != %d", len(pkValues), len(pkColumns))
	}
	pkValue := strings.Join(pkValues, ",")
	offset, ok := m.pkIndexMap[key][pkValue]
	if !ok {
		return nil
	}
	tableIndex := m.pkIndexMap[key]
	delete(tableIndex, pkValue)
	tableData := m.tables[key]
	tableData = append(tableData[:offset], tableData[offset+1:]...)
	m.tables[key] = tableData
	// update the offset of the other pk values
	for pk, idx := range tableIndex {
		if idx > offset {
			tableIndex[pk] = idx - 1
		}
	}
	return nil
}

// when onDuplicateUpdate is true, currently this mock executor
// only supports:
//  1. when there is no primary key, the columns should be the same as the full columns
//  2. when there is primary key, the columns should include all the primary key columns
//     and all the pks in the tuples should be found in the table
//  3. remove this constraint when nulls are supported in the future: TODO
func (m *mockSQLExecutor) Insert(
	dbName string,
	tableName string,
	columns []string,
	tuples [][]string,
	onDuplicateUpdate bool,
) error {
	m.Lock()
	defer m.Unlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; !ok {
		return moerr.NewInternalErrorNoCtxf("table %s not found", key)
	}
	// the table full columns are: ['a', 'b', 'c'] and the
	// columns here may be ['a', 'b'] or ['a', 'c'] or ['b', 'c']
	// no check the columns here
	fullColumns := m.columnNames[key]
	columnIds := m.columnIds[key]
	pkColumns, hasPK := m.pkColumnsMap[key]
	pkIndex := m.pkIndexMap[key]
	tableData := m.tables[key]

	// 0: invalid
	// 1: insert without dedup
	// 2: insert with dedup and error on duplicate
	// 3: update only
	var insertMode int

	// onDuplicateUpdate constraint check:
	if onDuplicateUpdate {
		if hasPK {
			for _, pk := range pkColumns {
				if !slices.Contains(columns, pk) {
					return moerr.NewInternalErrorNoCtxf("primary key %s not found in columns %v", pk, columns)
				}
			}
			insertMode = 3
		} else {
			// no primary key, the columns should be the same as the full columns
			if len(columns) != len(fullColumns) {
				return moerr.NewInternalErrorNoCtxf("columns length mismatch: %d != %d", len(columns), len(fullColumns))
			}
			insertMode = 1
		}
	} else {
		if len(columns) != len(fullColumns) {
			return moerr.NewInternalErrorNoCtxf("columns length mismatch: %d != %d", len(columns), len(fullColumns))
		}
		if hasPK {
			insertMode = 2
		} else {
			insertMode = 1
		}
	}

	columnsIdMap := make(map[int]int)
	for i, column := range columns {
		// input columns: ['b', 'a', 'c'], full columns: ['a', 'b', 'c']
		// columnsIdMap: {1: 0, 0: 1, 2: 2}
		columnsIdMap[columnIds[column]] = i
	}
	// if pkColumns is ['a', 'b'], the input columns is ['b', 'a', 'c']
	// pkColumnIds: [1, 0]
	pkColumnIds := make([]int, len(pkColumns))
	for i, pk := range pkColumns {
		fullColIdx, ok := columnIds[pk]
		if !ok {
			return moerr.NewInternalErrorNoCtxf("primary key column %s not found in table columns", pk)
		}
		inputColIdx, ok := columnsIdMap[fullColIdx]
		if !ok {
			return moerr.NewInternalErrorNoCtxf("primary key column %s not found in input columns", pk)
		}
		pkColumnIds[i] = inputColIdx
	}

	// insert mode check:
	switch insertMode {
	case 1:
		// insert without dedup
		for _, tuple := range tuples {
			row := make([]string, len(fullColumns))
			for i, cell := range tuple {
				row[columnsIdMap[i]] = cell
			}
		}
	case 2:
		// insert with dedup and error on duplicate
		newPKs := make([]string, 0, len(tuples))
		newRows := make([][]string, 0, len(tuples))
		for _, tuple := range tuples {
			pkValues := make([]string, len(pkColumns))
			for idx, id := range pkColumnIds {
				pkValues[idx] = tuple[id]
			}
			pkValue := strings.Join(pkValues, ",")
			if _, ok := pkIndex[pkValue]; ok {
				return moerr.NewInternalErrorNoCtxf("primary key %s already exists", pkValue)
			}
			newPKs = append(newPKs, pkValue)
			row := make([]string, len(fullColumns))
			for i, cell := range tuple {
				row[columnsIdMap[i]] = cell
			}
			newRows = append(newRows, row)
		}
		// insert the new rows
		for i, row := range newRows {
			tableData = append(tableData, row)
			pk := newPKs[i]
			pkIndex[pk] = len(pkIndex)
		}
		m.tables[key] = tableData
	case 3:
		// update only
		// find the old row by the pk and update the row
		pkValues := make([]string, len(pkColumns))
		for _, tuple := range tuples {
			pkValues = pkValues[:0]
			for _, id := range pkColumnIds {
				if id >= len(tuple) {
					return moerr.NewInternalErrorNoCtxf("primary key column index %d out of range (tuple length: %d)", id, len(tuple))
				}
				pkValues = append(pkValues, tuple[id])
			}
			pkValue := strings.Join(pkValues, ",")
			// if the pkValue is not found, skip update
			offset, ok := pkIndex[pkValue]
			if !ok {
				continue
			}
			oldRow := tableData[offset]
			// Only update columns that are present in the input tuple
			// i is the input column index, columns[i] is the column name
			// columnIds[columns[i]] is the full column index in the table
			for i, cell := range tuple {
				if i >= len(columns) {
					return moerr.NewInternalErrorNoCtxf("tuple index %d out of range (columns length: %d)", i, len(columns))
				}
				columnName := columns[i]
				fullColIdx, ok := columnIds[columnName]
				if !ok {
					// Column not found in table - skip (shouldn't happen if SQL is correct)
					continue
				}
				if fullColIdx >= len(oldRow) {
					return moerr.NewInternalErrorNoCtxf("column index %d out of range (row length: %d)", fullColIdx, len(oldRow))
				}
				oldRow[fullColIdx] = cell
			}
			tableData[offset] = oldRow
		}
		m.tables[key] = tableData
	}
	return nil
}

func (m *mockSQLExecutor) RowCount(
	dbName string,
	tableName string,
) int {
	m.RLock()
	defer m.RUnlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; !ok {
		return 0
	}
	return len(m.tables[key])
}

func (m *mockSQLExecutor) GetTableDataByPK(
	dbName string,
	tableName string,
	pkValues []string,
) ([]string, error) {
	m.RLock()
	defer m.RUnlock()
	key := GenDbTblKey(dbName, tableName)
	if _, ok := m.tables[key]; !ok {
		return nil, moerr.NewInternalErrorNoCtxf("table %s not found", key)
	}
	pkColumns, hasPK := m.pkColumnsMap[key]
	if !hasPK {
		return nil, moerr.NewInternalErrorNoCtxf("table %s has no primary key", key)
	}
	if len(pkValues) != len(pkColumns) {
		return nil, moerr.NewInternalErrorNoCtxf("pk values length mismatch: %d != %d", len(pkValues), len(pkColumns))
	}
	columns := m.columnNames[key]
	pkIndex := m.pkIndexMap[key]
	pkValue := strings.Join(pkValues, ",")
	offset, ok := pkIndex[pkValue]
	if !ok {
		return nil, nil
	}
	tableData := m.tables[key]
	row := tableData[offset]
	ret := make([]string, 0, len(columns))
	for i := range columns {
		ret = append(ret, row[i])
	}

	return ret, nil
}

func initMockSQLExecutorForWatermarkUpdater(t *testing.T) *mockSQLExecutor {
	ie := NewMockSQLExecutor()
	ie.CreateTable(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	return ie
}

func InitCDCWatermarkUpdaterForTest(t *testing.T) (*CDCWatermarkUpdater, *mockSQLExecutor) {
	ie := initMockSQLExecutorForWatermarkUpdater(t)
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
		WithCronJobInterval(time.Millisecond*1),
	)
	return u, ie
}

type MysqlResultSetForTest struct {
	//column information
	Columns []string
	//column name --> column index
	Name2Index map[string]uint64
	//data
	Data [][]interface{}
}

type InternalExecResultForTest struct {
	affectedRows uint64
	resultSet    *MysqlResultSetForTest
	err          error
}

func (res *InternalExecResultForTest) GetUint64(ctx context.Context, i uint64, j uint64) (uint64, error) {
	return strconv.ParseUint(res.resultSet.Data[i][j].(string), 10, 64)
}

func (res *InternalExecResultForTest) Error() error {
	return res.err
}

func (res *InternalExecResultForTest) ColumnCount() uint64 {
	return 1
}

func (res *InternalExecResultForTest) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	return "test", 1, true, nil
}

func (res *InternalExecResultForTest) RowCount() uint64 {
	return uint64(len(res.resultSet.Data))
}

func (res *InternalExecResultForTest) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return res.resultSet.Data[i], nil
}

func (res *InternalExecResultForTest) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return nil, nil
}

func (res *InternalExecResultForTest) GetFloat64(ctx context.Context, ridx uint64, cid uint64) (float64, error) {
	return 0.0, nil
}

func (res *InternalExecResultForTest) GetString(ctx context.Context, i uint64, j uint64) (string, error) {
	return res.resultSet.Data[i][j].(string), nil
}
