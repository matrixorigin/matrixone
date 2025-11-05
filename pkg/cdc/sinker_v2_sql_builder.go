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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

// CDCStatementBuilder constructs SQL statements for CDC sink operations.
//
// Design Principles:
// - Stateless: Each method is a pure function with no side effects
// - Reusable: Can be shared across multiple sinkers
// - Testable: Easy to unit test in isolation
// - Efficient: Minimizes allocations through buffer reuse
//
// The builder handles:
// - INSERT statements from snapshot/tail data
// - DELETE statements with single or composite primary keys
// - SQL size limits (splits large statements into multiple)
// - All MatrixOne data types serialization
type CDCStatementBuilder struct {
	// Table information
	dbName    string
	tableName string

	// Table schema
	tableDef *plan.TableDef

	// Column types (excluding internal columns like __mo_rowid)
	insertColTypes []*types.Type

	// Primary key information
	pkColNames []string
	pkColTypes []*types.Type
	isSinglePK bool

	// SQL size limit
	maxSQLSize uint64

	// Flags
	isMO bool // Whether target is MatrixOne (affects DELETE syntax)
}

// NewCDCStatementBuilder creates a new SQL statement builder for a specific table
func NewCDCStatementBuilder(
	dbName, tableName string,
	tableDef *plan.TableDef,
	maxSQLSize uint64,
	isMO bool,
) (*CDCStatementBuilder, error) {
	if tableDef == nil {
		return nil, fmt.Errorf("tableDef is required")
	}

	b := &CDCStatementBuilder{
		dbName:     dbName,
		tableName:  tableName,
		tableDef:   tableDef,
		maxSQLSize: maxSQLSize,
		isMO:       isMO,
	}

	// Extract column types (excluding internal columns)
	for _, col := range tableDef.Cols {
		if _, ok := catalog.InternalColumns[col.Name]; ok {
			continue
		}
		b.insertColTypes = append(b.insertColTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
	}

	// Extract primary key information
	for _, pkName := range tableDef.Pkey.Names {
		b.pkColNames = append(b.pkColNames, pkName)
		col := tableDef.Cols[tableDef.Name2ColIndex[pkName]]
		b.pkColTypes = append(b.pkColTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
	}
	b.isSinglePK = len(b.pkColNames) == 1

	return b, nil
}

// BuildInsertSQL constructs INSERT SQL statements from a batch
//
// Returns multiple SQL statements if the batch is too large to fit in one statement.
// Each returned []byte has 5-byte header reserved for mysql driver.
//
// Format:
//
//	/* [fromTs, toTs) */ REPLACE INTO `db`.`table` VALUES (row1),(row2),...;
func (b *CDCStatementBuilder) BuildInsertSQL(
	ctx context.Context,
	bat *batch.Batch,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	if bat == nil || bat.RowCount() == 0 {
		return nil, nil
	}

	var sqls [][]byte
	var currentSQL []byte

	// Prepare SQL prefix with timestamp comment
	prefix := b.buildInsertPrefix(fromTs, toTs)
	suffix := []byte(";")

	// Initialize first SQL statement
	currentSQL = make([]byte, v2SQLBufReserved, b.maxSQLSize)
	currentSQL = append(currentSQL, prefix...)
	firstRow := true

	// Process each row
	rowCount := bat.RowCount()
	for i := 0; i < rowCount; i++ {
		// Extract row data
		row := make([]any, len(b.insertColTypes))
		if err := extractRowFromEveryVector(ctx, bat, i, row); err != nil {
			return nil, err
		}

		// Convert row to SQL value tuple: (val1,val2,...)
		rowSQL, err := b.formatInsertRow(ctx, row)
		if err != nil {
			return nil, err
		}

		// Check if adding this row would exceed max size
		neededSpace := len(rowSQL)
		if !firstRow {
			neededSpace += 1 // For comma separator
		}
		neededSpace += len(suffix)

		if len(currentSQL)+neededSpace > int(b.maxSQLSize) {
			// Finish current SQL
			if !firstRow {
				currentSQL = append(currentSQL, suffix...)
				sqls = append(sqls, currentSQL)

				// Start new SQL
				currentSQL = make([]byte, v2SQLBufReserved, b.maxSQLSize)
				currentSQL = append(currentSQL, prefix...)
				firstRow = true
			} else {
				// Single row too large!
				return nil, moerr.NewInternalError(ctx,
					fmt.Sprintf("single row too large for max SQL size: row=%d bytes, max=%d bytes",
						len(rowSQL), b.maxSQLSize))
			}
		}

		// Append row to current SQL
		if !firstRow {
			currentSQL = append(currentSQL, ',')
		}
		currentSQL = append(currentSQL, rowSQL...)
		firstRow = false
	}

	// Finish last SQL statement
	if !firstRow {
		currentSQL = append(currentSQL, suffix...)
		sqls = append(sqls, currentSQL)
	}

	return sqls, nil
}

// BuildDeleteSQL constructs DELETE SQL statements from atomic batches
//
// Returns multiple SQL statements if the batch is too large.
// Supports two formats:
// 1. Single PK: DELETE FROM t WHERE pk IN ((val1),(val2),...)
// 2. Composite PK (MO): DELETE FROM t WHERE pk1=a1 AND pk2=a2 OR pk1=b1 AND pk2=b2 ...
// 3. Composite PK (MySQL): DELETE FROM t WHERE (pk1,pk2) IN ((a1,a2),(b1,b2),...)
func (b *CDCStatementBuilder) BuildDeleteSQL(
	ctx context.Context,
	atmBatch *AtomicBatch,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	if atmBatch == nil || atmBatch.RowCount() == 0 {
		return nil, nil
	}

	var sqls [][]byte
	var currentSQL []byte

	// Prepare SQL prefix with timestamp comment
	prefix := b.buildDeletePrefix(fromTs, toTs)
	suffix := b.buildDeleteSuffix()

	// Initialize first SQL statement
	currentSQL = make([]byte, v2SQLBufReserved, b.maxSQLSize)
	currentSQL = append(currentSQL, prefix...)
	firstRow := true

	// Get row iterator
	iter := atmBatch.GetRowIterator()
	defer iter.Close()

	// Process each row
	for iter.Next() {
		// Extract primary key value(s)
		pkRow := make([]any, 1) // AtomicBatch stores PK as single value
		if err := iter.Row(ctx, pkRow); err != nil {
			return nil, err
		}

		// Convert PK to SQL format
		rowSQL, err := b.formatDeleteRow(ctx, pkRow[0])
		if err != nil {
			return nil, err
		}

		// Check if adding this row would exceed max size
		neededSpace := len(rowSQL)
		if !firstRow {
			neededSpace += len(b.buildDeleteRowSeparator())
		}
		neededSpace += len(suffix)

		if len(currentSQL)+neededSpace > int(b.maxSQLSize) {
			// Finish current SQL
			if !firstRow {
				currentSQL = append(currentSQL, suffix...)
				sqls = append(sqls, currentSQL)

				// Start new SQL
				currentSQL = make([]byte, v2SQLBufReserved, b.maxSQLSize)
				currentSQL = append(currentSQL, prefix...)
				firstRow = true
			} else {
				// Single row too large!
				return nil, moerr.NewInternalError(ctx,
					"single row too large for max SQL size")
			}
		}

		// Append row to current SQL
		if !firstRow {
			currentSQL = append(currentSQL, b.buildDeleteRowSeparator()...)
		}
		currentSQL = append(currentSQL, rowSQL...)
		firstRow = false
	}

	// Finish last SQL statement
	if !firstRow {
		currentSQL = append(currentSQL, suffix...)
		sqls = append(sqls, currentSQL)
	}

	return sqls, nil
}

// buildInsertPrefix builds the INSERT statement prefix with timestamp comment
func (b *CDCStatementBuilder) buildInsertPrefix(fromTs, toTs types.TS) []byte {
	tsComment := fmt.Sprintf("/* [%s, %s) */ ", fromTs.ToString(), toTs.ToString())
	prefix := fmt.Sprintf("%sREPLACE INTO `%s`.`%s` VALUES ", tsComment, b.dbName, b.tableName)
	return []byte(prefix)
}

// buildDeletePrefix builds the DELETE statement prefix with timestamp comment
func (b *CDCStatementBuilder) buildDeletePrefix(fromTs, toTs types.TS) []byte {
	tsComment := fmt.Sprintf("/* [%s, %s) */ ", fromTs.ToString(), toTs.ToString())

	if b.isMO && !b.isSinglePK {
		// MO multi-column PK: DELETE FROM t WHERE pk1=a1 AND pk2=a2 OR ...
		prefix := fmt.Sprintf("%sDELETE FROM `%s`.`%s` WHERE ", tsComment, b.dbName, b.tableName)
		return []byte(prefix)
	} else {
		// Single PK or MySQL: DELETE FROM t WHERE (pk1,pk2) IN (...)
		pkStr := b.buildPKColumnList()
		prefix := fmt.Sprintf("%sDELETE FROM `%s`.`%s` WHERE %s IN (",
			tsComment, b.dbName, b.tableName, pkStr)
		return []byte(prefix)
	}
}

// buildDeleteSuffix builds the DELETE statement suffix
func (b *CDCStatementBuilder) buildDeleteSuffix() []byte {
	if b.isMO && !b.isSinglePK {
		return []byte(";")
	}
	return []byte(");")
}

// buildDeleteRowSeparator returns the separator between DELETE row conditions
func (b *CDCStatementBuilder) buildDeleteRowSeparator() []byte {
	if b.isMO && !b.isSinglePK {
		return []byte(" or ")
	}
	return []byte(",")
}

// buildPKColumnList builds the primary key column list for DELETE IN clause
// Single PK: "pk1"
// Composite PK: "(pk1,pk2,pk3)"
func (b *CDCStatementBuilder) buildPKColumnList() string {
	if b.isSinglePK {
		return b.pkColNames[0]
	}

	result := "("
	for i, name := range b.pkColNames {
		if i > 0 {
			result += ","
		}
		result += name
	}
	result += ")"
	return result
}

// formatInsertRow formats a row for INSERT statement: (val1,val2,...)
func (b *CDCStatementBuilder) formatInsertRow(ctx context.Context, row []any) ([]byte, error) {
	buf := make([]byte, 0, 256) // Estimated row size
	buf = append(buf, '(')

	for i, val := range row {
		if i > 0 {
			buf = append(buf, ',')
		}

		var err error
		buf, err = convertColIntoSql(ctx, val, b.insertColTypes[i], buf)
		if err != nil {
			return nil, err
		}
	}

	buf = append(buf, ')')
	return buf, nil
}

// formatDeleteRow formats a primary key value for DELETE statement
//
// For single PK: (val)
// For composite PK (MO): pk1=a1 AND pk2=a2
// For composite PK (MySQL): (val1,val2,...)
func (b *CDCStatementBuilder) formatDeleteRow(ctx context.Context, pkValue any) ([]byte, error) {
	buf := make([]byte, 0, 128)

	if b.isSinglePK {
		// Single PK: (value)
		buf = append(buf, '(')
		var err error
		buf, err = convertColIntoSql(ctx, pkValue, b.pkColTypes[0], buf)
		if err != nil {
			return nil, err
		}
		buf = append(buf, ')')
		return buf, nil
	}

	// Composite PK: unpack tuple
	pkBytes, ok := pkValue.([]byte)
	if !ok {
		return nil, fmt.Errorf("composite PK must be []byte, got %T", pkValue)
	}

	pkTuple, _, err := types.UnpackWithSchema(pkBytes)
	if err != nil {
		return nil, err
	}

	if len(pkTuple) != len(b.pkColTypes) {
		return nil, fmt.Errorf("PK tuple length mismatch: expected %d, got %d",
			len(b.pkColTypes), len(pkTuple))
	}

	if b.isMO {
		// MO format: pk1=a1 AND pk2=a2
		for i, val := range pkTuple {
			if i > 0 {
				buf = append(buf, []byte(" and ")...)
			}
			buf = append(buf, []byte(b.pkColNames[i]+"=")...)
			buf, err = convertColIntoSql(ctx, val, b.pkColTypes[i], buf)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// MySQL format: (val1,val2,...)
		buf = append(buf, '(')
		for i, val := range pkTuple {
			if i > 0 {
				buf = append(buf, ',')
			}
			buf, err = convertColIntoSql(ctx, val, b.pkColTypes[i], buf)
			if err != nil {
				return nil, err
			}
		}
		buf = append(buf, ')')
	}

	return buf, nil
}

// EstimateInsertRowSize estimates the SQL size needed for one row
// Used to determine batch sizing
func (b *CDCStatementBuilder) EstimateInsertRowSize() int {
	// Conservative estimate: 50 bytes per column on average
	return len(b.insertColTypes) * 50
}

// EstimateDeleteRowSize estimates the SQL size needed for one delete condition
func (b *CDCStatementBuilder) EstimateDeleteRowSize() int {
	if b.isSinglePK {
		return 50 // (value)
	}
	if b.isMO {
		// pk1=val1 AND pk2=val2 ...
		return len(b.pkColNames) * 60
	}
	// (val1,val2,...)
	return len(b.pkColNames) * 50
}
