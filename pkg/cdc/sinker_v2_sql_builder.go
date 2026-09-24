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
	"strings"

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
	// Column types (excluding internal columns like __mo_rowid)
	insertColTypes []*types.Type

	// Primary key information
	pkColTypes []*types.Type
	isSinglePK bool

	// Immutable statement fragments. The timestamp comment is call-local.
	insertStem   []byte
	insertSuffix []byte
	deleteStem   []byte

	// SQL size limit
	maxSQLSize uint64
}

// NewCDCStatementBuilder creates a new SQL statement builder for a specific table
func NewCDCStatementBuilder(
	dbName, tableName string,
	tableDef *plan.TableDef,
	maxSQLSize uint64,
	_ bool,
) (*CDCStatementBuilder, error) {
	if tableDef == nil {
		return nil, moerr.NewInternalErrorNoCtx("tableDef is required")
	}

	b := &CDCStatementBuilder{
		maxSQLSize: maxSQLSize,
	}
	qualifiedTable := quoteSQLIdentifier(dbName) + "." + quoteSQLIdentifier(tableName)
	insertColNames := make([]string, 0, len(tableDef.Cols))

	// Extract column types (excluding internal columns)
	for i, col := range tableDef.Cols {
		if col == nil {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("column %d is nil", i))
		}
		if col.Name == "" {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("column %d has an empty name", i))
		}
		if _, ok := catalog.InternalColumns[col.Name]; ok {
			continue
		}
		b.insertColTypes = append(b.insertColTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
		insertColNames = append(insertColNames, col.Name)
	}
	if len(b.insertColTypes) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("tableDef has no visible columns")
	}

	// Extract primary key information
	if tableDef.Pkey == nil || len(tableDef.Pkey.Names) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("primary key metadata is required")
	}
	pkColNames := make([]string, 0, len(tableDef.Pkey.Names))
	for _, pkName := range tableDef.Pkey.Names {
		if pkName == "" {
			return nil, moerr.NewInternalErrorNoCtx("primary key column name is empty")
		}
		idx, ok := tableDef.Name2ColIndex[pkName]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("primary key column %q has no column mapping", pkName))
		}
		if idx < 0 || int64(idx) >= int64(len(tableDef.Cols)) {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("primary key column %q has invalid column index %d", pkName, idx))
		}
		col := tableDef.Cols[int(idx)]
		if col == nil || col.Name != pkName {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("primary key column %q mapping does not match column metadata", pkName))
		}
		if _, internal := catalog.InternalColumns[col.Name]; internal {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("primary key column %q is internal", pkName))
		}
		pkColNames = append(pkColNames, pkName)
		b.pkColTypes = append(b.pkColTypes, &types.Type{
			Oid:   types.T(col.Typ.Id),
			Width: col.Typ.Width,
			Scale: col.Typ.Scale,
		})
	}
	b.isSinglePK = len(pkColNames) == 1

	useReplace := false
	for i, indexDef := range tableDef.Indexes {
		if indexDef == nil {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("index %d is nil", i))
		}
		useReplace = useReplace || indexDef.Unique
	}

	if useReplace {
		b.insertStem = []byte("REPLACE INTO " + qualifiedTable + " VALUES ")
		b.insertSuffix = []byte(";")
	} else {
		b.insertStem = []byte("INSERT INTO " + qualifiedTable + " VALUES ")
		b.insertSuffix = buildUpsertSuffix(insertColNames)
	}
	b.deleteStem = []byte("DELETE FROM " + qualifiedTable + " WHERE " + buildPKColumnList(pkColNames) + " IN (")

	return b, nil
}

func quoteSQLIdentifier(raw string) string {
	return "`" + strings.ReplaceAll(raw, "`", "``") + "`"
}

func buildUpsertSuffix(colNames []string) []byte {
	suffix := make([]byte, 0, 32*len(colNames)+len(" ON DUPLICATE KEY UPDATE ")+1)
	suffix = append(suffix, " ON DUPLICATE KEY UPDATE "...)
	for i, name := range colNames {
		if i > 0 {
			suffix = append(suffix, ',')
		}
		quoted := quoteSQLIdentifier(name)
		suffix = append(suffix, quoted...)
		suffix = append(suffix, "=VALUES("...)
		suffix = append(suffix, quoted...)
		suffix = append(suffix, ')')
	}
	return append(suffix, ';')
}

func buildPKColumnList(names []string) string {
	if len(names) == 1 {
		return quoteSQLIdentifier(names[0])
	}
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = quoteSQLIdentifier(name)
	}
	return "(" + strings.Join(quoted, ",") + ")"
}

type statementAssembler struct {
	ctx       context.Context
	prefix    []byte
	suffix    []byte
	limit     uint64
	current   []byte
	completed [][]byte
	hasRows   bool
}

// A small payload-sized floor avoids repeated slice growth for statements near
// the common small limit without returning to maxSQLSize-sized allocations.
const initialStatementCapacity = 256

func newStatementAssembler(ctx context.Context, prefix, suffix []byte, limit uint64) *statementAssembler {
	return &statementAssembler{ctx: ctx, prefix: prefix, suffix: suffix, limit: limit}
}

func fitsWithin(limit uint64, lengths ...int) bool {
	remaining := limit
	for _, length := range lengths {
		if length < 0 || uint64(length) > remaining {
			return false
		}
		remaining -= uint64(length)
	}
	return true
}

func checkedTotalLength(lengths ...int) (int, bool) {
	total := 0
	for _, length := range lengths {
		if length < 0 || length > int(^uint(0)>>1)-total {
			return 0, false
		}
		total += length
	}
	return total, true
}

func (a *statementAssembler) appendRow(rowSQL []byte) error {
	// A row must fit in a fresh complete statement independently of preceding rows.
	if !fitsWithin(a.limit, v2SQLBufReserved, len(a.prefix), len(rowSQL), len(a.suffix)) {
		return moerr.NewInternalError(a.ctx,
			fmt.Sprintf("single row too large for max SQL size: row=%d bytes, max=%d bytes", len(rowSQL), a.limit))
	}

	if a.hasRows && !fitsWithin(a.limit, len(a.current), 1, len(rowSQL), len(a.suffix)) {
		a.flush()
	}
	if !a.hasRows {
		capacity, ok := checkedTotalLength(v2SQLBufReserved, len(a.prefix), len(rowSQL), len(a.suffix))
		if !ok {
			return moerr.NewInternalError(a.ctx, "SQL statement length exceeds platform capacity")
		}
		if capacity < initialStatementCapacity && a.limit >= initialStatementCapacity {
			capacity = initialStatementCapacity
		}
		a.current = make([]byte, v2SQLBufReserved, capacity)
		a.current = append(a.current, a.prefix...)
	} else {
		a.current = append(a.current, ',')
	}
	a.current = append(a.current, rowSQL...)
	a.hasRows = true
	return nil
}

func (a *statementAssembler) flush() {
	if !a.hasRows {
		return
	}
	a.current = append(a.current, a.suffix...)
	a.completed = append(a.completed, a.current)
	a.current = nil
	a.hasRows = false
}

func (a *statementAssembler) finish() [][]byte {
	a.flush()
	return a.completed
}

// BuildInsertSQL constructs INSERT SQL statements from a batch
//
// Returns multiple SQL statements if the batch is too large to fit in one statement.
// Each returned []byte has 5-byte header reserved for mysql driver.
//
// Format:
//
//	/* [fromTs, toTs) */ INSERT INTO `db`.`table` VALUES (row1),(row2),...
//	ON DUPLICATE KEY UPDATE `col1`=VALUES(`col1`),...;
func (b *CDCStatementBuilder) BuildInsertSQL(
	ctx context.Context,
	bat *batch.Batch,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	if bat == nil || bat.RowCount() == 0 {
		return nil, nil
	}
	return b.buildInsertSQL(ctx, newBatchRowIterator(bat), fromTs, toTs)
}

// buildAtomicInsertSQL constructs bounded INSERT statements from the ordered,
// deduplicated rows in an AtomicBatch. It intentionally does not iterate
// AtomicBatch.Batches: a tail range can contain one source batch per commit
// timestamp, and processing those independently would turn one sink command
// back into one SQL statement per timestamp.
func (b *CDCStatementBuilder) buildAtomicInsertSQL(
	ctx context.Context,
	bat *AtomicBatch,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	if bat == nil || bat.RowCount() == 0 {
		return nil, nil
	}
	return b.buildInsertSQL(ctx, bat.GetRowIterator(), fromTs, toTs)
}

func (b *CDCStatementBuilder) buildInsertSQL(
	ctx context.Context,
	iter RowIterator,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	defer iter.Close()

	prefix := b.buildInsertPrefix(fromTs, toTs)
	assembler := newStatementAssembler(ctx, prefix, b.insertSuffix, b.maxSQLSize)

	row := make([]any, len(b.insertColTypes))
	for iter.Next() {
		if err := iter.Row(ctx, row); err != nil {
			return nil, err
		}

		// Convert row to SQL value tuple: (val1,val2,...)
		rowSQL, err := b.formatInsertRow(ctx, row)
		if err != nil {
			return nil, err
		}

		if err = assembler.appendRow(rowSQL); err != nil {
			return nil, err
		}
	}
	return assembler.finish(), nil
}

type batchRowIterator struct {
	bat    *batch.Batch
	offset int
}

func newBatchRowIterator(bat *batch.Batch) *batchRowIterator {
	return &batchRowIterator{bat: bat, offset: -1}
}

func (iter *batchRowIterator) Next() bool {
	iter.offset++
	return iter.offset < iter.bat.RowCount()
}

func (iter *batchRowIterator) Row(ctx context.Context, row []any) error {
	return extractRowFromEveryVector(ctx, iter.bat, iter.offset, row)
}

func (iter *batchRowIterator) Close() {}

// BuildDeleteSQL constructs DELETE SQL statements from atomic batches
//
// Returns multiple SQL statements if the batch is too large.
// Single and composite keys use the same row-value IN syntax on both sinks.
func (b *CDCStatementBuilder) BuildDeleteSQL(
	ctx context.Context,
	atmBatch *AtomicBatch,
	fromTs, toTs types.TS,
) ([][]byte, error) {
	if atmBatch == nil || atmBatch.RowCount() == 0 {
		return nil, nil
	}

	prefix := b.buildDeletePrefix(fromTs, toTs)
	assembler := newStatementAssembler(ctx, prefix, []byte(");"), b.maxSQLSize)

	// Get row iterator
	iter := atmBatch.GetRowIterator()
	defer iter.Close()

	pkRow := make([]any, 1) // AtomicBatch stores PK as one scalar or packed tuple.
	for iter.Next() {
		if err := iter.Row(ctx, pkRow); err != nil {
			return nil, err
		}

		// Convert PK to SQL format
		rowSQL, err := b.formatDeleteRow(ctx, pkRow[0])
		if err != nil {
			return nil, err
		}

		if err = assembler.appendRow(rowSQL); err != nil {
			return nil, err
		}
	}
	return assembler.finish(), nil
}

// buildInsertPrefix builds the INSERT statement prefix with timestamp comment
func (b *CDCStatementBuilder) buildInsertPrefix(fromTs, toTs types.TS) []byte {
	tsComment := fmt.Sprintf("/* [%s, %s) */ ", fromTs.ToString(), toTs.ToString())
	prefix := make([]byte, 0, len(tsComment)+len(b.insertStem))
	prefix = append(prefix, tsComment...)
	return append(prefix, b.insertStem...)
}

// buildDeletePrefix builds the DELETE statement prefix with timestamp comment
func (b *CDCStatementBuilder) buildDeletePrefix(fromTs, toTs types.TS) []byte {
	tsComment := fmt.Sprintf("/* [%s, %s) */ ", fromTs.ToString(), toTs.ToString())
	prefix := make([]byte, 0, len(tsComment)+len(b.deleteStem))
	prefix = append(prefix, tsComment...)
	return append(prefix, b.deleteStem...)
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
// For composite PK: (val1,val2,...)
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
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("composite PK must be []byte, got %T", pkValue))
	}

	pkTuple, _, err := types.UnpackWithSchema(pkBytes)
	if err != nil {
		return nil, err
	}

	if len(pkTuple) != len(b.pkColTypes) {
		return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("PK tuple length mismatch: expected %d, got %d",
			len(b.pkColTypes), len(pkTuple)))
	}

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
	// (val1,val2,...)
	return len(b.pkColTypes) * 50
}
