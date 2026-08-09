// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type checkConstraintRow struct {
	schema string
	name   string
	clause string
}

type checkConstraintsState struct {
	simpleOneBatchState
	collectRows func(*process.Process) ([]checkConstraintRow, error)
}

const (
	checkConstraintCatalogColumn = iota
	checkConstraintSchemaColumn
	checkConstraintNameColumn
	checkConstraintClauseColumn
)

// checkConstraintOutputPositions maps the logical CHECK_CONSTRAINTS columns to
// the vectors retained by the optimizer.  A projection such as COUNT(*) may
// prune CHECK_CLAUSE (or any other column), so the executor must not assume
// that all four vectors are present.
func checkConstraintOutputPositions(attrs []string) [4]int {
	positions := [4]int{-1, -1, -1, -1}
	for i, attr := range attrs {
		switch strings.ToLower(attr) {
		case "constraint_catalog":
			positions[checkConstraintCatalogColumn] = i
		case "constraint_schema":
			positions[checkConstraintSchemaColumn] = i
		case "constraint_name":
			positions[checkConstraintNameColumn] = i
		case "check_clause":
			positions[checkConstraintClauseColumn] = i
		}
	}
	return positions
}

func checkConstraintsPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &checkConstraintsState{collectRows: collectCheckConstraintRows}, nil
}

func (s *checkConstraintsState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	s.startPreamble(tf, proc, nthRow)
	if nthRow != 0 {
		return nil
	}

	collectRows := s.collectRows
	if collectRows == nil {
		collectRows = collectCheckConstraintRows
	}
	rows, err := collectRows(proc)
	if err != nil {
		return err
	}

	positions := checkConstraintOutputPositions(s.batch.Attrs)
	for _, row := range rows {
		if err := appendCheckConstraintRow(s.batch.Vecs, positions, row, proc); err != nil {
			return err
		}
	}
	s.batch.SetRowCount(len(rows))
	return nil
}

func appendCheckConstraintRow(
	vectors []*vector.Vector,
	positions [4]int,
	row checkConstraintRow,
	proc *process.Process,
) error {
	values := [...]string{
		catalog.SystemCatalogName,
		row.schema,
		row.name,
		row.clause,
	}
	for column, value := range values {
		vectorIndex := positions[column]
		if vectorIndex < 0 {
			continue
		}
		if vectorIndex >= len(vectors) {
			return moerr.NewInternalErrorf(proc.Ctx,
				"check constraints output vector %d is unavailable", vectorIndex)
		}
		if err := vector.AppendBytes(vectors[vectorIndex], []byte(value), false, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func collectCheckConstraintRows(proc *process.Process) ([]checkConstraintRow, error) {
	catalogQuery := "SELECT tbl.reldatabase, tbl.extra_info, " +
		"tbl.rel_createsql, tbl.relkind " +
		"FROM mo_catalog.mo_tables tbl " +
		"WHERE tbl.account_id = current_account_id() AND " +
		catalog.NonTemporaryTableSQLPredicate("tbl")
	result, err := sqlexec.RunSql(sqlexec.NewSqlProcess(proc), catalogQuery)
	if err != nil {
		return nil, err
	}
	return collectCheckConstraintRowsFromResult(result)
}

// collectCheckConstraintRowsFromResult decodes the catalog result separately
// from the SQL execution boundary.  Besides keeping the executor path small,
// this makes the metadata decoding testable without starting a MatrixOne
// service in a table-function unit test.
func collectCheckConstraintRowsFromResult(result executor.Result) ([]checkConstraintRow, error) {
	defer result.Close()

	rows := make([]checkConstraintRow, 0)
	for _, bat := range result.Batches {
		if len(bat.Vecs) < 2 {
			continue
		}
		databaseNames := bat.Vecs[0]
		extraInfos := bat.Vecs[1]
		for row := 0; row < bat.RowCount(); row++ {
			if databaseNames.IsNull(uint64(row)) {
				continue
			}
			schema := databaseNames.GetStringAt(row)
			var extraBytes []byte
			if !extraInfos.IsNull(uint64(row)) {
				extraBytes = extraInfos.GetBytesAt(row)
			}
			createSQL := ""
			if len(bat.Vecs) > 2 && !bat.Vecs[2].IsNull(uint64(row)) {
				createSQL = bat.Vecs[2].GetStringAt(row)
			}
			isExternal := len(bat.Vecs) > 3 &&
				!bat.Vecs[3].IsNull(uint64(row)) &&
				bat.Vecs[3].GetStringAt(row) == catalog.SystemExternalRel

			structuredChecks := false
			if len(extraBytes) != 0 {
				var err error
				structuredChecks, err = appendEncodedCheckConstraintRows(
					&rows, schema, extraBytes,
				)
				if err != nil {
					return nil, err
				}
			}
			if !structuredChecks && createSQL != "" && !isExternal {
				if err := appendLegacyCheckConstraintRows(&rows, schema, createSQL); err != nil {
					return nil, err
				}
			}
		}
	}

	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].schema != rows[j].schema {
			return rows[i].schema < rows[j].schema
		}
		if rows[i].name != rows[j].name {
			return rows[i].name < rows[j].name
		}
		return false
	})
	return rows, nil
}

func appendEncodedCheckConstraintRows(rows *[]checkConstraintRow, schema string, data []byte) (bool, error) {
	extra := &api.SchemaExtra{}
	if err := extra.Unmarshal(data); err != nil {
		return false, err
	}
	checks := extra.GetChecks()
	before := len(*rows)
	appendCheckConstraintRows(rows, schema, checks)
	return len(*rows) != before, nil
}

func appendCheckConstraintRows(rows *[]checkConstraintRow, schema string, checks []*planpb.CheckDef) {
	for _, check := range checks {
		if check == nil {
			continue
		}
		*rows = append(*rows, checkConstraintRow{
			schema: schema,
			name:   check.Name,
			clause: check.OriginSql,
		})
	}
}

// appendLegacyCheckConstraintRows mirrors the existing rel_createsql parser
// path used by bindLegacyChecksForCreateLike: parse table-level and
// column-level CHECK clauses, and synthesize the same names used for unnamed
// checks by the DDL binder.
func appendLegacyCheckConstraintRows(rows *[]checkConstraintRow, schema, createSQL string) error {
	if !strings.Contains(strings.ToUpper(createSQL), "CHECK") {
		return nil
	}
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, createSQL, 1)
	if err != nil {
		return err
	}
	defer stmt.Free()

	createTable, ok := stmt.(*tree.CreateTable)
	if !ok {
		return nil
	}
	checkOrdinal := 0
	appendCheck := func(name string, expr tree.Expr) {
		checkOrdinal++
		if name == "" {
			name = fmt.Sprintf("__mo_chk_%d", checkOrdinal)
		}
		*rows = append(*rows, checkConstraintRow{
			schema: schema,
			name:   name,
			clause: tree.StringWithOpts(
				expr,
				dialect.MYSQL,
				tree.WithSingleQuoteString(),
				tree.WithQuoteIdentifier(),
				tree.WithModeIndependentStringLiterals(),
			),
		})
	}

	for _, def := range createTable.Defs {
		switch typedDef := def.(type) {
		case *tree.CheckIndex:
			appendCheck(typedDef.ConstraintSymbol, typedDef.Expr)
		case *tree.ColumnTableDef:
			for _, attr := range typedDef.Attributes {
				if check, ok := attr.(*tree.AttributeCheckConstraint); ok {
					appendCheck(check.Name, check.Expr)
				}
			}
		}
	}
	return nil
}
