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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
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
	return &checkConstraintsState{}, nil
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

	rows, err := collectCheckConstraintRows(proc)
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
	catalogQuery := "SELECT tbl.reldatabase, tbl.extra_info " +
		"FROM mo_catalog.mo_tables tbl " +
		"WHERE tbl.account_id = current_account_id() AND " +
		catalog.NonTemporaryTableSQLPredicate("tbl")
	result, err := sqlexec.RunSql(sqlexec.NewSqlProcess(proc), catalogQuery)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	rows := make([]checkConstraintRow, 0)
	for _, bat := range result.Batches {
		if len(bat.Vecs) < 2 {
			continue
		}
		databaseNames := bat.Vecs[0]
		extraInfos := bat.Vecs[1]
		for row := 0; row < bat.RowCount(); row++ {
			if databaseNames.IsNull(uint64(row)) || extraInfos.IsNull(uint64(row)) {
				continue
			}
			extraBytes := extraInfos.GetBytesAt(row)
			if len(extraBytes) == 0 {
				continue
			}
			if err := appendEncodedCheckConstraintRows(
				&rows,
				databaseNames.GetStringAt(row),
				extraBytes,
			); err != nil {
				return nil, err
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

func appendEncodedCheckConstraintRows(rows *[]checkConstraintRow, schema string, data []byte) error {
	extra := &api.SchemaExtra{}
	if err := extra.Unmarshal(data); err != nil {
		return err
	}
	appendCheckConstraintRows(rows, schema, extra.GetChecks())
	return nil
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
