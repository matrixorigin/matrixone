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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	checkConstraintCatalogColumn = iota
	checkConstraintSchemaColumn
	checkConstraintNameColumn
	checkConstraintClauseColumn
	checkConstraintTableColumn
	checkConstraintTypeColumn
	checkConstraintEnforcedColumn
)

const checkConstraintBatchSize = 8192

var checkConstraintCatalogQuery = "SELECT tbl.reldatabase, tbl.relname, tbl.rel_createsql, tbl.extra_info " +
	"FROM mo_catalog.mo_tables tbl " +
	"WHERE tbl.account_id = current_account_id() AND " +
	catalog.NonTemporaryTableSQLPredicate("tbl") +
	" ORDER BY tbl.reldatabase, tbl.relname"

type checkConstraintRow struct {
	schema         string
	table          string
	name           string
	clause         string
	constraintType string
	enforced       string
}

// checkConstraintsState deliberately does not embed simpleOneBatchState.  The
// catalog query can produce many result batches, so retaining one batch for
// every table until RunSql returns would make CHECK_CONSTRAINTS an avoidable
// tenant-controlled memory amplifier.  One internal result is decoded into at
// most one output batch and is released before the next result is consumed.
type checkConstraintsState struct {
	batch *batch.Batch

	// collectRows is injected by unit tests.  Production uses the streaming
	// catalog path below.
	collectRows func(*process.Process) ([]checkConstraintRow, error)
	pending     []checkConstraintRow
	called      bool
	streaming   bool
	streamCh    chan executor.Result
	errCh       chan error
	streamDone  chan struct{}
	streamClose context.CancelFunc
	streamEnded bool
}

// checkConstraintOutputPositions maps logical columns to vectors retained by
// the optimizer.  The table function exposes table metadata for
// TABLE_CONSTRAINTS, while CHECK_CONSTRAINTS projects only the first four
// columns.
func checkConstraintOutputPositions(attrs []string) [7]int {
	positions := [7]int{-1, -1, -1, -1, -1, -1, -1}
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
		case "table_name":
			positions[checkConstraintTableColumn] = i
		case "constraint_type":
			positions[checkConstraintTypeColumn] = i
		case "enforced":
			positions[checkConstraintEnforcedColumn] = i
		}
	}
	return positions
}

func checkConstraintsPrepare(_ *process.Process, _ *TableFunction) (tvfState, error) {
	return &checkConstraintsState{}, nil
}

func (s *checkConstraintsState) reset(_ *TableFunction, proc *process.Process) {
	s.stopStreaming(proc)
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
	s.pending = nil
	s.called = false
	s.streamEnded = false
}

func (s *checkConstraintsState) free(_ *TableFunction, proc *process.Process, _ bool, _ error) {
	s.stopStreaming(proc)
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
	s.pending = nil
}

func (s *checkConstraintsState) end(_ *TableFunction, _ *process.Process) error {
	return nil
}

func (s *checkConstraintsState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	s.stopStreaming(proc)
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}
	s.batch.SetRowCount(0)
	s.called = false
	s.pending = nil
	s.streamEnded = false

	// A child-dependent invocation has no metadata input.  Keep the normal
	// table-function lifecycle and return an empty result for non-zero rows.
	if nthRow != 0 {
		return nil
	}

	if s.collectRows != nil {
		rows, err := s.collectRows(proc)
		if err != nil {
			return err
		}
		s.pending = rows
		return s.fillBatch(tf, proc)
	}

	if err := s.startStreaming(proc); err != nil {
		return err
	}
	return s.fillBatch(tf, proc)
}

func (s *checkConstraintsState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.called {
		if s.batch != nil {
			s.batch.CleanOnlyData()
			s.batch.SetRowCount(0)
		}
		if err := s.fillBatch(tf, proc); err != nil {
			return vm.CancelResult, err
		}
	}
	s.called = true
	if s.batch == nil || s.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}

func (s *checkConstraintsState) startStreaming(proc *process.Process) error {
	ctx, cancel := context.WithCancel(proc.GetTopContext())
	s.streamClose = cancel
	s.streamCh = make(chan executor.Result, 8)
	s.errCh = make(chan error, 1)
	s.streamDone = make(chan struct{})
	s.streaming = true

	go func() {
		defer close(s.streamCh)
		defer close(s.streamDone)
		_, err := sqlexec.RunStreamingSql(
			ctx,
			sqlexec.NewSqlProcess(proc),
			checkConstraintCatalogQuery,
			s.streamCh,
			s.errCh,
		)
		if err != nil {
			select {
			case s.errCh <- err:
			case <-ctx.Done():
			}
		}
	}()
	return nil
}

func (s *checkConstraintsState) stopStreaming(_ *process.Process) {
	if !s.streaming {
		return
	}
	if s.streamClose != nil {
		s.streamClose()
	}
	for result := range s.streamCh {
		result.Close()
	}
	if s.streamDone != nil {
		<-s.streamDone
	}
	s.streaming = false
	s.streamCh = nil
	s.errCh = nil
	s.streamDone = nil
	s.streamClose = nil
}

func (s *checkConstraintsState) fillBatch(tf *TableFunction, proc *process.Process) error {
	positions := checkConstraintOutputPositions(s.batch.Attrs)
	rowCount := 0
	for rowCount < checkConstraintBatchSize {
		if len(s.pending) == 0 {
			if s.streamEnded || !s.streaming {
				break
			}
			if err := s.readStreamResult(proc); err != nil {
				return err
			}
			continue
		}

		space := checkConstraintBatchSize - rowCount
		count := len(s.pending)
		if count > space {
			count = space
		}
		for i := 0; i < count; i++ {
			if err := appendCheckConstraintRow(s.batch.Vecs, positions, s.pending[i], proc); err != nil {
				return err
			}
		}
		rowCount += count
		s.pending = s.pending[count:]
	}
	if rowCount == 0 && s.streamEnded {
		return nil
	}
	s.batch.SetRowCount(rowCount)
	return nil
}

func (s *checkConstraintsState) readStreamResult(proc *process.Process) error {
	for {
		select {
		case err := <-s.errCh:
			if err != nil {
				return err
			}
		case result, ok := <-s.streamCh:
			if !ok {
				s.streamEnded = true
				select {
				case err := <-s.errCh:
					if err != nil {
						return err
					}
				default:
				}
				return nil
			}
			rows, err := collectCheckConstraintRowsFromResult(result)
			if err != nil {
				return err
			}
			if len(rows) != 0 {
				s.pending = rows
			}
			return nil
		case <-proc.Ctx.Done():
			return moerr.NewInternalError(proc.Ctx, "check constraints metadata query cancelled")
		}
	}
}

func appendCheckConstraintRow(
	vectors []*vector.Vector,
	positions [7]int,
	row checkConstraintRow,
	proc *process.Process,
) error {
	values := [...]string{
		catalog.SystemCatalogName,
		row.schema,
		row.name,
		row.clause,
		row.table,
		row.constraintType,
		row.enforced,
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

// collectCheckConstraintRowsFromResult decodes catalog batches and closes the
// result as soon as the batch is consumed.  The four catalog columns are,
// respectively, database, table, original CREATE SQL and SchemaExtra.
func collectCheckConstraintRowsFromResult(result executor.Result) ([]checkConstraintRow, error) {
	defer result.Close()

	capacity := 0
	for _, bat := range result.Batches {
		if bat != nil {
			capacity += bat.RowCount()
		}
	}
	rows := make([]checkConstraintRow, 0, capacity)
	for _, bat := range result.Batches {
		decoded, err := decodeCheckConstraintBatch(bat)
		if err != nil {
			return nil, err
		}
		rows = append(rows, decoded...)
	}
	sortCheckConstraintRows(rows)
	return rows, nil
}

func decodeCheckConstraintBatch(bat *batch.Batch) ([]checkConstraintRow, error) {
	if bat == nil || len(bat.Vecs) < 4 {
		return nil, nil
	}
	databaseNames := bat.Vecs[0]
	tableNames := bat.Vecs[1]
	createSQLs := bat.Vecs[2]
	extraInfos := bat.Vecs[3]
	rows := make([]checkConstraintRow, 0, bat.RowCount())
	for row := 0; row < bat.RowCount(); row++ {
		if databaseNames.IsNull(uint64(row)) || tableNames.IsNull(uint64(row)) {
			continue
		}
		createSQL := ""
		if !createSQLs.IsNull(uint64(row)) {
			createSQL = createSQLs.GetStringAt(row)
		}
		// Older catalog rows may have no SchemaExtra payload at all.  Their
		// CHECK definitions are still recoverable from rel_createsql.
		if extraInfos.IsNull(uint64(row)) {
			legacy, err := parseLegacyCheckConstraintRows(
				context.Background(),
				databaseNames.GetStringAt(row),
				tableNames.GetStringAt(row),
				createSQL,
			)
			if err != nil {
				return nil, err
			}
			rows = append(rows, legacy...)
			continue
		}
		extraBytes := extraInfos.GetBytesAt(row)
		if len(extraBytes) == 0 {
			legacy, err := parseLegacyCheckConstraintRows(
				context.Background(),
				databaseNames.GetStringAt(row),
				tableNames.GetStringAt(row),
				createSQL,
			)
			if err != nil {
				return nil, err
			}
			rows = append(rows, legacy...)
			continue
		}
		if err := appendEncodedCheckConstraintRows(
			&rows,
			databaseNames.GetStringAt(row),
			tableNames.GetStringAt(row),
			createSQL,
			extraBytes,
		); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func appendEncodedCheckConstraintRows(
	rows *[]checkConstraintRow,
	schema string,
	table string,
	createSQL string,
	data []byte,
) error {
	extra := &api.SchemaExtra{}
	if err := extra.Unmarshal(data); err != nil {
		return err
	}
	if features.IsPartition(extra.FeatureFlag) {
		return nil
	}
	checks := extra.GetChecks()
	before := len(*rows)
	if len(checks) != 0 {
		appendCheckConstraintRows(rows, schema, table, checks)
		if len(*rows) != before {
			return nil
		}
	}
	legacy, err := parseLegacyCheckConstraintRows(context.Background(), schema, table, createSQL)
	if err != nil {
		return err
	}
	*rows = append(*rows, legacy...)
	return nil
}

func appendCheckConstraintRows(
	rows *[]checkConstraintRow,
	schema string,
	table string,
	checks []*planpb.CheckDef,
) {
	for _, check := range checks {
		if check == nil {
			continue
		}
		*rows = append(*rows, checkConstraintRow{
			schema:         schema,
			table:          table,
			name:           check.Name,
			clause:         check.OriginSql,
			constraintType: "CHECK",
			enforced:       "YES",
		})
	}
}

func sortCheckConstraintRows(rows []checkConstraintRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].schema != rows[j].schema {
			return rows[i].schema < rows[j].schema
		}
		if rows[i].table != rows[j].table {
			return rows[i].table < rows[j].table
		}
		return rows[i].name < rows[j].name
	})
}
