// Copyright 2026 Matrix Origin
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

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/changedtables"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const MoChangedTablesName = "mo_changed_tables"

const (
	changedTablesBatchRows = 256
	changedTablesMaxRows   = changedtables.MaxTables
)

type moChangedTablesState struct {
	batch         *batch.Batch
	result        changedtables.Result
	next          int
	baselineCount uint64
	changedCount  uint64
	endEmitted    bool
}

func moChangedTablesPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	if remote, _ := proc.Ctx.Value(defines.RemoteRunContext{}).(bool); remote {
		return nil, moerr.NewNotSupported(proc.Ctx, "mo_changed_tables must run on the query coordinator")
	}
	if tf.NumChildren() != 0 || len(tf.Args) < 1 || len(tf.Args) > 2 || len(tf.Rets) != 14 || len(tf.Attrs) != 14 {
		return nil, moerr.NewInternalError(proc.Ctx, "invalid mo_changed_tables execution plan")
	}
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return nil, err
	}
	tf.ctr.executorsForArgs = executors
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	return &moChangedTablesState{}, nil
}

func (s *moChangedTablesState) start(tf *TableFunction, proc *process.Process, row int, _ process.Analyzer) error {
	s.reset(tf, proc)

	marker, err := changedTablesArgument(proc, tf.ctr.argVecs[0], row, "marker", types.MaxMediumTextLen)
	if err != nil {
		return err
	}
	var observer *string
	if len(tf.ctr.argVecs) == 2 {
		observer, err = changedTablesArgument(proc, tf.ctr.argVecs[1], row, "observer name", 1200)
		if err != nil {
			return err
		}
		if observer == nil || *observer == "" {
			return moerr.NewInvalidInput(proc.Ctx, "mo_changed_tables observer name must be non-empty")
		}
	}

	result, err := changedtables.Read(proc, marker, observer)
	if err != nil {
		return err
	}
	return s.admitResult(proc, result)
}

func changedTablesArgument(proc *process.Process, vec *vector.Vector, row int, name string, width int) (*string, error) {
	if vec.IsNull(uint64(row)) {
		return nil, nil
	}
	if !vec.GetType().Oid.IsMySQLString() {
		return nil, moerr.NewInvalidInputf(proc.Ctx, "mo_changed_tables %s must be a string", name)
	}
	if len(vec.GetBytesAt(row)) > width {
		return nil, moerr.NewInvalidInputf(proc.Ctx, "mo_changed_tables %s exceeds %d bytes", name, width)
	}
	value := vec.GetStringAt(row)
	return &value, nil
}

func (s *moChangedTablesState) admitResult(proc *process.Process, result changedtables.Result) error {
	if len(result.Rows) > changedTablesMaxRows || result.Marker == "" || len(result.Marker) > types.MaxMediumTextLen ||
		result.To.PhysicalTime <= 0 || (result.From != nil && result.From.Greater(result.To)) {
		return moerr.NewInternalError(proc.Ctx, "invalid mo_changed_tables result")
	}
	for _, row := range result.Rows {
		switch row.Kind {
		case "BASELINE":
			s.baselineCount++
		case "CHANGED":
			s.changedCount++
		default:
			return moerr.NewInternalError(proc.Ctx, "invalid mo_changed_tables row kind")
		}
	}
	s.result = result
	return nil
}

func (s *moChangedTablesState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.endEmitted {
		return vm.CancelResult, nil
	}
	if err := proc.Ctx.Err(); err != nil {
		return vm.CancelResult, err
	}
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}

	if s.next == len(s.result.Rows) {
		if err := s.appendEnd(proc); err != nil {
			return vm.CancelResult, err
		}
		s.result = changedtables.Result{}
		s.endEmitted = true
		s.batch.SetRowCount(1)
	} else {
		until := min(s.next+changedTablesBatchRows, len(s.result.Rows))
		for _, row := range s.result.Rows[s.next:until] {
			if err := s.appendRow(proc, row); err != nil {
				return vm.CancelResult, err
			}
		}
		s.batch.SetRowCount(until - s.next)
		s.next = until
	}

	result := vm.NewCallResult()
	result.Batch = s.batch
	return result, nil
}

func (s *moChangedTablesState) appendRow(proc *process.Process, row changedtables.Row) error {
	vs, mp := s.batch.Vecs, proc.Mp()
	for _, column := range [...]struct {
		i     int
		value string
		null  bool
	}{
		{0, row.Kind, false},
		{1, row.Reason, row.Reason == ""},
		{3, row.AccountName, false},
		{5, row.DatabaseName, false},
		{7, row.TableName, false},
	} {
		if err := vector.AppendBytes(vs[column.i], []byte(column.value), column.null, mp); err != nil {
			return err
		}
	}
	if err := vector.AppendFixed(vs[2], row.AccountID, false, mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(vs[4], row.DatabaseID, row.DatabaseID == 0, mp); err != nil {
		return err
	}
	if err := vector.AppendFixed(vs[6], row.TableID, row.TableID == 0, mp); err != nil {
		return err
	}
	return appendChangedTablesNulls(proc, vs[8:])
}

func (s *moChangedTablesState) appendEnd(proc *process.Process) error {
	vs, mp := s.batch.Vecs, proc.Mp()
	if err := vector.AppendBytes(vs[0], []byte("END"), false, mp); err != nil {
		return err
	}
	if err := appendChangedTablesNulls(proc, vs[1:8]); err != nil {
		return err
	}
	if err := vector.AppendBytes(vs[8], []byte(s.result.Marker), false, mp); err != nil {
		return err
	}
	from := ""
	if s.result.From != nil {
		from = s.result.From.DebugString()
	}
	if err := vector.AppendBytes(vs[9], []byte(from), s.result.From == nil, mp); err != nil {
		return err
	}
	if err := vector.AppendBytes(vs[10], []byte(s.result.To.DebugString()), false, mp); err != nil {
		return err
	}
	for i, count := range [...]uint64{s.result.SourceCount, s.baselineCount, s.changedCount} {
		if err := vector.AppendFixed(vs[11+i], count, false, mp); err != nil {
			return err
		}
	}
	return nil
}

func appendChangedTablesNulls(proc *process.Process, cols []*vector.Vector) error {
	for _, col := range cols {
		if err := vector.AppendNull(col, proc.Mp()); err != nil {
			return err
		}
	}
	return nil
}

func (s *moChangedTablesState) reset(_ *TableFunction, _ *process.Process) {
	s.result = changedtables.Result{}
	s.next = 0
	s.baselineCount = 0
	s.changedCount = 0
	s.endEmitted = false
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
}

func (s *moChangedTablesState) end(_ *TableFunction, _ *process.Process) error {
	return nil
}

func (s *moChangedTablesState) free(tf *TableFunction, proc *process.Process, _ bool, _ error) {
	s.reset(tf, proc)
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
}
