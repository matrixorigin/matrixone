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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/external"
	"github.com/matrixorigin/matrixone/pkg/sql/foreigntvf"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// foreignTVFState executes esql_tvf / sql_tvf: it resolves a session-cached
// foreign connection, runs the query, and materializes the CSV result. With a
// schema it reuses the external CSV reader for typed columns; without one it
// emits a single JSON-array column per row.
type foreignTVFState struct {
	kind     foreigntvf.Kind
	noSchema bool
	// fullSchemaNames is the original declared column order, fixing CSV field
	// positions independent of output-column pruning/reordering.
	fullSchemaNames []string

	batch     *batch.Batch
	reader    *external.ForeignTVFReader    // schema mode
	rawReader *external.ForeignTVFRawReader // no-schema mode
	exhausted bool
}

func esqlTvfPrepare(proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	return foreignTVFPrepare(foreigntvf.KindESQL, proc, tblArg)
}

func sqlTvfPrepare(proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	return foreignTVFPrepare(foreigntvf.KindSQL, proc, tblArg)
}

func foreignTVFPrepare(kind foreigntvf.Kind, proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	var p plan.ForeignTVFParam
	if err := json.Unmarshal(tblArg.Params, &p); err != nil {
		return nil, err
	}
	st := &foreignTVFState{kind: kind, noSchema: p.NoSchema}
	if len(p.Cols) > 0 {
		st.fullSchemaNames = make([]string, len(p.Cols))
		for i := range p.Cols {
			st.fullSchemaNames[i] = p.Cols[i].Name
		}
	}

	var err error
	tblArg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tblArg.Args)
	if err != nil {
		return nil, err
	}
	tblArg.ctr.argVecs = make([]*vector.Vector, len(tblArg.Args))
	return st, nil
}

// start resolves the connection and opens the result stream for input row nthRow.
func (st *foreignTVFState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	st.closeReaders()
	st.exhausted = false

	// arg 0: query text (required, non-null). The binder enforces a string
	// type, but a prepare parameter is bound as T_any there, so re-check the
	// runtime vector before the varlen accessor.
	queryVec := tf.ctr.argVecs[0]
	if queryVec == nil || queryVec.IsConstNull() || queryVec.GetNulls().Contains(uint64(nthRow)) {
		return moerr.NewInvalidInput(proc.Ctx, "esql_tvf/sql_tvf: the query argument must not be NULL")
	}
	if !queryVec.GetType().IsVarlen() {
		return moerr.NewInvalidInputf(proc.Ctx, "esql_tvf/sql_tvf: the query argument must be a string, not %s", queryVec.GetType().Oid.String())
	}
	queryStr := queryVec.GetStringAt(nthRow)

	cache, ok := proc.GetSession().(process.ForeignConnCache)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "esql_tvf/sql_tvf can only run in an interactive session")
	}

	// arg 1 (optional): connection handle. NULL/absent falls back to the
	// @esql_tvf_config / @sql_tvf_config default connection.
	var conn foreigntvf.Conn
	var err error
	connGiven := false
	if len(tf.ctr.argVecs) >= 2 {
		cv := tf.ctr.argVecs[1]
		if cv != nil && !cv.IsConstNull() && !cv.GetNulls().Contains(uint64(nthRow)) {
			if !cv.GetType().IsVarlen() {
				return moerr.NewInvalidInputf(proc.Ctx, "esql_tvf/sql_tvf: the conn argument must be a string handle, not %s", cv.GetType().Oid.String())
			}
			handle := cv.GetStringAt(nthRow)
			if conn, err = foreigntvf.ByHandle(proc.Ctx, cache, handle); err != nil {
				return err
			}
			// The CSV dialect and header handling are selected by the TVF's
			// kind; a handle of the other kind would silently drop or invent a
			// header row.
			if conn.Kind() != st.kind {
				return moerr.NewInvalidInputf(proc.Ctx,
					"connection handle %q is a %s connection; %s_tvf accepts only %s connections",
					handle, conn.Kind(), st.kind, st.kind)
			}
			connGiven = true
		}
	}
	if !connGiven {
		cfg, cfgErr := foreigntvf.ConfigFromSessionVar(proc.Ctx, proc, st.kind)
		if cfgErr != nil {
			return cfgErr
		}
		if conn, _, err = foreigntvf.ResolveOrConnect(proc.Ctx, cache, st.kind, cfg); err != nil {
			return err
		}
	}

	stream, err := conn.Query(proc.Ctx, queryStr)
	if err != nil {
		return err
	}

	src := external.ForeignTVFSourceSQL
	if st.kind == foreigntvf.KindESQL {
		src = external.ForeignTVFSourceESQL
	}
	param := external.BuildForeignTVFExternParam(proc, tf.Rets, st.fullSchemaNames, src)
	if st.noSchema {
		st.rawReader, err = external.NewForeignTVFRawReader(param, stream)
	} else {
		st.reader, err = external.NewForeignTVFReader(param, stream)
	}
	if err != nil {
		_ = stream.Close()
		return err
	}

	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}
	return nil
}

func (st *foreignTVFState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	st.batch.CleanOnlyData()
	if st.exhausted {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}

	if st.noSchema {
		cnt := 0
		for cnt < 8192 {
			fields, ok, err := st.rawReader.ReadRow()
			if err != nil {
				return vm.CallResult{}, err
			}
			if !ok {
				st.exhausted = true
				break
			}
			encoded, err := json.Marshal(fields)
			if err != nil {
				return vm.CallResult{}, err
			}
			bj, err := types.ParseStringToByteJson(string(encoded))
			if err != nil {
				return vm.CallResult{}, err
			}
			if err := vector.AppendByteJson(st.batch.Vecs[0], bj, false, proc.Mp()); err != nil {
				return vm.CallResult{}, err
			}
			cnt++
		}
		if cnt == 0 {
			return vm.CallResult{Status: vm.ExecStop}, nil
		}
		st.batch.SetRowCount(cnt)
		return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
	}

	// schema mode: reuse the external CSV batch reader, which sets the row
	// count and closes the stream itself on EOF.
	finished, err := st.reader.ReadBatch(proc.Ctx, st.batch, proc, nil)
	if err != nil {
		return vm.CallResult{}, err
	}
	if finished {
		st.exhausted = true
	}
	if st.batch.RowCount() == 0 {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
}

func (st *foreignTVFState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (st *foreignTVFState) reset(tf *TableFunction, proc *process.Process) {
	st.closeReaders()
	st.exhausted = false
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
}

func (st *foreignTVFState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	st.closeReaders()
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
		st.batch = nil
	}
}

// closeReaders closes whichever result reader is open. Each reader owns and
// closes its underlying stream.
func (st *foreignTVFState) closeReaders() {
	if st.reader != nil {
		_ = st.reader.Close()
		st.reader = nil
	}
	if st.rawReader != nil {
		_ = st.rawReader.Close()
		st.rawReader = nil
	}
}
