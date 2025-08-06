// Copyright 2022 - 2025 Matrix Origin
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
	"bufio"
	"io"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/crt"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	json2 "github.com/segmentio/encoding/json"
)

type parseJsonlState struct {
	reader   io.ReadCloser
	dec      *json2.Decoder
	scanner  *bufio.Scanner
	batch    *batch.Batch
	fromData bool
	opts     plan.ParseJsonlOptions
	appender []func(proc *process.Process, vec *vector.Vector, v any) error
}

func parseJsonlDataPrepare(proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	return parseJsonlPrepare(true, proc, tblArg)
}

func parseJsonlFilePrepare(proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	return parseJsonlPrepare(false, proc, tblArg)
}

func parseJsonlPrepare(fromData bool, proc *process.Process, tblArg *TableFunction) (tvfState, error) {
	// parse the options
	var st parseJsonlState
	st.fromData = fromData
	if err := json2.Unmarshal(tblArg.Params, &st.opts); err != nil {
		return nil, err
	}

	st.appender = make([]func(proc *process.Process, vec *vector.Vector, v any) error, len(st.opts.Cols))
	for i, col := range st.opts.Cols {
		switch col.Type {
		case plan.ParseJsonlTypeBool:
			st.appender[i] = typedAppend_bool
		case plan.ParseJsonlTypeInt32:
			st.appender[i] = typedAppend_int32
		case plan.ParseJsonlTypeInt64:
			st.appender[i] = typedAppend_int64
		case plan.ParseJsonlTypeFloat32:
			st.appender[i] = typedAppend_float32
		case plan.ParseJsonlTypeFloat64:
			st.appender[i] = typedAppend_float64
		case plan.ParseJsonlTypeString:
			st.appender[i] = typedAppend_string
		case plan.ParseJsonlTypeTimestamp:
			st.appender[i] = typedAppend_timestamp
		default:
			// Should never reach here.
			return nil, moerr.NewInternalError(proc.Ctx, "invalid type")
		}
	}

	var err error
	tblArg.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tblArg.Args)
	if err != nil {
		return nil, err
	}
	tblArg.ctr.argVecs = make([]*vector.Vector, len(tblArg.Args))

	return &st, nil
}

func (st *parseJsonlState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	// get the first parameter, which is the data source.
	// 2nd parameter is opitonal, and must be constant, and
	// it has been processed during query compilation.
	//
	var err error
	var dataSrcStr string
	dataSrc := tf.ctr.argVecs[0]
	if dataSrc.GetNulls().Contains(uint64(nthRow)) {
		return nil
	}

	if dataSrc.GetType().Oid == types.T_varchar || dataSrc.GetType().Oid == types.T_text {
		dataSrcStr = dataSrc.GetStringAt(nthRow)
	} else {
		return moerr.NewInvalidInput(proc.Ctx, "data source must be varchar or text")
	}

	// Now open the reader.
	st.reader, err = crt.GetIOReadCloserSimple(proc, st.fromData, dataSrcStr)
	if err != nil {
		return err
	}

	// Only uncompress the file if it's not from data,
	if !st.fromData {
		st.reader, err = crt.GetUnCompressReader(proc, tree.AUTO, dataSrcStr, st.reader)
		if err != nil {
			return err
		}
	}

	if st.opts.Format == plan.ParseJsonlFormatLine {
		st.scanner = bufio.NewScanner(st.reader)
	} else {
		st.dec = json2.NewDecoder(st.reader)
	}

	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}

	return nil
}

func (st *parseJsonlState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (st *parseJsonlState) reset(tf *TableFunction, proc *process.Process) {
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
}

func (st *parseJsonlState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
	}

	if st.scanner != nil {
		st.scanner = nil
	}

	if st.dec != nil {
		st.dec = nil
	}

	if st.reader != nil {
		st.reader.Close()
		st.reader = nil
	}
}

func (st *parseJsonlState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	st.batch.CleanOnlyData()
	var cnt int

	switch st.opts.Format {
	case plan.ParseJsonlFormatLine:
		for st.scanner.Scan() {
			line := st.scanner.Text()
			vector.AppendBytes(st.batch.Vecs[0], []byte(line), false, proc.Mp())
			cnt++
			if cnt >= 8192 {
				break
			}
		}
	case plan.ParseJsonlFormatArray:
		var vv [][]any
		for {
			var v []any
			err := st.dec.Decode(&v)
			if err != nil {
				if err == io.EOF {
					break
				}
				return vm.CallResult{}, err
			}
			if len(v) > len(st.opts.Cols) {
				return vm.CallResult{}, moerr.NewInvalidInput(proc.Ctx, "too many columns in the jsonl file")
			}

			vv = append(vv, v)
			cnt++
			if cnt >= 8192 {
				break
			}
		}

		for col := range st.opts.Cols {
			for _, v := range vv {
				var colv any
				if col < len(v) {
					colv = v[col]
				}
				err := st.appender[col](proc, st.batch.Vecs[col], colv)
				if err != nil {
					return vm.CallResult{}, err
				}
			}
		}

	case plan.ParseJsonlFormatObject:
		var objs []map[string]any
		for {
			var v map[string]any
			err := st.dec.Decode(&v)
			if err != nil {
				if err == io.EOF {
					break
				}
				return vm.CallResult{}, err
			}
			objs = append(objs, v)
			cnt++
			if cnt >= 8192 {
				break
			}
		}

		for col := range st.opts.Cols {
			for _, v := range objs {
				err := st.appender[col](proc, st.batch.Vecs[col], v[st.opts.Cols[col].Name])
				if err != nil {
					return vm.CallResult{}, err
				}
			}
		}
	default:
		return vm.CallResult{}, moerr.NewInternalError(proc.Ctx, "invalid format")
	}

	if cnt == 0 {
		return vm.CallResult{Status: vm.ExecStop}, nil
	}
	st.batch.SetRowCount(cnt)
	return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
}

// typed appenders.
func typedAppend_bool(proc *process.Process, vec *vector.Vector, v any) error {
	if v == nil {
		vector.AppendFixed(vec, false, true, proc.Mp())
		return nil
	}
	b, ok := v.(bool)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "invalid bool value")
	}
	vector.AppendFixed(vec, b, false, proc.Mp())
	return nil
}

func typedAppend_T[T int32 | int64 | float32 | float64](proc *process.Process, vec *vector.Vector, v any) error {
	if v == nil {
		vector.AppendFixed[T](vec, 0, true, proc.Mp())
		return nil
	}
	f, ok := v.(float64)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "invalid number value")
	}
	vector.AppendFixed(vec, T(f), false, proc.Mp())
	return nil
}

func typedAppend_int32(proc *process.Process, vec *vector.Vector, v any) error {
	return typedAppend_T[int32](proc, vec, v)
}
func typedAppend_int64(proc *process.Process, vec *vector.Vector, v any) error {
	return typedAppend_T[int64](proc, vec, v)
}
func typedAppend_float32(proc *process.Process, vec *vector.Vector, v any) error {
	return typedAppend_T[float32](proc, vec, v)
}

func typedAppend_float64(proc *process.Process, vec *vector.Vector, v any) error {
	return typedAppend_T[float64](proc, vec, v)
}

func typedAppend_string(proc *process.Process, vec *vector.Vector, v any) error {
	if v == nil {
		vector.AppendBytes(vec, nil, true, proc.Mp())
		return nil
	}
	s, err := json2.Marshal(v)
	if err != nil {
		return err
	}
	vector.AppendBytes(vec, s, false, proc.Mp())
	return nil
}

func typedAppend_timestamp(proc *process.Process, vec *vector.Vector, v any) error {
	if v == nil {
		vector.AppendFixed[types.Timestamp](vec, -1, true, proc.Mp())
		return nil
	}
	s, ok := v.(string)
	if !ok {
		return moerr.NewInvalidInput(proc.Ctx, "invalid string value")
	}

	// TODO: for now, use local timezone to parse the timestamp
	// timezone support will need to wait until we have a timestamptz type.
	ts, err := types.ParseTimestamp(time.Local, s, 6)
	if err != nil {
		return err
	}
	vector.AppendFixed(vec, ts, false, proc.Mp())
	return nil
}
