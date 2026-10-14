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
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/jsonvalue"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type jsonTableColumn struct {
	spec     jsonvalue.TableColumn
	path     bytejson.Path
	pos      int
	children []*jsonTableColumn
}

type jsonTableFrame struct {
	iterator  *bytejson.PathIterator
	columns   []*jsonTableColumn
	inherited []jsonvalue.Result
	row       []jsonvalue.Result
	ordinal   uint32
	child     int
	produced  bool
	value     bytejson.ByteJson
}

type jsonTableState struct {
	root        bytejson.Path
	columns     []*jsonTableColumn
	columnCount int
	output      []int
	frames      []*jsonTableFrame
	batch       *batch.Batch
	warned      map[int]bool
	warnings    process.WarningAccumulator
}

func jsonTablePrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	var spec jsonvalue.TableSpec
	if err := json.Unmarshal(tf.Params, &spec); err != nil {
		return nil, err
	}
	if spec.Version != 1 || len(spec.Columns) == 0 || len(tf.Args) != 1 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "invalid JSON_TABLE payload or version")
	}
	st := &jsonTableState{warned: make(map[int]bool)}
	var err error
	st.root, err = types.ParseStringToPath(spec.RootPath)
	if err != nil {
		return nil, err
	}
	names := make(map[string]int)
	var compile func([]jsonvalue.TableColumn) ([]*jsonTableColumn, error)
	compile = func(columns []jsonvalue.TableColumn) ([]*jsonTableColumn, error) {
		out := make([]*jsonTableColumn, 0, len(columns))
		for _, spec := range columns {
			column := &jsonTableColumn{spec: spec, pos: -1}
			if spec.Kind != "ordinality" {
				column.path, err = types.ParseStringToPath(spec.Path)
				if err != nil {
					return nil, err
				}
			}
			switch spec.Kind {
			case "nested":
				column.children, err = compile(spec.Children)
				if err != nil {
					return nil, err
				}
			case "ordinality", "path", "exists":
				if _, duplicate := names[spec.Name]; duplicate {
					return nil, moerr.NewInvalidInput(proc.Ctx, "duplicate JSON_TABLE payload column")
				}
				column.pos = st.columnCount
				names[spec.Name] = column.pos
				st.columnCount++
			default:
				return nil, moerr.NewInvalidInput(proc.Ctx, "unknown JSON_TABLE column kind")
			}
			out = append(out, column)
		}
		return out, nil
	}
	st.columns, err = compile(spec.Columns)
	if err != nil {
		return nil, err
	}
	st.output = make([]int, len(tf.Attrs))
	for i, name := range tf.Attrs {
		pos, ok := names[strings.ToLower(name)]
		if !ok {
			return nil, moerr.NewInvalidInput(proc.Ctx, "unknown JSON_TABLE projected column")
		}
		st.output[i] = pos
	}
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return nil, err
	}
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	st.warnings.SetWarningRetentionForProcess(proc)
	return st, nil
}

func (st *jsonTableState) closeFrames() {
	for _, frame := range st.frames {
		frame.iterator.Close()
	}
	st.frames = nil
}

func (st *jsonTableState) reset(tf *TableFunction, proc *process.Process) {
	st.closeFrames()
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
	clear(st.warned)
	st.warnings.Reset()
}

func (st *jsonTableState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	st.closeFrames()
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
		st.batch = nil
	}
	st.warnings.Reset()
}

func (st *jsonTableState) end(tf *TableFunction, proc *process.Process) error {
	st.closeFrames()
	st.warnings.Flush(proc)
	return nil
}

func (st *jsonTableState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	st.closeFrames()
	source := tf.ctr.argVecs[0]
	if source.IsNull(uint64(nthRow)) {
		return nil
	}
	var document bytejson.ByteJson
	var err error
	switch source.GetType().Oid {
	case types.T_json:
		document = types.DecodeJson(source.GetBytesAt(nthRow))
	case types.T_char, types.T_varchar, types.T_text:
		document, err = types.ParseSliceToByteJson(source.GetBytesAt(nthRow))
	case types.T_datalink:
		document, err = readJSONTableDatalink(proc, source.GetStringAt(nthRow))
	default:
		err = moerr.NewInvalidInput(proc.Ctx, "JSON_TABLE source must be JSON, text or DATALINK")
	}
	if err != nil {
		return err
	}
	row := make([]jsonvalue.Result, st.columnCount)
	for i := range row {
		row[i].Status = jsonvalue.StatusJSONNull
	}
	st.frames = append(st.frames, &jsonTableFrame{iterator: bytejson.NewPathIterator(document, &st.root), columns: st.columns, inherited: row})
	return nil
}

// Stage resolution and file-service admission stay with the existing Datalink
// owner. Read at most one varlen document and close the reader before traversal.
func readJSONTableDatalink(proc *process.Process, source string) (bytejson.ByteJson, error) {
	dl, err := datalink.NewDatalink(source, proc)
	if err != nil {
		return bytejson.ByteJson{}, err
	}
	r, err := dl.NewReadCloser(proc)
	if err != nil {
		return bytejson.ByteJson{}, err
	}
	defer r.Close()
	data, err := io.ReadAll(io.LimitReader(r, int64(types.MaxBlobLen)+1))
	if err != nil {
		return bytejson.ByteJson{}, err
	}
	if len(data) > types.MaxBlobLen {
		return bytejson.ByteJson{}, moerr.NewInvalidInput(proc.Ctx, "JSON_TABLE input exceeds maximum document size")
	}
	if err = proc.Ctx.Err(); err != nil {
		return bytejson.ByteJson{}, err
	}
	return types.ParseSliceToByteJson(data)
}

func (st *jsonTableState) convertColumn(proc *process.Process, column *jsonTableColumn, value bytejson.ByteJson, ordinal uint32) jsonvalue.Result {
	if column.spec.Kind == "ordinality" {
		return jsonvalue.Result{Value: ordinal, Status: jsonvalue.StatusSuccess}
	}
	iterator := bytejson.NewPathIterator(value, &column.path)
	defer iterator.Close()
	options := jsonvalue.ConversionOptions{Location: proc.GetSessionInfo().TimeZone}
	if column.spec.Kind == "exists" {
		_, exists, err := iterator.NextContext(proc.Ctx)
		if err != nil {
			return jsonvalue.Result{Status: jsonvalue.StatusStatementError, Err: err}
		}
		text := "0"
		if exists {
			text = "1"
		}
		flag, err := types.ParseStringToByteJson(text)
		if err != nil {
			return jsonvalue.Result{Status: jsonvalue.StatusStatementError, Err: err}
		}
		return jsonvalue.ConvertScalarWithContext(proc.Ctx, flag, column.spec.Type, options)
	}
	result := jsonvalue.ConvertTablePathMatchesWithOptions(proc.Ctx, iterator, column.spec.Type, options, types.MaxBlobLen)
	var response jsonvalue.TableResponse
	switch result.Status {
	case jsonvalue.StatusMissing:
		response = column.spec.OnEmpty
	case jsonvalue.StatusComposite, jsonvalue.StatusConversionError, jsonvalue.StatusRangeError:
		response = column.spec.OnError
	default:
		return result
	}
	switch response.Action {
	case "null":
		return jsonvalue.Result{Status: jsonvalue.StatusJSONNull}
	case "default":
		fallback, err := types.ParseStringToByteJson(response.Default)
		if err != nil {
			return jsonvalue.Result{Status: jsonvalue.StatusStatementError, Err: err}
		}
		return jsonvalue.ConvertTableScalarWithContext(proc.Ctx, fallback, column.spec.Type, options)
	case "error":
		if result.Err == nil {
			result.Err = moerr.NewInvalidInputf(proc.Ctx, "JSON_TABLE column '%s' has no value", column.spec.Name)
		}
		return result
	default:
		return jsonvalue.Result{Status: jsonvalue.StatusStatementError, Err: moerr.NewInvalidInput(proc.Ctx, "unknown JSON_TABLE response action")}
	}
}

// A frame retains only its current match and row. Sibling nested sources are
// concatenated; only a parent with no emitted child receives a complement row.
func (st *jsonTableState) nextRow(proc *process.Process) ([]jsonvalue.Result, error) {
	for len(st.frames) > 0 {
		if err := proc.Ctx.Err(); err != nil {
			return nil, err
		}
		frame := st.frames[len(st.frames)-1]
		if frame.row == nil {
			value, ok, err := frame.iterator.NextContext(proc.Ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				frame.iterator.Close()
				st.frames = st.frames[:len(st.frames)-1]
				continue
			}
			if frame.ordinal == math.MaxUint32 {
				return nil, moerr.NewInvalidInput(proc.Ctx, "JSON_TABLE ordinality overflow")
			}
			frame.ordinal++
			frame.row = append([]jsonvalue.Result(nil), frame.inherited...)
			frame.child, frame.produced = 0, false
			for _, column := range frame.columns {
				if column.spec.Kind == "nested" {
					continue
				}
				result := st.convertColumn(proc, column, value, frame.ordinal)
				switch result.Status {
				case jsonvalue.StatusSuccess, jsonvalue.StatusJSONNull, jsonvalue.StatusTruncated:
				default:
					if result.Err != nil {
						return nil, result.Err
					}
					return nil, moerr.NewInvalidInputf(proc.Ctx, "JSON_TABLE conversion failed for '%s'", column.spec.Name)
				}
				if result.Warning != nil && !st.warned[column.pos] {
					st.warned[column.pos] = true
					name := column.spec.OriginName
					if name == "" {
						name = column.spec.Name
					}
					st.warnings.Add(result.Warning.Code, fmt.Sprintf("Data truncated for column '%s' at row 1", name))
				}
				frame.row[column.pos] = result
			}
			frame.value = value
		}
		pushed := false
		for frame.child < len(frame.columns) {
			column := frame.columns[frame.child]
			frame.child++
			if column.spec.Kind != "nested" {
				continue
			}
			st.frames = append(st.frames, &jsonTableFrame{iterator: bytejson.NewPathIterator(frame.value, &column.path), columns: column.children, inherited: frame.row})
			pushed = true
			break
		}
		if pushed {
			continue
		}
		row, produced := frame.row, frame.produced
		frame.row = nil
		if produced {
			continue
		}
		for _, ancestor := range st.frames[:len(st.frames)-1] {
			ancestor.produced = true
		}
		return row, nil
	}
	return nil, nil
}

func (st *jsonTableState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}
	for st.batch.RowCount() < colexec.DefaultBatchSize {
		row, err := st.nextRow(proc)
		if err != nil {
			st.closeFrames()
			st.batch.CleanOnlyData()
			st.warnings.Reset()
			return vm.CancelResult, err
		}
		if row == nil {
			break
		}
		for i, pos := range st.output {
			if err = jsonvalue.AppendResult(st.batch.Vecs[i], row[pos], proc.Mp()); err != nil {
				st.closeFrames()
				st.batch.CleanOnlyData()
				st.warnings.Reset()
				return vm.CancelResult, err
			}
		}
		st.batch.SetRowCount(st.batch.RowCount() + 1)
	}
	if st.batch.RowCount() == 0 {
		return vm.NewCallResult(), nil
	}
	// Publish completed work to the existing attempt-owned sink, not directly
	// to Session. Compile.Run commits that sink only when the entire attempt
	// succeeds, and discards it on error/cancel/retry. Flushing here also covers
	// successful early LIMIT, where end is not necessarily called. The finite
	// per-column key set survives this flush and all rows/batches until Reset.
	// Cross-instance/CN keyed-once transport is a separate, still missing gate.
	st.warnings.Flush(proc)
	return vm.CallResult{Batch: st.batch}, nil
}
