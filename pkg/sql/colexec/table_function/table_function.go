// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const opName = "table_function"

const (
	FULLTEXT_INDEX_SCAN     = "fulltext_index_scan"
	FULLTEXT_INDEX_TOKENIZE = "fulltext_index_tokenize"
)

func (tableFunction *TableFunction) isTbFuncSourceOp() bool {
	return tableFunction.NumChildren() == 0
}

func (tableFunction *TableFunction) Call(proc *process.Process) (vm.CallResult, error) {
	analyzer := tableFunction.OpAnalyzer

	// we know this cannot be true but check anyway
	if tableFunction.ctr.state == nil {
		return vm.CancelResult, moerr.NewInternalErrorf(proc.Ctx, "table function %s state is nil", tableFunction.FuncName)
	}

	var err error

	// loop
	for {
		if tableFunction.ctr.inputBatch.IsDone() || tableFunction.ctr.nextRow >= tableFunction.ctr.inputBatch.RowCount() {
			// get to next input batch
			if tableFunction.isTbFuncSourceOp() {
				if tableFunction.ctr.isDone {
					// End of Input
					err := tableFunction.ctr.state.end(tableFunction, proc)
					if err != nil {
						return vm.CancelResult, err
					}
					return vm.NewCallResult(), nil
				}
				tableFunction.ctr.inputBatch = batch.EmptyForConstFoldBatch
				tableFunction.ctr.isDone = true
			} else {
				input, err := vm.ChildrenCall(tableFunction.GetChildren(0), proc, analyzer)
				if err != nil {
					return input, err
				}
				tableFunction.ctr.inputBatch = input.Batch
				if input.Batch.IsDone() {
					// End of Input
					err := tableFunction.ctr.state.end(tableFunction, proc)
					if err != nil {
						return input, err
					}
					return input, nil
				}
			}

			// Got a valid batch, eval tbf args
			for i := range tableFunction.ctr.executorsForArgs {
				tableFunction.ctr.argVecs[i], err = tableFunction.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{tableFunction.ctr.inputBatch}, nil)
				if err != nil {
					return vm.CancelResult, err
				}
			}

			// Now position nextRow, we are ready to call the table function
			tableFunction.ctr.nextRow = 0
			if err = tableFunction.ctr.state.start(tableFunction, proc, 0, analyzer); err != nil {
				return vm.CancelResult, err
			}
		}

		// call the table function
		res, err := tableFunction.ctr.state.call(tableFunction, proc)
		if err != nil {
			return vm.CancelResult, err
		}

		if res.Batch.IsDone() {
			tableFunction.ctr.nextRow++
			if tableFunction.ctr.nextRow < tableFunction.ctr.inputBatch.RowCount() {
				if err = tableFunction.ctr.state.start(tableFunction, proc, tableFunction.ctr.nextRow, analyzer); err != nil {
					return vm.CancelResult, err
				}
			}
			continue
		}
		return res, nil
	} // end of loop
}

func (tableFunction *TableFunction) String(buf *bytes.Buffer) {
	buf.WriteString(opName)
	buf.WriteString(tableFunction.FuncName)
}

func (tableFunction *TableFunction) OpType() vm.OpType {
	return vm.TableFunction
}

func (tableFunction *TableFunction) Prepare(proc *process.Process) error {
	if tableFunction.OpAnalyzer == nil {
		tableFunction.OpAnalyzer = process.NewAnalyzer(tableFunction.GetIdx(), tableFunction.IsFirst, tableFunction.IsLast, "tableFunction")
	} else {
		tableFunction.OpAnalyzer.Reset()
	}

	var err error
	tblArg := tableFunction

	retSchema := make([]types.Type, len(tblArg.Rets))
	for i := range tblArg.Rets {
		typ := tblArg.Rets[i].Typ
		retSchema[i] = types.New(types.T(typ.Id), typ.Width, typ.Scale)
	}
	tblArg.ctr.retSchema = retSchema

	switch tblArg.FuncName {
	case "unnest":
		tblArg.ctr.state, err = unnestPrepare(proc, tblArg)
	case "generate_series":
		if !tblArg.CanOpt {
			tblArg.ctr.state, err = generateSeriesPrepare(proc, tblArg)
		}
	case "generate_random_int64":
		tblArg.ctr.state, err = generateRandomInt64Prepare(proc, tblArg)
	case "generate_random_float64":
		tblArg.ctr.state, err = generateRandomFloat64Prepare(proc, tblArg)
	case "meta_scan":
		tblArg.ctr.state, err = metaScanPrepare(proc, tblArg)
	case "current_account":
		tblArg.ctr.state, err = currentAccountPrepare(proc, tblArg)
	case "metadata_scan":
		tblArg.ctr.state, err = metadataScanPrepare(proc, tblArg)
	case "processlist":
		tblArg.ctr.state, err = processlistPrepare(proc, tblArg)
	case "mo_locks":
		tblArg.ctr.state, err = moLocksPrepare(proc, tblArg)
	case "mo_configurations":
		tblArg.ctr.state, err = moConfigurationsPrepare(proc, tblArg)
	case "mo_transactions":
		tblArg.ctr.state, err = moTransactionsPrepare(proc, tblArg)
	case "mo_cache":
		tblArg.ctr.state, err = moCachePrepare(proc, tblArg)
	case "fulltext_index_scan":
		tblArg.ctr.state, err = fulltextIndexScanPrepare(proc, tblArg)
	case "fulltext_index_tokenize":
		tblArg.ctr.state, err = fulltextIndexTokenizePrepare(proc, tblArg)
	case "stage_list":
		tblArg.ctr.state, err = stageListPrepare(proc, tblArg)
	case "moplugin_table":
		tblArg.ctr.state, err = pluginPrepare(proc, tblArg)
	case "hnsw_create":
		tblArg.ctr.state, err = hnswCreatePrepare(proc, tblArg)
	case "hnsw_search":
		tblArg.ctr.state, err = hnswSearchPrepare(proc, tblArg)
	case "ivf_create":
		tblArg.ctr.state, err = ivfCreatePrepare(proc, tblArg)
	case "ivf_search":
		tblArg.ctr.state, err = ivfSearchPrepare(proc, tblArg)
	case "parse_jsonl_data":
		tblArg.ctr.state, err = parseJsonlDataPrepare(proc, tblArg)
	case "parse_jsonl_file":
		tblArg.ctr.state, err = parseJsonlFilePrepare(proc, tblArg)
	case "table_stats":
		tblArg.ctr.state, err = tableStatsPrepare(proc, tblArg)
	default:
		tblArg.ctr.state = nil
		err = moerr.NewNotSupported(proc.Ctx, fmt.Sprintf("table function %s is not supported", tblArg.FuncName))
	}

	return err
}

func (tableFunction *TableFunction) createResultBatch() *batch.Batch {
	bat := batch.NewWithSize(len(tableFunction.Attrs))
	bat.Attrs = tableFunction.Attrs
	for i := range tableFunction.ctr.retSchema {
		bat.Vecs[i] = vector.NewVec(tableFunction.ctr.retSchema[i])
	}
	return bat
}

func (tableFunction *TableFunction) ApplyPrepare(proc *process.Process) error {
	return tableFunction.Prepare(proc)
}

func (tableFunction *TableFunction) ApplyArgsEval(inbat *batch.Batch, proc *process.Process) error {
	var err error
	for i := range tableFunction.ctr.executorsForArgs {
		tableFunction.ctr.argVecs[i], err = tableFunction.ctr.executorsForArgs[i].Eval(proc, []*batch.Batch{inbat}, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tableFunction *TableFunction) ApplyStart(nthRow int, proc *process.Process, analyzer process.Analyzer) error {
	return tableFunction.ctr.state.start(tableFunction, proc, nthRow, analyzer)
}

func (tableFunction *TableFunction) ApplyCall(proc *process.Process) (vm.CallResult, error) {
	return tableFunction.ctr.state.call(tableFunction, proc)
}

func (tableFunction *TableFunction) ApplyEnd(proc *process.Process) error {
	return tableFunction.ctr.state.end(tableFunction, proc)
}

func (tableFunction *TableFunction) GenerateSeriesCtrNumState(start, end, step, next int64) {
	tableFunction.ctr.state = &generateSeriesArg{i64State: genNumState[int64]{start: start, end: end, step: step, next: next}}
}

func (tableFunction *TableFunction) GetGenerateSeriesCtrNumStateStep() int64 {
	arg := tableFunction.ctr.state.(*generateSeriesArg)
	return arg.i64State.step
}
