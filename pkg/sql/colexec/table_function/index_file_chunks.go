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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/indexfile"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// indexFileChunksBatchRows bounds the chunks one output batch holds.
const indexFileChunksBatchRows = 64

type indexFileChunksState struct {
	batch *batch.Batch
	src   *indexfile.Source
	next  int64
	stop  int64
	buf   []byte
}

func indexFileChunksPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	var err error
	tf.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tf.Args)
	if err != nil {
		return nil, err
	}
	tf.ctr.argVecs = make([]*vector.Vector, len(tf.Args))
	return &indexFileChunksState{}, nil
}

func (st *indexFileChunksState) reset(tf *TableFunction, proc *process.Process) {
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
	st.src = nil
}

func (st *indexFileChunksState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
	}
	st.src = nil
}

func (st *indexFileChunksState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (st *indexFileChunksState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	st.src = nil
	// The source registry is local to the CN that registered it; a function with an
	// input may be compiled into remote scopes.
	if !tf.isTbFuncSourceOp() {
		return moerr.NewNotSupported(proc.Ctx, indexfile.FuncName+" cannot take an input")
	}
	if len(tf.ctr.argVecs) != 3 {
		return moerr.NewInvalidInput(proc.Ctx, indexfile.FuncName+" requires 3 arguments (source, first chunk, chunk count)")
	}
	tokenVec := tf.ctr.argVecs[0]
	if tokenVec.IsNull(uint64(nthRow)) || !tokenVec.GetType().Oid.IsMySQLString() {
		return moerr.NewInvalidInput(proc.Ctx, indexfile.FuncName+": invalid source")
	}
	start, err := indexFileChunksInt64(proc, tf.ctr.argVecs[1], nthRow)
	if err != nil {
		return err
	}
	count, err := indexFileChunksInt64(proc, tf.ctr.argVecs[2], nthRow)
	if err != nil {
		return err
	}
	src, err := indexfile.LookupSource(tokenVec.GetStringAt(nthRow), proc.GetTxnOperator().Txn().ID)
	if err != nil {
		return err
	}
	numChunks := (src.Size + indexfile.ChunkSize - 1) / indexfile.ChunkSize
	if count <= 0 || start < src.FirstChunk || count > src.FirstChunk+numChunks-start {
		return moerr.NewInvalidInputf(proc.Ctx, "%s: chunks [%d, %d) outside the file", indexfile.FuncName, start, start+count)
	}

	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}
	if st.buf == nil {
		st.buf = make([]byte, indexfile.ChunkSize)
	}
	st.src, st.next, st.stop = src, start, start+count
	return nil
}

func (st *indexFileChunksState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if st.src == nil || st.next >= st.stop {
		return vm.CancelResult, nil
	}
	st.batch.CleanOnlyData()
	rows := 0
	for ; rows < indexFileChunksBatchRows && st.next < st.stop; rows++ {
		data, err := st.src.ReadChunk(st.next, st.buf)
		if err != nil {
			return vm.CancelResult, err
		}
		for i, attr := range tf.Attrs {
			vec := st.batch.Vecs[i]
			switch strings.ToLower(attr) {
			case catalog.IndexStorage_TblCol_Index_Id:
				err = vector.AppendBytes(vec, []byte(st.src.IndexID), false, proc.Mp())
			case catalog.IndexStorage_TblCol_Chunk_Id:
				err = vector.AppendFixed(vec, st.next, false, proc.Mp())
			case catalog.IndexStorage_TblCol_Data:
				err = vector.AppendBytes(vec, data, false, proc.Mp())
			case catalog.IndexStorage_TblCol_Tag:
				err = vector.AppendFixed(vec, st.src.Tag, false, proc.Mp())
			default:
				err = moerr.NewInvalidInputf(proc.Ctx, "%s: invalid column %s", indexfile.FuncName, attr)
			}
			if err != nil {
				return vm.CancelResult, err
			}
		}
		st.next++
	}
	st.batch.SetRowCount(rows)
	return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
}

func indexFileChunksInt64(proc *process.Process, vec *vector.Vector, nthRow int) (int64, error) {
	if vec.IsNull(uint64(nthRow)) {
		return 0, moerr.NewInvalidInputf(proc.Ctx, "%s: chunk arguments cannot be NULL", indexfile.FuncName)
	}
	switch vec.GetType().Oid {
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, nthRow), nil
	case types.T_int32:
		return int64(vector.GetFixedAtNoTypeCheck[int32](vec, nthRow)), nil
	}
	return 0, moerr.NewInvalidInputf(proc.Ctx, "%s: chunk arguments must be integers", indexfile.FuncName)
}
