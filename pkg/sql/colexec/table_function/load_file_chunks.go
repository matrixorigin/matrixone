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
	"io"
	"math"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/datalink"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type loadFileChunksState struct {
	batch       *batch.Batch
	dl          datalink.Datalink
	curOffset   int64
	endOffset   int64
	chunkSize   int64
	nextChunk   int64
	initialized bool
}

func loadFileChunksPrepare(proc *process.Process, tableFunction *TableFunction) (tvfState, error) {
	var err error
	tableFunction.ctr.executorsForArgs, err = colexec.NewExpressionExecutorsFromPlanExpressions(proc, tableFunction.Args)
	if err != nil {
		return nil, err
	}
	tableFunction.ctr.argVecs = make([]*vector.Vector, len(tableFunction.Args))
	return &loadFileChunksState{}, nil
}

func (st *loadFileChunksState) reset(tf *TableFunction, proc *process.Process) {
	if st.batch != nil {
		st.batch.CleanOnlyData()
	}
	st.initialized = false
}

func (st *loadFileChunksState) free(tf *TableFunction, proc *process.Process, pipelineFailed bool, err error) {
	if st.batch != nil {
		st.batch.Clean(proc.Mp())
	}
}

func (st *loadFileChunksState) end(tf *TableFunction, proc *process.Process) error {
	return nil
}

func (st *loadFileChunksState) start(tf *TableFunction, proc *process.Process, nthRow int, analyzer process.Analyzer) error {
	st.initialized = false

	if len(tf.ctr.argVecs) < 2 {
		return moerr.NewInvalidInput(proc.Ctx, "load_file_chunks requires 2 arguments (datalink, chunk_size)")
	}

	srcVec := tf.ctr.argVecs[0]
	if srcVec.GetNulls().Contains(uint64(nthRow)) {
		return nil
	}

	var src string
	switch srcVec.GetType().Oid {
	case types.T_datalink, types.T_varchar, types.T_text, types.T_char:
		src = srcVec.GetStringAt(nthRow)
	default:
		return moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: datalink must be string or datalink type")
	}

	sizeVec := tf.ctr.argVecs[1]
	chunkSize, err := getInt64FromVector(proc, sizeVec, nthRow)
	if err != nil {
		return err
	}
	if chunkSize <= 0 {
		return moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: chunk_size must be positive")
	}
	if chunkSize > int64(types.MaxBlobLen) {
		return moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: chunk_size exceeds max blob length")
	}

	dl, err := datalink.NewDatalink(src, proc)
	if err != nil {
		return err
	}

	endOffset := int64(0)
	if dl.Size < 0 {
		etlFS, readPath, err := fileservice.GetForETL(proc.Ctx, proc.GetFileService(), dl.MoPath)
		if err != nil {
			return err
		}
		entry, err := etlFS.StatFile(proc.Ctx, readPath)
		if err != nil {
			return err
		}
		if dl.Offset > entry.Size {
			return moerr.NewInternalError(proc.Ctx, "offset exceeds file size")
		}
		endOffset = entry.Size
	} else {
		endOffset = dl.Offset + dl.Size
	}

	if st.batch == nil {
		st.batch = tf.createResultBatch()
	} else {
		st.batch.CleanOnlyData()
	}

	st.dl = dl
	st.curOffset = dl.Offset
	st.endOffset = endOffset
	st.chunkSize = chunkSize
	st.nextChunk = 0
	st.initialized = true

	return nil
}

func (st *loadFileChunksState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if !st.initialized {
		return vm.CancelResult, nil
	}
	if st.curOffset >= st.endOffset {
		return vm.CancelResult, nil
	}

	st.batch.CleanOnlyData()

	maxRows := int(int64(types.MaxBlobLen) / st.chunkSize)
	if maxRows < 1 {
		maxRows = 1
	}
	if maxRows > 1024 {
		maxRows = 1024
	}

	var cnt int
	for cnt < maxRows && st.curOffset < st.endOffset {
		size := st.chunkSize
		remain := st.endOffset - st.curOffset
		if remain < size {
			size = remain
		}
		dl := st.dl
		dl.Offset = st.curOffset
		dl.Size = size
		r, err := dl.NewReadCloser(proc)
		if err != nil {
			return vm.CancelResult, err
		}
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			return vm.CancelResult, err
		}
		if int64(len(data)) != size {
			return vm.CancelResult, moerr.NewInternalError(proc.Ctx, "read size mismatch in load_file_chunks")
		}

		for colIdx, attr := range tf.Attrs {
			switch strings.ToLower(attr) {
			case "chunk_id":
				if err := vector.AppendFixed[int64](st.batch.Vecs[colIdx], st.nextChunk, false, proc.Mp()); err != nil {
					return vm.CancelResult, err
				}
			case "offset":
				if err := vector.AppendFixed[int64](st.batch.Vecs[colIdx], st.curOffset, false, proc.Mp()); err != nil {
					return vm.CancelResult, err
				}
			case "data":
				if err := vector.AppendBytes(st.batch.Vecs[colIdx], data, false, proc.Mp()); err != nil {
					return vm.CancelResult, err
				}
			default:
				return vm.CancelResult, moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: invalid column name")
			}
		}

		st.curOffset += size
		st.nextChunk++
		cnt++
	}

	if cnt == 0 {
		return vm.CancelResult, nil
	}
	st.batch.SetRowCount(cnt)
	return vm.CallResult{Status: vm.ExecNext, Batch: st.batch}, nil
}

func getInt64FromVector(proc *process.Process, vec *vector.Vector, nthRow int) (int64, error) {
	if vec.GetNulls().Contains(uint64(nthRow)) {
		return 0, moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: chunk_size cannot be NULL")
	}
	switch vec.GetType().Oid {
	case types.T_int32:
		return int64(vector.GetFixedAtNoTypeCheck[int32](vec, nthRow)), nil
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, nthRow), nil
	case types.T_uint32:
		return int64(vector.GetFixedAtNoTypeCheck[uint32](vec, nthRow)), nil
	case types.T_uint64:
		v := vector.GetFixedAtNoTypeCheck[uint64](vec, nthRow)
		if v > math.MaxInt64 {
			return 0, moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: chunk_size too large")
		}
		return int64(v), nil
	default:
		return 0, moerr.NewInvalidInput(proc.Ctx, "load_file_chunks: chunk_size must be integer type")
	}
}
