// Copyright 2022 Matrix Origin
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

package colexec

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type Batches struct {
	Buf []*batch.Batch
}

func (bs *Batches) Clean(mp *mpool.MPool) {
	for i := range bs.Buf {
		bs.Buf[i].Clean(mp)
	}
	bs.Buf = nil
}

func (bs *Batches) CleanOnlyData() {
	for i := range bs.Buf {
		bs.Buf[i].CleanOnlyData()
	}
	bs.Buf = nil
}
func (bs *Batches) Reset() {
	if bs.Buf != nil {
		bs.Buf = nil
	}
}

// copy from input batch into batches
// the batches structure hold data in fix size 8192 rows, and continue to append from next batch
// if error return , the batches will clean itself
func (bs *Batches) CopyIntoBatches(src *batch.Batch, proc *process.Process) (err error) {
	defer func() {
		if err != nil {
			bs.Clean(proc.Mp())
		}
	}()

	if bs.Buf == nil {
		bs.Buf = make([]*batch.Batch, 0, 16)
	}

	var tmp *batch.Batch
	if src.RowCount() == DefaultBatchSize {
		tmp, err = src.Dup(proc.Mp())
		if err != nil {
			return err
		}
		bs.Buf = append(bs.Buf, tmp)
		lenBuf := len(bs.Buf)
		if lenBuf > 1 && bs.Buf[lenBuf-2].RowCount() != DefaultBatchSize {
			//swap the last 2 batches to get right order
			bs.Buf[lenBuf-2], bs.Buf[lenBuf-1] = bs.Buf[lenBuf-1], bs.Buf[lenBuf-2]
		}
		return nil
	}

	offset := 0
	appendRows := 0
	length := src.RowCount()

	for offset < length {
		lenBuf := len(bs.Buf)
		if lenBuf > 0 && bs.Buf[lenBuf-1].RowCount() != DefaultBatchSize {
			tmp = bs.Buf[lenBuf-1]
		} else {
			tmp, err = proc.NewBatchFromSrc(src, 0)
			if err != nil {
				return err
			}
			bs.Buf = append(bs.Buf, tmp)
		}
		appendRows, err = appendToFixedSizeFromOffset(tmp, src, offset, proc)
		if err != nil {
			return err
		}
		offset += appendRows
	}
	return nil
}

func appendToFixedSizeFromOffset(dst *batch.Batch, src *batch.Batch, offset int, proc *process.Process) (int, error) {
	var err error
	if dst == nil {
		panic("should not be nil")
	}
	if dst.RowCount() >= DefaultBatchSize {
		panic("can't call AppendToFixedSizeFromOffset when batch is full!")
	}
	if len(dst.Vecs) != len(src.Vecs) {
		return 0, moerr.NewInternalError(proc.Ctx, "unexpected error happens in batch append")
	}
	length := DefaultBatchSize - dst.RowCount()
	if length+offset > src.RowCount() {
		length = src.RowCount() - offset
	}
	for i := range dst.Vecs {
		if err = dst.Vecs[i].UnionBatch(src.Vecs[i], int64(offset), length, nil, proc.Mp()); err != nil {
			return 0, err
		}
		dst.Vecs[i].SetSorted(false)
	}
	dst.AddRowCount(length)
	return length, nil
}
