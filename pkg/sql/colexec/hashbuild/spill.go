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

package hashbuild

import (
	"fmt"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMaxPass    = 3
	spillMagic      = 0x12345678DEADBEEF
	spillBufferSize = 8192 // Buffer 8192 rows before flushing
)

func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, file *os.File, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize
	batchSizePos := ctr.spillWriteBuf.Len()
	ctr.spillWriteBuf.Write(types.EncodeInt64(new(int64)))

	// Write batch data directly to spillWriteBuf
	batchStartPos := ctr.spillWriteBuf.Len()
	bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false)
	batchSize := int64(ctr.spillWriteBuf.Len() - batchStartPos)

	// Write batchSize at reserved position
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(ctr.spillWriteBuf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)

	magic := uint64(spillMagic)
	ctr.spillWriteBuf.Write(types.EncodeUint64(&magic))

	written, err := file.Write(ctr.spillWriteBuf.Bytes())
	if err != nil {
		return 0, err
	}
	analyzer.Spill(int64(written))
	analyzer.SpillRows(cnt)

	return cnt, nil
}

func createSpillFiles(proc *process.Process) ([]string, []*os.File, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, nil, err
	}

	uid, _ := uuid.NewV7()
	baseName := fmt.Sprintf("join_build_%s", uid.String())
	logutil.Infof("creating spill files, base: %s", baseName)

	buckets := make([]string, spillNumBuckets)
	files := make([]*os.File, spillNumBuckets)

	for i := 0; i < spillNumBuckets; i++ {
		buckets[i] = fmt.Sprintf("%s_%d", baseName, i)
		if files[i], err = spillfs.CreateFile(proc.Ctx, buckets[i]); err != nil {
			// Close any opened files on error
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return nil, nil, err
		}
	}

	return buckets, files, nil
}

func (ctr *container) appendBuildBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, hashOnPK bool, conditions []*plan.Expr, analyzer process.Analyzer) error {
	if bat.RowCount() == 0 {
		return nil
	}

	// Evaluate hash keys
	executors := make([]colexec.ExpressionExecutor, len(conditions))
	var err error
	for i, expr := range conditions {
		if executors[i], err = colexec.NewExpressionExecutor(proc, expr); err != nil {
			return err
		}
	}
	defer func() {
		for _, exec := range executors {
			if exec != nil {
				exec.Free()
			}
		}
	}()

	keyVecs := make([]*vector.Vector, len(executors))
	for i, exec := range executors {
		vec, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return err
		}
		keyVecs[i] = vec
	}

	// Reuse hashValues buffer
	rowCount := bat.RowCount()
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]

	if err := computeXXHash(keyVecs, hashValues); err != nil {
		return err
	}

	// Reuse bucketRowIds buffer
	if cap(ctr.spillBucketRowIds) < spillNumBuckets {
		ctr.spillBucketRowIds = make([][]int32, spillNumBuckets)
	}
	bucketRowIds := ctr.spillBucketRowIds[:spillNumBuckets]
	for i := range bucketRowIds {
		bucketRowIds[i] = bucketRowIds[i][:0]
	}
	for row := 0; row < bat.RowCount(); row++ {
		bucketId := hashValues[row] & (spillNumBuckets - 1)
		bucketRowIds[bucketId] = append(bucketRowIds[bucketId], int32(row))
	}

	// Add rows to buffers and flush when needed
	for bucketId := 0; bucketId < spillNumBuckets; bucketId++ {
		sels := bucketRowIds[bucketId]
		if len(sels) == 0 {
			continue
		}

		buf := buffers[bucketId]
		if buf == nil {
			buf = batch.NewOffHeapWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				typ := *vec.GetType()
				buf.Vecs[i] = vector.NewOffHeapVecWithType(typ)
				buf.Vecs[i].PreExtend(spillBufferSize, proc.Mp())
			}
			buffers[bucketId] = buf
		}

		// Append rows to buffer
		for i, vec := range bat.Vecs {
			if err := buf.Vecs[i].UnionInt32(vec, sels, proc.Mp()); err != nil {
				return err
			}
		}
		buf.SetRowCount(buf.RowCount() + len(sels))

		// Flush if buffer is full
		if buf.RowCount() >= spillBufferSize {
			if _, err := ctr.flushBucketBuffer(proc, buf, files[bucketId], analyzer); err != nil {
				return err
			}
			buf.CleanOnlyData()
		}
	}

	return nil
}

func (ctr *container) memUsed() int64 {
	sz := ctr.hashmapBuilder.GetSize()
	for _, bat := range ctr.hashmapBuilder.Batches.Buf {
		sz += int64(bat.Size())
	}
	return sz
}

func (ctr *container) rowCnt() int64 {
	sz := 0
	for _, bat := range ctr.hashmapBuilder.Batches.Buf {
		sz += bat.RowCount()
	}
	return int64(sz)
}

func (hashBuild *HashBuild) shouldSpillBatches() bool {
	ctr := &hashBuild.ctr
	if !hashBuild.IsShuffle || ctr.spillThreshold <= 0 {
		return false
	}
	if ctr.spillThreshold <= 100000 {
		return ctr.rowCnt() >= ctr.spillThreshold
	} else {
		return ctr.memUsed() > ctr.spillThreshold
	}
}

// computeXXHash computes xxhash values for partitioning
// Encodes key vectors and hashes them directly with xxhash
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) error {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return nil
	}

	rowCount := len(hashValues)
	buf := make([]byte, 0, 128)

	for i := 0; i < rowCount; i++ {
		buf = buf[:0]

		// Encode all key columns for this row
		for _, vec := range keyVecs {
			// For constant vectors, always use index 0
			idx := i
			if vec.IsConst() {
				idx = 0
			} else if i >= vec.Length() {
				continue
			}

			buf = append(buf, vec.GetRawBytesAt(idx)...)
		}

		// Compute xxhash
		hashValues[i] = xxhash.Sum64(buf)
	}

	return nil
}
