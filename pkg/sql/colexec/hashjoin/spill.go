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

package hashjoin

import (
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMagic      = 0x12345678DEADBEEF
	spillBufferSize = 8192
)

type bucketBuffer struct {
	rowIds []int32
	bat    *batch.Batch
}

func (hashJoin *HashJoin) getSpilledInputBatch(proc *process.Process) (vm.CallResult, error) {
	var result vm.CallResult
	ctr := &hashJoin.ctr

	for {
		// Load next bucket if needed
		if ctr.mp == nil || ctr.bucketProbeIdx >= len(ctr.bucketProbeBatches) {
			// If current bucket probe is done and we have rightRowsMatched, return nil to trigger finalization
			if ctr.mp != nil && ctr.rightRowsMatched != nil {
				return result, nil
			}

			if ctr.nextBucketIdx >= len(ctr.spilledBuildBuckets) {
				return result, nil
			}

			buildBatches, err := loadSpilledBuildBucket(proc, ctr.spilledBuildBuckets[ctr.nextBucketIdx])
			if err != nil {
				return result, err
			}

			probeBatches, err := hashJoin.loadSpilledProbeBucket(proc, ctr.nextBucketIdx)
			if err != nil {
				for _, bat := range buildBatches {
					bat.Clean(proc.Mp())
				}
				return result, err
			}

			ctr.nextBucketIdx++

			if len(buildBatches) == 0 || len(probeBatches) == 0 {
				for _, bat := range buildBatches {
					bat.Clean(proc.Mp())
				}
				for _, bat := range probeBatches {
					bat.Clean(proc.Mp())
				}
				continue
			}

			// Rebuild hashmap for this bucket
			tmpJoinMap, err := hashJoin.rebuildHashmapForBucket(proc, buildBatches)
			if err != nil {
				for _, bat := range buildBatches {
					bat.Clean(proc.Mp())
				}
				for _, bat := range probeBatches {
					bat.Clean(proc.Mp())
				}
				return result, err
			}

			ctr.bucketBuildBatches = buildBatches
			ctr.bucketProbeBatches = probeBatches
			ctr.bucketProbeIdx = 0
			ctr.mp = tmpJoinMap
			ctr.itr = nil

			ctr.rightBats = ctr.mp.GetBatches()
			ctr.rightRowCnt = ctr.mp.GetRowCount()

			if hashJoin.IsRightJoin {
				if ctr.rightRowCnt > 0 {
					ctr.rightRowsMatched = &bitmap.Bitmap{}
					ctr.rightRowsMatched.InitWithSize(ctr.rightRowCnt)
					ctr.rightMatchedIter = nil
				}
			}

			ctr.probeState = psNextBatch
			ctr.lastIdx = 0
			ctr.vsIdx = 0
		}

		if ctr.bucketProbeIdx < len(ctr.bucketProbeBatches) {
			result.Batch = ctr.bucketProbeBatches[ctr.bucketProbeIdx]
			ctr.bucketProbeIdx++
			return result, nil
		}
	}
}

func (ctr *container) flushBucketBuffer(proc *process.Process, buf *bucketBuffer, file *os.File, analyzer process.Analyzer) (int64, error) {
	if buf.bat == nil || buf.bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(buf.bat.RowCount())
	ctr.spillWriteBuf.Reset()
	ctr.spillWriteBuf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize
	batchSizePos := ctr.spillWriteBuf.Len()
	ctr.spillWriteBuf.Write(types.EncodeInt64(new(int64)))
	
	// Write batch data directly to spillWriteBuf
	batchStartPos := ctr.spillWriteBuf.Len()
	buf.bat.MarshalBinaryWithBuffer(&ctr.spillWriteBuf, false)
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

	buf.bat.Clean(proc.Mp())
	buf.bat = nil
	buf.rowIds = buf.rowIds[:0]
	return cnt, nil
}

func createProbeSpillFiles(proc *process.Process) ([]string, []*os.File, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, nil, err
	}

	uid, _ := uuid.NewV7()
	baseName := fmt.Sprintf("join_probe_%s", uid.String())
	logutil.Infof("creating probe spill files, base: %s", baseName)

	buckets := make([]string, spillNumBuckets)
	files := make([]*os.File, spillNumBuckets)

	for i := 0; i < spillNumBuckets; i++ {
		buckets[i] = fmt.Sprintf("%s_%d", baseName, i)
		if files[i], err = spillfs.CreateFile(proc.Ctx, buckets[i]); err != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return nil, nil, err
		}
	}

	return buckets, files, nil
}

func (ctr *container) appendProbeBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*bucketBuffer, analyzer process.Analyzer) error {
	ctr.evalJoinCondition(bat, proc)

	// Reuse hashValues buffer
	rowCount := bat.RowCount()
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]

	if err := computeXXHash(ctr.eqCondVecs, hashValues); err != nil {
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
		if buf.bat == nil {
			buf.bat = batch.NewWithSize(len(bat.Vecs))
			for i, vec := range bat.Vecs {
				typ := *vec.GetType()
				buf.bat.Vecs[i] = vector.NewVec(typ)
			}
		}

		// Append rows to buffer
		for _, sel := range sels {
			for i, vec := range bat.Vecs {
				if err := buf.bat.Vecs[i].UnionOne(vec, int64(sel), proc.Mp()); err != nil {
					return err
				}
			}
		}
		buf.bat.SetRowCount(buf.bat.RowCount() + len(sels))
		analyzer.SpillRows(int64(len(sels)))

		// Flush if buffer is full
		if buf.bat.RowCount() >= spillBufferSize {
			if _, err := ctr.flushBucketBuffer(proc, buf, files[bucketId], analyzer); err != nil {
				return err
			}
		}
	}

	return nil
}

// computeXXHash computes xxhash values for partitioning
// Must match the hash logic in hashbuild/spill.go
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

func (hashJoin *HashJoin) loadSpilledProbeBucket(proc *process.Process, bucketIdx int) ([]*batch.Batch, error) {
	if bucketIdx >= len(hashJoin.ctr.spilledProbeBuckets) {
		return nil, nil
	}

	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}

	file, err := spillfs.OpenFile(proc.Ctx, hashJoin.ctr.spilledProbeBuckets[bucketIdx])
	if err != nil {
		return nil, err
	}
	defer file.Close()

	batches := make([]*batch.Batch, 0)
	buf := make([]byte, 8)

	for {
		// Read count
		if _, err := io.ReadFull(file, buf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		cnt := types.DecodeInt64(buf)

		// Read batch size
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}
		batchSize := types.DecodeInt64(buf)

		// Read batch data
		batchData := make([]byte, batchSize)
		if _, err := io.ReadFull(file, batchData); err != nil {
			return nil, err
		}

		// Read magic
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}
		magic := types.DecodeUint64(buf)
		if magic != spillMagic {
			return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
		}

		bat := batch.NewWithSize(0)
		if err := bat.UnmarshalBinary(batchData); err != nil {
			return nil, err
		}

		if bat.RowCount() != int(cnt) {
			return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
		}

		batches = append(batches, bat)
	}

	// Delete the spill file after successful load
	spillfs.Delete(proc.Ctx, hashJoin.ctr.spilledProbeBuckets[bucketIdx])

	return batches, nil
}

func loadSpilledBuildBucket(proc *process.Process, bucketName string) ([]*batch.Batch, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}

	file, err := spillfs.OpenFile(proc.Ctx, bucketName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	batches := make([]*batch.Batch, 0)
	buf := make([]byte, 8)

	for {
		// Read count
		if _, err := io.ReadFull(file, buf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		cnt := types.DecodeInt64(buf)

		// Read batch size
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}
		batchSize := types.DecodeInt64(buf)

		// Read batch data
		batchData := make([]byte, batchSize)
		if _, err := io.ReadFull(file, batchData); err != nil {
			return nil, err
		}

		// Read magic
		if _, err := io.ReadFull(file, buf); err != nil {
			return nil, err
		}
		magic := types.DecodeUint64(buf)
		if magic != spillMagic {
			return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
		}

		bat := batch.NewWithSize(0)
		if err := bat.UnmarshalBinary(batchData); err != nil {
			return nil, err
		}

		if bat.RowCount() != int(cnt) {
			return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
		}

		batches = append(batches, bat)
	}

	// Delete the spill file after successful load
	spillfs.Delete(proc.Ctx, bucketName)

	return batches, nil
}

func (hashJoin *HashJoin) rebuildHashmapForBucket(proc *process.Process, buildBatches []*batch.Batch) (*message.JoinMap, error) {
	// Create a temporary hashmap builder
	builder := &hashbuild.HashmapBuilder{}
	if err := builder.Prepare(hashJoin.EqConds[1], -1, proc); err != nil {
		return nil, err
	}

	// Copy batches into builder
	for _, bat := range buildBatches {
		if err := builder.Batches.CopyIntoBatches(bat, proc); err != nil {
			builder.Free(proc)
			return nil, err
		}
		builder.InputBatchRowCount += bat.RowCount()
	}

	// Build hashmap
	if err := builder.BuildHashmap(hashJoin.HashOnPK, true, false, proc); err != nil {
		builder.Free(proc)
		return nil, err
	}

	jm := message.NewJoinMap(builder.MultiSels, builder.IntHashMap, builder.StrHashMap, nil, builder.Batches.Buf, proc.Mp())
	jm.SetRowCount(int64(builder.InputBatchRowCount))
	jm.IncRef(1)

	// Free only the executors - hashmaps, MultiSels, and batches are now owned by JoinMap
	builder.FreeExecutors()

	return jm, nil
}
