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
	"bufio"
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

type spillBucketReader struct {
	file   *os.File
	reader *bufio.Reader
	buf    []byte
}

func newSpillBucketReader(proc *process.Process, bucketName string) (*spillBucketReader, error) {
	spillfs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}

	file, err := spillfs.OpenFile(proc.Ctx, bucketName)
	if err != nil {
		return nil, err
	}

	return &spillBucketReader{
		file:   file,
		reader: bufio.NewReaderSize(file, spillBufferSize),
		buf:    make([]byte, 8),
	}, nil
}

func (r *spillBucketReader) readBatch(proc *process.Process, reuseBat *batch.Batch) (*batch.Batch, error) {
	// Read count
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	cnt := types.DecodeInt64(r.buf)

	// Read batch size
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		return nil, err
	}
	batchSize := types.DecodeInt64(r.buf)

	// Clean previous data
	reuseBat.CleanOnlyData()

	// Create limited reader for exact batch size
	limitedReader := io.LimitReader(r.reader, batchSize)

	// Unmarshal directly from reader
	if err := reuseBat.UnmarshalFromReader(limitedReader, proc.Mp()); err != nil {
		return nil, err
	}

	// Read magic
	if _, err := io.ReadFull(r.reader, r.buf); err != nil {
		return nil, err
	}
	magic := types.DecodeUint64(r.buf)
	if magic != spillMagic {
		return nil, moerr.NewInternalError(proc.Ctx, "corrupted spill file")
	}

	if reuseBat.RowCount() != int(cnt) {
		return nil, moerr.NewInternalError(proc.Ctx, "row count mismatch")
	}

	return reuseBat, nil
}

func (r *spillBucketReader) close() {
	if r.file != nil {
		r.file.Close()
		r.file = nil
	}
}

func (hashJoin *HashJoin) getSpilledInputBatch(proc *process.Process) (vm.CallResult, error) {
	var result vm.CallResult
	ctr := &hashJoin.ctr

	for {
		// Try to read next probe batch from current bucket
		if ctr.probeBucketReader != nil {
			// Initialize reusable batch if needed
			if ctr.spillProbeReadBatch == nil {
				ctr.spillProbeReadBatch = batch.NewOffHeapWithSize(0)
			}

			bat, err := ctr.probeBucketReader.readBatch(proc, ctr.spillProbeReadBatch)
			if err != nil && err != io.EOF {
				return result, err
			}

			if bat != nil {
				result.Batch = bat
				return result, nil
			}

			// EOF - done with this bucket, close the file
			ctr.probeBucketReader.close()
			ctr.probeBucketReader = nil

			// Delete probe file
			spillfs, _ := proc.GetSpillFileService()
			spillfs.Delete(proc.Ctx, ctr.spilledProbeBuckets[ctr.nextBucketIdx-1])

			// If we have rightRowsMatched, return nil to trigger finalization
			// The hashmap will be cleaned after finalize completes
			if ctr.rightRowsMatched != nil {
				return result, nil
			}

			// For non-right joins, clean up immediately
			ctr.cleanHashMap()
		}

		// Load next bucket if needed
		if ctr.mp == nil {
			if ctr.nextBucketIdx >= len(ctr.spilledBuildBuckets) {
				return result, nil
			}

			// Stream build batches into hashmap
			tmpJoinMap, err := hashJoin.rebuildHashmapForBucket(proc, ctr.spilledBuildBuckets[ctr.nextBucketIdx])
			if err != nil {
				return result, err
			}

			// Delete build file
			spillfs, _ := proc.GetSpillFileService()
			spillfs.Delete(proc.Ctx, ctr.spilledBuildBuckets[ctr.nextBucketIdx])

			// Open probe reader
			probeReader, err := newSpillBucketReader(proc, ctr.spilledProbeBuckets[ctr.nextBucketIdx])
			if err != nil {
				tmpJoinMap.Free()
				return result, err
			}

			ctr.nextBucketIdx++
			ctr.probeBucketReader = probeReader
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
	}
}

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

func (ctr *container) appendProbeBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*batch.Batch, analyzer process.Analyzer) error {
	ctr.evalJoinCondition(bat, proc)

	// Reuse hashValues buffer
	rowCount := bat.RowCount()
	if cap(ctr.spillHashValues) < rowCount {
		ctr.spillHashValues = make([]uint64, rowCount)
	}
	hashValues := ctr.spillHashValues[:rowCount]

	if err := computeXXHash(ctr.eqCondVecs, hashValues, &ctr.spillHashBuf); err != nil {
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

// computeXXHash computes xxhash values for partitioning
// Must match the hash logic in hashbuild/spill.go
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64, buf *[]byte) error {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return nil
	}

	rowCount := len(hashValues)
	if cap(*buf) < 128 {
		*buf = make([]byte, 0, 128)
	}

	for i := 0; i < rowCount; i++ {
		*buf = (*buf)[:0]

		// Encode all key columns for this row
		for _, vec := range keyVecs {
			// For constant vectors, always use index 0
			idx := i
			if vec.IsConst() {
				idx = 0
			} else if i >= vec.Length() {
				continue
			}

			*buf = append(*buf, vec.GetRawBytesAt(idx)...)
		}

		// Compute xxhash
		hashValues[i] = xxhash.Sum64(*buf)
	}

	return nil
}

func (hashJoin *HashJoin) rebuildHashmapForBucket(proc *process.Process, bucketName string) (*message.JoinMap, error) {
	ctr := &hashJoin.ctr

	// Create a temporary hashmap builder
	builder := &hashbuild.HashmapBuilder{}
	if err := builder.Prepare(hashJoin.EqConds[1], -1, proc); err != nil {
		return nil, err
	}

	// Stream batches from file
	reader, err := newSpillBucketReader(proc, bucketName)
	if err != nil {
		builder.Free(proc)
		return nil, err
	}
	defer reader.close()

	// Initialize reusable batch if needed
	if ctr.spillBuildReadBatch == nil {
		ctr.spillBuildReadBatch = batch.NewOffHeapWithSize(0)
	}

	for {
		bat, err := reader.readBatch(proc, ctr.spillBuildReadBatch)
		if err == io.EOF {
			break
		}
		if err != nil {
			builder.Free(proc)
			return nil, err
		}

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
