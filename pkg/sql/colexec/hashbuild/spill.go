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
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

type bucketBuffer struct {
	rowIds []int32
	bat    *batch.Batch
}

func flushBucketBuffer(proc *process.Process, buf *bucketBuffer, file *os.File, analyzer process.Analyzer) (int64, error) {
	if buf.bat == nil || buf.bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(buf.bat.RowCount())
	batchData := &bytes.Buffer{}
	buf.bat.MarshalBinaryWithBuffer(batchData, false)
	batchSize := int64(batchData.Len())

	writeBuf := bytes.NewBuffer(make([]byte, 0, batchSize+32))
	writeBuf.Write(types.EncodeInt64(&cnt))
	writeBuf.Write(types.EncodeInt64(&batchSize))
	writeBuf.Write(batchData.Bytes())
	magic := uint64(spillMagic)
	writeBuf.Write(types.EncodeUint64(&magic))

	written, err := file.Write(writeBuf.Bytes())
	if err != nil {
		return 0, err
	}
	analyzer.Spill(int64(written))

	buf.bat.Clean(proc.Mp())
	buf.bat = nil
	buf.rowIds = buf.rowIds[:0]
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

func appendBatchToSpillFiles(proc *process.Process, bat *batch.Batch, files []*os.File, buffers []*bucketBuffer, hashOnPK bool, conditions []*plan.Expr, analyzer process.Analyzer) ([]int64, error) {
	if bat.RowCount() == 0 {
		return make([]int64, spillNumBuckets), nil
	}

	rowCnts := make([]int64, spillNumBuckets)

	// Evaluate hash keys
	executors := make([]colexec.ExpressionExecutor, len(conditions))
	var err error
	for i, expr := range conditions {
		if executors[i], err = colexec.NewExpressionExecutor(proc, expr); err != nil {
			return nil, err
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
			return nil, err
		}
		keyVecs[i] = vec
	}

	// Compute hash for each row
	hashValues := make([]uint64, bat.RowCount())
	if err := computeXXHash(keyVecs, hashValues); err != nil {
		return nil, err
	}

	bucketRowIds := make([][]int32, spillNumBuckets)
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
					return nil, err
				}
			}
		}
		buf.bat.SetRowCount(buf.bat.RowCount() + len(sels))
		rowCnts[bucketId] += int64(len(sels))
		analyzer.SpillRows(int64(len(sels)))

		// Flush if buffer is full
		if buf.bat.RowCount() >= spillBufferSize {
			if _, err := flushBucketBuffer(proc, buf, files[bucketId], analyzer); err != nil {
				return nil, err
			}
		}
	}

	return rowCnts, nil
}

func (hashBuild *HashBuild) shouldSpillBatches() bool {
	if !hashBuild.IsShuffle || hashBuild.SpillThreshold <= 0 {
		return false
	}
	var totalSize int64
	for _, bat := range hashBuild.ctr.hashmapBuilder.Batches.Buf {
		totalSize += int64(bat.Size())
	}
	return totalSize > hashBuild.SpillThreshold
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

			typ := vec.GetType()

			if typ.IsFixedLen() {
				// For fixed-length types, get raw bytes using type size
				switch typ.TypeSize() {
				case 1:
					v := vector.GetFixedAtNoTypeCheck[uint8](vec, idx)
					buf = append(buf, types.EncodeUint8(&v)...)
				case 2:
					v := vector.GetFixedAtNoTypeCheck[uint16](vec, idx)
					buf = append(buf, types.EncodeUint16(&v)...)
				case 4:
					v := vector.GetFixedAtNoTypeCheck[uint32](vec, idx)
					buf = append(buf, types.EncodeUint32(&v)...)
				case 8:
					v := vector.GetFixedAtNoTypeCheck[uint64](vec, idx)
					buf = append(buf, types.EncodeUint64(&v)...)
				case 16:
					v := vector.GetFixedAtNoTypeCheck[[16]byte](vec, idx)
					buf = append(buf, v[:]...)
				default:
					// Fallback for other fixed sizes
					data := vec.GetBytesAt(idx)
					buf = append(buf, data...)
				}
			} else {
				data := vec.GetBytesAt(idx)
				buf = append(buf, data...)
			}
		}

		// Compute xxhash
		hashValues[i] = xxhash.Sum64(buf)
	}

	return nil
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
