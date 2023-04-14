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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type S3Writer struct {
	sortIndex int
	pk        map[string]struct{}
	idx       int16

	writer  *blockio.BlockWriter
	lengths []uint64

	metaLocBat *batch.Batch

	// buffers[i] stands the i-th buffer batch used
	// for merge sort (corresponding to table_i,table_i could be unique
	// table or main table)
	buffer *batch.Batch

	// tableBatches[i] used to store the batches of table_i
	// when the batches' size is over 64M, we will use merge
	// sort, and then write a segment in s3
	Bats []*batch.Batch

	// tableBatchSizes are used to record the table_i's batches'
	// size in tableBatches
	Batsize uint64

	sels []int64
}

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 64 * mpool.MB

	TagS3Size uint64 = 10 * mpool.MB
)

func (w *S3Writer) GetMetaLocBat() *batch.Batch {
	return w.metaLocBat
}

func (w *S3Writer) SetMp(attrs []*engine.Attribute) {
	for i := 0; i < len(attrs); i++ {
		if attrs[i].Primary {
			w.pk[attrs[i].Name] = struct{}{}
		}
		if attrs[i].Default == nil {
			continue
		}
	}
}

func (w *S3Writer) Init() {
	w.pk = make(map[string]struct{})
	w.sels = make([]int64, options.DefaultBlockMaxRows)
	for i := 0; i < int(options.DefaultBlockMaxRows); i++ {
		w.sels[i] = int64(i)
	}
	w.ResetMetaLocBat()
}

func (w *S3Writer) SetSortIdx(sortIdx int) {
	w.sortIndex = sortIdx
}

// AllocS3Writers Alloc S3 writers for origin table.
func AllocS3Writers(tableDef *plan.TableDef) ([]*S3Writer, error) {
	uniqueNums := 0
	for _, idx := range tableDef.Indexes {
		if idx.Unique {
			uniqueNums++
		}
	}
	writers := make([]*S3Writer, 1+uniqueNums)
	for i := range writers {
		writers[i] = &S3Writer{
			sortIndex: -1,
			pk:        make(map[string]struct{}),
			idx:       int16(i),
			sels:      make([]int64, options.DefaultBlockMaxRows),
		}
		for j := 0; j < int(options.DefaultBlockMaxRows); j++ {
			writers[i].sels[j] = int64(j)
		}
		writers[i].ResetMetaLocBat()
		//handle origin/main table's sort index.
		if i == 0 {
			//tableDef := insertCtx.TableDef
			if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
				// the serialized cpk col is located in the last of the bat.vecs
				writers[i].sortIndex = len(tableDef.Cols)
			} else {
				// Get Single Col pk index
				for idx, colDef := range tableDef.Cols {
					if colDef.Primary {
						writers[i].sortIndex = idx
						break
					}
				}
				if tableDef.ClusterBy != nil {
					if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
						// the serialized clusterby col is located in the last of the bat.vecs
						writers[i].sortIndex = len(tableDef.Cols)
					} else {
						for idx, colDef := range tableDef.Cols {
							if colDef.Name == tableDef.ClusterBy.Name {
								writers[i].sortIndex = idx
							}
						}
					}
				}
			}
			// get Primary
			for _, def := range tableDef.Cols {
				if def.Primary {
					writers[i].pk[def.Name] = struct{}{}
				}
			}

			// Check whether the composite primary key column is included
			if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
				writers[i].pk[tableDef.Pkey.CompPkeyCol.Name] = struct{}{}
			}
			continue
		}
		//TODO::to handle unique index table's sort index;
	}
	return writers, nil
}

func (w *S3Writer) ResetMetaLocBat() {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc}
	metaLocBat := batch.New(true, attrs)
	metaLocBat.Vecs[0] = vector.NewVec(types.T_int16.ToType())
	metaLocBat.Vecs[1] = vector.NewVec(types.T_text.ToType())
	w.metaLocBat = metaLocBat
}

func (w *S3Writer) WriteEnd(proc *process.Process) {
	if w.metaLocBat.Vecs[0].Length() > 0 {
		w.metaLocBat.SetZs(w.metaLocBat.Vecs[0].Length(), proc.GetMPool())
		proc.SetInputBatch(w.metaLocBat)
		w.ResetMetaLocBat()
	}
}

func (w *S3Writer) WriteS3CacheBatch(proc *process.Process) error {
	if w.Batsize >= TagS3Size {
		if err := w.MergeBlock(len(w.Bats), proc, false); err != nil {
			return err
		}
		w.metaLocBat.SetZs(w.metaLocBat.Vecs[0].Length(), proc.GetMPool())
		return nil
	}
	for _, bat := range w.Bats {
		if err := vector.AppendFixed(
			w.metaLocBat.Vecs[0], -w.idx-1,
			false, proc.GetMPool()); err != nil {
			return err
		}
		bytes, err := bat.MarshalBinary()
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(
			w.metaLocBat.Vecs[1], bytes,
			false, proc.GetMPool()); err != nil {
			return err
		}

	}
	w.metaLocBat.SetZs(w.metaLocBat.Vecs[0].Length(), proc.GetMPool())
	return nil
}

func (w *S3Writer) InitBuffers(bat *batch.Batch) {
	if w.buffer == nil {
		w.buffer = getNewBatch(bat)
	}
}

// the return value can be 1,0,-1
// 1: the tableBatches[idx] is over threshold
// 0: the tableBatches[idx] is equal to threshold
// -1: the tableBatches[idx] is less than threshold
func (w *S3Writer) Put(bat *batch.Batch) int {
	w.Batsize += uint64(bat.Size())
	w.Bats = append(w.Bats, bat)
	if w.Batsize == WriteS3Threshold {
		return 0
	} else if w.Batsize > WriteS3Threshold {
		return 1
	}
	return -1
}

func getFixedCols[T types.FixedSizeT](bats []*batch.Batch, idx int, stopIdx int) (cols [][]T) {
	cols = make([][]T, 0, len(bats))
	for i := range bats {
		cols = append(cols, vector.MustFixedCol[T](bats[i].Vecs[idx]))
	}
	if stopIdx != -1 {
		cols[len(cols)-1] = cols[len(cols)-1][:stopIdx+1]
	}
	return
}

func getStrCols(bats []*batch.Batch, idx int, stopIdx int) (cols [][]string) {
	cols = make([][]string, 0, len(bats))
	for i := range bats {
		cols = append(cols, vector.MustStrCol(bats[i].Vecs[idx]))
	}
	if stopIdx != -1 {
		cols[len(cols)-1] = cols[len(cols)-1][:stopIdx+1]
	}
	return
}

func (w *S3Writer) MergeBlock(length int, proc *process.Process, cacheOvershold bool) error {
	bats := w.Bats[:length]
	stopIdx := -1
	var hackLogic bool
	if w.Batsize > WriteS3Threshold && cacheOvershold {
		w.buffer.CleanOnlyData()
		lastBatch := w.Bats[length-1]
		size := w.Batsize - uint64(lastBatch.Size())
		unionOneFuncs := make([]func(v, w *vector.Vector, sel int64) error, 0, len(lastBatch.Vecs))
		for j := 0; j < len(lastBatch.Vecs); j++ {
			unionOneFuncs = append(unionOneFuncs, vector.GetUnionOneFunction(*lastBatch.Vecs[j].GetType(), proc.GetMPool()))
		}
		for i := 0; i < len(lastBatch.Zs); i++ {
			for j := 0; j < len(lastBatch.Vecs); j++ {
				unionOneFuncs[j](w.buffer.Vecs[j], lastBatch.Vecs[j], int64(i))
			}
			if size+uint64(w.buffer.Size()) == WriteS3Threshold {
				stopIdx = i
				break
			} else if size+uint64(w.buffer.Size()) > WriteS3Threshold {
				// hack logic:
				// 1. if the first row of lastBatch result the size is over WriteS3Threshold
				// the stopIdx will be -1, that's not true, because -1 means
				// the batches' size of all batch (include the last batch) is
				// equal to WriteS3Threshold
				// 2. and there is another extreme situation: the the first row
				// of lastBatch result the size is over WriteS3Threshold and the
				// last batch is the first batch
				// for above, we just care about the fisrt one,the second is no need
				stopIdx = i - 1
				if stopIdx == -1 {
					hackLogic = true
				}
				break
			}
		}
		if stopIdx != -1 {
			w.buffer.SetZs(stopIdx+1, proc.GetMPool())
			w.buffer.Shrink(w.sels[:stopIdx+1])
		}
	}
	if stopIdx == -1 && hackLogic {
		bats = bats[:len(bats)-1]
	}
	sortIdx := -1
	for i := range bats {
		// sort bats firstly
		// for main table
		//TODO::sort unique index table.
		if w.idx == 0 && w.sortIndex != -1 {
			sortByKey(proc, bats[i], w.sortIndex, proc.GetMPool())
			sortIdx = w.sortIndex
		}
	}
	// just write ahead, no need to sort
	if sortIdx == -1 {
		if err := w.generateWriter(proc); err != nil {
			return err
		}

		for i := range bats {
			// stopIdx!=-1 means the all batches' size is over 64M
			if stopIdx != -1 && i == len(bats)-1 {
				break
			}
			if err := w.writeBlock(bats[i]); err != nil {
				return err
			}
		}
		if stopIdx != -1 {
			if err := w.writeBlock(w.buffer); err != nil {
				return err
			}
			w.buffer.CleanOnlyData()
		}
		if err := w.writeEndBlocks(proc); err != nil {
			return err
		}
	} else {
		var merge MergeInterface
		var nulls []*nulls.Nulls
		for i := 0; i < len(bats); i++ {
			nulls = append(nulls, bats[i].Vecs[w.sortIndex].GetNulls())
		}
		pos := w.sortIndex
		switch bats[0].Vecs[sortIdx].GetType().Oid {
		case types.T_bool:
			merge = NewMerge(len(bats), sort.NewBoolLess(), getFixedCols[bool](bats, pos, stopIdx), nulls)
		case types.T_int8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int8](), getFixedCols[int8](bats, pos, stopIdx), nulls)
		case types.T_int16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int16](), getFixedCols[int16](bats, pos, stopIdx), nulls)
		case types.T_int32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int32](), getFixedCols[int32](bats, pos, stopIdx), nulls)
		case types.T_int64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int64](), getFixedCols[int64](bats, pos, stopIdx), nulls)
		case types.T_uint8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint8](), getFixedCols[uint8](bats, pos, stopIdx), nulls)
		case types.T_uint16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint16](), getFixedCols[uint16](bats, pos, stopIdx), nulls)
		case types.T_uint32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint32](), getFixedCols[uint32](bats, pos, stopIdx), nulls)
		case types.T_uint64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint64](), getFixedCols[uint64](bats, pos, stopIdx), nulls)
		case types.T_float32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float32](), getFixedCols[float32](bats, pos, stopIdx), nulls)
		case types.T_float64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float64](), getFixedCols[float64](bats, pos, stopIdx), nulls)
		case types.T_date:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Date](), getFixedCols[types.Date](bats, pos, stopIdx), nulls)
		case types.T_datetime:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Datetime](), getFixedCols[types.Datetime](bats, pos, stopIdx), nulls)
		case types.T_time:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Time](), getFixedCols[types.Time](bats, pos, stopIdx), nulls)
		case types.T_timestamp:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Timestamp](), getFixedCols[types.Timestamp](bats, pos, stopIdx), nulls)
		case types.T_decimal64:
			merge = NewMerge(len(bats), sort.NewDecimal64Less(), getFixedCols[types.Decimal64](bats, pos, stopIdx), nulls)
		case types.T_decimal128:
			merge = NewMerge(len(bats), sort.NewDecimal128Less(), getFixedCols[types.Decimal128](bats, pos, stopIdx), nulls)
		case types.T_uuid:
			merge = NewMerge(len(bats), sort.NewUuidCompLess(), getFixedCols[types.Uuid](bats, pos, stopIdx), nulls)
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[string](), getStrCols(bats, pos, stopIdx), nulls)
		}
		if err := w.generateWriter(proc); err != nil {
			return err
		}
		lens := 0
		size := len(bats)
		w.buffer.CleanOnlyData()
		var batchIndex int
		var rowIndex int
		for size > 0 {
			batchIndex, rowIndex, size = merge.GetNextPos()
			for i := range w.buffer.Vecs {
				w.buffer.Vecs[i].UnionOne(bats[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
			}
			lens++
			if lens == int(options.DefaultBlockMaxRows) {
				lens = 0
				if err := w.writeBlock(w.buffer); err != nil {
					return err
				}
				// force clean
				w.buffer.CleanOnlyData()
			}
		}
		if lens > 0 {
			if err := w.writeBlock(w.buffer); err != nil {
				return err
			}
			w.buffer.CleanOnlyData()
		}
		if err := w.writeEndBlocks(proc); err != nil {
			return err
		}
		// force clean
		w.buffer.CleanOnlyData()
	}
	if stopIdx == -1 {
		if hackLogic {
			w.Batsize = uint64(w.Bats[length-1].Size())
			w.Bats = w.Bats[length-1:]
		} else {
			w.Batsize = 0
			w.Bats = w.Bats[:0]
		}
	} else {
		lastBatch := w.Bats[length-1]
		lastBatch.Shrink(w.sels[stopIdx+1 : lastBatch.Length()])
		w.Bats = w.Bats[:0]
		w.Bats = append(w.Bats, lastBatch)
		w.Batsize = uint64(lastBatch.Size())
	}
	return nil
}

// WriteS3Batch logic:
// S3Writer caches the batches in memory
// and when the batches size reaches 10M, we
// add a tag to indicate we need to write these data into
// s3, but not immediately. We continue to wait until
// no more data or the data size reaches 64M, at that time
// we will trigger write s3.
func (w *S3Writer) WriteS3Batch(bat *batch.Batch, proc *process.Process) error {
	w.InitBuffers(bat)
	res := w.Put(bat)
	switch res {
	case 1:
		w.MergeBlock(len(w.Bats), proc, true)
	case 0:
		w.MergeBlock(len(w.Bats), proc, true)
	case -1:
		//proc.SetInputBatch(&batch.Batch{})
	}
	return nil
}

func getNewBatch(bat *batch.Batch) *batch.Batch {
	attrs := make([]string, len(bat.Attrs))
	copy(attrs, bat.Attrs)
	newBat := batch.New(true, attrs)
	for i := range bat.Vecs {
		newBat.Vecs[i] = vector.NewVec(*bat.Vecs[i].GetType())
	}
	return newBat
}

func (w *S3Writer) generateWriter(proc *process.Process) error {
	// Use uuid as segment id
	// TODO: multiple 64m file in one segment
	id := common.NewSegmentid()
	s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	name := objectio.BuildObjectName(id, 0)
	w.writer, err = blockio.NewBlockWriterNew(s3, name)
	if err != nil {
		return err
	}
	w.lengths = w.lengths[:0]
	return nil
}

// reference to pkg/sql/colexec/order/order.go logic
func sortByKey(proc *process.Process, bat *batch.Batch, sortIndex int, m *mpool.MPool) error {
	// Not-Null Check
	if nulls.Any(bat.Vecs[sortIndex].GetNulls()) {
		// return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", n.InsertCtx.TableDef.Cols[i].GetName()))
		return moerr.NewConstraintViolation(proc.Ctx, "Primary key can not be null")
	}
	var strCol []string
	sels := make([]int64, len(bat.Zs))
	for i := 0; i < len(bat.Zs); i++ {
		sels[i] = int64(i)
	}
	ovec := bat.GetVector(int32(sortIndex))
	if ovec.GetType().IsVarlen() {
		strCol = vector.MustStrCol(ovec)
	} else {
		strCol = nil
	}
	sort.Sort(false, false, false, sels, ovec, strCol)
	return bat.Shuffle(sels, m)
}

func getPrimaryKeyIdx(pk map[string]struct{}, attrs []string) (uint16, bool) {
	for i := range attrs {
		if _, ok := pk[attrs[i]]; ok {
			return uint16(i), true
		}
	}
	return 0, false
}

// writeBlock writes one batch to a buffer and generate related indexes for this batch
// For more information, please refer to the comment about func Write in Writer interface
func (w *S3Writer) writeBlock(bat *batch.Batch) error {
	if idx, ok := getPrimaryKeyIdx(w.pk, bat.Attrs); ok {
		w.writer.SetPrimaryKey(idx)
	}

	_, err := w.writer.WriteBatch(bat)
	if err != nil {
		return err
	}
	w.lengths = append(w.lengths, uint64(bat.Vecs[0].Length()))
	return nil
}

// writeEndBlocks writes batches in buffer to fileservice(aka s3 in this feature) and get meta data about block on fileservice and put it into metaLocBat
// For more information, please refer to the comment about func WriteEnd in Writer interface
func (w *S3Writer) writeEndBlocks(proc *process.Process) error {
	blocks, _, err := w.writer.Sync(proc.Ctx)
	if err != nil {
		return err
	}
	for j := range blocks {
		metaLoc := blockio.EncodeLocation(
			w.writer.GetName(),
			blocks[j].GetExtent(),
			uint32(w.lengths[j]),
			blocks[j].GetID(),
		).String()

		if err := vector.AppendFixed(
			w.metaLocBat.Vecs[0],
			w.idx,
			false,
			proc.GetMPool()); err != nil {
			return err
		}
		if err := vector.AppendBytes(
			w.metaLocBat.Vecs[1],
			[]byte(metaLoc),
			false,
			proc.GetMPool()); err != nil {
			return err
		}
	}
	w.metaLocBat.SetZs(w.metaLocBat.Vecs[0].Length(), proc.GetMPool())
	return nil
}
