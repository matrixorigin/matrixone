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
	// in fact, len(sortIndex) is 1 at most.
	sortIndex []int
	pk        map[string]struct{}

	writer  *blockio.BlockWriter
	lengths []uint64

	metaLocBat *batch.Batch

	// buffers[i] stands the i-th buffer batch used
	// for merge sort (corresponding to table_i,table_i could be unique
	// table or main table)
	buffers []*batch.Batch

	// tableBatches[i] used to store the batches of table_i
	// when the batches' size is over 64M, we will use merge
	// sort, and then write a segment in s3
	tableBatches [][]*batch.Batch

	// tableBatchSizes are used to record the table_i's batches'
	// size in tableBatches
	tableBatchSizes []uint64

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

func (w *S3Writer) Init(num int) {
	w.tableBatchSizes = make([]uint64, num)
	w.tableBatches = make([][]*batch.Batch, num)
	w.buffers = make([]*batch.Batch, num)
	w.pk = make(map[string]struct{})
	w.sels = make([]int64, options.DefaultBlockMaxRows)
	for i := 0; i < int(options.DefaultBlockMaxRows); i++ {
		w.sels[i] = int64(i)
	}
	w.resetMetaLocBat()
}

func (w *S3Writer) AddSortIdx(sortIdx int) {
	w.sortIndex = append(w.sortIndex, sortIdx)
}

func NewS3Writer(tableDef *plan.TableDef) *S3Writer {
	uniqueNums := 0
	for _, idx := range tableDef.Indexes {
		if idx.Unique {
			uniqueNums++
		}
	}

	s3Writer := &S3Writer{
		sortIndex: make([]int, 0, 1),
		pk:        make(map[string]struct{}),
		// main table and unique tables
		buffers:         make([]*batch.Batch, uniqueNums+1),
		tableBatches:    make([][]*batch.Batch, uniqueNums+1),
		tableBatchSizes: make([]uint64, uniqueNums+1),
		sels:            make([]int64, options.DefaultBlockMaxRows),
	}

	for i := 0; i < int(options.DefaultBlockMaxRows); i++ {
		s3Writer.sels[i] = int64(i)
	}

	// Get CPkey index
	//if tableDef.CompositePkey != nil {
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		// the serialized cpk col is located in the last of the bat.vecs
		s3Writer.sortIndex = append(s3Writer.sortIndex, len(tableDef.Cols))
	} else {
		// Get Single Col pk index
		for num, colDef := range tableDef.Cols {
			if colDef.Primary {
				s3Writer.sortIndex = append(s3Writer.sortIndex, num)
				break
			}
		}
		if tableDef.ClusterBy != nil {
			if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
				// the serialized clusterby col is located in the last of the bat.vecs
				s3Writer.sortIndex = append(s3Writer.sortIndex, len(tableDef.Cols))
			} else {
				for num, colDef := range tableDef.Cols {
					if colDef.Name == tableDef.ClusterBy.Name {
						s3Writer.sortIndex = append(s3Writer.sortIndex, num)
					}
				}
			}
		}
	}

	// get Primary
	for _, def := range tableDef.Cols {
		if def.Primary {
			s3Writer.pk[def.Name] = struct{}{}
		}
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		s3Writer.pk[tableDef.Pkey.CompPkeyCol.Name] = struct{}{}
	}

	s3Writer.resetMetaLocBat()
	return s3Writer
}

func (w *S3Writer) resetMetaLocBat() {
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
		w.resetMetaLocBat()
	}
}

func (w *S3Writer) WriteS3CacheBatch(proc *process.Process) error {
	for i := range w.tableBatches {
		if w.tableBatchSizes[i] >= TagS3Size {
			if err := w.MergeBlock(i, len(w.tableBatches[i]), proc, true); err != nil {
				return err
			}
		} else if w.tableBatchSizes[i] < TagS3Size && w.tableBatchSizes[i] > 0 {
			for j := 0; j < len(w.tableBatches[i]); j++ {
				// use negative value to show it's a normal batch
				vector.AppendFixed(w.metaLocBat.Vecs[0], int16(-i-1), false, proc.GetMPool())
				bytes, err := w.tableBatches[i][j].MarshalBinary()
				if err != nil {
					return err
				}
				vector.AppendBytes(w.metaLocBat.Vecs[1], bytes, false, proc.GetMPool())
			}
		}
	}
	w.WriteEnd(proc)
	return nil
}

func (w *S3Writer) InitBuffers(bat *batch.Batch, idx int) {
	if w.buffers[idx] == nil {
		w.buffers[idx] = getNewBatch(bat)
	}
}

// the return value can be 1,0,-1
// 1: the tableBatches[idx] is over threshold
// 0: the tableBatches[idx] is equal to threshold
// -1: the tableBatches[idx] is less than threshold
func (w *S3Writer) Put(bat *batch.Batch, idx int) int {
	w.tableBatchSizes[idx] += uint64(bat.Size())
	w.tableBatches[idx] = append(w.tableBatches[idx], bat)
	if w.tableBatchSizes[idx] == WriteS3Threshold {
		return 0
	} else if w.tableBatchSizes[idx] > WriteS3Threshold {
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

// cacheOvershold means whether we need to cahce the data part which is over 64M
func (w *S3Writer) MergeBlock(idx int, length int, proc *process.Process, cacheOvershold bool) error {
	bats := w.tableBatches[idx][:length]
	stopIdx := -1
	var hackLogic bool
	if w.tableBatchSizes[idx] > WriteS3Threshold && cacheOvershold {
		w.buffers[idx].CleanOnlyData()
		lastBatch := w.tableBatches[idx][length-1]
		size := w.tableBatchSizes[idx] - uint64(lastBatch.Size())
		unionOneFuncs := make([]func(v, w *vector.Vector, sel int64) error, 0, len(lastBatch.Vecs))
		for j := 0; j < len(lastBatch.Vecs); j++ {
			unionOneFuncs = append(unionOneFuncs, vector.GetUnionOneFunction(*lastBatch.Vecs[j].GetType(), proc.GetMPool()))
		}
		for i := 0; i < len(lastBatch.Zs); i++ {
			for j := 0; j < len(lastBatch.Vecs); j++ {
				unionOneFuncs[j](w.buffers[idx].Vecs[j], lastBatch.Vecs[j], int64(i))
			}
			if size+uint64(w.buffers[idx].Size()) == WriteS3Threshold {
				stopIdx = i
				break
			} else if size+uint64(w.buffers[idx].Size()) > WriteS3Threshold {
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
			w.buffers[idx].SetZs(stopIdx+1, proc.GetMPool())
			w.buffers[idx].Shrink(w.sels[:stopIdx+1])
		}
	}
	if stopIdx == -1 && hackLogic {
		bats = bats[:len(bats)-1]
	}
	sortIdx := -1
	for i := range bats {
		// sort bats firstly
		// for main table
		if idx == 0 && len(w.sortIndex) != 0 {
			sortByKey(proc, bats[i], w.sortIndex, proc.GetMPool())
			sortIdx = w.sortIndex[0]
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
			if err := w.writeBlock(w.buffers[idx]); err != nil {
				return err
			}
			w.buffers[idx].CleanOnlyData()
		}
		if err := w.writeEndBlocks(proc, idx); err != nil {
			return err
		}
	} else {
		var merge MergeInterface
		var nulls []*nulls.Nulls
		for i := 0; i < len(bats); i++ {
			nulls = append(nulls, bats[i].Vecs[w.sortIndex[0]].GetNulls())
		}
		pos := w.sortIndex[0]
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
		w.buffers[idx].CleanOnlyData()
		var batchIndex int
		var rowIndex int
		for size > 0 {
			batchIndex, rowIndex, size = merge.GetNextPos()
			for i := range w.buffers[idx].Vecs {
				w.buffers[idx].Vecs[i].UnionOne(bats[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
			}
			lens++
			if lens == int(options.DefaultBlockMaxRows) {
				lens = 0
				if err := w.writeBlock(w.buffers[idx]); err != nil {
					return err
				}
				// force clean
				w.buffers[idx].CleanOnlyData()
			}
		}
		if lens > 0 {
			if err := w.writeBlock(w.buffers[idx]); err != nil {
				return err
			}
			w.buffers[idx].CleanOnlyData()
		}
		if err := w.writeEndBlocks(proc, idx); err != nil {
			return err
		}
		// force clean
		w.buffers[idx].CleanOnlyData()
	}
	if stopIdx == -1 {
		if hackLogic {
			w.tableBatchSizes[idx] = uint64(w.tableBatches[idx][length-1].Size())
			w.tableBatches[idx] = w.tableBatches[idx][length-1:]
		} else {
			w.tableBatchSizes[idx] = 0
			w.tableBatches[idx] = w.tableBatches[idx][:0]
		}
	} else {
		lastBatch := w.tableBatches[idx][length-1]
		lastBatch.Shrink(w.sels[stopIdx+1 : lastBatch.Length()])
		w.tableBatches[idx] = w.tableBatches[idx][:0]
		w.tableBatches[idx] = append(w.tableBatches[idx], lastBatch)
		w.tableBatchSizes[idx] = uint64(lastBatch.Size())
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
func (w *S3Writer) WriteS3Batch(bat *batch.Batch, proc *process.Process, idx int) error {
	w.InitBuffers(bat, idx)
	res := w.Put(bat, idx)
	switch res {
	case 1:
		w.MergeBlock(idx, len(w.tableBatches[idx]), proc, true)
	case 0:
		w.MergeBlock(idx, len(w.tableBatches[idx]), proc, true)
	case -1:
		proc.SetInputBatch(&batch.Batch{})
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
func sortByKey(proc *process.Process, bat *batch.Batch, sortIndex []int, m *mpool.MPool) error {
	// Not-Null Check
	for i := 0; i < len(sortIndex); i++ {
		if nulls.Any(bat.Vecs[i].GetNulls()) {
			// return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", n.InsertCtx.TableDef.Cols[i].GetName()))
			return moerr.NewConstraintViolation(proc.Ctx, "Primary key can not be null")
		}
	}

	var strCol []string
	sels := make([]int64, len(bat.Zs))
	for i := 0; i < len(bat.Zs); i++ {
		sels[i] = int64(i)
	}
	ovec := bat.GetVector(int32(sortIndex[0]))
	if ovec.GetType().IsString() {
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
func (w *S3Writer) writeEndBlocks(proc *process.Process, idx int) error {
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

		vector.AppendFixed(w.metaLocBat.Vecs[0], int16(idx), false, proc.GetMPool())
		vector.AppendBytes(w.metaLocBat.Vecs[1], []byte(metaLoc), false, proc.GetMPool())
	}
	return nil
}
