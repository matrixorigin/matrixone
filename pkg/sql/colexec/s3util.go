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
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type S3Writer struct {
	// in fact, len(sortIndex) is 1 at most.
	sortIndex []int
	pk        map[string]struct{}

	writer  dataio.Writer
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
}

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 64 * mpool.MB
)

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
	}

	// Get CPkey index
	if tableDef.CompositePkey != nil {
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
	if tableDef.CompositePkey != nil {
		s3Writer.pk[tableDef.CompositePkey.Name] = struct{}{}
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
	metaLocBat.Vecs[0] = vector.NewVec(types.T_uint16.ToType())
	metaLocBat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

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
		if w.tableBatchSizes[i] > 0 {
			if err := w.mergeBlock(i, len(w.tableBatches[i]), proc); err != nil {
				return err
			}
		}
	}
	w.WriteEnd(proc)
	return nil
}

func (w *S3Writer) initBuffers(bat *batch.Batch, idx int) {
	if w.buffers[idx] == nil {
		w.buffers[idx] = getNewBatch(bat)
	}
}

// the return value can be 1,0,-1
// 1: the tableBatches[idx] is over threshold
// 0: the tableBatches[idx] is equal to threshold
// -1: the tableBatches[idx] is less than threshold
func (w *S3Writer) put(bat *batch.Batch, idx int) int {
	w.tableBatchSizes[idx] += uint64(bat.Size())
	w.tableBatches[idx] = append(w.tableBatches[idx], bat)
	if w.tableBatchSizes[idx] == WriteS3Threshold {
		return 0
	} else if w.tableBatchSizes[idx] > WriteS3Threshold {
		return 1
	}
	return -1
}

func getFixedCols[T types.FixedSizeT](bats []*batch.Batch, idx int) [][]T {
	cols := make([][]T, 0, len(bats))
	for _, bat := range bats {
		cols = append(cols, vector.ExpandFixedCol[T](bat.Vecs[idx]))
	}
	return cols
}

func getStrCols(bats []*batch.Batch, idx int) [][]string {
	cols := make([][]string, 0, len(bats))
	for _, bat := range bats {
		cols = append(cols, vector.ExpandStrCol(bat.Vecs[idx]))
	}
	return cols
}

// cacheOvershold means whether we need to cahce the data part which is over 64M
// len(sortIndex) is always only one.
func (w *S3Writer) mergeBlock(idx int, length int, proc *process.Process) error {
	bats := w.tableBatches[idx][:length]
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
			if err := w.writeBlock(bats[i]); err != nil {
				return err
			}
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
			merge = NewMerge(len(bats), sort.NewBoolLess(), getFixedCols[bool](bats, pos), nulls)
		case types.T_int8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int8](), getFixedCols[int8](bats, pos), nulls)
		case types.T_int16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int16](), getFixedCols[int16](bats, pos), nulls)
		case types.T_int32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int32](), getFixedCols[int32](bats, pos), nulls)
		case types.T_int64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int64](), getFixedCols[int64](bats, pos), nulls)
		case types.T_uint8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint8](), getFixedCols[uint8](bats, pos), nulls)
		case types.T_uint16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint16](), getFixedCols[uint16](bats, pos), nulls)
		case types.T_uint32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint32](), getFixedCols[uint32](bats, pos), nulls)
		case types.T_uint64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint64](), getFixedCols[uint64](bats, pos), nulls)
		case types.T_float32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float32](), getFixedCols[float32](bats, pos), nulls)
		case types.T_float64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float64](), getFixedCols[float64](bats, pos), nulls)
		case types.T_date:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Date](), getFixedCols[types.Date](bats, pos), nulls)
		case types.T_datetime:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Datetime](), getFixedCols[types.Datetime](bats, pos), nulls)
		case types.T_time:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Time](), getFixedCols[types.Time](bats, pos), nulls)
		case types.T_timestamp:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Timestamp](), getFixedCols[types.Timestamp](bats, pos), nulls)
		case types.T_decimal64:
			merge = NewMerge(len(bats), sort.NewDecimal64Less(), getFixedCols[types.Decimal64](bats, pos), nulls)
		case types.T_decimal128:
			merge = NewMerge(len(bats), sort.NewDecimal128Less(), getFixedCols[types.Decimal128](bats, pos), nulls)
		case types.T_uuid:
			merge = NewMerge(len(bats), sort.NewUuidCompLess(), getFixedCols[types.Uuid](bats, pos), nulls)
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[string](), getStrCols(bats, pos), nulls)
		}
		if err := w.generateWriter(proc); err != nil {
			return err
		}
		lens := 0
		size := len(bats)
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
		}
		if err := w.writeEndBlocks(proc, idx); err != nil {
			return err
		}
	}
	left := w.tableBatches[idx][length:]
	w.tableBatches[idx] = left
	w.tableBatchSizes[idx] = 0
	for _, bat := range left {
		w.tableBatchSizes[idx] += uint64(bat.Size())
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
	w.initBuffers(bat, idx)
	res := w.put(bat, idx)
	switch res {
	case 0, 1:
		w.mergeBlock(idx, len(w.tableBatches[idx]), proc)
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
	segId, err := Srv.GenerateSegment()
	if err != nil {
		return err
	}
	s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	w.writer, err = blockio.NewBlockWriter(s3, segId)
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
	if len(sortIndex) == 1 {
		return bat.Shuffle(sels, m)
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(sortIndex); i < j; i++ {
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := bat.Vecs[sortIndex[i]]
		if vec.GetType().IsString() {
			strCol = vector.MustStrCol(vec)
		} else {
			strCol = nil
		}
		for i, j := 0, len(ps); i < j; i++ {
			if i == j-1 {
				sort.Sort(false, false, false, sels[ps[i]:], vec, strCol)
			} else {
				sort.Sort(false, false, false, sels[ps[i]:ps[i+1]], vec, strCol)
			}
		}
		ovec = vec
	}
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
		metaLoc, err := blockio.EncodeLocation(
			blocks[j].GetExtent(),
			uint32(w.lengths[j]),
			blocks,
		)
		if err != nil {
			return err
		}
		vector.AppendFixed(w.metaLocBat.Vecs[0], uint16(idx), false, proc.GetMPool())
		vector.AppendBytes(w.metaLocBat.Vecs[1], []byte(metaLoc), false, proc.GetMPool())
	}
	return nil
}
