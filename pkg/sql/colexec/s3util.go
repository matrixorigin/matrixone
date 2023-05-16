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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type S3Writer struct {
	sortIndex int
	pk        map[string]struct{}
	idx       int16

	schemaVersion uint32
	seqnums       []uint16

	writer  *blockio.BlockWriter
	lengths []uint64

	metaLocBat *batch.Batch

	// buffers[i] stands the i-th buffer batch used
	// for merge sort (corresponding to table_i,table_i could be unique
	// table or main table)
	buffer *batch.Batch

	//for memory multiplexing.
	tableBatchBuffers []*batch.Batch

	// tableBatches[i] used to store the batches of table_i
	// when the batches' size is over 64M, we will use merge
	// sort, and then write a segment in s3
	Bats []*batch.Batch

	// tableBatchSizes are used to record the table_i's batches'
	// size in tableBatches
	batSize uint64

	typs []types.Type
	ufs  []func(*vector.Vector, *vector.Vector, int64) error // function pointers for type conversion
}

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 64 * mpool.MB

	TagS3Size uint64 = 10 * mpool.MB
)

func (w *S3Writer) Free(proc *process.Process) {
	if w.metaLocBat != nil {
		w.metaLocBat.Clean(proc.Mp())
		w.metaLocBat = nil
	}
	if w.buffer != nil {
		w.buffer.Clean(proc.Mp())
		w.buffer = nil
	}
	for _, bat := range w.tableBatchBuffers {
		bat.Clean(proc.Mp())
	}
	w.tableBatchBuffers = nil
	for _, bat := range w.Bats {
		bat.Clean(proc.Mp())
	}
	w.Bats = nil
}

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

func (w *S3Writer) Init(proc *process.Process) {
	w.pk = make(map[string]struct{})
	w.ResetMetaLocBat(proc)
}

func (w *S3Writer) SetSortIdx(sortIdx int) {
	w.sortIndex = sortIdx
}

func AllocS3Writer(proc *process.Process, tableDef *plan.TableDef) (*S3Writer, error) {
	writer := &S3Writer{
		sortIndex: -1,
		pk:        make(map[string]struct{}),
		idx:       0,
	}
	writer.ResetMetaLocBat(proc)

	writer.schemaVersion = tableDef.Version
	writer.seqnums = make([]uint16, 0)
	for _, colDef := range tableDef.Cols {
		if colDef.Name != catalog.Row_ID {
			writer.seqnums = append(writer.seqnums, uint16(colDef.Seqnum))
		}
	}

	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		// the serialized cpk col is located in the last of the bat.vecs
		writer.sortIndex = len(tableDef.Cols) - 1
	} else {
		// Get Single Col pk index
		for idx, colDef := range tableDef.Cols {
			if colDef.Primary && !colDef.Hidden {
				writer.sortIndex = idx
				break
			}
		}
		if tableDef.ClusterBy != nil {
			if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
				// the serialized clusterby col is located in the last of the bat.vecs
				writer.sortIndex = len(tableDef.Cols) - 1
			} else {
				for idx, colDef := range tableDef.Cols {
					if colDef.Name == tableDef.ClusterBy.Name {
						writer.sortIndex = idx
					}
				}
			}
		}
	}
	// get Primary
	for _, def := range tableDef.Cols {
		if def.Primary {
			writer.pk[def.Name] = struct{}{}
		}
	}

	// Check whether the composite primary key column is included
	if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
		writer.pk[tableDef.Pkey.CompPkeyCol.Name] = struct{}{}
	}
	return writer, nil
}

// AllocPartitionS3Writer Alloc S3 writers for partitioned table.
func AllocPartitionS3Writer(proc *process.Process, tableDef *plan.TableDef) ([]*S3Writer, error) {
	partitionNum := len(tableDef.Partition.PartitionTableNames)
	writers := make([]*S3Writer, partitionNum)
	for i := range writers {
		writers[i] = &S3Writer{
			sortIndex: -1,
			pk:        make(map[string]struct{}),
			idx:       int16(i), // This value is aligned with the partition number
		}
		writers[i].ResetMetaLocBat(proc)

		writers[i].schemaVersion = tableDef.Version
		writers[i].seqnums = make([]uint16, 0)
		for _, colDef := range tableDef.Cols {
			if colDef.Name != catalog.Row_ID {
				writers[i].seqnums = append(writers[i].seqnums, uint16(colDef.Seqnum))
			}
		}

		if tableDef.Pkey != nil && tableDef.Pkey.CompPkeyCol != nil {
			// the serialized cpk col is located in the last of the bat.vecs
			writers[i].sortIndex = len(tableDef.Cols) - 1
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
					writers[i].sortIndex = len(tableDef.Cols) - 1
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
	}
	return writers, nil
}

func (w *S3Writer) ResetMetaLocBat(proc *process.Process) {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	if w.metaLocBat != nil {
		w.metaLocBat.Clean(proc.GetMPool())
	}
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc}
	metaLocBat := batch.NewWithSize(len(attrs))
	metaLocBat.Attrs = attrs
	metaLocBat.Vecs[0] = proc.GetVector(types.T_int16.ToType())
	metaLocBat.Vecs[1] = proc.GetVector(types.T_text.ToType())
	w.metaLocBat = metaLocBat
}

//func (w *S3Writer) WriteEnd(proc *process.Process) {
//	if w.metaLocBat.Vecs[0].Length() > 0 {
//		w.metaLocBat.SetZs(w.metaLocBat.Vecs[0].Length(), proc.GetMPool())
//		proc.SetInputBatch(w.metaLocBat)
//	}
//}

func (w *S3Writer) Output(proc *process.Process) error {
	bat := batch.NewWithSize(len(w.metaLocBat.Attrs))
	bat.SetAttributes(w.metaLocBat.Attrs)

	for i := range w.metaLocBat.Attrs {
		vec := proc.GetVector(*w.metaLocBat.Vecs[i].GetType())
		if err := vec.UnionBatch(w.metaLocBat.Vecs[i], 0, w.metaLocBat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			return err
		}
		bat.SetVector(int32(i), vec)
	}
	bat.Zs = append(bat.Zs, w.metaLocBat.Zs...)
	w.ResetMetaLocBat(proc)
	proc.SetInputBatch(bat)
	return nil
}

func (w *S3Writer) WriteS3CacheBatch(proc *process.Process) error {
	if w.batSize >= TagS3Size {
		if err := w.SortAndFlush(proc); err != nil {
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

func (w *S3Writer) InitBuffers(proc *process.Process, bat *batch.Batch) {
	if w.buffer == nil {
		w.buffer = getNewBatch(proc, bat)
	}
}

// Put batch into w.bats , and make sure that each batch in w.bats
//
//	contains options.DefaultBlockMaxRows rows except for the last one.
//	true: the tableBatches[idx] is over threshold
//	false: the tableBatches[idx] is less than or equal threshold
func (w *S3Writer) Put(bat *batch.Batch, proc *process.Process) bool {
	var rbat *batch.Batch

	if len(w.typs) == 0 {
		for i := 0; i < bat.VectorCount(); i++ {
			typ := *bat.GetVector(int32(i)).GetType()
			w.typs = append(w.typs, typ)
			w.ufs = append(w.ufs, vector.GetUnionOneFunction(typ, proc.Mp()))
		}
	}
	res := false
	start, end := 0, bat.Length()
	for start < end {
		n := len(w.Bats)
		if n == 0 || w.Bats[n-1].Length() >=
			int(options.DefaultBlockMaxRows) {
			if len(w.tableBatchBuffers) > 0 {
				rbat = w.tableBatchBuffers[0]
				w.tableBatchBuffers = w.tableBatchBuffers[1:]
				rbat.CleanOnlyData()
			} else {
				rbat = batch.NewWithSize(bat.VectorCount())
				rbat.SetAttributes(bat.Attrs)
				for i := range w.typs {
					rbat.Vecs[i] = proc.GetVector(w.typs[i])
				}
			}
			w.Bats = append(w.Bats, rbat)
		} else {
			rbat = w.Bats[n-1]
		}
		rows := end - start
		if left := int(options.DefaultBlockMaxRows) - rbat.Length(); rows > left {
			rows = left
		}
		for i := 0; i < bat.VectorCount(); i++ {
			vec := rbat.GetVector(int32(i))
			srcVec := bat.GetVector(int32(i))
			for j := 0; j < rows; j++ {
				if err := w.ufs[i](vec, srcVec, int64(j+start)); err != nil {
					panic(err)
				}
			}
		}
		for j := 0; j < rows; j++ {
			rbat.Zs = append(rbat.Zs, bat.Zs[j+start])
		}
		start += rows
		if w.batSize = w.batSize + uint64(rbat.Size()); w.batSize > WriteS3Threshold {
			res = true
		}
	}
	return res
}

func getFixedCols[T types.FixedSizeT](bats []*batch.Batch, idx int) (cols [][]T) {
	cols = make([][]T, 0, len(bats))
	for i := range bats {
		cols = append(cols, vector.MustFixedCol[T](bats[i].Vecs[idx]))
	}
	return
}

func getStrCols(bats []*batch.Batch, idx int) (cols [][]string) {
	cols = make([][]string, 0, len(bats))
	for i := range bats {
		cols = append(cols, vector.MustStrCol(bats[i].Vecs[idx]))
	}
	return
}

func (w *S3Writer) SortAndFlush(proc *process.Process) error {
	//bats := w.Bats[:length]
	sortIdx := -1
	for i := range w.Bats {
		// sort bats firstly
		// for main/orgin table and unique index table.
		if w.sortIndex != -1 {
			err := sortByKey(proc, w.Bats[i], w.sortIndex, proc.GetMPool())
			if err != nil {
				return err
			}
			sortIdx = w.sortIndex
		}
	}
	// just write ahead, no need to sort
	if sortIdx == -1 {
		if _, err := w.generateWriter(proc); err != nil {
			return err
		}

		for i := range w.Bats {
			if err := w.WriteBlock(w.Bats[i]); err != nil {
				return err
			}
		}
		if err := w.writeEndBlocks(proc); err != nil {
			return err
		}
	} else {
		var merge MergeInterface
		var nulls []*nulls.Nulls
		for i := 0; i < len(w.Bats); i++ {
			nulls = append(nulls, w.Bats[i].Vecs[w.sortIndex].GetNulls())
		}
		pos := w.sortIndex
		switch w.Bats[0].Vecs[sortIdx].GetType().Oid {
		case types.T_bool:
			merge = NewMerge(len(w.Bats), sort.NewBoolLess(), getFixedCols[bool](w.Bats, pos), nulls)
		case types.T_int8:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[int8](), getFixedCols[int8](w.Bats, pos), nulls)
		case types.T_int16:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[int16](), getFixedCols[int16](w.Bats, pos), nulls)
		case types.T_int32:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[int32](), getFixedCols[int32](w.Bats, pos), nulls)
		case types.T_int64:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[int64](), getFixedCols[int64](w.Bats, pos), nulls)
		case types.T_uint8:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[uint8](), getFixedCols[uint8](w.Bats, pos), nulls)
		case types.T_uint16:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[uint16](), getFixedCols[uint16](w.Bats, pos), nulls)
		case types.T_uint32:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[uint32](), getFixedCols[uint32](w.Bats, pos), nulls)
		case types.T_uint64:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[uint64](), getFixedCols[uint64](w.Bats, pos), nulls)
		case types.T_float32:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[float32](), getFixedCols[float32](w.Bats, pos), nulls)
		case types.T_float64:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[float64](), getFixedCols[float64](w.Bats, pos), nulls)
		case types.T_date:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[types.Date](), getFixedCols[types.Date](w.Bats, pos), nulls)
		case types.T_datetime:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[types.Datetime](), getFixedCols[types.Datetime](w.Bats, pos), nulls)
		case types.T_time:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[types.Time](), getFixedCols[types.Time](w.Bats, pos), nulls)
		case types.T_timestamp:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[types.Timestamp](), getFixedCols[types.Timestamp](w.Bats, pos), nulls)
		case types.T_decimal64:
			merge = NewMerge(len(w.Bats), sort.NewDecimal64Less(), getFixedCols[types.Decimal64](w.Bats, pos), nulls)
		case types.T_decimal128:
			merge = NewMerge(len(w.Bats), sort.NewDecimal128Less(), getFixedCols[types.Decimal128](w.Bats, pos), nulls)
		case types.T_uuid:
			merge = NewMerge(len(w.Bats), sort.NewUuidCompLess(), getFixedCols[types.Uuid](w.Bats, pos), nulls)
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
			merge = NewMerge(len(w.Bats), sort.NewGenericCompLess[string](), getStrCols(w.Bats, pos), nulls)
		}
		if _, err := w.generateWriter(proc); err != nil {
			return err
		}
		lens := 0
		size := len(w.Bats)
		w.buffer.CleanOnlyData()
		var batchIndex int
		var rowIndex int
		for size > 0 {
			batchIndex, rowIndex, size = merge.GetNextPos()
			for i := range w.buffer.Vecs {
				err := w.buffer.Vecs[i].UnionOne(w.Bats[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
				if err != nil {
					return err
				}
			}
			lens++
			if lens == int(options.DefaultBlockMaxRows) {
				lens = 0
				if err := w.WriteBlock(w.buffer); err != nil {
					return err
				}
				// force clean
				w.buffer.CleanOnlyData()
			}
		}
		if lens > 0 {
			if err := w.WriteBlock(w.buffer); err != nil {
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
	for i := 0; i < len(w.Bats); i++ {
		//recycle the batch
		w.putBatch(w.Bats[i])
		w.batSize -= uint64(w.Bats[i].Size())
	}
	w.Bats = w.Bats[:0]
	return nil
}

// WriteS3Batch logic:
// S3Writer caches the batches in memory
// and when the batches size reaches 10M, we
// add a tag to indicate we need to write these data into
// s3, but not immediately. We continue to wait until
// no more data or the data size reaches 64M, at that time
// we will trigger write s3.
func (w *S3Writer) WriteS3Batch(proc *process.Process, bat *batch.Batch) error {
	w.InitBuffers(proc, bat)
	if w.Put(bat, proc) {
		w.SortAndFlush(proc)
	}
	return nil
}

func (w *S3Writer) putBatch(bat *batch.Batch) {
	w.tableBatchBuffers = append(w.tableBatchBuffers, bat)
}

func getNewBatch(proc *process.Process, bat *batch.Batch) *batch.Batch {
	newBat := batch.NewWithSize(bat.VectorCount())
	newBat.SetAttributes(bat.Attrs)
	for i := range bat.Vecs {
		newBat.Vecs[i] = proc.GetVector(*bat.Vecs[i].GetType())
	}
	return newBat
}

func (w *S3Writer) GenerateWriter(proc *process.Process) (objectio.ObjectName, error) {
	return w.generateWriter(proc)
}

func (w *S3Writer) generateWriter(proc *process.Process) (objectio.ObjectName, error) {
	// Use uuid as segment id
	// TODO: multiple 64m file in one segment
	segId := Srv.GenerateSegment()
	s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	w.writer, err = blockio.NewBlockWriterNew(s3, segId, w.schemaVersion, nil)
	if err != nil {
		return nil, err
	}
	w.lengths = w.lengths[:0]
	return segId, err
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

func (w *S3Writer) WriteBlock(bat *batch.Batch) error {
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

func (w *S3Writer) writeEndBlocks(proc *process.Process) error {
	metaLocs, err := w.WriteEndBlocks(proc)
	if err != nil {
		return err
	}
	for _, metaLoc := range metaLocs {
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

// WriteEndBlocks writes batches in buffer to fileservice(aka s3 in this feature) and get meta data about block on fileservice and put it into metaLocBat
// For more information, please refer to the comment about func WriteEnd in Writer interface
func (w *S3Writer) WriteEndBlocks(proc *process.Process) ([]string, error) {
	blocks, _, err := w.writer.Sync(proc.Ctx)
	if err != nil {
		return nil, err
	}
	metaLocs := make([]string, 0, len(blocks))
	for j := range blocks {
		metaLoc := blockio.EncodeLocation(
			w.writer.GetName(),
			blocks[j].GetExtent(),
			uint32(w.lengths[j]),
			blocks[j].GetID(),
		).String()
		metaLocs = append(metaLocs, metaLoc)
	}
	return metaLocs, err
}
