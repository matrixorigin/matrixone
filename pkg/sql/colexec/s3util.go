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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// S3Writer is used to write table data to S3 and package a series of `BlockWriter` write operations
// Currently there are two scenarios will let cn write s3 directly
// scenario 1 is insert operator directly go s3, when a one-time insert/load data volume is relatively large will trigger the scenario.
// scenario 2 is txn.workspace exceeds the threshold value, in the txn.dumpBatch function trigger a write s3
type S3Writer struct {
	sortIndex      int // When writing table data, if table has sort key, need to sort data and then write to S3
	pk             int
	partitionIndex int16 // This value is aligned with the partition number
	isClusterBy    bool

	schemaVersion uint32
	seqnums       []uint16
	tablename     string

	isTombstone bool

	writer *blockio.BlockWriter

	// the third vector only has several rows, not aligns with the other two vectors.
	blockInfoBat *batch.Batch

	// An intermediate cache after the merge sort of all `batches` data
	buffer *batch.Batch

	//for memory multiplexing.
	tableBatchPool []*batch.Batch

	// batches[i] used to store the batches of table
	// Each batch in batches will be sorted internally, and all batches correspond to only one table
	// when the batches' size is over 64M, we will use merge sort, and then write a segment in s3
	batches []*batch.Batch

	// tableBatchSizes are used to record the table_i's batches'
	// size in tableBatches
	batSize uint64

	typs []types.Type
	ufs  []func(*vector.Vector, *vector.Vector, int64) error // function pointers for type conversion
}

const (
	// WriteS3Threshold when batches'  size of table reaches this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 128 * mpool.MB

	TagS3SizeForMOLogger uint64 = 1 * mpool.MB
)

func (w *S3Writer) Free(proc *process.Process) {
	if w.blockInfoBat != nil {
		w.blockInfoBat.Clean(proc.Mp())
		w.blockInfoBat = nil
	}
	if w.buffer != nil {
		w.buffer.Clean(proc.Mp())
		w.buffer = nil
	}
	for _, bat := range w.tableBatchPool {
		bat.Clean(proc.Mp())
	}
	w.tableBatchPool = nil
	for _, bat := range w.batches {
		bat.Clean(proc.Mp())
	}
	w.batches = nil
}

func (w *S3Writer) GetBlockInfoBat() *batch.Batch {
	return w.blockInfoBat
}

func NewS3TombstoneWriter() (*S3Writer, error) {
	return &S3Writer{
		sortIndex:   0,
		pk:          0,
		isTombstone: true,
	}, nil
}

func NewS3Writer(proc *process.Process, tableDef *plan.TableDef, partitionIdx int16) (*S3Writer, error) {
	writer := &S3Writer{
		tablename:      tableDef.GetName(),
		seqnums:        make([]uint16, 0, len(tableDef.Cols)),
		schemaVersion:  tableDef.Version,
		sortIndex:      -1,
		pk:             -1,
		partitionIndex: partitionIdx,
	}

	writer.ResetBlockInfoBat(proc)
	for i, colDef := range tableDef.Cols {
		if colDef.Name != catalog.Row_ID {
			writer.seqnums = append(writer.seqnums, uint16(colDef.Seqnum))
		} else {
			// check rowid as the last column
			if i != len(tableDef.Cols)-1 {
				logutil.Errorf("bad rowid position for %q, %+v", writer.tablename, colDef)
			}
		}
	}
	logutil.Debugf("s3 table set from NewS3Writer %q seqnums: %+v", writer.tablename, writer.seqnums)

	// Get Single Col pk index
	for idx, colDef := range tableDef.Cols {
		if colDef.Name == tableDef.Pkey.PkeyColName && colDef.Name != catalog.FakePrimaryKeyColName {
			writer.sortIndex = idx
			writer.pk = idx
			break
		}
	}

	if tableDef.ClusterBy != nil {
		writer.isClusterBy = true

		// the `rowId` column has been excluded from target table's `TableDef` for insert statements (insert, load),
		// link: `/pkg/sql/plan/build_constraint_util.go` -> func setTableExprToDmlTableInfo
		// and the `sortIndex` position can be directly obtained using a name that matches the sorting key
		for idx, colDef := range tableDef.Cols {
			if colDef.Name == tableDef.ClusterBy.Name {
				writer.sortIndex = idx
			}
		}
	}

	return writer, nil
}

// NewPartitionS3Writer Alloc S3 writers for partitioned table.
func NewPartitionS3Writer(proc *process.Process, tableDef *plan.TableDef) ([]*S3Writer, error) {
	partitionNum := len(tableDef.Partition.PartitionTableNames)
	writers := make([]*S3Writer, partitionNum)
	for i := range writers {
		writer, err := NewS3Writer(proc, tableDef, int16(i))
		if err != nil {
			return nil, err
		}
		writers[i] = writer
	}
	return writers, nil
}

func (w *S3Writer) ResetBlockInfoBat(proc *process.Process) {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	if w.blockInfoBat != nil {
		w.blockInfoBat.CleanOnlyData()
	} else {
		attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
		blockInfoBat := batch.NewWithSize(len(attrs))
		blockInfoBat.Attrs = attrs
		blockInfoBat.Vecs[0] = vector.NewVec(types.T_int16.ToType())
		blockInfoBat.Vecs[1] = vector.NewVec(types.T_text.ToType())
		blockInfoBat.Vecs[2] = vector.NewVec(types.T_binary.ToType())

		w.blockInfoBat = blockInfoBat
	}
}

func (w *S3Writer) Output(proc *process.Process, result *vm.CallResult) error {
	var err error
	result.Batch, err = result.Batch.Append(proc.Ctx, proc.GetMPool(), w.blockInfoBat)
	if err != nil {
		return err
	}
	w.ResetBlockInfoBat(proc)
	return nil
}

func (w *S3Writer) writeBatsToBlockInfoBat(mpool *mpool.MPool) error {
	for _, bat := range w.batches {
		if err := vector.AppendFixed(
			w.blockInfoBat.Vecs[0], -w.partitionIndex-1,
			false, mpool); err != nil {
			return err
		}
		bytes, err := bat.MarshalBinary()
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(
			w.blockInfoBat.Vecs[1], bytes,
			false, mpool); err != nil {
			return err
		}
	}
	w.blockInfoBat.SetRowCount(w.blockInfoBat.Vecs[0].Length())
	return nil
}

func (w *S3Writer) initBuffers(proc *process.Process, bat *batch.Batch) {
	if w.buffer != nil {
		return
	}
	buffer, err := proc.NewBatchFromSrc(bat, int(options.DefaultBlockMaxRows))
	if err != nil {
		panic(err)
	}
	w.buffer = buffer
}

// StashBatch batch into w.bats.
// true: the tableBatches[idx] is over threshold
// false: the tableBatches[idx] is less than or equal threshold
func (w *S3Writer) StashBatch(proc *process.Process, bat *batch.Batch) bool {
	var rbat *batch.Batch

	if len(w.typs) == 0 {
		for i := 0; i < bat.VectorCount(); i++ {
			typ := *bat.GetVector(int32(i)).GetType()
			w.typs = append(w.typs, typ)
			w.ufs = append(w.ufs, vector.GetUnionOneFunction(typ, proc.Mp()))
		}
	}
	res := false
	start, end := 0, bat.RowCount()
	for start < end {
		n := len(w.batches)
		if n != 0 && w.batches[n-1].RowCount() < int(options.DefaultBlockMaxRows) {
			// w.batches[n-1] is not full.
			rbat = w.batches[n-1]
		} else {
			// w.batches[n-1] is full, use a new batch.
			if len(w.tableBatchPool) > 0 {
				rbat = w.tableBatchPool[0]
				w.tableBatchPool = w.tableBatchPool[1:]
				rbat.CleanOnlyData()
			} else {
				var err error
				rbat, err = proc.NewBatchFromSrc(bat, int(options.DefaultBlockMaxRows))
				if err != nil {
					panic(err)
				}
			}
			w.batches = append(w.batches, rbat)
		}
		rows := end - start
		if left := int(options.DefaultBlockMaxRows) - rbat.RowCount(); rows > left {
			rows = left
		}

		var err error
		for i := 0; i < bat.VectorCount(); i++ {
			vec := rbat.GetVector(int32(i))
			srcVec := bat.GetVector(int32(i))
			for j := 0; j < rows; j++ {
				if err = w.ufs[i](vec, srcVec, int64(j+start)); err != nil {
					panic(err)
				}
			}
		}
		rbat.AddRowCount(rows)
		start += rows
		w.batSize += uint64(rbat.Size())
		if w.batSize > WriteS3Threshold {
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
		cols = append(cols, vector.InefficientMustStrCol(bats[i].Vecs[idx]))
	}
	return
}

func (w *S3Writer) FlushTailBatch(proc *process.Process) ([]objectio.BlockInfo, []objectio.ObjectStats, error) {
	if w.batSize >= TagS3SizeForMOLogger {
		return w.SortAndSync(proc)
	}
	return nil, nil, w.writeBatsToBlockInfoBat(proc.GetMPool())
}

func (w *S3Writer) SortAndSync(proc *process.Process) ([]objectio.BlockInfo, []objectio.ObjectStats, error) {
	if len(w.batches) == 0 {
		return nil, nil, nil
	}

	defer func() {
		// clean
		for i := range w.batches {
			//recycle the batch
			w.putBatch(w.batches[i])
		}
		w.batches = w.batches[:0]
		w.batSize = 0
	}()

	if w.sortIndex == -1 {
		if _, err := w.generateWriter(proc); err != nil {
			return nil, nil, err
		}

		for i := range w.batches {
			if _, err := w.writer.WriteBatch(w.batches[i]); err != nil {
				return nil, nil, err
			}
		}
		return w.sync(proc)
	}

	for i := range w.batches {
		err := sortByKey(proc, w.batches[i], w.sortIndex, w.isClusterBy, proc.GetMPool())
		if err != nil {
			return nil, nil, err
		}
	}

	w.initBuffers(proc, w.batches[0])

	var merge MergeInterface
	nulls := make([]*nulls.Nulls, 0, len(w.batches))
	for i := 0; i < len(w.batches); i++ {
		nulls = append(nulls, w.batches[i].Vecs[w.sortIndex].GetNulls())
	}
	pos := w.sortIndex
	switch w.batches[0].Vecs[w.sortIndex].GetType().Oid {
	case types.T_bool:
		merge = newMerge(sort.BoolLess, getFixedCols[bool](w.batches, pos), nulls)
	case types.T_bit:
		merge = newMerge(sort.GenericLess[uint64], getFixedCols[uint64](w.batches, pos), nulls)
	case types.T_int8:
		merge = newMerge(sort.GenericLess[int8], getFixedCols[int8](w.batches, pos), nulls)
	case types.T_int16:
		merge = newMerge(sort.GenericLess[int16], getFixedCols[int16](w.batches, pos), nulls)
	case types.T_int32:
		merge = newMerge(sort.GenericLess[int32], getFixedCols[int32](w.batches, pos), nulls)
	case types.T_int64:
		merge = newMerge(sort.GenericLess[int64], getFixedCols[int64](w.batches, pos), nulls)
	case types.T_uint8:
		merge = newMerge(sort.GenericLess[uint8], getFixedCols[uint8](w.batches, pos), nulls)
	case types.T_uint16:
		merge = newMerge(sort.GenericLess[uint16], getFixedCols[uint16](w.batches, pos), nulls)
	case types.T_uint32:
		merge = newMerge(sort.GenericLess[uint32], getFixedCols[uint32](w.batches, pos), nulls)
	case types.T_uint64:
		merge = newMerge(sort.GenericLess[uint64], getFixedCols[uint64](w.batches, pos), nulls)
	case types.T_float32:
		merge = newMerge(sort.GenericLess[float32], getFixedCols[float32](w.batches, pos), nulls)
	case types.T_float64:
		merge = newMerge(sort.GenericLess[float64], getFixedCols[float64](w.batches, pos), nulls)
	case types.T_date:
		merge = newMerge(sort.GenericLess[types.Date], getFixedCols[types.Date](w.batches, pos), nulls)
	case types.T_datetime:
		merge = newMerge(sort.GenericLess[types.Datetime], getFixedCols[types.Datetime](w.batches, pos), nulls)
	case types.T_time:
		merge = newMerge(sort.GenericLess[types.Time], getFixedCols[types.Time](w.batches, pos), nulls)
	case types.T_timestamp:
		merge = newMerge(sort.GenericLess[types.Timestamp], getFixedCols[types.Timestamp](w.batches, pos), nulls)
	case types.T_enum:
		merge = newMerge(sort.GenericLess[types.Enum], getFixedCols[types.Enum](w.batches, pos), nulls)
	case types.T_decimal64:
		merge = newMerge(sort.Decimal64Less, getFixedCols[types.Decimal64](w.batches, pos), nulls)
	case types.T_decimal128:
		merge = newMerge(sort.Decimal128Less, getFixedCols[types.Decimal128](w.batches, pos), nulls)
	case types.T_uuid:
		merge = newMerge(sort.UuidLess, getFixedCols[types.Uuid](w.batches, pos), nulls)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
		merge = newMerge(sort.GenericLess[string], getStrCols(w.batches, pos), nulls)
	case types.T_Rowid:
		merge = newMerge(sort.RowidLess, getFixedCols[types.Rowid](w.batches, pos), nulls)
	//TODO: check if we need T_array here? T_json is missing here.
	// Update Oct 20 2023: I don't think it is necessary to add T_array here. Keeping this comment,
	// in case anything fails in vector S3 flush in future.
	default:
		panic(fmt.Sprintf("invalid type: %s", w.batches[0].Vecs[w.sortIndex].GetType().Oid))
	}
	if _, err := w.generateWriter(proc); err != nil {
		return nil, nil, err
	}
	lens := 0
	size := len(w.batches)
	w.buffer.CleanOnlyData()
	var batchIndex int
	var rowIndex int
	for size > 0 {
		batchIndex, rowIndex, size = merge.getNextPos()
		for i := range w.buffer.Vecs {
			err := w.buffer.Vecs[i].UnionOne(w.batches[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
			if err != nil {
				return nil, nil, err
			}
		}
		lens++
		if lens == int(options.DefaultBlockMaxRows) {
			lens = 0
			w.buffer.SetRowCount(int(options.DefaultBlockMaxRows))
			if _, err := w.writer.WriteBatch(w.buffer); err != nil {
				return nil, nil, err
			}
			// force clean
			w.buffer.CleanOnlyData()
		}
	}
	if lens > 0 {
		w.buffer.SetRowCount(lens)
		if _, err := w.writer.WriteBatch(w.buffer); err != nil {
			return nil, nil, err
		}
		w.buffer.CleanOnlyData()
	}
	return w.sync(proc)
}

func (w *S3Writer) putBatch(bat *batch.Batch) {
	w.tableBatchPool = append(w.tableBatchPool, bat)
}

func (w *S3Writer) generateWriter(proc *process.Process) (objectio.ObjectName, error) {
	// Use uuid as segment id
	// TODO: multiple 64m file in one segment
	obj := Get().GenerateObject()
	s3, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	w.writer, err = blockio.NewBlockWriterNew(s3, obj, w.schemaVersion, w.seqnums)
	if err != nil {
		return nil, err
	}

	if w.sortIndex > -1 {
		w.writer.SetSortKey(uint16(w.sortIndex))
	}

	if w.isTombstone {
		w.writer.SetDataType(objectio.SchemaTombstone)
		if w.pk > -1 {
			w.writer.SetPrimaryKeyWithType(
				uint16(w.pk),
				index.HBF,
				index.ObjectPrefixFn,
				index.BlockPrefixFn,
			)
		}
	} else {
		w.writer.SetDataType(objectio.SchemaData)
		if w.pk > -1 {
			w.writer.SetPrimaryKey(uint16(w.pk))
		}
	}

	return obj, err
}

// reference to pkg/sql/colexec/order/order.go logic
func sortByKey(proc *process.Process, bat *batch.Batch, sortIndex int, allow_null bool, m *mpool.MPool) error {
	hasNull := false
	// Not-Null Check, notice that cluster by support null value
	if nulls.Any(bat.Vecs[sortIndex].GetNulls()) {
		hasNull = true
		if !allow_null {
			return moerr.NewConstraintViolationf(proc.Ctx,
				"sort key can not be null, sortIndex = %d, sortCol = %s",
				sortIndex, bat.Attrs[sortIndex])
		}
	}
	rowCount := bat.RowCount()
	sels := make([]int64, rowCount)
	for i := 0; i < rowCount; i++ {
		sels[i] = int64(i)
	}
	ovec := bat.GetVector(int32(sortIndex))
	if allow_null {
		// null last
		sort.Sort(false, true, hasNull, sels, ovec)
	} else {
		sort.Sort(false, false, hasNull, sels, ovec)
	}
	return bat.Shuffle(sels, m)
}

// FillBlockInfoBat put blockInfo generated by sync into metaLocBat
func (w *S3Writer) FillBlockInfoBat(blkInfos []objectio.BlockInfo, stats []objectio.ObjectStats, mpool *mpool.MPool) error {
	for _, blkInfo := range blkInfos {
		if err := vector.AppendFixed(
			w.blockInfoBat.Vecs[0],
			w.partitionIndex,
			false,
			mpool); err != nil {
			return err
		}
		if err := vector.AppendBytes(
			w.blockInfoBat.Vecs[1],
			objectio.EncodeBlockInfo(blkInfo),
			false,
			mpool); err != nil {
			return err
		}
	}

	// append the object stats to bat,
	// at most one will append in
	for idx := range stats {
		if stats[idx].IsZero() {
			continue
		}

		if err := vector.AppendBytes(w.blockInfoBat.Vecs[2],
			stats[idx].Marshal(), false, mpool); err != nil {
			return err
		}
	}

	w.blockInfoBat.SetRowCount(w.blockInfoBat.Vecs[0].Length())
	return nil
}

// sync writes batches in buffer to fileservice(aka s3 in this feature) and get metadata about block on fileservice.
// For more information, please refer to the comment about func WriteEnd in Writer interface
func (w *S3Writer) sync(proc *process.Process) ([]objectio.BlockInfo, []objectio.ObjectStats, error) {
	blocks, _, err := w.writer.Sync(proc.Ctx)
	if err != nil {
		return nil, nil, err
	}
	blkInfos := make([]objectio.BlockInfo, 0, len(blocks))
	for j := range blocks {
		blkInfos = append(blkInfos,
			blocks[j].GenerateBlockInfo(w.writer.GetName(), w.sortIndex != -1),
		)
	}

	stats := w.writer.GetObjectStats()
	for i := range stats {
		if !stats[i].IsZero() {
			stats[i].SetCNCreated()
		}
	}

	return blkInfos, stats, err
}
