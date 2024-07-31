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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

// S3Writer is used to write table data to S3 and package a series of `BlockWriter` write operations
// Currently there are two scenarios will let cn write s3 directly
// scenario 1 is insert operator directly go s3, when a one-time insert/load data volume is relatively large will trigger the scenario
// scenario 2 is txn.workspace exceeds the threshold value, in the txn.dumpBatch function trigger a write s3
type S3Writer struct {
	sortIndex      int // When writing table data, if table has sort key, need to sort data and then write to S3
	pk             int
	partitionIndex int16 // This value is aligned with the partition number
	isClusterBy    bool

	schemaVersion uint32
	seqnums       []uint16
	tablename     string
	attrs         []string

	writer  *blockio.BlockWriter
	lengths []uint64

	// the third vector only has several rows, not aligns with the other two vectors.
	blockInfoBat *batch.Batch

	// An intermediate cache after the merge sort of all `Bats` data
	buffer *batch.Batch

	//for memory multiplexing.
	tableBatchBuffers []*batch.Batch

	// Bats[i] used to store the batches of table
	// Each batch in Bats will be sorted internally, and all batches correspond to only one table
	// when the batches' size is over 64M, we will use merge sort, and then write a segment in s3
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
	WriteS3Threshold uint64 = 128 * mpool.MB

	TagS3Size            uint64 = 10 * mpool.MB
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
	for _, bat := range w.tableBatchBuffers {
		bat.Clean(proc.Mp())
	}
	w.tableBatchBuffers = nil
	for _, bat := range w.Bats {
		bat.Clean(proc.Mp())
	}
	w.Bats = nil
}

func (w *S3Writer) GetBlockInfoBat() *batch.Batch {
	return w.blockInfoBat
}

func (w *S3Writer) SetSortIdx(sortIdx int) {
	w.sortIndex = sortIdx
}

func (w *S3Writer) SetSchemaVer(ver uint32) {
	w.schemaVersion = ver
}

func (w *S3Writer) SetTableName(name string) {
	w.tablename = name
}

func (w *S3Writer) SetSeqnums(seqnums []uint16) {
	w.seqnums = seqnums
	logutil.Debugf("s3 table set directly %q seqnums: %+v", w.tablename, w.seqnums)
}

func AllocS3Writer(proc *process.Process, tableDef *plan.TableDef) (*S3Writer, error) {
	writer := &S3Writer{
		tablename:      tableDef.GetName(),
		seqnums:        make([]uint16, 0, len(tableDef.Cols)),
		schemaVersion:  tableDef.Version,
		sortIndex:      -1,
		pk:             -1,
		partitionIndex: 0,
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
	logutil.Debugf("s3 table set from AllocS3Writer %q seqnums: %+v", writer.tablename, writer.seqnums)

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

// AllocPartitionS3Writer Alloc S3 writers for partitioned table.
func AllocPartitionS3Writer(proc *process.Process, tableDef *plan.TableDef) ([]*S3Writer, error) {
	partitionNum := len(tableDef.Partition.PartitionTableNames)
	writers := make([]*S3Writer, partitionNum)
	for i := range writers {
		writers[i] = &S3Writer{
			tablename:      tableDef.GetName(),
			seqnums:        make([]uint16, 0, len(tableDef.Cols)),
			schemaVersion:  tableDef.Version,
			sortIndex:      -1,
			pk:             -1,
			partitionIndex: int16(i), // This value is aligned with the partition number
		}

		writers[i].ResetBlockInfoBat(proc)
		for j, colDef := range tableDef.Cols {
			if colDef.Name != catalog.Row_ID {
				writers[i].seqnums = append(writers[i].seqnums, uint16(colDef.Seqnum))
			} else {
				// check rowid as the last column
				if j != len(tableDef.Cols)-1 {
					logutil.Errorf("bad rowid position for %q, %+v", writers[j].tablename, colDef)
				}
			}
		}
		logutil.Debugf("s3 table set from AllocS3WriterP%d %q seqnums: %+v", i, writers[i].tablename, writers[i].seqnums)

		// Get Single Col pk index
		for idx, colDef := range tableDef.Cols {
			if colDef.Name == tableDef.Pkey.PkeyColName {
				if colDef.Name != catalog.FakePrimaryKeyColName {
					writers[i].sortIndex = idx
					writers[i].pk = idx
				}
				break
			}
		}

		if tableDef.ClusterBy != nil {
			writers[i].isClusterBy = true
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
	return writers, nil
}

func (w *S3Writer) ResetBlockInfoBat(proc *process.Process) {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	if w.blockInfoBat != nil {
		proc.PutBatch(w.blockInfoBat)
	}
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_BlockInfo, catalog.ObjectMeta_ObjectStats}
	blockInfoBat := batch.NewWithSize(len(attrs))
	blockInfoBat.Attrs = attrs
	blockInfoBat.Vecs[0] = proc.GetVector(types.T_int16.ToType())
	blockInfoBat.Vecs[1] = proc.GetVector(types.T_text.ToType())
	blockInfoBat.Vecs[2] = proc.GetVector(types.T_binary.ToType())

	w.blockInfoBat = blockInfoBat
}

//func (w *S3Writer) WriteEnd(proc *process.Process) {
//	if w.metaLocBat.vecs[0].Length() > 0 {
//		w.metaLocBat.SetZs(w.metaLocBat.vecs[0].Length(), proc.GetMPool())
//		proc.SetInputBatch(w.metaLocBat)
//	}
//}

func (w *S3Writer) Output(proc *process.Process, result *vm.CallResult) error {
	bat := batch.NewWithSize(len(w.blockInfoBat.Attrs))
	bat.SetAttributes(w.blockInfoBat.Attrs)

	for i := range w.blockInfoBat.Attrs {
		vec := proc.GetVector(*w.blockInfoBat.Vecs[i].GetType())
		if err := vec.UnionBatch(w.blockInfoBat.Vecs[i], 0, w.blockInfoBat.Vecs[i].Length(), nil, proc.GetMPool()); err != nil {
			vec.Free(proc.Mp())
			return err
		}
		bat.SetVector(int32(i), vec)
	}
	bat.SetRowCount(w.blockInfoBat.RowCount())
	w.ResetBlockInfoBat(proc)
	result.Batch = bat
	return nil
}

func (w *S3Writer) WriteS3CacheBatch(proc *process.Process) error {
	var S3SizeThreshold = TagS3SizeForMOLogger

	if proc != nil && proc.Ctx != nil {
		isMoLogger, ok := proc.Ctx.Value(defines.IsMoLogger{}).(bool)
		if ok && isMoLogger {
			logutil.Debug("WriteS3CacheBatch proc", zap.Bool("isMoLogger", isMoLogger))
			S3SizeThreshold = TagS3SizeForMOLogger
		}
	}

	if proc.GetSessionInfo() != nil && proc.GetSessionInfo().GetUser() == db_holder.MOLoggerUser {
		logutil.Debug("WriteS3CacheBatch", zap.String("user", proc.GetSessionInfo().GetUser()))
		S3SizeThreshold = TagS3SizeForMOLogger
	}
	if w.batSize >= S3SizeThreshold {
		if err := w.SortAndFlush(proc); err != nil {
			return err
		}
		w.blockInfoBat.SetRowCount(w.blockInfoBat.Vecs[0].Length())
		return nil
	}
	for _, bat := range w.Bats {
		if err := vector.AppendFixed(
			w.blockInfoBat.Vecs[0], -w.partitionIndex-1,
			false, proc.GetMPool()); err != nil {
			return err
		}
		bytes, err := bat.MarshalBinary()
		if err != nil {
			return err
		}
		if err = vector.AppendBytes(
			w.blockInfoBat.Vecs[1], bytes,
			false, proc.GetMPool()); err != nil {
			return err
		}
	}
	w.blockInfoBat.SetRowCount(w.blockInfoBat.Vecs[0].Length())
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
	start, end := 0, bat.RowCount()
	for start < end {
		n := len(w.Bats)
		if n == 0 || w.Bats[n-1].RowCount() >=
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
		cols = append(cols, vector.InefficientMustStrCol(bats[i].Vecs[idx]))
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
			err := sortByKey(proc, w.Bats[i], w.sortIndex, w.isClusterBy, proc.GetMPool())
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
			merge = newMerge(len(w.Bats), sort.BoolLess, getFixedCols[bool](w.Bats, pos), nulls)
		case types.T_bit:
			merge = newMerge(len(w.Bats), sort.GenericLess[uint64], getFixedCols[uint64](w.Bats, pos), nulls)
		case types.T_int8:
			merge = newMerge(len(w.Bats), sort.GenericLess[int8], getFixedCols[int8](w.Bats, pos), nulls)
		case types.T_int16:
			merge = newMerge(len(w.Bats), sort.GenericLess[int16], getFixedCols[int16](w.Bats, pos), nulls)
		case types.T_int32:
			merge = newMerge(len(w.Bats), sort.GenericLess[int32], getFixedCols[int32](w.Bats, pos), nulls)
		case types.T_int64:
			merge = newMerge(len(w.Bats), sort.GenericLess[int64], getFixedCols[int64](w.Bats, pos), nulls)
		case types.T_uint8:
			merge = newMerge(len(w.Bats), sort.GenericLess[uint8], getFixedCols[uint8](w.Bats, pos), nulls)
		case types.T_uint16:
			merge = newMerge(len(w.Bats), sort.GenericLess[uint16], getFixedCols[uint16](w.Bats, pos), nulls)
		case types.T_uint32:
			merge = newMerge(len(w.Bats), sort.GenericLess[uint32], getFixedCols[uint32](w.Bats, pos), nulls)
		case types.T_uint64:
			merge = newMerge(len(w.Bats), sort.GenericLess[uint64], getFixedCols[uint64](w.Bats, pos), nulls)
		case types.T_float32:
			merge = newMerge(len(w.Bats), sort.GenericLess[float32], getFixedCols[float32](w.Bats, pos), nulls)
		case types.T_float64:
			merge = newMerge(len(w.Bats), sort.GenericLess[float64], getFixedCols[float64](w.Bats, pos), nulls)
		case types.T_date:
			merge = newMerge(len(w.Bats), sort.GenericLess[types.Date], getFixedCols[types.Date](w.Bats, pos), nulls)
		case types.T_datetime:
			merge = newMerge(len(w.Bats), sort.GenericLess[types.Datetime], getFixedCols[types.Datetime](w.Bats, pos), nulls)
		case types.T_time:
			merge = newMerge(len(w.Bats), sort.GenericLess[types.Time], getFixedCols[types.Time](w.Bats, pos), nulls)
		case types.T_timestamp:
			merge = newMerge(len(w.Bats), sort.GenericLess[types.Timestamp], getFixedCols[types.Timestamp](w.Bats, pos), nulls)
		case types.T_enum:
			merge = newMerge(len(w.Bats), sort.GenericLess[types.Enum], getFixedCols[types.Enum](w.Bats, pos), nulls)
		case types.T_decimal64:
			merge = newMerge(len(w.Bats), sort.Decimal64Less, getFixedCols[types.Decimal64](w.Bats, pos), nulls)
		case types.T_decimal128:
			merge = newMerge(len(w.Bats), sort.Decimal128Less, getFixedCols[types.Decimal128](w.Bats, pos), nulls)
		case types.T_uuid:
			merge = newMerge(len(w.Bats), sort.UuidLess, getFixedCols[types.Uuid](w.Bats, pos), nulls)
		case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_datalink:
			merge = newMerge(len(w.Bats), sort.GenericLess[string], getStrCols(w.Bats, pos), nulls)
			//TODO: check if we need T_array here? T_json is missing here.
			// Update Oct 20 2023: I don't think it is necessary to add T_array here. Keeping this comment,
			// in case anything fails in vector S3 flush in future.
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
			batchIndex, rowIndex, size = merge.getNextPos()
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
	obj := Get().GenerateObject()
	s3, err := fileservice.Get[fileservice.FileService](proc.GetFileService(), defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	w.writer, err = blockio.NewBlockWriterNew(s3, obj, w.schemaVersion, w.seqnums)
	if err != nil {
		return nil, err
	}
	w.lengths = w.lengths[:0]
	return obj, err
}

// reference to pkg/sql/colexec/order/order.go logic
func sortByKey(proc *process.Process, bat *batch.Batch, sortIndex int, allow_null bool, m *mpool.MPool) error {
	hasNull := false
	// Not-Null Check, notice that cluster by support null value
	if nulls.Any(bat.Vecs[sortIndex].GetNulls()) {
		hasNull = true
		if !allow_null {
			return moerr.NewConstraintViolation(proc.Ctx,
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

func (w *S3Writer) WriteBlock(bat *batch.Batch, dataType ...objectio.DataMetaType) error {
	if w.pk > -1 {
		pkIdx := uint16(w.pk)
		w.writer.SetPrimaryKey(pkIdx)
	}
	if w.sortIndex > -1 {
		w.writer.SetSortKey(uint16(w.sortIndex))
	}
	if w.attrs == nil {
		w.attrs = bat.Attrs
	}
	if len(w.seqnums) != len(bat.Vecs) {
		// just warn becase writing delete s3 file does not need seqnums.
		// print the attrs to tell if it is a delete batch
		logutil.Warnf("CN write s3 table %q: seqnums length not match seqnums: %v, attrs: %v",
			w.tablename, w.seqnums, bat.Attrs)
	}
	// logutil.Infof("write s3 batch(%d) %q: %v, %v", bat.vecs[0].Length(), w.tablename, w.seqnums, w.attrs)
	if len(dataType) > 0 && dataType[0] == objectio.SchemaTombstone {
		_, err := w.writer.WriteTombstoneBatch(bat)
		if err != nil {
			return err
		}
	} else {
		_, err := w.writer.WriteBatch(bat)
		if err != nil {
			return err
		}
	}
	w.lengths = append(w.lengths, uint64(bat.Vecs[0].Length()))
	return nil
}

func (w *S3Writer) writeEndBlocks(proc *process.Process) error {
	blkInfos, stats, err := w.WriteEndBlocks(proc)
	if err != nil {
		return err
	}
	for _, blkInfo := range blkInfos {
		if err := vector.AppendFixed(
			w.blockInfoBat.Vecs[0],
			w.partitionIndex,
			false,
			proc.GetMPool()); err != nil {
			return err
		}
		if err := vector.AppendBytes(
			w.blockInfoBat.Vecs[1],
			//[]byte(metaLoc),
			objectio.EncodeBlockInfo(blkInfo),
			false,
			proc.GetMPool()); err != nil {
			return err
		}
	}

	// append the object stats to bat,
	// at most one will append in
	for idx := 0; idx < len(stats); idx++ {
		if stats[idx].IsZero() {
			continue
		}

		if err = vector.AppendBytes(w.blockInfoBat.Vecs[2],
			stats[idx].Marshal(), false, proc.GetMPool()); err != nil {
			return err
		}
	}

	w.blockInfoBat.SetRowCount(w.blockInfoBat.Vecs[0].Length())
	return nil
}

// WriteEndBlocks writes batches in buffer to fileservice(aka s3 in this feature) and get meta data about block on fileservice and put it into metaLocBat
// For more information, please refer to the comment about func WriteEnd in Writer interface
func (w *S3Writer) WriteEndBlocks(proc *process.Process) ([]objectio.BlockInfo, []objectio.ObjectStats, error) {
	blocks, _, err := w.writer.Sync(proc.Ctx)
	logutil.Debugf("write s3 table %q: %v, %v", w.tablename, w.seqnums, w.attrs)
	if err != nil {
		return nil, nil, err
	}
	blkInfos := make([]objectio.BlockInfo, 0, len(blocks))
	//TODO::block id ,segment id and location should be get from BlockObject.
	for j := range blocks {
		location := blockio.EncodeLocation(
			w.writer.GetName(),
			blocks[j].GetExtent(),
			uint32(w.lengths[j]),
			blocks[j].GetID(),
		)

		sid := location.Name().SegmentId()
		blkInfo := objectio.BlockInfo{
			BlockID: *objectio.NewBlockid(
				&sid,
				location.Name().Num(),
				location.ID()),
			SegmentID: sid,
			//non-appendable block
			EntryState: false,
		}
		blkInfo.SetMetaLocation(location)
		if w.sortIndex != -1 {
			blkInfo.Sorted = true
		}
		blkInfos = append(blkInfos, blkInfo)
	}
	return blkInfos, w.writer.GetObjectStats(), err
}
