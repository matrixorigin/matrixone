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
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type WriteS3Container struct {
	// in fact, len(sortIndex) is 1 at most.
	sortIndex        []int
	nameToNullablity map[string]bool
	pk               map[string]bool

	writer  objectio.Writer
	lengths []uint64

	UniqueRels []engine.Relation

	metaLocBat *batch.Batch

	// buffers[i] stands the i-th buffer batch used
	// for merge sort (corresponding to table_i,table_i could be unique
	// table or main table)
	buffers []*batch.Batch

	// tableBatches[i] used to store the batches of table_i
	// when the batches' size is over 64M, we will use merge
	// sort, and then write a segment in s3
	tableBatches [][]*batch.Batch

	// tableBatchSizes are used to record the table_i's batches's
	// size in tableBatches
	tableBatchSizes []uint64
}

const (
	// when batches's  size of table is over this, we will
	// trigger write s3
	WriteS3Threshold uint64 = 64 * mpool.MB
)

func NewWriteS3Container(tableDef *plan.TableDef) *WriteS3Container {
	unique_nums := 0
	for _, idx := range tableDef.Indexes {
		if idx.Unique {
			unique_nums++
		}
	}

	container := &WriteS3Container{
		sortIndex:        make([]int, 0, 1),
		pk:               make(map[string]bool),
		nameToNullablity: make(map[string]bool),
		// main table and unique tables
		buffers:         make([]*batch.Batch, unique_nums+1),
		tableBatches:    make([][]*batch.Batch, unique_nums+1),
		tableBatchSizes: make([]uint64, unique_nums+1),
	}

	// Get CPkey index
	if tableDef.CompositePkey != nil {
		// the serialized cpk col is located in the last of the bat.vecs
		container.sortIndex = append(container.sortIndex, len(tableDef.Cols))
	} else {
		// Get Single Col pk index
		for num, colDef := range tableDef.Cols {
			if colDef.Primary {
				container.sortIndex = append(container.sortIndex, num)
				break
			}
		}
		if tableDef.ClusterBy != nil {
			if util.JudgeIsCompositeClusterByColumn(tableDef.ClusterBy.Name) {
				// the serialized clusterby col is located in the last of the bat.vecs
				container.sortIndex = append(container.sortIndex, len(tableDef.Cols))
			} else {
				for num, colDef := range tableDef.Cols {
					if colDef.Name == tableDef.ClusterBy.Name {
						container.sortIndex = append(container.sortIndex, num)
					}
				}
			}
		}
	}

	// get NameNullAbility
	for _, def := range tableDef.Cols {
		container.nameToNullablity[def.Name] = def.Default.NullAbility
		if def.Primary {
			container.pk[def.Name] = true
		}
	}
	if tableDef.CompositePkey != nil {
		def := tableDef.CompositePkey
		container.nameToNullablity[def.Name] = def.Default.NullAbility
		container.pk[def.Name] = true
	}

	//if tableDef.Indexes != nil {
	//	for _, indexdef := range tableDef.Indexes {
	//		if indexdef.Unique {
	//			for j := range indexdef.Field.Cols {
	//				coldef := indexdef.Field.Cols[j]
	//				container.nameToNullablity[coldef.Name] = coldef.Default.NullAbility
	//			}
	//		} else {
	//			continue
	//		}
	//	}
	//}

	if tableDef.ClusterBy != nil {
		container.nameToNullablity[tableDef.ClusterBy.Name] = true
	}
	container.resetMetaLocBat()

	return container
}

func (container *WriteS3Container) resetMetaLocBat() {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc}
	metaLocBat := batch.New(true, attrs)
	metaLocBat.Vecs[0] = vector.New(types.Type{Oid: types.T(types.T_uint16)})
	metaLocBat.Vecs[1] = vector.New(types.New(types.T_varchar, types.MaxVarcharLen, 0))

	container.metaLocBat = metaLocBat
}

func (container *WriteS3Container) WriteEnd(proc *process.Process) {
	if container.metaLocBat.Vecs[0].Length() > 0 {
		container.metaLocBat.SetZs(container.metaLocBat.Vecs[0].Length(), proc.GetMPool())
		proc.SetInputBatch(container.metaLocBat)
		container.resetMetaLocBat()
	}
}

func (container *WriteS3Container) WriteS3CacheBatch(proc *process.Process) error {
	for i := range container.tableBatches {
		if container.tableBatchSizes[i] > 0 {
			if err := container.MergeBlock(i, len(container.tableBatches[i]), proc); err != nil {
				return err
			}
		}
	}
	container.WriteEnd(proc)
	return nil
}

func (container *WriteS3Container) InitBuffers(bat *batch.Batch, idx int) {
	if container.buffers[idx] == nil {
		container.buffers[idx] = getNewBatch(bat)
	}
}

// the return value can be 1,0,-1
// 1: the tableBatches[idx] is over threshold
// 0: the tableBatches[idx] is equal to threshold
// -1: the tableBatches[idx] is less than threshold
func (container *WriteS3Container) Put(bat *batch.Batch, idx int) int {
	container.tableBatchSizes[idx] += uint64(bat.Size())
	container.tableBatches[idx] = append(container.tableBatches[idx], bat)
	if container.tableBatchSizes[idx] == WriteS3Threshold {
		return 0
	} else if container.tableBatchSizes[idx] > WriteS3Threshold {
		return 1
	}
	return -1
}

func GetFixedCols[T types.FixedSizeT](bats []*batch.Batch, idx int) (cols [][]T) {
	for i := range bats {
		cols = append(cols, vector.GetFixedVectorValues[T](bats[i].Vecs[idx]))
	}
	return
}

func GetStrCols(bats []*batch.Batch, idx int) (cols [][]string) {
	for i := range bats {
		cols = append(cols, vector.GetStrVectorValues(bats[i].Vecs[idx]))
	}
	return
}

// len(sortIndex) is always only one.
func (container *WriteS3Container) MergeBlock(idx int, length int, proc *process.Process) error {
	bats := container.tableBatches[idx][:length]
	sortIdx := -1
	for i := range bats {
		// sort bats firstly
		// for main table
		if idx == 0 && len(container.sortIndex) != 0 {
			SortByKey(proc, bats[i], container.sortIndex, proc.GetMPool())
			sortIdx = container.sortIndex[0]
		}
	}
	// just write ahead, no need to sort
	if sortIdx == -1 {
		if err := GenerateWriter(container, proc); err != nil {
			return err
		}
		for i := range bats {
			if err := WriteBlock(container, bats[i], proc); err != nil {
				return err
			}
		}
		if err := WriteEndBlocks(container, proc, idx); err != nil {
			return err
		}
	} else {
		var merge MergeInterface
		var nulls []*nulls.Nulls
		for i := 0; i < len(bats); i++ {
			nulls = append(nulls, bats[i].Vecs[container.sortIndex[0]].Nsp)
		}
		pos := container.sortIndex[0]
		switch bats[0].Vecs[sortIdx].Typ.Oid {
		case types.T_bool:
			merge = NewMerge(len(bats), sort.NewBoolLess(), GetFixedCols[bool](bats, pos), nulls)
		case types.T_int8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int8](), GetFixedCols[int8](bats, pos), nulls)
		case types.T_int16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int16](), GetFixedCols[int16](bats, pos), nulls)
		case types.T_int32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int32](), GetFixedCols[int32](bats, pos), nulls)
		case types.T_int64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[int64](), GetFixedCols[int64](bats, pos), nulls)
		case types.T_uint8:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint8](), GetFixedCols[uint8](bats, pos), nulls)
		case types.T_uint16:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint16](), GetFixedCols[uint16](bats, pos), nulls)
		case types.T_uint32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint32](), GetFixedCols[uint32](bats, pos), nulls)
		case types.T_uint64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[uint64](), GetFixedCols[uint64](bats, pos), nulls)
		case types.T_float32:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float32](), GetFixedCols[float32](bats, pos), nulls)
		case types.T_float64:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[float64](), GetFixedCols[float64](bats, pos), nulls)
		case types.T_date:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Date](), GetFixedCols[types.Date](bats, pos), nulls)
		case types.T_datetime:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Datetime](), GetFixedCols[types.Datetime](bats, pos), nulls)
		case types.T_time:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Time](), GetFixedCols[types.Time](bats, pos), nulls)
		case types.T_timestamp:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[types.Timestamp](), GetFixedCols[types.Timestamp](bats, pos), nulls)
		case types.T_decimal64:
			merge = NewMerge(len(bats), sort.NewDecimal64Less(), GetFixedCols[types.Decimal64](bats, pos), nulls)
		case types.T_decimal128:
			merge = NewMerge(len(bats), sort.NewDecimal128Less(), GetFixedCols[types.Decimal128](bats, pos), nulls)
		case types.T_uuid:
			merge = NewMerge(len(bats), sort.NewUuidCompLess(), GetFixedCols[types.Uuid](bats, pos), nulls)
		case types.T_char, types.T_varchar, types.T_blob, types.T_text:
			merge = NewMerge(len(bats), sort.NewGenericCompLess[string](), GetStrCols(bats, pos), nulls)
		}
		if err := GenerateWriter(container, proc); err != nil {
			return err
		}
		lens := 0
		size := len(bats)
		var batchIndex int
		var rowIndex int
		for size > 0 {
			batchIndex, rowIndex, size = merge.GetNextPos()
			for i := range container.buffers[idx].Vecs {
				vector.UnionOne(container.buffers[idx].Vecs[i], bats[batchIndex].Vecs[i], int64(rowIndex), proc.GetMPool())
			}
			lens++
			if lens == int(options.DefaultBlockMaxRows) {
				lens = 0
				if err := WriteBlock(container, container.buffers[idx], proc); err != nil {
					return err
				}
				// force clean
				container.buffers[idx].CleanOnlyData()
			}
		}
		if lens > 0 {
			if err := WriteBlock(container, container.buffers[idx], proc); err != nil {
				return err
			}
		}
		if err := WriteEndBlocks(container, proc, idx); err != nil {
			return err
		}
	}
	left := container.tableBatches[idx][length:]
	container.tableBatches[idx] = left
	container.tableBatchSizes[idx] = 0
	for _, bat := range left {
		container.tableBatchSizes[idx] += uint64(bat.Size())
	}
	return nil
}

// the first pr for cn-write-s3 logic of insert will result this:
// and now we need to change it, there will be 64Mb data in one seg.
func (container *WriteS3Container) WriteS3Batch(bat *batch.Batch, proc *process.Process, idx int) error {
	container.InitBuffers(bat, idx)
	res := container.Put(bat, idx)
	switch res {
	case 1:
		container.MergeBlock(idx, len(container.tableBatches[idx])-1, proc)
	case 0:
		container.MergeBlock(idx, len(container.tableBatches[idx]), proc)
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
		newBat.Vecs[i] = vector.New(bat.Vecs[i].GetType())
	}
	return newBat
}

func GenerateWriter(container *WriteS3Container, proc *process.Process) error {
	segId, err := Srv.GenerateSegment()

	if err != nil {
		return err
	}
	s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	container.writer, err = objectio.NewObjectWriter(segId, s3)
	if err != nil {
		return err
	}
	container.lengths = container.lengths[:0]
	return nil
}

// referece to pkg/sql/colexec/order/order.go logic
func SortByKey(proc *process.Process, bat *batch.Batch, sortIndex []int, m *mpool.MPool) error {
	// Not-Null Check
	for i := 0; i < len(sortIndex); i++ {
		if nulls.Any(bat.Vecs[i].Nsp) {
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
	if ovec.Typ.IsString() {
		strCol = vector.GetStrVectorValues(ovec)
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
		if vec.Typ.IsString() {
			strCol = vector.GetStrVectorValues(vec)
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

// WriteBlock WriteBlock writes one batch to a buffer and generate related indexes for this batch
// For more information, please refer to the comment about func Write in Writer interface
func WriteBlock(container *WriteS3Container, bat *batch.Batch, proc *process.Process) error {
	fd, err := container.writer.Write(bat)

	if err != nil {
		return err
	}
	// atomic.AddUint64(&n.Affected, uint64(bat.Vecs[0].Length()))

	container.lengths = append(container.lengths, uint64(bat.Vecs[0].Length()))
	if err := GenerateIndex(container, fd, container.writer, bat); err != nil {
		return err
	}

	return nil
}

// GenerateIndex generates relative indexes for the batch writed directly to s3 from cn
// For more information, please refer to the comment about func WriteIndex in Writer interface
func GenerateIndex(container *WriteS3Container, fd objectio.BlockObject, objectWriter objectio.Writer, bat *batch.Batch) error {
	for i, mvec := range bat.Vecs {
		err := getIndexDataFromVec(fd, objectWriter, uint16(i), mvec, container.nameToNullablity[bat.Attrs[i]], container.pk[bat.Attrs[i]])
		if err != nil {
			return err
		}
	}
	return nil
}

func getIndexDataFromVec(block objectio.BlockObject, writer objectio.Writer,
	idx uint16,
	vec *vector.Vector, nullAbliaty bool, isPk bool) error {
	var err error
	columnData := containers.NewVectorWithSharedMemory(vec, nullAbliaty)
	zmPos := 0
	zoneMapWriter := indexwrapper.NewZMWriter()
	if err = zoneMapWriter.Init(writer, block, common.Plain, idx, uint16(zmPos)); err != nil {
		return err
	}
	err = zoneMapWriter.AddValues(columnData)
	if err != nil {
		return err
	}
	_, err = zoneMapWriter.Finalize()
	if err != nil {
		return err
	}
	if !isPk {
		return nil
	}
	bfPos := 1
	bfWriter := indexwrapper.NewBFWriter()
	if err = bfWriter.Init(writer, block, common.Plain, idx, uint16(bfPos)); err != nil {
		return err
	}
	if err = bfWriter.AddValues(columnData); err != nil {
		return err
	}
	_, err = bfWriter.Finalize()
	if err != nil {
		return err
	}
	return nil
}

// WriteEndBlocks WriteEndBlocks write batches in buffer to fileservice(aka s3 in this feature) and get meta data about block on fileservice and put it into metaLocBat
// For more information, please refer to the comment about func WriteEnd in Writer interface
func WriteEndBlocks(container *WriteS3Container, proc *process.Process, idx int) error {
	blocks, err := container.writer.WriteEnd(proc.Ctx)
	if err != nil {
		return err
	}
	for j := range blocks {
		metaLoc, err := blockio.EncodeMetaLocWithObject(
			blocks[j].GetExtent(),
			uint32(container.lengths[j]),
			blocks,
		)
		if err != nil {
			return err
		}
		container.metaLocBat.Vecs[0].Append(uint16(idx), false, proc.GetMPool())
		container.metaLocBat.Vecs[1].Append([]byte(metaLoc), false, proc.GetMPool())
	}
	// for i := range container.unique_writer {
	// 	if blocks, err = container.unique_writer[i].WriteEnd(proc.Ctx); err != nil {
	// 		return err
	// 	}
	// 	for j := range blocks {
	// 		metaLoc, err := blockio.EncodeMetaLocWithObject(
	// 			blocks[0].GetExtent(),
	// 			uint32(container.unique_lengths[i][j]),
	// 			blocks,
	// 		)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		metaLocBat.Vecs[0].Append(uint16(i+1), false, proc.GetMPool())
	// 		metaLocBat.Vecs[1].Append([]byte(metaLoc), false, proc.GetMPool())
	// 	}
	// }
	return nil
}
