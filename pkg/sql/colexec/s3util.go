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
	sortIndex        []int
	nameToNullablity map[string]bool
	pk               map[string]bool

	writer   objectio.Writer
	lengths  []uint64
	cacheBat []*batch.Batch

	UniqueRels []engine.Relation

	metaLocBat *batch.Batch
}

func NewWriteS3Container(tableDef *plan.TableDef) *WriteS3Container {
	container := &WriteS3Container{
		sortIndex:        make([]int, 0, 1),
		pk:               make(map[string]bool),
		nameToNullablity: make(map[string]bool),
	}

	// get pk indexes
	if tableDef.CompositePkey != nil {
		names := util.SplitCompositePrimaryKeyColumnName(tableDef.CompositePkey.Name)
		for num, colDef := range tableDef.Cols {
			for _, name := range names {
				if colDef.Name == name {
					container.sortIndex = append(container.sortIndex, num)
				}
			}
		}
	} else {
		// Get Single Col pk index
		for num, colDef := range tableDef.Cols {
			if colDef.Primary {
				container.sortIndex = append(container.sortIndex, num)
				break
			}
		}
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

	if tableDef.Indexes != nil {
		for _, indexdef := range tableDef.Indexes {
			if indexdef.Unique {
				for j := range indexdef.Field.Cols {
					coldef := indexdef.Field.Cols[j]
					container.nameToNullablity[coldef.Name] = coldef.Default.NullAbility
				}
			} else {
				continue
			}
		}
	}

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
	metaLocBat.Vecs[1] = vector.New(types.New(types.T_varchar,
		types.MaxVarcharLen, 0, 0))

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
	if len(container.cacheBat) > 0 {
		for i, bat := range container.cacheBat {
			if bat != nil {
				err := GetBlockMeta([]*batch.Batch{bat}, container, proc, i)
				if err != nil {
					return err
				}
			}
		}
		container.WriteEnd(proc)
	}
	return nil
}

func (container *WriteS3Container) WriteS3Batch(bat *batch.Batch, proc *process.Process, idx int) error {
	bats := reSizeBatch(container, bat, proc, idx)
	if len(bats) == 0 {
		proc.SetInputBatch(&batch.Batch{})
		return nil
	}
	return GetBlockMeta(bats, container, proc, idx)
}

// After cn writes the data to s3, it will get meta data about the block (aka metaloc) by calling func WriteEndBlocks
// and cn needs to pass it to dn for conflict detection
// Except for the case of writing s3 directly, cn doesn't need to sense how dn is labeling the blocks on s3
func GetBlockMeta(bats []*batch.Batch, container *WriteS3Container, proc *process.Process, idx int) error {
	for i := range bats {
		if err := GenerateWriter(container, proc); err != nil {
			return err
		}
		if idx == 0 && len(container.sortIndex) != 0 {
			SortByPrimaryKey(proc, bats[i], container.sortIndex, proc.GetMPool())
		}
		if bats[i].Length() == 0 {
			continue
		}
		if err := WriteBlock(container, bats[i], proc); err != nil {
			return err
		}
		if err := WriteEndBlocks(container, proc, idx); err != nil {
			return err
		}
	}

	// send it to connector operator.
	// vitually, first it will be recieved by output, then transfer it to connector by rpc
	// metaLocBat.SetZs(metaLocBat.Vecs[0].Length(), proc.GetMPool())
	return nil
}

// reSizeBatch will try to set the batch with the length of DefaultBlockMaxRows
// consider DefaultBlockMaxRows as unit
// case 1. If the length of bat and cacheBat together is larger than DefaultBlockMaxRows, then split the batch into unit batchs and return, the smaller part store in cacheBat
// case 2. If the length of bat and cacheBat together is less than DefaultBlockMaxRows, then bat is merged into cacheBat
// The expected result is : unitBatch1, unitBatch2, ... unitBatchx, the last Batch that batchSize less than DefaultBlockMaxRows
//
// limit : one segment has only one block, this limit exists because currently, tae caches blocks in memory (instead of disk) before writing them to s3, which means that if limit 1 is removed, it may cause memory problems
func reSizeBatch(container *WriteS3Container, bat *batch.Batch, proc *process.Process, batIdx int) (bats []*batch.Batch) {
	var newBat *batch.Batch
	var cacheLen uint32
	if len(container.cacheBat) <= batIdx {
		container.cacheBat = append(container.cacheBat, nil)
	}
	if container.cacheBat[batIdx] != nil {
		cacheLen = uint32(container.cacheBat[batIdx].Length())
	}
	idx := int(cacheLen)
	cnt := cacheLen + uint32(bat.Length())

	if cnt >= options.DefaultBlockMaxRows { // case 1
		if container.cacheBat[batIdx] != nil {
			newBat = container.cacheBat[batIdx]
			container.cacheBat[batIdx] = nil
		} else {
			newBat = getNewBatch(bat)
		}

		for cnt >= options.DefaultBlockMaxRows {
			for i := range newBat.Vecs {
				vector.UnionOne(newBat.Vecs[i], bat.Vecs[i], int64(idx)-int64(cacheLen), proc.GetMPool())
			}
			idx++
			if idx%int(options.DefaultBlockMaxRows) == 0 {
				newBat.SetZs(int(options.DefaultBlockMaxRows), proc.GetMPool())
				bats = append(bats, newBat)
				newBat = getNewBatch(bat)
				cnt -= options.DefaultBlockMaxRows
			}
		}
	}

	if len(bats) == 0 { // implying the end of this operator, the last Batch that batchSize less than DefaultBlockMaxRows
		if container.cacheBat[batIdx] == nil {
			container.cacheBat[batIdx] = getNewBatch(bat)
		}
		for i := 0; i < bat.Length(); i++ {
			for j := range container.cacheBat[batIdx].Vecs {
				vector.UnionOne(container.cacheBat[batIdx].Vecs[j], bat.Vecs[j], int64(i), proc.GetMPool())
			}
		}
		container.cacheBat[batIdx].SetZs(container.cacheBat[batIdx].Vecs[0].Length(), proc.GetMPool())
	} else {
		if cnt > 0 { // the part less than DefaultBlockMaxRows stored in cacheBat
			if newBat == nil {
				newBat = getNewBatch(bat)
			}
			for cnt > 0 {
				for i := range newBat.Vecs {
					vector.UnionOne(newBat.Vecs[i], bat.Vecs[i], int64(idx)-int64(cacheLen), proc.GetMPool())
				}
				idx++
				cnt--
			}
			container.cacheBat[batIdx] = newBat
			container.cacheBat[batIdx].SetZs(container.cacheBat[batIdx].Vecs[0].Length(), proc.GetMPool())
		}
	}
	return
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
func SortByPrimaryKey(proc *process.Process, bat *batch.Batch, pkIdx []int, m *mpool.MPool) error {
	// Not-Null Check
	for i := 0; i < len(pkIdx); i++ {
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
	ovec := bat.GetVector(int32(pkIdx[0]))
	if ovec.Typ.IsString() {
		strCol = vector.GetStrVectorValues(ovec)
	} else {
		strCol = nil
	}
	sort.Sort(false, false, false, sels, ovec, strCol)
	if len(pkIdx) == 1 {
		return bat.Shuffle(sels, m)
	}
	ps := make([]int64, 0, 16)
	ds := make([]bool, len(sels))
	for i, j := 1, len(pkIdx); i < j; i++ {
		ps = partition.Partition(sels, ds, ps, ovec)
		vec := bat.Vecs[pkIdx[i]]
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
			blocks[0].GetExtent(),
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
