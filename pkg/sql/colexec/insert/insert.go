// Copyright 2021 Matrix Origin
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

package insert

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/partition"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/update"

	"github.com/matrixorigin/matrixone/pkg/sql/util"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(_ any, buf *bytes.Buffer) {
	buf.WriteString("insert select")
}

// After cn writes the data to s3, it will get meta data about the block (aka metaloc) by calling func WriteEndBlocks
// and cn needs to pass it to dn for conflict detection
// Except for the case of writing s3 directly, cn doesn't need to sense how dn is labeling the blocks on s3
func GetBlockMeta(bats []*batch.Batch, n *Argument, proc *process.Process) (*batch.Batch, error) {
	// A simple explanation of the two vectors held by metaLocBat
	// vecs[0] to mark which table this metaLoc belongs to: [0] means insertTable itself, [1] means the first uniqueIndex table, [2] means the second uniqueIndex table and so on
	// vecs[1] store relative block metadata
	attrs := []string{catalog.BlockMeta_TableIdx_Insert, catalog.BlockMeta_MetaLoc}
	metaLocBat := batch.New(true, attrs)
	metaLocBat.Vecs[0] = vector.New(types.Type{Oid: types.T(types.T_uint16)})
	metaLocBat.Vecs[1] = vector.New(types.New(types.T_varchar,
		types.MaxVarcharLen, 0, 0))

	for i := range bats {
		if err := GenerateWriter(n, proc); err != nil {
			return nil, err
		}
		if len(n.container.pkIndex) != 0 {
			SortByPrimaryKey(proc, n, bats[i], n.container.pkIndex, proc.GetMPool())
		}
		if bats[i].Length() == 0 {
			continue
		}
		if err := WriteBlock(n, bats[i], proc); err != nil {
			return nil, err
		}
		WriteEndBlocks(n, proc, metaLocBat)
	}

	// send it to connector operator.
	// vitually, first it will be recieved by output, then transfer it to connector by rpc
	metaLocBat.SetZs(metaLocBat.Vecs[0].Length(), proc.GetMPool())
	return metaLocBat, nil
}

func GenerateWriter(ap *Argument, proc *process.Process) error {
	segId, err := colexec.Srv.GenerateSegment()

	if err != nil {
		return err
	}
	s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
	if err != nil {
		return err
	}
	ap.container.writer, err = objectio.NewObjectWriter(segId, s3)
	ap.container.lengths = ap.container.lengths[:0]
	if err != nil {
		return err
	}
	if ap.UniqueIndexDef == nil {
		return nil
	}
	ap.container.unique_writer = ap.container.unique_writer[:0]
	ap.container.unique_lengths = ap.container.unique_lengths[:0]
	for i := range ap.UniqueIndexDef.TableExists {
		if ap.UniqueIndexDef.TableExists[i] {
			segId, err := colexec.Srv.GenerateSegment()
			if err != nil {
				return err
			}
			s3, err := fileservice.Get[fileservice.FileService](proc.FileService, defines.SharedFileServiceName)
			if err != nil {
				return err
			}
			writer, err := objectio.NewObjectWriter(segId, s3)
			if err != nil {
				return err
			}
			ap.container.unique_writer = append(ap.container.unique_writer, writer)
			ap.container.unique_lengths = append(ap.container.unique_lengths, make([]uint64, 0, 1))
		}
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

// reSizeBatch will try to set the batch with the length of DefaultBlockMaxRows
// consider DefaultBlockMaxRows as unit
// case 1. If the length of bat and cacheBat together is larger than DefaultBlockMaxRows, then split the batch into unit batchs and return, the smaller part store in cacheBat
// case 2. If the length of bat and cacheBat together is less than DefaultBlockMaxRows, then bat is merged into cacheBat
// The expected result is : unitBatch1, unitBatch2, ... unitBatchx, the last Batch that batchSize less than DefaultBlockMaxRows
//
// limit : one segment has only one block, this limit exists because currently, tae caches blocks in memory (instead of disk) before writing them to s3, which means that if limit 1 is removed, it may cause memory problems
func reSizeBatch(n *Argument, bat *batch.Batch, proc *process.Process) (bats []*batch.Batch) {
	var newBat *batch.Batch
	var cacheLen uint32
	if n.container.cacheBat != nil {
		cacheLen = uint32(n.container.cacheBat.Length())
	}
	idx := int(cacheLen)
	cnt := cacheLen + uint32(bat.Length())

	if cnt >= options.DefaultBlockMaxRows { // case 1
		if n.container.cacheBat != nil {
			newBat = n.container.cacheBat
			n.container.cacheBat = nil
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
		if n.container.cacheBat == nil {
			n.container.cacheBat = getNewBatch(bat)
		}
		for i := 0; i < bat.Length(); i++ {
			for j := range n.container.cacheBat.Vecs {
				vector.UnionOne(n.container.cacheBat.Vecs[j], bat.Vecs[j], int64(i), proc.GetMPool())
			}
		}
		n.container.cacheBat.SetZs(n.container.cacheBat.Vecs[0].Length(), proc.GetMPool())
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
			n.container.cacheBat = newBat
			n.container.cacheBat.SetZs(n.container.cacheBat.Vecs[0].Length(), proc.GetMPool())
		}
	}
	return
}

func Prepare(proc *process.Process, arg any) error {
	ap := arg.(*Argument)
	if ap.IsRemote {
		ap.container = new(Container)
		ap.GetPkIndexes()
		ap.container.nameToNullablity = make(map[string]bool)
		ap.container.pk = make(map[string]bool)
		ap.GetNameNullAbility()
	}
	return nil
}

func handleWrite(n *Argument, proc *process.Process, ctx context.Context, bat *batch.Batch) error {
	// XXX The original logic was buggy and I had to temporarily circumvent it
	if bat.Length() == 0 {
		bat.SetZs(bat.GetVector(0).Length(), proc.Mp())
	}
	var err error
	var metaLocBat *batch.Batch
	// notice the number of the index def not equal to the number of the index table
	// in some special cases, we don't create index table.
	if n.UniqueIndexDef != nil {
		primaryKeyName := update.GetTablePriKeyName(n.TargetColDefs, n.CPkeyColDef)
		idx := 0
		for i := range n.UniqueIndexDef.TableNames {
			if n.UniqueIndexDef.TableExists[i] {
				b, rowNum := util.BuildUniqueKeyBatch(bat.Vecs, bat.Attrs, n.UniqueIndexDef.Fields[i].Parts, primaryKeyName, proc)
				if rowNum != 0 {
					b.SetZs(rowNum, proc.Mp())
					if !n.IsRemote {
						err = n.UniqueIndexTables[idx].Write(ctx, b)
					}
					if err != nil {
						return err
					}
				}
				b.Clean(proc.Mp())
				idx++
			}
		}
	}
	if !n.IsRemote {
		if err := n.TargetTable.Write(ctx, bat); err != nil {
			return err
		}
	} else {
		bats := reSizeBatch(n, bat, proc)
		if len(bats) == 0 {
			proc.SetInputBatch(&batch.Batch{})
			return nil
		}
		metaLocBat, err = GetBlockMeta(bats, n, proc)
		if err != nil {
			return err
		}
		proc.SetInputBatch(metaLocBat)
	}
	atomic.AddUint64(&n.Affected, uint64(bat.Vecs[0].Length()))
	return nil
}

// referece to pkg/sql/colexec/order/order.go logic
func SortByPrimaryKey(proc *process.Process, n *Argument, bat *batch.Batch, pkIdx []int, m *mpool.MPool) error {
	// Not-Null Check
	for i := 0; i < len(pkIdx); i++ {
		if nulls.Any(bat.Vecs[i].Nsp) {
			return moerr.NewConstraintViolation(proc.Ctx, fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
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

// GenerateIndex generates relative indexes for the batch writed directly to s3 from cn
// For more information, please refer to the comment about func WriteIndex in Writer interface
func GenerateIndex(n *Argument, fd objectio.BlockObject, objectWriter objectio.Writer, bat *batch.Batch) error {
	for i, mvec := range bat.Vecs {
		err := getIndexDataFromVec(fd, objectWriter, uint16(i), mvec, n.container.nameToNullablity[bat.Attrs[i]], n.container.pk[bat.Attrs[i]])
		if err != nil {
			return err
		}
	}
	return nil
}

// WriteBlock WriteBlock writes one batch to a buffer and generate related indexes for this batch
// For more information, please refer to the comment about func Write in Writer interface
func WriteBlock(n *Argument, bat *batch.Batch, proc *process.Process) error {
	fd, err := n.container.writer.Write(bat)

	if err != nil {
		return err
	}
	// atomic.AddUint64(&n.Affected, uint64(bat.Vecs[0].Length()))
	n.container.lengths = append(n.container.lengths, uint64(bat.Vecs[0].Length()))
	if err := GenerateIndex(n, fd, n.container.writer, bat); err != nil {
		return err
	}
	if n.UniqueIndexDef != nil {
		primaryKeyName := update.GetTablePriKeyName(n.TargetColDefs, n.CPkeyColDef)
		idx := 0
		for i := range n.UniqueIndexDef.TableNames {
			if n.UniqueIndexDef.TableExists[i] {
				b, rowNum := util.BuildUniqueKeyBatch(bat.Vecs, bat.Attrs, n.UniqueIndexDef.Fields[i].Parts, primaryKeyName, proc)
				if rowNum != 0 {
					b.SetZs(rowNum, proc.Mp())
					n.container.unique_lengths[idx] = append(n.container.unique_lengths[idx], uint64(rowNum))
					fd, err = n.container.unique_writer[idx].Write(b)
					if err != nil {
						return err
					}
					if err := GenerateIndex(n, fd, n.container.unique_writer[idx], b); err != nil {
						return err
					}
				}
				b.Clean(proc.Mp())
				idx++
			}
		}
	}
	return nil
}

// WriteEndBlocks WriteEndBlocks write batches in buffer to fileservice(aka s3 in this feature) and get meta data about block on fileservice and put it into metaLocBat
// For more information, please refer to the comment about func WriteEnd in Writer interface
func WriteEndBlocks(n *Argument, proc *process.Process, metaLocBat *batch.Batch) error {
	blocks, err := n.container.writer.WriteEnd(context.Background())
	if err != nil {
		return err
	}
	for j := range blocks {
		metaLoc, err := blockio.EncodeMetaLocWithObject(
			blocks[0].GetExtent(),
			uint32(n.container.lengths[j]),
			blocks,
		)
		if err != nil {
			return err
		}
		metaLocBat.Vecs[0].Append(uint16(0), false, proc.GetMPool())
		metaLocBat.Vecs[1].Append([]byte(metaLoc), false, proc.GetMPool())
	}
	for i := range n.container.unique_writer {
		if blocks, err = n.container.unique_writer[i].WriteEnd(proc.Ctx); err != nil {
			return err
		}
		for j := range blocks {
			metaLoc, err := blockio.EncodeMetaLocWithObject(
				blocks[0].GetExtent(),
				uint32(n.container.unique_lengths[i][j]),
				blocks,
			)
			if err != nil {
				return err
			}
			metaLocBat.Vecs[0].Append(uint16(i+1), false, proc.GetMPool())
			metaLocBat.Vecs[1].Append([]byte(metaLoc), false, proc.GetMPool())
		}
	}
	return nil
}

func Call(idx int, proc *process.Process, arg any, isFirst bool, isLast bool) (bool, error) {
	n := arg.(*Argument)
	bat := proc.Reg.InputBatch
	t1 := time.Now()
	if bat == nil {
		if n.IsRemote {
			// handle the last Batch that batchSize less than DefaultBlockMaxRows
			// for more info, refer to the comments about reSizeBatch
			if n.container.cacheBat != nil {
				metaLocBat, err := GetBlockMeta([]*batch.Batch{n.container.cacheBat}, n, proc)
				if err != nil {
					return true, err
				}
				proc.SetInputBatch(metaLocBat)
			}
		}
		return true, nil
	}
	if len(bat.Zs) == 0 {
		return false, nil
	}
	ctx := proc.Ctx
	clusterTable := n.ClusterTable

	if clusterTable.GetIsClusterTable() {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	}
	defer func() {
		bat.Clean(proc.Mp())
		anal := proc.GetAnalyze(idx)
		anal.AddInsertTime(t1)
	}()
	{
		for i := range bat.Vecs {
			// Not-null check, for more information, please refer to the comments in func InsertValues
			if (n.TargetColDefs[i].Primary && !n.TargetColDefs[i].Typ.AutoIncr) || (n.TargetColDefs[i].Default != nil && !n.TargetColDefs[i].Default.NullAbility && !n.TargetColDefs[i].Typ.AutoIncr) {
				if nulls.Any(bat.Vecs[i].Nsp) {
					return false, moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
				}
			}
		}
	}
	{
		bat.Ro = false
		bat.Attrs = make([]string, len(bat.Vecs))
		// scalar vector's extension
		for i := range bat.Vecs {
			bat.Attrs[i] = n.TargetColDefs[i].GetName()
			bat.Vecs[i] = bat.Vecs[i].ConstExpand(false, proc.Mp())
			if bat.Vecs[i].IsScalarNull() && n.TargetColDefs[i].GetTyp().GetAutoIncr() {
				bat.Vecs[i].ConstExpand(true, proc.Mp())
			}
		}
	}
	if clusterTable.GetIsClusterTable() {
		accountIdColumnDef := n.TargetColDefs[clusterTable.GetColumnIndexOfAccountId()]
		accountIdExpr := accountIdColumnDef.GetDefault().GetExpr()
		accountIdConst := accountIdExpr.GetC()

		vecLen := vector.Length(bat.Vecs[0])
		tmpBat := batch.NewWithSize(0)
		tmpBat.Zs = []int64{1}
		//save auto_increment column if necessary
		savedAutoIncrVectors := make([]*vector.Vector, 0)
		defer func() {
			for _, vec := range savedAutoIncrVectors {
				vector.Clean(vec, proc.Mp())
			}
		}()
		for i, colDef := range n.TargetColDefs {
			if colDef.GetTyp().GetAutoIncr() {
				vec2, err := vector.Dup(bat.Vecs[i], proc.Mp())
				if err != nil {
					return false, err
				}
				savedAutoIncrVectors = append(savedAutoIncrVectors, vec2)
			}
		}
		for idx, accountId := range clusterTable.GetAccountIDs() {
			//update accountId in the accountIdExpr
			accountIdConst.Value = &plan.Const_U32Val{U32Val: accountId}
			accountIdVec := bat.Vecs[clusterTable.GetColumnIndexOfAccountId()]
			//clean vector before fill it
			vector.Clean(accountIdVec, proc.Mp())
			//the i th row
			for i := 0; i < vecLen; i++ {
				err := fillRow(tmpBat, accountIdExpr, accountIdVec, proc)
				if err != nil {
					return false, err
				}
			}
			if idx != 0 { //refill the auto_increment column vector
				j := 0
				for colIdx, colDef := range n.TargetColDefs {
					if colDef.GetTyp().GetAutoIncr() {
						targetVec := bat.Vecs[colIdx]
						vector.Clean(targetVec, proc.Mp())
						for k := int64(0); k < int64(vecLen); k++ {
							err := vector.UnionOne(targetVec, savedAutoIncrVectors[j], k, proc.Mp())
							if err != nil {
								return false, err
							}
						}
						j++
					}
				}
			}
			b, err := writeBatch(ctx, n, proc, bat)
			if err != nil {
				return b, err
			}
		}
		return false, nil
	} else {
		return writeBatch(ctx, n, proc, bat)
	}
}

/*
fillRow evaluates the expression and put the result into the targetVec.
tmpBat: store temporal vector
expr: the expression to be evaluated at the position (colIdx,rowIdx)
targetVec: the destination where the evaluated result of expr saved into
*/
func fillRow(tmpBat *batch.Batch,
	expr *plan.Expr,
	targetVec *vector.Vector,
	proc *process.Process) error {
	vec, err := colexec.EvalExpr(tmpBat, proc, expr)
	if err != nil {
		return err
	}
	if vec.Size() == 0 {
		vec = vec.ConstExpand(false, proc.Mp())
	}
	if err := vector.UnionOne(targetVec, vec, 0, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return err
	}
	vec.Free(proc.Mp())
	return err
}

// writeBatch saves the batch into the storage
// and updates the auto increment table, index table.
func writeBatch(ctx context.Context,
	n *Argument,
	proc *process.Process,
	bat *batch.Batch) (bool, error) {

	if n.HasAutoCol {
		if err := colexec.UpdateInsertBatch(n.Engine, ctx, proc, n.TargetColDefs, bat, n.TableID, n.DBName, n.TableName); err != nil {
			return false, err
		}
	}
	if n.CPkeyColDef != nil {
		err := util.FillCompositePKeyBatch(bat, n.CPkeyColDef, proc)
		if err != nil {
			names := util.SplitCompositePrimaryKeyColumnName(n.CPkeyColDef.Name)
			for _, name := range names {
				for i := range bat.Vecs {
					if n.TargetColDefs[i].Name == name {
						if nulls.Any(bat.Vecs[i].Nsp) {
							return false, moerr.NewConstraintViolation(ctx, fmt.Sprintf("Column '%s' cannot be null", n.TargetColDefs[i].GetName()))
						}
					}
				}
			}
		}
	} else if n.ClusterByDef != nil && util.JudgeIsCompositeClusterByColumn(n.ClusterByDef.Name) {
		util.FillCompositeClusterByBatch(bat, n.ClusterByDef.Name, proc)
	}
	// set null value's data
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.CheckInsertVector(bat.Vecs[i], proc.Mp())
	}
	if n.IsRemote {
		return false, handleWrite(n, proc, ctx, bat)
	}
	return false, handleWrite(n, proc, ctx, bat)
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
