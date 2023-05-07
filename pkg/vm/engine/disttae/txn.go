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

package disttae

import (
	"context"
	"math"
	"strings"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (txn *Transaction) getBlockInfos(
	ctx context.Context,
	tbl *txnTable,
) (blocks [][]catalog.BlockInfo, err error) {
	blocks = make([][]catalog.BlockInfo, len(txn.dnStores))
	ts := types.TimestampToTS(txn.meta.SnapshotTS)
	states, err := tbl.getParts(ctx)
	if err != nil {
		return nil, err
	}
	for i := range txn.dnStores {
		if i >= len(states) {
			continue
		}
		state := states[i]
		iter := state.Blocks.Iter()
		var objectName objectio.ObjectNameShort
		for ok := iter.First(); ok; ok = iter.Next() {
			entry := iter.Item()
			if !entry.Visible(ts) {
				continue
			}
			location := entry.BlockInfo.MetaLocation()
			if !objectio.IsSameObjectLocVsShort(location, &objectName) {
				// Prefetch object meta
				if err = blockio.PrefetchMeta(txn.proc.FileService, location); err != nil {
					return
				}
				objectName = *location.Name().Short()
			}
			blocks[i] = append(blocks[i], entry.BlockInfo)
		}
		iter.Release()
	}
	return
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// Write used to write data to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteBatch(
	typ int,
	databaseId uint64,
	tableId uint64,
	databaseName string,
	tableName string,
	bat *batch.Batch,
	dnStore DNStore,
	primaryIdx int, // pass -1 to indicate no primary key or disable primary key checking
) error {
	txn.readOnly = false
	bat.Cnt = 1
	if typ == INSERT {
		txn.genBlock()
		len := bat.Length()
		vec := vector.NewVec(types.T_Rowid.ToType())
		for i := 0; i < len; i++ {
			if err := vector.AppendFixed(vec, txn.genRowId(), false,
				txn.proc.Mp()); err != nil {
				return err
			}
		}
		bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
		// for TestPrimaryKeyCheck
		if txn.blockId_raw_batch != nil {
			txn.blockId_raw_batch[txn.getCurrentBlockId()] = bat
		}
		txn.workspaceSize += uint64(bat.Size())
	}
	txn.Lock()
	txn.writes = append(txn.writes, Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
	})
	txn.Unlock()
	return nil
}

func (txn *Transaction) CleanNilBatch() {
	for i := 0; i < len(txn.writes); i++ {
		if txn.writes[i].bat == nil || txn.writes[i].bat.Length() == 0 {
			txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
			i--
		}
	}
}

func (txn *Transaction) DumpBatch(force bool, offset int) error {
	var size uint64

	if !(offset > 0 || txn.workspaceSize >= colexec.WriteS3Threshold ||
		(force && txn.workspaceSize >= colexec.TagS3Size)) {
		return nil
	}
	txn.Lock()
	for i := offset; i < len(txn.writes); i++ {
		if txn.writes[i].bat == nil {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			size += uint64(txn.writes[i].bat.Size())
		}
	}
	if offset > 0 && size < txn.workspaceSize {
		txn.Unlock()
		return nil
	}
	mp := make(map[[2]string][]*batch.Batch)
	for i := offset; i < len(txn.writes); i++ {
		// TODO: after shrink, we should update workspace size
		if txn.writes[i].bat == nil || txn.writes[i].bat.Length() == 0 {
			continue
		}
		if txn.writes[i].typ == INSERT && txn.writes[i].fileName == "" {
			key := [2]string{txn.writes[i].databaseName, txn.writes[i].tableName}
			bat := txn.writes[i].bat
			// skip rowid
			bat.Attrs = bat.Attrs[1:]
			bat.Vecs = bat.Vecs[1:]
			mp[key] = append(mp[key], bat)
			// DON'T MODIFY THE IDX OF AN ENTRY IN LOG
			// THIS IS VERY IMPORTANT FOR CN BLOCK COMPACTION
			// maybe this will cause that the log imcrements unlimitly
			// txn.writes = append(txn.writes[:i], txn.writes[i+1:]...)
			// i--
			txn.writes[i].bat = nil
		}
	}
	txn.Unlock()
	for key := range mp {
		s3Writer, tbl, err := txn.getS3Writer(key)
		if err != nil {
			return err
		}
		s3Writer.InitBuffers(mp[key][0])
		for i := 0; i < len(mp[key]); i++ {
			s3Writer.Put(mp[key][i], txn.proc)
		}
		err = s3Writer.MergeBlock(len(s3Writer.Bats), txn.proc, false)

		if err != nil {
			return err
		}
		metaLoc := s3Writer.GetMetaLocBat()

		lenVecs := len(metaLoc.Attrs)
		// only remain the metaLoc col
		metaLoc.Vecs = metaLoc.Vecs[lenVecs-1:]
		metaLoc.Attrs = metaLoc.Attrs[lenVecs-1:]
		metaLoc.SetZs(metaLoc.Vecs[0].Length(), txn.proc.GetMPool())
		err = tbl.Write(txn.proc.Ctx, metaLoc)
		if err != nil {
			return err
		}
		// free batches
		for _, bat := range mp[key] {
			bat.Clean(txn.proc.GetMPool())
		}
	}
	txn.workspaceSize -= size
	return nil
}

func (txn *Transaction) getS3Writer(key [2]string) (*colexec.S3Writer, engine.Relation, error) {
	sortIdx, attrs, tbl, err := txn.getSortIdx(key)
	if err != nil {
		return nil, nil, err
	}
	s3Writer := &colexec.S3Writer{}
	s3Writer.SetSortIdx(-1)
	s3Writer.Init()
	s3Writer.SetMp(attrs)
	if sortIdx != -1 {
		s3Writer.SetSortIdx(sortIdx)
	}
	return s3Writer, tbl, nil
}

func (txn *Transaction) getSortIdx(key [2]string) (int, []*engine.Attribute, engine.Relation, error) {
	databaseName := key[0]
	tableName := key[1]

	database, err := txn.engine.Database(txn.proc.Ctx, databaseName, txn.proc.TxnOperator)
	if err != nil {
		return -1, nil, nil, err
	}
	tbl, err := database.Relation(txn.proc.Ctx, tableName)
	if err != nil {
		return -1, nil, nil, err
	}
	attrs, err := tbl.TableColumns(txn.proc.Ctx)
	if err != nil {
		return -1, nil, nil, err
	}
	for i := 0; i < len(attrs); i++ {
		if attrs[i].ClusterBy ||
			(attrs[i].Primary && !attrs[i].IsHidden) {
			return i, attrs, tbl, err
		}
	}
	return -1, attrs, tbl, nil
}

func (txn *Transaction) updatePosForCNBlock(vec *vector.Vector, idx int) error {
	metaLocs := vector.MustStrCol(vec)
	for i, metaLoc := range metaLocs {
		if location, err := blockio.EncodeLocationFromString(metaLoc); err != nil {
			return err
		} else {
			sid := location.Name().SegmentId()
			blkid := objectio.NewBlockid(&sid, location.Name().Num(), uint16(location.ID()))
			txn.cnBlkId_Pos[string(blkid[:])] = Pos{idx: idx, offset: int64(i)}
		}
	}
	return nil
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(typ int, databaseId, tableId uint64,
	databaseName, tableName string, fileName string, bat *batch.Batch, dnStore DNStore) error {
	idx := len(txn.writes)
	// used for cn block compaction (next pr)
	if typ == COMPACTION_CN {
		typ = INSERT
	} else if typ == INSERT {
		txn.updatePosForCNBlock(bat.GetVector(0), idx)
	}
	txn.readOnly = false
	txn.writes = append(txn.writes, Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          bat,
		dnStore:      dnStore,
	})

	if uid, err := types.ParseUuid(strings.Split(fileName, "_")[0]); err != nil {
		panic("fileName parse Uuid error")
	} else {
		// get uuid string
		if typ == INSERT {
			colexec.Srv.PutCnSegment(string(uid[:]), colexec.CnBlockIdType)
		}
	}
	return nil
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {
	mp := make(map[types.Rowid]uint8)
	deleteBlkId := make(map[types.Blockid]bool)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(0))
	min1 := uint32(math.MaxUint32)
	max1 := uint32(0)
	cnRowIdOffsets := make([]int64, 0, len(rowids))
	for i, rowid := range rowids {
		// process cn block deletes
		uid := rowid.GetSegid()
		blkid := *rowid.GetBlockid()
		deleteBlkId[blkid] = true
		mp[rowid] = 0
		rowOffset := rowid.GetRowOffset()
		if colexec.Srv != nil && colexec.Srv.GetCnSegmentType(string(uid[:])) == colexec.CnBlockIdType {
			txn.deletedBlocks.addDeletedBlocks(string(blkid[:]), []int64{int64(rowOffset)})
			cnRowIdOffsets = append(cnRowIdOffsets, int64(i))
			continue
		}
		if rowOffset < (min1) {
			min1 = rowOffset
		}

		if rowOffset > max1 {
			max1 = rowOffset
		}
		// update workspace
	}
	// cn rowId antiShrink
	bat.AntiShrink(cnRowIdOffsets)
	if bat.Length() == 0 {
		return bat
	}
	sels := txn.proc.Mp().GetSels()
	txn.Lock()
	// txn worksapce will have four batch type:
	// 1.RawBatch 2.DN Block RowId(mixed rowid from different block)
	// 3.CN block Meta batch(record block meta generated by cn insert write s3)
	// 4.DN delete Block Meta batch(record block meta generated by cn delete write s3)
	for _, e := range txn.writes {
		// nil batch will generated by comapction or dumpBatch
		if e.bat == nil {
			continue
		}
		// for 3 and 4 above.
		if e.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			continue
		}
		sels = sels[:0]
		if e.tableId == tableId && e.databaseId == databaseId {
			vs := vector.MustFixedCol[types.Rowid](e.bat.GetVector(0))
			if len(vs) == 0 {
				continue
			}
			// skip 2 above
			if !vs[0].GetSegid().Eq(txn.segId) {
				continue
			}
			// current batch is not be deleted
			if !deleteBlkId[*vs[0].GetBlockid()] {
				continue
			}
			min2 := vs[0].GetRowOffset()
			max2 := vs[len(vs)-1].GetRowOffset()
			if min1 > max2 || max1 < min2 {
				continue
			}
			for k, v := range vs {
				if _, ok := mp[v]; !ok {
					sels = append(sels, int64(k))
				} else {
					mp[v]++
				}
			}
			if len(sels) != len(vs) {
				e.bat.Shrink(sels)
				rowIds := vector.MustFixedCol[types.Rowid](e.bat.GetVector(0))
				for i := range rowIds {
					(&rowIds[i]).SetRowOffset(uint32(i))
				}
			}
		}
	}
	txn.Unlock()
	sels = sels[:0]
	for k, rowid := range rowids {
		if mp[rowid] == 0 {
			sels = append(sels, int64(k))
		}
	}
	bat.Shrink(sels)
	txn.proc.Mp().PutSels(sels)
	return bat
}

func (txn *Transaction) allocateID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return txn.idGen.AllocateID(ctx)
}

func (txn *Transaction) genBlock() {
	txn.rowId[4]++
	txn.rowId[5] = INIT_ROWID_OFFSET
}

func (txn *Transaction) getCurrentBlockId() string {
	rowId := types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
	blkId := rowId.GetBlockid()
	return string(blkId[:])
}

func (txn *Transaction) genRowId() types.Rowid {
	if txn.rowId[5] != INIT_ROWID_OFFSET {
		txn.rowId[5]++
	} else {
		txn.rowId[5] = 0
	}
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

// needRead determine if a block needs to be read
func needRead(ctx context.Context, expr *plan.Expr, meta objectio.ObjectMeta, blkInfo catalog.BlockInfo, tableDef *plan.TableDef, columnMap map[int]int, columns []int, maxCol int, proc *process.Process) bool {
	var err error
	if expr == nil {
		return true
	}
	notReportErrCtx := errutil.ContextWithNoReport(ctx, true)

	// if expr match no columns, just eval expr
	if len(columns) == 0 {
		bat := batch.NewWithSize(0)
		defer bat.Clean(proc.Mp())
		ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, expr, bat, proc)
		if err != nil {
			return true
		}
		return ifNeed
	}

	// // get min max data from Meta
	// datas, dataTypes, err := getZonemapDataFromMeta(columns, blkInfo, tableDef)
	// if err != nil || datas == nil {
	//  return true
	// }

	// // use all min/max data to build []vectors.
	// buildVectors := plan2.BuildVectorsByData(datas, dataTypes, proc.Mp())
	buildVectors, err := buildColumnsZMVectors(meta, int(blkInfo.MetaLocation().ID()), columns, tableDef, proc.Mp())
	if err != nil || len(buildVectors) == 0 {
		return true
	}
	bat := batch.NewWithSize(maxCol + 1)
	defer bat.Clean(proc.Mp())
	for k, v := range columnMap {
		for i, realIdx := range columns {
			if realIdx == v {
				bat.SetVector(int32(k), buildVectors[i])
				break
			}
		}
	}
	bat.SetZs(buildVectors[0].Length(), proc.Mp())

	ifNeed, err := plan2.EvalFilterExpr(notReportErrCtx, expr, bat, proc)
	if err != nil {
		return true
	}

	return ifNeed
}

func blockInfoMarshal(meta catalog.BlockInfo) []byte {
	sz := unsafe.Sizeof(meta)
	return unsafe.Slice((*byte)(unsafe.Pointer(&meta)), sz)
}

func BlockInfoUnmarshal(data []byte) *catalog.BlockInfo {
	return (*catalog.BlockInfo)(unsafe.Pointer(&data[0]))
}

/* used by multi-dn
func needSyncDnStores(ctx context.Context, expr *plan.Expr, tableDef *plan.TableDef,
	priKeys []*engine.Attribute, dnStores []DNStore, proc *process.Process) []int {
	var pk *engine.Attribute

	fullList := func() []int {
		dnList := make([]int, len(dnStores))
		for i := range dnStores {
			dnList[i] = i
		}
		return dnList
	}
	if len(dnStores) == 1 {
		return []int{0}
	}
	for _, key := range priKeys {
		// If it is a composite primary key, skip
		if key.Name == catalog.CPrimaryKeyColName {
			continue
		}
		pk = key
		break
	}
	// have no PrimaryKey, return all the list
	if expr == nil || pk == nil || tableDef == nil {
		return fullList()
	}
	if pk.Type.IsIntOrUint() {
		canComputeRange, intPkRange := computeRangeByIntPk(expr, pk.Name, "")
		if !canComputeRange {
			return fullList()
		}
		if intPkRange.isRange {
			r := intPkRange.ranges
			if r[0] == math.MinInt64 || r[1] == math.MaxInt64 || r[1]-r[0] > MAX_RANGE_SIZE {
				return fullList()
			}
			intPkRange.isRange = false
			for i := intPkRange.ranges[0]; i <= intPkRange.ranges[1]; i++ {
				intPkRange.items = append(intPkRange.items, i)
			}
		}
		return getListByItems(dnStores, intPkRange.items)
	}
	canComputeRange, hashVal := computeRangeByNonIntPk(ctx, expr, pk.Name, proc)
	if !canComputeRange {
		return fullList()
	}
	listLen := uint64(len(dnStores))
	idx := hashVal % listLen
	return []int{int(idx)}
}
*/
