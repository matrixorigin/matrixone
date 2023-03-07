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
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage/memorytable"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (txn *Transaction) getTableMeta(
	ctx context.Context,
	databaseId uint64,
	tableId uint64,
	needUpdated bool,
	columnLength int,
	prefetch bool,
) (*tableMeta, error) {
	blocks := make([][]BlockMeta, len(txn.dnStores))
	name := genMetaTableName(tableId)

	ts := types.TimestampToTS(txn.meta.SnapshotTS)
	if needUpdated {
		key := [2]uint64{databaseId, tableId}
		states := txn.engine.partitions[key].Snapshot()

		for i := range txn.dnStores {
			if i >= len(states) {
				continue
			}

			var blockInfos []catalog.BlockInfo
			state := states[i]
			iter := state.Blocks.Iter()
			for ok := iter.First(); ok; ok = iter.Next() {
				entry := iter.Item()
				if !entry.Visible(ts) {
					continue
				}
				blockInfos = append(blockInfos, entry.BlockInfo)
			}
			iter.Release()

			var err error
			blocks[i], err = genBlockMetas(ctx, blockInfos, columnLength, txn.proc.FileService,
				txn.proc.GetMPool(), prefetch)
			if err != nil {
				return nil, moerr.NewInternalError(ctx, "disttae: getTableMeta err: %v, table: %v", err.Error(), name)
			}

		}
	}

	return &tableMeta{
		tableName: name,
		blocks:    blocks,
		defs:      catalog.MoTableMetaDefs,
	}, nil
}

// detecting whether a transaction is a read-only transaction
func (txn *Transaction) ReadOnly() bool {
	return txn.readOnly
}

// use for solving halloween problem
func (txn *Transaction) IncStatementId() {
	txn.statementId++
	txn.writes = append(txn.writes, make([]Entry, 0, 1))
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
		len := bat.Length()
		vec := vector.New(types.New(types.T_Rowid, 0, 0))
		for i := 0; i < len; i++ {
			if err := vec.Append(txn.genRowId(), false,
				txn.proc.Mp()); err != nil {
				return err
			}
		}
		bat.Vecs = append([]*vector.Vector{vec}, bat.Vecs...)
		bat.Attrs = append([]string{catalog.Row_ID}, bat.Attrs...)
		txn.workspaceSize += uint64(bat.Size())
	}
	txn.Lock()
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		bat:          bat,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		dnStore:      dnStore,
	})
	txn.Unlock()

	if err := txn.checkPrimaryKey(typ, primaryIdx, bat, tableName, tableId); err != nil {
		return err
	}

	txn.DumpBatch(false)

	return nil
}

func (txn *Transaction) DumpBatch(force bool) error {
	// if txn.workspaceSize >= colexec.WriteS3Threshold {
	if txn.workspaceSize >= colexec.WriteS3Threshold || force && txn.workspaceSize >= colexec.TagS3Size {
		mp := make(map[[2]string][]*batch.Batch)
		for i := 0; i < len(txn.writes); i++ {
			idx := -1
			for j := 0; j < len(txn.writes[i]); j++ {
				if txn.writes[i][j].typ == INSERT && txn.writes[i][j].fileName == "" {
					key := [2]string{txn.writes[i][j].databaseName, txn.writes[i][j].tableName}
					bat := txn.writes[i][j].bat
					// skip rowid
					bat.Attrs = bat.Attrs[1:]
					bat.Vecs = bat.Vecs[1:]
					mp[key] = append(mp[key], bat)
				} else {
					txn.writes[i][idx+1] = txn.writes[i][j]
					idx++
				}
			}
			txn.writes[i] = txn.writes[i][:idx+1]
		}
		for key := range mp {
			container, tbl, err := txn.getContainer(key)
			if err != nil {
				return err
			}
			container.InitBuffers(mp[key][0], 0)
			for i := 0; i < len(mp[key]); i++ {
				container.Put(mp[key][i], 0)
			}
			container.MergeBlock(0, len(mp[key]), txn.proc, false)
			metaLoc := container.GetMetaLocBat()

			lenVecs := len(metaLoc.Attrs)
			// only remain the metaLoc col
			metaLoc.Vecs = metaLoc.Vecs[lenVecs-1:]
			metaLoc.Attrs = metaLoc.Attrs[lenVecs-1:]
			metaLoc.SetZs(metaLoc.Vecs[0].Length(), txn.proc.GetMPool())
			err = tbl.Write(txn.proc.Ctx, metaLoc)
			if err != nil {
				return err
			}
		}
		txn.workspaceSize = 0
	}
	return nil
}

func (txn *Transaction) getContainer(key [2]string) (*colexec.WriteS3Container, engine.Relation, error) {
	sortIdx, attrs, tbl, err := txn.getSortIdx(key)
	if err != nil {
		return nil, nil, err
	}
	container := &colexec.WriteS3Container{}
	container.Init(1)
	container.SetMp(attrs)
	if sortIdx != -1 {
		container.AddSortIdx(sortIdx)
	}
	return container, tbl, nil
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
		if attrs[i].ClusterBy || attrs[i].Primary {
			return i, attrs, tbl, err
		}
	}
	return -1, attrs, tbl, nil
}

func (txn *Transaction) checkPrimaryKey(
	typ int,
	primaryIdx int,
	bat *batch.Batch,
	tableName string,
	tableId uint64,
) error {

	// no primary key
	if primaryIdx < 0 {
		return nil
	}

	//TODO ignore these buggy auto incr tables for now
	if strings.Contains(tableName, "%!%mo_increment") {
		return nil
	}

	t := txn.nextLocalTS()
	tx := memorytable.NewTransaction(t)
	iter := memorytable.NewBatchIter(bat)
	for {
		tuple := iter()
		if len(tuple) == 0 {
			break
		}

		rowID := RowID(tuple[0].Value.(types.Rowid))

		switch typ {

		case INSERT:
			var indexes []memorytable.Tuple

			idx := primaryIdx + 1 // skip the first row id column
			primaryKey := memorytable.ToOrdered(tuple[idx].Value)
			index := memorytable.Tuple{
				index_TableID_PrimaryKey,
				memorytable.ToOrdered(tableId),
				primaryKey,
			}

			// check primary key
			entries, err := txn.workspace.Index(tx, index)
			if err != nil {
				return err
			}
			if len(entries) > 0 {
				return moerr.NewDuplicateEntry(
					txn.proc.Ctx,
					common.TypeStringValue(bat.Vecs[idx].Typ, tuple[idx].Value),
					bat.Attrs[idx],
				)
			}

			// add primary key
			indexes = append(indexes, index)

			row := &workspaceRow{
				rowID:   rowID,
				tableID: tableId,
				indexes: indexes,
			}
			err = txn.workspace.Insert(tx, row)
			if err != nil {
				return err
			}

		case DELETE:
			err := txn.workspace.Delete(tx, rowID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}

		}
	}
	if err := tx.Commit(t); err != nil {
		return err
	}

	return nil
}

func (txn *Transaction) nextLocalTS() timestamp.Timestamp {
	txn.localTS = txn.localTS.Next()
	return txn.localTS
}

// WriteFile used to add a s3 file information to the transaction buffer
// insert/delete/update all use this api
func (txn *Transaction) WriteFile(typ int, databaseId, tableId uint64,
	databaseName, tableName string, fileName string, bat *batch.Batch, dnStore DNStore) error {
	txn.readOnly = false
	txn.writes[txn.statementId] = append(txn.writes[txn.statementId], Entry{
		typ:          typ,
		tableId:      tableId,
		databaseId:   databaseId,
		tableName:    tableName,
		databaseName: databaseName,
		fileName:     fileName,
		bat:          bat,
		dnStore:      dnStore,
	})
	return nil
}

func (txn *Transaction) deleteBatch(bat *batch.Batch,
	databaseId, tableId uint64) *batch.Batch {

	// tx for workspace operations
	t := txn.nextLocalTS()
	tx := memorytable.NewTransaction(t)
	defer func() {
		if err := tx.Commit(t); err != nil {
			panic(err)
		}
	}()

	mp := make(map[types.Rowid]uint8)
	rowids := vector.MustTCols[types.Rowid](bat.GetVector(0))
	for _, rowid := range rowids {
		mp[rowid] = 0
		// update workspace
		err := txn.workspace.Delete(tx, RowID(rowid))
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			panic(err)
		}
	}

	sels := txn.proc.Mp().GetSels()
	for i := range txn.writes {
		for j, e := range txn.writes[i] {
			sels = sels[:0]
			if e.tableId == tableId && e.databaseId == databaseId {
				vs := vector.MustTCols[types.Rowid](e.bat.GetVector(0))
				for k, v := range vs {
					if _, ok := mp[v]; !ok {
						sels = append(sels, int64(k))
					} else {
						mp[v]++
					}
				}
				if len(sels) != len(vs) {
					txn.writes[i][j].bat.Shrink(sels)
				}
			}
		}
	}
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

func (txn *Transaction) genRowId() types.Rowid {
	txn.rowId[1]++
	return types.DecodeFixed[types.Rowid](types.EncodeSlice(txn.rowId[:]))
}

// needRead determine if a block needs to be read
func needRead(ctx context.Context, expr *plan.Expr, blkInfo BlockMeta, tableDef *plan.TableDef, columnMap map[int]int, columns []int, maxCol int, proc *process.Process) bool {
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

	// get min max data from Meta
	datas, dataTypes, err := getZonemapDataFromMeta(ctx, columns, blkInfo, tableDef)
	if err != nil || datas == nil {
		return true
	}

	// use all min/max data to build []vectors.
	buildVectors := plan2.BuildVectorsByData(datas, dataTypes, proc.Mp())
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

// get row count of block
func blockRows(meta BlockMeta) int64 {
	return meta.Rows
}

func blockMarshal(meta BlockMeta) []byte {
	data, _ := types.Encode(meta)
	return data
}

func blockUnmarshal(data []byte) BlockMeta {
	var meta BlockMeta

	types.Decode(data, &meta)
	return meta
}

// write a block to s3
func blockWrite(ctx context.Context, bat *batch.Batch, fs fileservice.FileService) ([]objectio.BlockObject, error) {
	// 1. write bat
	accountId, _, _ := getAccessInfo(ctx)
	s3FileName, err := getNewBlockName(accountId)
	if err != nil {
		return nil, err
	}
	writer, err := objectio.NewObjectWriter(s3FileName, fs)
	if err != nil {
		return nil, err
	}
	fd, err := writer.Write(bat)
	if err != nil {
		return nil, err
	}

	// 2. write index (index and zonemap)
	for i, vec := range bat.Vecs {
		bloomFilter, zoneMap, err := getIndexDataFromVec(uint16(i), vec)
		if err != nil {
			return nil, err
		}
		if bloomFilter != nil {
			err = writer.WriteIndex(fd, bloomFilter)
			if err != nil {
				return nil, err
			}
		}
		if zoneMap != nil {
			err = writer.WriteIndex(fd, zoneMap)
			if err != nil {
				return nil, err
			}
		}
	}

	// 3. get return
	return writer.WriteEnd(ctx)
}

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
