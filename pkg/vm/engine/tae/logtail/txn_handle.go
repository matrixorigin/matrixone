// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"context"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

const (
	blkMetaInsBatch int8 = iota
	blkMetaDelBatch
	segMetaDelBatch
	dataInsBatch
	dataDelBatch
	dbInsBatch
	dbDelBatch
	tblInsBatch
	tblDelBatch
	columnInsBatch
	columnDelBatch
	batchTotalNum
)

type TxnLogtailRespBuilder struct {
	rt                        *dbutils.Runtime
	currDBName, currTableName string
	currDBID, currTableID     uint64
	txn                       txnif.AsyncTxn

	batches []*containers.Batch

	logtails *[]logtail.TableLogtail

	batchToClose []*containers.Batch
	insertBatch  *roaring.Bitmap
}

func NewTxnLogtailRespBuilder(rt *dbutils.Runtime) *TxnLogtailRespBuilder {
	logtails := make([]logtail.TableLogtail, 0)
	return &TxnLogtailRespBuilder{
		rt:           rt,
		batches:      make([]*containers.Batch, batchTotalNum),
		logtails:     &logtails,
		batchToClose: make([]*containers.Batch, 0),
		insertBatch:  roaring.New(),
	}
}

func (b *TxnLogtailRespBuilder) Close() {
	for i, bat := range b.batchToClose {
		if b.insertBatch.Contains(uint32(i)) {
			// b.closeInsertBatch(bat)
			bat.Close()
		} else {
			bat.Close()
		}
	}
}

func (b *TxnLogtailRespBuilder) CollectLogtail(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
	b.txn = txn
	txn.GetStore().ObserveTxn(
		b.visitDatabase,
		b.visitTable,
		b.rotateTable,
		b.visitMetadata,
		b.visitSegment,
		b.visitAppend,
		b.visitDelete)
	b.BuildResp()
	logtails := b.logtails
	newlogtails := make([]logtail.TableLogtail, 0)
	b.logtails = &newlogtails
	return logtails, b.Close
}

func (b *TxnLogtailRespBuilder) visitSegment(iseg any) {
	seg := iseg.(*catalog.SegmentEntry)
	node := seg.GetLatestNodeLocked()
	if !node.DeletedAt.Equal(txnif.UncommitTS) {
		return
	}

	deleteAt := b.txn.GetPrepareTS()
	rowid := segid2rowid(&seg.ID)

	if b.batches[segMetaDelBatch] == nil {
		b.batches[segMetaDelBatch] = makeRespBatchFromSchema(DelSchema)
	}

	b.batches[segMetaDelBatch].GetVectorByName(catalog.AttrRowID).Append(rowid, false)
	b.batches[segMetaDelBatch].GetVectorByName(catalog.AttrCommitTs).Append(deleteAt, false)
}

func (b *TxnLogtailRespBuilder) visitMetadata(iblk any) {
	blk := iblk.(*catalog.BlockEntry)
	node := blk.GetLatestNodeLocked()
	if node.BaseNode.MetaLoc.IsEmpty() {
		return
	}
	if b.batches[blkMetaInsBatch] == nil {
		b.batches[blkMetaInsBatch] = makeRespBatchFromSchema(BlkMetaSchema)
	}
	if b.batches[blkMetaDelBatch] == nil {
		b.batches[blkMetaDelBatch] = makeRespBatchFromSchema(DelSchema)
	}
	commitTS := b.txn.GetPrepareTS()
	createAt := node.CreatedAt
	if createAt.Equal(txnif.UncommitTS) {
		createAt = b.txn.GetPrepareTS()
	}
	deleteAt := node.DeletedAt
	if deleteAt.Equal(txnif.UncommitTS) {
		deleteAt = b.txn.GetPrepareTS()
	}
	visitBlkMeta(blk, node, b.batches[blkMetaInsBatch], b.batches[blkMetaDelBatch], node.DeletedAt.Equal(txnif.UncommitTS), commitTS, createAt, deleteAt)
}

func (b *TxnLogtailRespBuilder) visitAppend(ibat any) {
	src := ibat.(*containers.BatchWithVersion)
	// sort by seqnums
	sort.Sort(src)
	mybat := containers.NewBatchWithCapacity(int(src.NextSeqnum) + 2)
	mybat.AddVector(
		catalog.AttrRowID,
		src.GetVectorByName(catalog.AttrRowID).CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Transient),
	)
	tsType := types.T_TS.ToType()
	commitVec := b.rt.VectorPool.Transient.GetVector(&tsType)
	commitVec.PreExtend(src.Length())
	for i := 0; i < src.Length(); i++ {
		commitVec.Append(b.txn.GetPrepareTS(), false)
	}
	mybat.AddVector(catalog.AttrCommitTs, commitVec)

	for i, seqnum := range src.Seqnums {
		if seqnum >= objectio.SEQNUM_UPPER {
			continue
		}
		for len(mybat.Vecs) < 2+int(seqnum) {
			mybat.AppendPlaceholder()
		}
		vec := src.Vecs[i].CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Transient)
		mybat.AddVector(src.Attrs[i], vec)
	}

	if b.batches[dataInsBatch] == nil {
		b.batches[dataInsBatch] = mybat
	} else {
		b.batches[dataInsBatch].Extend(mybat)
		b.closeInsertBatch(mybat)
	}
}

// closeInsertBatch closes rowid and committs
// Other vectors are closed in localsegment
func (b *TxnLogtailRespBuilder) closeInsertBatch(bat *containers.Batch) {
	bat.Vecs[0].Close()
	bat.Vecs[1].Close()
}

func (b *TxnLogtailRespBuilder) visitDelete(ctx context.Context, vnode txnif.DeleteNode) {
	if b.batches[dataDelBatch] == nil {
		b.batches[dataDelBatch] = makeRespBatchFromSchema(DelSchema)
	}
	node := vnode.(*updates.DeleteNode)
	meta := node.GetMeta()
	pkDef := meta.GetSchema().GetPrimaryKey()
	deletes := node.GetRowMaskRefLocked()

	batch := b.batches[dataDelBatch]
	rowIDVec := batch.GetVectorByName(catalog.AttrRowID)
	commitTSVec := batch.GetVectorByName(catalog.AttrCommitTs)
	var pkVec containers.Vector
	if len(batch.Vecs) == 2 {
		pkVec = containers.MakeVector(pkDef.Type)
		batch.AddVector(pkDef.Name, pkVec)
	} else {
		pkVec = batch.GetVectorByName(pkDef.Name)
	}

	it := deletes.Iterator()
	dels := nulls.Nulls{}
	for it.HasNext() {
		del := it.Next()
		rowid := objectio.NewRowid(&meta.ID, del)
		rowIDVec.Append(*rowid, false)
		commitTSVec.Append(b.txn.GetPrepareTS(), false)
		dels.Add(uint64(del))
	}
	_ = meta.GetBlockData().Foreach(
		ctx,
		pkDef.Idx,
		func(v any, isNull bool, row int) error {
			pkVec.Append(v, false)
			return nil
		},
		&dels,
	)
}

func (b *TxnLogtailRespBuilder) visitTable(itbl any) {
	tbl := itbl.(*catalog.TableEntry)
	node := tbl.GetLatestNodeLocked()
	// delete table
	if node.DeletedAt.Equal(txnif.UncommitTS) {
		if b.batches[columnDelBatch] == nil {
			b.batches[columnDelBatch] = makeRespBatchFromSchema(DelSchema)
		}
		for _, usercol := range node.BaseNode.Schema.ColDefs {
			b.batches[columnDelBatch].GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tbl.ID, usercol.Name))), false)
			b.batches[columnDelBatch].GetVectorByName(catalog.AttrCommitTs).Append(b.txn.GetPrepareTS(), false)
		}
		if b.batches[tblDelBatch] == nil {
			b.batches[tblDelBatch] = makeRespBatchFromSchema(DelSchema)
		}
		catalogEntry2Batch(b.batches[tblDelBatch], tbl, node, DelSchema, txnimpl.FillTableRow, u64ToRowID(tbl.GetID()), b.txn.GetPrepareTS())
	}
	// create table
	if node.CreatedAt.Equal(txnif.UncommitTS) {
		if b.batches[columnInsBatch] == nil {
			b.batches[columnInsBatch] = makeRespBatchFromSchema(catalog.SystemColumnSchema)
		}
		for _, syscol := range catalog.SystemColumnSchema.ColDefs {
			txnimpl.FillColumnRow(tbl, node, syscol.Name, b.batches[columnInsBatch].GetVectorByName(syscol.Name))
		}
		for _, usercol := range node.BaseNode.Schema.ColDefs {
			b.batches[columnInsBatch].GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tbl.ID, usercol.Name))), false)
			b.batches[columnInsBatch].GetVectorByName(catalog.AttrCommitTs).Append(b.txn.GetPrepareTS(), false)
		}
		if b.batches[tblInsBatch] == nil {
			b.batches[tblInsBatch] = makeRespBatchFromSchema(catalog.SystemTableSchema)
		}
		catalogEntry2Batch(b.batches[tblInsBatch], tbl, node, catalog.SystemTableSchema, txnimpl.FillTableRow, u64ToRowID(tbl.GetID()), b.txn.GetPrepareTS())
	}
	// alter table
	if !node.CreatedAt.Equal(txnif.UncommitTS) && !node.DeletedAt.Equal(txnif.UncommitTS) {
		if b.batches[columnInsBatch] == nil {
			b.batches[columnInsBatch] = makeRespBatchFromSchema(catalog.SystemColumnSchema)
		}
		if b.batches[columnDelBatch] == nil {
			b.batches[columnDelBatch] = makeRespBatchFromSchema(DelSchema)
		}
		for _, syscol := range catalog.SystemColumnSchema.ColDefs {
			txnimpl.FillColumnRow(tbl, node, syscol.Name, b.batches[columnInsBatch].GetVectorByName(syscol.Name))
		}
		for _, usercol := range node.BaseNode.Schema.ColDefs {
			b.batches[columnInsBatch].GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tbl.ID, usercol.Name))), false)
			b.batches[columnInsBatch].GetVectorByName(catalog.AttrCommitTs).Append(b.txn.GetPrepareTS(), false)
		}
		for _, name := range node.BaseNode.Schema.Extra.DroppedAttrs {
			b.batches[columnDelBatch].GetVectorByName(catalog.AttrRowID).Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tbl.ID, name))), false)
			b.batches[columnDelBatch].GetVectorByName(catalog.AttrCommitTs).Append(b.txn.GetPrepareTS(), false)
		}
		if b.batches[tblInsBatch] == nil {
			b.batches[tblInsBatch] = makeRespBatchFromSchema(catalog.SystemTableSchema)
		}
		catalogEntry2Batch(b.batches[tblInsBatch], tbl, node, catalog.SystemTableSchema, txnimpl.FillTableRow, u64ToRowID(tbl.GetID()), b.txn.GetPrepareTS())
	}
}
func (b *TxnLogtailRespBuilder) visitDatabase(idb any) {
	db := idb.(*catalog.DBEntry)
	node := db.GetLatestNodeLocked()
	if node.DeletedAt.Equal(txnif.UncommitTS) {
		if b.batches[dbDelBatch] == nil {
			b.batches[dbDelBatch] = makeRespBatchFromSchema(DelSchema)
		}
		catalogEntry2Batch(b.batches[dbDelBatch], db, node, DelSchema, txnimpl.FillDBRow, u64ToRowID(db.GetID()), b.txn.GetPrepareTS())
	}
	if node.CreatedAt.Equal(txnif.UncommitTS) {
		if b.batches[dbInsBatch] == nil {
			b.batches[dbInsBatch] = makeRespBatchFromSchema(catalog.SystemDBSchema)
		}
		catalogEntry2Batch(b.batches[dbInsBatch], db, node, catalog.SystemDBSchema, txnimpl.FillDBRow, u64ToRowID(db.GetID()), b.txn.GetPrepareTS())
	}
}

func (b *TxnLogtailRespBuilder) buildLogtailEntry(tid, dbid uint64, tableName, dbName string, batchIdx int8, delete bool) {
	bat := b.batches[batchIdx]
	if bat == nil || bat.Length() == 0 {
		return
	}
	apiBat, err := containersBatchToProtoBatch(bat)
	logutil.Debugf("[logtail] from table %d-%s, delete %v, batch length %d @%s", tid, tableName, delete, bat.Length(), b.txn.GetPrepareTS().ToString())
	if err != nil {
		panic(err)
	}
	entryType := api.Entry_Insert
	if delete {
		entryType = api.Entry_Delete
	}
	entry := &api.Entry{
		EntryType:    entryType,
		TableId:      tid,
		TableName:    tableName,
		DatabaseId:   dbid,
		DatabaseName: dbName,
		Bat:          apiBat,
	}
	ts := b.txn.GetPrepareTS().ToTimestamp()
	tableID := &api.TableID{
		DbId: dbid,
		TbId: tid,
	}
	tail := logtail.TableLogtail{
		Ts:       &ts,
		Table:    tableID,
		Commands: []api.Entry{*entry},
	}
	*b.logtails = append(*b.logtails, tail)
}

func (b *TxnLogtailRespBuilder) rotateTable(dbName, tableName string, dbid, tid uint64) {
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_meta", b.currTableID), b.currDBName, blkMetaInsBatch, false)
	if b.batches[blkMetaInsBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[blkMetaInsBatch])
		b.batches[blkMetaInsBatch] = nil
	}
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_meta", b.currTableID), b.currDBName, blkMetaDelBatch, true)
	if b.batches[blkMetaDelBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[blkMetaDelBatch])
		b.batches[blkMetaDelBatch] = nil
	}
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_seg", b.currTableID), b.currDBName, segMetaDelBatch, true)
	if b.batches[segMetaDelBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[segMetaDelBatch])
		b.batches[segMetaDelBatch] = nil
	}
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataDelBatch, true)
	if b.batches[dataDelBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[dataDelBatch])
		b.batches[dataDelBatch] = nil
	}
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataInsBatch, false)
	if b.batches[dataInsBatch] != nil {
		b.insertBatch.Add(uint32(len(b.batchToClose)))
		b.batchToClose = append(b.batchToClose, b.batches[dataInsBatch])
		b.batches[dataInsBatch] = nil
	}
	b.currTableID = tid
	b.currDBID = dbid
	b.currTableName = tableName
	b.currDBName = dbName
}

func (b *TxnLogtailRespBuilder) BuildResp() {
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_meta", b.currTableID), b.currDBName, blkMetaInsBatch, false)
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_meta", b.currTableID), b.currDBName, blkMetaDelBatch, true)
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_seg", b.currTableID), b.currDBName, segMetaDelBatch, true)
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataDelBatch, true)
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataInsBatch, false)

	b.buildLogtailEntry(pkgcatalog.MO_TABLES_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_TABLES, pkgcatalog.MO_CATALOG, tblInsBatch, false)
	b.buildLogtailEntry(pkgcatalog.MO_TABLES_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_TABLES, pkgcatalog.MO_CATALOG, tblDelBatch, true)
	b.buildLogtailEntry(pkgcatalog.MO_COLUMNS_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_COLUMNS, pkgcatalog.MO_CATALOG, columnInsBatch, false)
	b.buildLogtailEntry(pkgcatalog.MO_COLUMNS_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_COLUMNS, pkgcatalog.MO_CATALOG, columnDelBatch, true)
	b.buildLogtailEntry(pkgcatalog.MO_DATABASE_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_DATABASE, pkgcatalog.MO_CATALOG, dbInsBatch, false)
	b.buildLogtailEntry(pkgcatalog.MO_DATABASE_ID, pkgcatalog.MO_CATALOG_ID, pkgcatalog.MO_DATABASE, pkgcatalog.MO_CATALOG, dbDelBatch, true)
	for i := range b.batches {
		if b.batches[i] != nil {
			if int8(i) == dataInsBatch {
				b.insertBatch.Add(uint32(len(b.batchToClose)))
			}
			b.batchToClose = append(b.batchToClose, b.batches[i])
			b.batches[i] = nil
		}
	}
}
