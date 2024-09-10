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
	"fmt"
	"sort"
	"time"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/RoaringBitmap/roaring"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	dataInsBatch int8 = iota
	dataDelBatch
	dbInsBatch
	dbDelBatch
	tblInsBatch
	tblDelBatch
	columnInsBatch
	columnDelBatch
	dataObjectInfoBatch
	tombstoneObjectInfoBatch
	batchTotalNum
)

type TxnLogtailRespBuilder struct {
	rt                        *dbutils.Runtime
	currDBName, currTableName string
	currDBID, currTableID     uint64
	currentAccID              uint32
	currentPkSeqnum           uint32
	txn                       txnif.AsyncTxn

	batches []*containers.Batch

	logtails       *[]logtail.TableLogtail
	currentLogtail *logtail.TableLogtail

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
	for _, bat := range b.batchToClose {
		bat.Close()
	}
}

func (b *TxnLogtailRespBuilder) CollectLogtail(txn txnif.AsyncTxn) (*[]logtail.TableLogtail, func()) {
	now := time.Now()
	defer func() {
		v2.LogTailPushCollectionDurationHistogram.Observe(time.Since(now).Seconds())
	}()

	b.txn = txn
	txn.GetStore().ObserveTxn(
		b.visitDatabase,
		b.visitTable,
		b.rotateTable,
		b.visitObject,
		b.visitAppend)
	b.BuildResp()
	logtails := b.logtails
	newlogtails := make([]logtail.TableLogtail, 0)
	b.logtails = &newlogtails
	return logtails, b.Close
}

func (b *TxnLogtailRespBuilder) visitObject(iobj any) {
	obj := iobj.(*catalog.ObjectEntry).GetLatestNode()
	node := obj.GetLastMVCCNode()
	var batchIdx int8
	if obj.IsTombstone {
		batchIdx = tombstoneObjectInfoBatch
	} else {
		batchIdx = dataObjectInfoBatch
	}
	if !obj.DeletedAt.Equal(&txnif.UncommitTS) {
		if b.batches[batchIdx] == nil {
			b.batches[batchIdx] = makeRespBatchFromSchema(ObjectInfoSchema, common.LogtailAllocator)
		}
		visitObject(b.batches[batchIdx], obj, node, true, true, b.txn.GetPrepareTS())
		return
	}

	if b.batches[batchIdx] == nil {
		b.batches[batchIdx] = makeRespBatchFromSchema(ObjectInfoSchema, common.LogtailAllocator)
	}
	visitObject(b.batches[batchIdx], obj, node, false, true, b.txn.GetPrepareTS())
}

func (b *TxnLogtailRespBuilder) visitAppend(ibat any, isTombstone bool) {
	src := ibat.(*containers.BatchWithVersion)
	// sort by seqnums
	if isTombstone {
		b.visitAppendTombstone(src)
	} else {
		b.visitAppendData(src)
	}
}
func (b *TxnLogtailRespBuilder) visitAppendTombstone(src *containers.BatchWithVersion) {

	mybat := containers.NewBatchWithCapacity(3)
	mybat.AddVector(
		catalog.AttrRowID,
		src.GetVectorByName(catalog.AttrRowID).CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Small),
	)
	tsType := types.T_TS.ToType()
	commitVec := b.rt.VectorPool.Small.GetVector(&tsType)
	commitVec.PreExtend(src.Length())
	for i := 0; i < src.Length(); i++ {
		commitVec.Append(b.txn.GetPrepareTS(), false)
	}
	mybat.AddVector(catalog.AttrCommitTs, commitVec)
	mybat.AddVector(
		catalog.AttrPKVal,
		src.GetVectorByName(catalog.AttrPKVal).CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Small),
	)
	mybat.AddVector(
		catalog.PhyAddrColumnName,
		src.GetVectorByName(catalog.PhyAddrColumnName).CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Small),
	)

	if b.batches[dataDelBatch] == nil {
		b.batches[dataDelBatch] = mybat
	} else {
		b.batches[dataDelBatch].Extend(mybat)
		mybat.Close()
	}
}
func (b *TxnLogtailRespBuilder) visitAppendData(src *containers.BatchWithVersion) {
	sort.Sort(src)
	mybat := containers.NewBatchWithCapacity(int(src.NextSeqnum) + 2)
	mybat.AddVector(
		catalog.PhyAddrColumnName,
		src.GetVectorByName(catalog.PhyAddrColumnName).CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Small),
	)
	tsType := types.T_TS.ToType()
	commitVec := b.rt.VectorPool.Small.GetVector(&tsType)
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
		vec := src.Vecs[i].CloneWindowWithPool(0, src.Length(), b.rt.VectorPool.Small)
		mybat.AddVector(src.Attrs[i], vec)
	}

	if b.batches[dataInsBatch] == nil {
		b.batches[dataInsBatch] = mybat
	} else {
		b.batches[dataInsBatch].Extend(mybat)
		mybat.Close()
	}
}

func (b *TxnLogtailRespBuilder) visitTable(itbl any) {
	/* push data change in mo_tables & mo_columns table */
}
func (b *TxnLogtailRespBuilder) visitDatabase(idb any) {
	/* push data change in mo_database table */
}

func (b *TxnLogtailRespBuilder) buildLogtailEntry(tid, dbid uint64, tableName, dbName string, batchIdx int8, delete bool) {
	bat := b.batches[batchIdx]
	if bat == nil || bat.Length() == 0 {
		return
	}
	// if tid == pkgcatalog.MO_DATABASE_ID || tid == pkgcatalog.MO_TABLES_ID || tid == pkgcatalog.MO_COLUMNS_ID {
	// 	logutil.Infof(
	// 		"yyyyyy txn logtail] from table %d-%s, is delete %v, batch %v @%s",
	// 		tid, tableName, delete, bat.PPString(5), b.txn.GetPrepareTS().ToString(),
	// 	)
	// }
	apiBat, err := containersBatchToProtoBatch(bat)
	common.DoIfDebugEnabled(func() {
		logutil.Debugf(
			"[logtail] from table %d-%s, delete %v, batch length %d @%s",
			tid, tableName, delete, bat.Length(), b.txn.GetPrepareTS().ToString(),
		)
	})
	if err != nil {
		panic(err)
	}
	entryType := api.Entry_Insert
	if delete {
		entryType = api.Entry_Delete
	}
	if b.txn.GetMemo().IsFlushOrMerge &&
		entryType == api.Entry_Delete &&
		tid <= pkgcatalog.MO_TABLES_ID &&
		tableName[0] != '_' /* noraml deletes in flush or merge for mo_database or mo_tables */ {
		tableName = fmt.Sprintf("trans_del-%v", tableName)
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
		AccId:         b.currentAccID,
		DbId:          dbid,
		TbId:          tid,
		DbName:        b.currDBName,
		TbName:        b.currTableName,
		PrimarySeqnum: b.currentPkSeqnum,
	}
	if b.currentLogtail == nil {
		b.currentLogtail = &logtail.TableLogtail{
			Ts:       &ts,
			Table:    tableID,
			Commands: make([]api.Entry, 0),
		}
	} else {
		if tid != b.currentLogtail.Table.TbId {
			*b.logtails = append(*b.logtails, *b.currentLogtail)
			b.currentLogtail = &logtail.TableLogtail{
				Ts:       &ts,
				Table:    tableID,
				Commands: make([]api.Entry, 0),
			}
		}
	}
	b.currentLogtail.Commands = append(b.currentLogtail.Commands, *entry)
}

func (b *TxnLogtailRespBuilder) rotateTable(aid uint32, dbName, tableName string, dbid, tid uint64, pkSeqnum uint16) {

	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_data_meta", b.currTableID), b.currDBName, dataObjectInfoBatch, false)
	if b.batches[dataObjectInfoBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[dataObjectInfoBatch])
		b.batches[dataObjectInfoBatch] = nil
	}
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_tombstone_meta", b.currTableID), b.currDBName, tombstoneObjectInfoBatch, false)
	if b.batches[tombstoneObjectInfoBatch] != nil {
		b.batchToClose = append(b.batchToClose, b.batches[tombstoneObjectInfoBatch])
		b.batches[tombstoneObjectInfoBatch] = nil
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
	b.currentAccID = aid
	b.currTableID = tid
	b.currDBID = dbid
	b.currTableName = tableName
	b.currDBName = dbName
	b.currentPkSeqnum = uint32(pkSeqnum)
}

func (b *TxnLogtailRespBuilder) BuildResp() {
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_data_meta", b.currTableID), b.currDBName, dataObjectInfoBatch, false)
	b.buildLogtailEntry(b.currTableID, b.currDBID, fmt.Sprintf("_%d_tombstone_meta", b.currTableID), b.currDBName, tombstoneObjectInfoBatch, false)
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataDelBatch, true)
	b.buildLogtailEntry(b.currTableID, b.currDBID, b.currTableName, b.currDBName, dataInsBatch, false)

	for i := range b.batches {
		if b.batches[i] != nil {
			if int8(i) == dataInsBatch {
				b.insertBatch.Add(uint32(len(b.batchToClose)))
			}
			b.batchToClose = append(b.batchToClose, b.batches[i])
			b.batches[i] = nil
		}
	}
	if b.currentLogtail != nil {
		*b.logtails = append(*b.logtails, *b.currentLogtail)
		b.currentLogtail = nil
	}
}
