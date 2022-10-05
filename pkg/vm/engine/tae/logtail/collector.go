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
	"encoding/binary"
	"fmt"
	"hash/fnv"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
)

// LogtailCollector holds a read only reader, knows how to iter entry.
// Drive a entry visitor, which acts as an api resp builder
type LogtailCollector struct {
	scope   Scope
	did     uint64
	tid     uint64
	catalog *catalog.Catalog
	reader  *LogtailReader
	visitor catalog.Processor
}

// BindInfo set collect env args and these args don't affect dirty seg/blk ids
func (c *LogtailCollector) BindCollectEnv(scope Scope, catalog *catalog.Catalog, visitor catalog.Processor) {
	c.scope, c.catalog, c.visitor = scope, catalog, visitor
}

func (c *LogtailCollector) Collect() error {
	switch c.scope {
	case ScopeDatabases:
		return c.collectCatalogDB()
	case ScopeTables, ScopeColumns:
		return c.collectCatalogTbl()
	case ScopeUserTables:
		return c.collectUserTbl()
	default:
		panic("unknown logtail collect scope")
	}
}

func (c *LogtailCollector) collectUserTbl() (err error) {
	var (
		db  *catalog.DBEntry
		tbl *catalog.TableEntry
		seg *catalog.SegmentEntry
		blk *catalog.BlockEntry
	)
	if db, err = c.catalog.GetDatabaseByID(c.did); err != nil {
		return
	}
	if tbl, err = db.GetTableEntryByID(c.tid); err != nil {
		return
	}
	dirty := c.reader.GetDirtyByTable(c.tid)
	for _, dirtySeg := range dirty.Segs {
		if seg, err = tbl.GetSegmentByID(dirtySeg.Sig); err != nil {
			return err
		}
		if err = c.visitor.OnSegment(seg); err != nil {
			return err
		}
		for _, blkid := range dirtySeg.Blks {
			if blk, err = seg.GetBlockEntryByID(blkid); err != nil {
				return err
			}
			if err = c.visitor.OnBlock(blk); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *LogtailCollector) collectCatalogDB() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		dbentry := dbIt.Get().GetPayload()
		if dbentry.IsSystemDB() {
			continue
		}
		if err := c.visitor.OnDatabase(dbentry); err != nil {
			return err
		}
	}
	return nil
}

func (c *LogtailCollector) collectCatalogTbl() error {
	if !c.reader.HasCatalogChanges() {
		return nil
	}
	dbIt := c.catalog.MakeDBIt(true)
	for ; dbIt.Valid(); dbIt.Next() {
		db := dbIt.Get().GetPayload()
		if db.IsSystemDB() {
			continue
		}
		tblIt := db.MakeTableIt(true)
		for ; tblIt.Valid(); tblIt.Next() {
			if err := c.visitor.OnTable(tblIt.Get().GetPayload()); err != nil {
				return err
			}
		}
	}
	return nil
}

type RespBuilder interface {
	catalog.Processor
	BuildResp() (api.SyncLogTailResp, error)
}

// CatalogLogtailRespBuilder knows how to make api-entry from catalog entry
// impl catalog.Processor interface, driven by LogtailCollector
type CatalogLogtailRespBuilder struct {
	*catalog.LoopProcessor
	scope      Scope
	start, end types.TS
	checkpoint string
	insBatch   *containers.Batch
	delBatch   *containers.Batch
}

func NewCatalogLogtailRespBuilder(scope Scope, ckp string, start, end types.TS) *CatalogLogtailRespBuilder {
	b := &CatalogLogtailRespBuilder{
		LoopProcessor: new(catalog.LoopProcessor),
		scope:         scope,
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	switch scope {
	case ScopeDatabases:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemDBSchema)
	case ScopeTables:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemTableSchema)
	case ScopeColumns:
		b.insBatch = makeRespBatchFromSchema(catalog.SystemColumnSchema)
	}
	b.delBatch = makeRespBatchFromSchema(DelSchema)
	b.DatabaseFn = b.VisitDB
	b.TableFn = b.VisitTbl

	return b
}

func (b *CatalogLogtailRespBuilder) VisitDB(entry *catalog.DBEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		dbNode := node.(*catalog.DBMVCCNode)
		if dbNode.HasDropped() {
			// delScehma is empty, it will just fill rowid / commit ts / abort
			catalogEntry2Batch(b.delBatch, entry, DelSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd(), dbNode.IsAborted())
		} else {
			catalogEntry2Batch(b.insBatch, entry, catalog.SystemDBSchema, txnimpl.FillDBRow, u64ToRowID(entry.GetID()), dbNode.GetEnd(), dbNode.IsAborted())
		}
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) VisitTbl(entry *catalog.TableEntry) error {
	entry.RLock()
	mvccNodes := entry.ClonePreparedInRange(b.start, b.end)
	entry.RUnlock()
	for _, node := range mvccNodes {
		tblNode := node.(*catalog.TableMVCCNode)
		if b.scope == ScopeColumns {
			var dstBatch *containers.Batch
			if !tblNode.HasDropped() {
				dstBatch = b.insBatch
				// fill unique syscol fields if inserting
				for _, syscol := range catalog.SystemColumnSchema.ColDefs {
					txnimpl.FillColumnRow(entry, syscol.Name, b.insBatch.GetVectorByName(syscol.Name))
				}
			} else {
				dstBatch = b.delBatch
			}

			// fill common syscol fields for every user column
			rowidVec := dstBatch.GetVectorByName(catalog.AttrRowID)
			commitVec := dstBatch.GetVectorByName(catalog.AttrCommitTs)
			abortVec := dstBatch.GetVectorByName(catalog.AttrAborted)
			tableID := entry.GetID()
			commitTs, aborted := tblNode.GetEnd(), tblNode.IsAborted()
			for _, usercol := range entry.GetSchema().ColDefs {
				rowidVec.Append(bytesToRowID([]byte(fmt.Sprintf("%d-%s", tableID, usercol.Name))))
				commitVec.Append(commitTs)
				abortVec.Append(aborted)
			}
		} else {
			if tblNode.HasDropped() {
				catalogEntry2Batch(b.delBatch, entry, DelSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd(), tblNode.IsAborted())
			} else {
				catalogEntry2Batch(b.insBatch, entry, catalog.SystemTableSchema, txnimpl.FillTableRow, u64ToRowID(entry.GetID()), tblNode.GetEnd(), tblNode.IsAborted())
			}
		}
	}
	return nil
}

func (b *CatalogLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	var tblID uint64
	var tblName string
	switch b.scope {
	case ScopeDatabases:
		tblID = pkgcatalog.MO_DATABASE_ID
		tblName = pkgcatalog.MO_DATABASE
	case ScopeTables:
		tblID = pkgcatalog.MO_TABLES_ID
		tblName = pkgcatalog.MO_TABLES
	case ScopeColumns:
		tblID = pkgcatalog.MO_COLUMNS_ID
		tblName = pkgcatalog.MO_COLUMNS
	}

	if b.insBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.insBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		insEntry := &api.Entry{
			EntryType:    api.Entry_Insert,
			TableId:      tblID,
			TableName:    tblName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, insEntry)
	}
	if b.delBatch.Length() > 0 {
		bat, err := containersBatchToProtoBatch(b.delBatch)
		if err != nil {
			return api.SyncLogTailResp{}, err
		}
		delEntry := &api.Entry{
			EntryType:    api.Entry_Delete,
			TableId:      tblID,
			TableName:    tblName,
			DatabaseId:   pkgcatalog.MO_CATALOG_ID,
			DatabaseName: pkgcatalog.MO_CATALOG,
			Bat:          bat,
		}
		entries = append(entries, delEntry)
	}
	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}

// this is used to collect ONE ROW of db or table change
func catalogEntry2Batch[T *catalog.DBEntry | *catalog.TableEntry](
	dstBatch *containers.Batch,
	e T,
	schema *catalog.Schema,
	fillDataRow func(e T, attr string, col containers.Vector),
	rowid types.Rowid,
	commitTs types.TS,
	aborted bool,
) {
	for _, col := range schema.ColDefs {
		fillDataRow(e, col.Name, dstBatch.GetVectorByName(col.Name))
	}
	dstBatch.GetVectorByName(catalog.AttrRowID).Append(rowid)
	dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(commitTs)
	dstBatch.GetVectorByName(catalog.AttrAborted).Append(aborted)
}

func u64ToRowID(v uint64) types.Rowid {
	var rowid types.Rowid
	binary.BigEndian.PutUint64(rowid[:8], v)
	return rowid
}

func bytesToRowID(bs []byte) types.Rowid {
	var rowid types.Rowid
	if size := len(bs); size <= types.RowidSize {
		copy(rowid[:size], bs[:size])
	} else {
		hasher := fnv.New128()
		hasher.Write(bs)
		hasher.Sum(rowid[:0])
	}
	return rowid
}

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	batch := containers.NewBatch()

	batch.AddVector(catalog.AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), false))
	batch.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), false))
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	nullables := schema.AllNullables()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		batch.AddVector(attr, containers.MakeVector(typs[i], nullables[i]))
	}
	batch.AddVector(catalog.AttrAborted, containers.MakeVector(types.T_bool.ToType(), false))
	return batch
}

// consume containers.Batch to construct api batch
func containersBatchToProtoBatch(batch *containers.Batch) (*api.Batch, error) {
	protoBatch := &api.Batch{
		Attrs: batch.Attrs,
		Vecs:  make([]*api.Vector, len(batch.Attrs)),
	}
	vecs := containers.CopyToMoVecs(batch.Vecs)
	for i, vec := range vecs {
		apivec, err := vector.VectorToProtoVector(vec)
		if err != nil {
			return nil, err
		}
		protoBatch.Vecs[i] = apivec
	}
	batch.Close()
	return protoBatch, nil
}

type TableLogtailRespBuilder struct {
	*catalog.LoopProcessor
	start, end      types.TS
	did, tid        uint64
	dname, tname    string
	checkpoint      string
	blkMetaInsBatch *containers.Batch
	blkMetaDelBatch *containers.Batch
	dataInsBatch    *containers.Batch
	dataDelBatch    *containers.Batch
}

func NewTableLogtailRespBuilder(ckp string, start, end types.TS, tbl *catalog.TableEntry) *TableLogtailRespBuilder {
	b := &TableLogtailRespBuilder{
		LoopProcessor: new(catalog.LoopProcessor),
		start:         start,
		end:           end,
		checkpoint:    ckp,
	}
	b.BlockFn = b.VisitBlk

	schema := tbl.GetSchema()

	b.did = tbl.GetDB().GetID()
	b.tid = tbl.ID
	b.dname = tbl.GetDB().GetName()
	b.tname = schema.Name

	b.dataInsBatch = makeRespBatchFromSchema(schema)
	b.dataDelBatch = makeRespBatchFromSchema(DelSchema)
	b.blkMetaInsBatch = makeRespBatchFromSchema(BlkMetaSchema)
	b.blkMetaDelBatch = makeRespBatchFromSchema(DelSchema)
	return b
}

func (b *TableLogtailRespBuilder) visitBlkMeta(e *catalog.BlockEntry) {
	e.RLock()
	mvccNodes := e.ClonePreparedInRange(b.start, b.end)
	e.RUnlock()
	for _, node := range mvccNodes {
		metaNode := node.(*catalog.MetadataMVCCNode)
		var dstBatch *containers.Batch
		if !metaNode.HasDropped() {
			dstBatch = b.blkMetaInsBatch
			dstBatch.GetVectorByName(blkMetaAttrBlockID).Append(e.ID)
			dstBatch.GetVectorByName(blkMetaAttrEntryState).Append(e.IsAppendable())
			dstBatch.GetVectorByName(blkMetaAttrCreateAt).Append(metaNode.CreatedAt)
			dstBatch.GetVectorByName(blkMetaAttrDeleteAt).Append(metaNode.DeletedAt)
			dstBatch.GetVectorByName(blkMetaAttrMetaLoc).Append([]byte(metaNode.MetaLoc))
			dstBatch.GetVectorByName(blkMetaAttrDeltaLoc).Append([]byte(metaNode.DeltaLoc))
		} else {
			dstBatch = b.blkMetaDelBatch
		}

		dstBatch.GetVectorByName(catalog.AttrRowID).Append(u64ToRowID(e.ID))
		dstBatch.GetVectorByName(catalog.AttrCommitTs).Append(metaNode.GetEnd())
		dstBatch.GetVectorByName(catalog.AttrAborted).Append(metaNode.IsAborted())
	}
}

func (b *TableLogtailRespBuilder) visitBlkData(e *catalog.BlockEntry) (err error) {
	block := e.GetBlockData()
	insBatch, err := block.CollectAppendInRange(b.start, b.end)
	if err != nil {
		return
	}
	if insBatch != nil && insBatch.Length() > 0 {
		b.dataInsBatch.GetVectorByName(catalog.AttrRowID).Extend(insBatch.GetVectorByName(catalog.PhyAddrColumnName))
		b.dataInsBatch.Extend(insBatch)
		// insBatch is freed, don't use anymore
	}
	delBatch, err := block.CollectDeleteInRange(b.start, b.end)
	if err != nil {
		return
	}
	if delBatch != nil && delBatch.Length() > 0 {
		b.dataDelBatch.GetVectorByName(catalog.AttrRowID).Extend(delBatch.GetVectorByName(catalog.PhyAddrColumnName))
		b.dataDelBatch.Extend(delBatch)
		// delBatch is freed, don't use anymore
	}
	return nil
}

func (b *TableLogtailRespBuilder) VisitBlk(entry *catalog.BlockEntry) error {
	b.visitBlkMeta(entry)
	return b.visitBlkData(entry)
}

func (b *TableLogtailRespBuilder) BuildResp() (api.SyncLogTailResp, error) {
	entries := make([]*api.Entry, 0)
	tryAppendEntry := func(typ api.Entry_EntryType, metaChange bool, batch *containers.Batch) error {
		if batch.Length() == 0 {
			return nil
		}
		bat, err := containersBatchToProtoBatch(batch)
		if err != nil {
			return err
		}

		blockID := uint64(0)
		tableName := b.tname
		if metaChange {
			blockID = 1 // make metadata change stand out, just a flag
			tableName = fmt.Sprintf("_%d_%s", b.tid, b.tname)
		}
		entry := &api.Entry{
			EntryType:    typ,
			TableId:      b.tid,
			TableName:    tableName,
			DatabaseId:   b.did,
			DatabaseName: b.dname,
			BlockId:      blockID,
			Bat:          bat,
		}
		entries = append(entries, entry)
		return nil
	}

	empty := api.SyncLogTailResp{}
	if err := tryAppendEntry(api.Entry_Insert, true, b.blkMetaInsBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, true, b.blkMetaDelBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Insert, false, b.dataInsBatch); err != nil {
		return empty, err
	}
	if err := tryAppendEntry(api.Entry_Delete, false, b.dataDelBatch); err != nil {
		return empty, err
	}

	return api.SyncLogTailResp{
		CkpLocation: b.checkpoint,
		Commands:    entries,
	}, nil
}
