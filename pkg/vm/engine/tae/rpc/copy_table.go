// Copyright 2021 - 2022 Matrix Origin
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

package rpc

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	CopyTableObjectListName = "object_list"
)

const (
	ObjectListAttr_ObjectType  = "object_type"
	ObjectListAttr_ID          = "id"
	ObjectListAttr_CreateTS    = "create_ts"
	ObjectListAttr_DeleteTS    = "delete_ts"
	ObjectListAttr_IsPersisted = "is_persisted"
)

const (
	ObjectListAttr_ObjectType_Idx = iota
	ObjectListAttr_ID_Idx
	ObjectListAttr_CreateTS_Idx
	ObjectListAttr_DeleteTS_Idx
	ObjectListAttr_IsPersisted_Idx
)

var ObjectListAttrs = []string{
	ObjectListAttr_ObjectType,
	ObjectListAttr_ID,
	ObjectListAttr_CreateTS,
	ObjectListAttr_DeleteTS,
	ObjectListAttr_IsPersisted,
}

var ObjectListTypes = []types.Type{
	types.T_int8.ToType(),
	types.T_varchar.ToType(),
	types.T_TS.ToType(),
	types.T_TS.ToType(),
	types.T_bool.ToType(),
}
var ObjectListSeqnums = []uint16{0, 1, 2, 3, 4}

func NewObjectListBatch() *batch.Batch {
	return batch.NewWithSchema(false, ObjectListAttrs, ObjectListTypes)
}

type CopyTableArg struct {
	ctx             context.Context
	txn             txnif.AsyncTxn
	table           *catalog.TableEntry
	dir             string
	inspectContext  *inspectContext
	objectListBatch *batch.Batch
	mp              *mpool.MPool
	fs              fileservice.FileService
}

func NewCopyTableArg(
	ctx context.Context,
	table *catalog.TableEntry,
	dir string,
	inspectContext *inspectContext,
	mp *mpool.MPool,
	fs fileservice.FileService,
) *CopyTableArg {
	return &CopyTableArg{
		ctx:             ctx,
		table:           table,
		dir:             dir,
		inspectContext:  inspectContext,
		objectListBatch: NewObjectListBatch(),
		mp:              mp,
		fs:              fs,
	}
}

func (c *CopyTableArg) Run() (err error) {
	txn, err := c.inspectContext.db.StartTxn(nil)
	if err != nil {
		return err
	}
	defer txn.Commit(c.ctx)

	p := &catalog.LoopProcessor{}
	p.ObjectFn = c.onObject
	p.TombstoneFn = c.onObject
	if err = c.table.RecurLoop(p); err != nil {
		return err
	}
	if err := c.flush(CopyTableObjectListName, c.objectListBatch); err != nil {
		return err
	}
	return nil
}

func (c *CopyTableArg) onObject(e *catalog.ObjectEntry) error {
	isPersisted, bat, err := c.visitObjectData(e)
	if err != nil {
		return err
	}
	if err := c.collectObjectList(e, isPersisted); err != nil {
		return err
	}
	cnBatch := containers.ToCNBatch(bat)
	objectName := objectio.BuildObjectNameWithObjectID(e.ID())
	if err := c.flush(objectName.String(), cnBatch); err != nil {
		return err
	}
	bat.Close()
	return nil
}

func (c *CopyTableArg) flush(name string, bat *batch.Batch) (err error) {
	nameWithDir := fmt.Sprintf("%s/%s", c.dir, name)
	writer, err := objectio.NewObjectWriterSpecial(objectio.WriterCopyTable, nameWithDir, c.fs)
	if err != nil {
		return
	}
	if _, err = writer.Write(bat); err != nil {
		return
	}
	_, err = writer.WriteEnd(c.ctx)
	if err != nil {
		return
	}
	return
}

func (c *CopyTableArg) collectObjectList(e *catalog.ObjectEntry, isPersisted bool) error {
	var objectType int8
	if e.IsTombstone {
		objectType = ckputil.ObjectType_Tombstone
	} else {
		objectType = ckputil.ObjectType_Data
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_ObjectType_Idx], objectType, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendBytes(
		c.objectListBatch.Vecs[ObjectListAttr_ID_Idx], []byte(e.ObjectStats[:]), false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_CreateTS_Idx], e.CreatedAt, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_DeleteTS_Idx], e.DeletedAt, false, c.mp,
	); err != nil {
		return err
	}
	if err := vector.AppendFixed(
		c.objectListBatch.Vecs[ObjectListAttr_IsPersisted_Idx], isPersisted, false, c.mp,
	); err != nil {
		return err
	}
	return nil
}

func (c *CopyTableArg) visitObjectData(e *catalog.ObjectEntry) (isPersisted bool, bat *containers.Batch, err error) {
	batches := make(map[uint32]*containers.BatchWithVersion)
	if err = e.GetObjectData().ScanInMemory(
		c.ctx, batches, types.TS{}, c.txn.GetStartTS(), c.mp,
	); err != nil {
		return
	}
	if len(batches) != 0 {
		return false, batches[e.GetTable().GetLastestSchema(e.IsTombstone).Version].Batch, nil
	}
	isPersisted = true
	schema := e.GetTable().GetLastestSchema(e.IsTombstone)
	colIdxes := make([]int, 0)
	// user rows, rowID, commitTS
	for i := range schema.ColDefs {
		colIdxes = append(colIdxes, i)
	}
	colIdxes = append(colIdxes, objectio.SEQNUM_COMMITTS)
	if err = e.GetObjectData().Scan(c.ctx, &bat, c.txn, schema, 0, colIdxes, c.mp); err != nil {
		return
	}
	return
}
