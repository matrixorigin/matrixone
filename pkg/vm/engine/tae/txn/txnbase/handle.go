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

package txnbase

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	apipb "github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type TxnDatabase struct {
	Txn txnif.AsyncTxn
}

type TxnRelation struct {
	Txn txnif.AsyncTxn
	DB  handle.Database
}

type TxnObject struct {
	Txn txnif.AsyncTxn
	Rel handle.Relation
}

type TxnBlock struct {
	Txn txnif.AsyncTxn
	Seg handle.Object
}

var _ handle.Relation = &TxnRelation{}

func (db *TxnDatabase) GetID() uint64                                                   { return 0 }
func (db *TxnDatabase) GetName() string                                                 { return "" }
func (db *TxnDatabase) String() string                                                  { return "" }
func (db *TxnDatabase) Close() error                                                    { return nil }
func (db *TxnDatabase) CreateRelation(def any) (rel handle.Relation, err error)         { return }
func (db *TxnDatabase) DropRelationByName(name string) (rel handle.Relation, err error) { return }
func (db *TxnDatabase) GetRelationByName(name string) (rel handle.Relation, err error)  { return }
func (db *TxnDatabase) UnsafeGetRelation(id uint64) (rel handle.Relation, err error)    { return }
func (db *TxnDatabase) RelationCnt() int64                                              { return 0 }
func (db *TxnDatabase) Relations() (rels []handle.Relation)                             { return }
func (db *TxnDatabase) MakeRelationIt() (it handle.RelationIt)                          { return }
func (db *TxnDatabase) GetMeta() any                                                    { return nil }

func (rel *TxnRelation) SimplePPString(_ common.PPLevel) string                   { return "" }
func (rel *TxnRelation) String() string                                           { return "" }
func (rel *TxnRelation) Close() error                                             { return nil }
func (rel *TxnRelation) ID() uint64                                               { return 0 }
func (rel *TxnRelation) Rows() int64                                              { return 0 }
func (rel *TxnRelation) Size(attr string) int64                                   { return 0 }
func (rel *TxnRelation) GetCardinality(attr string) int64                         { return 0 }
func (rel *TxnRelation) Schema(bool) any                                          { return nil }
func (rel *TxnRelation) MakeObjectIt(bool) handle.ObjectIt                        { return nil }
func (rel *TxnRelation) MakeObjectItOnSnap(bool) handle.ObjectIt                  { return nil }
func (rel *TxnRelation) BatchDedup(containers.Vector) error                       { return nil }
func (rel *TxnRelation) Append(ctx context.Context, data *containers.Batch) error { return nil }
func (rel *TxnRelation) AddObjsWithMetaLoc(context.Context, containers.Vector) error {
	return nil
}
func (rel *TxnRelation) GetMeta() any                    { return nil }
func (rel *TxnRelation) GetDB() (handle.Database, error) { return nil, nil }
func (rel *TxnRelation) GetObject(id *types.Objectid, isTombstone bool) (obj handle.Object, err error) {
	return
}
func (rel *TxnRelation) SoftDeleteObject(id *types.Objectid, isTombstone bool) (err error) { return }
func (rel *TxnRelation) CreateObject(bool) (obj handle.Object, err error)                  { return }
func (rel *TxnRelation) CreateNonAppendableObject(bool, *objectio.CreateObjOpt) (obj handle.Object, err error) {
	return
}
func (rel *TxnRelation) GetValue(*common.ID, uint32, uint16, bool) (v any, isNull bool, err error) {
	return
}
func (rel *TxnRelation) GetValueByPhyAddrKey(any, int) (v any, isNull bool, err error) {
	return
}
func (rel *TxnRelation) Update(*common.ID, uint32, uint16, any, bool) (err error)             { return }
func (rel *TxnRelation) DeleteByPhyAddrKey(any) (err error)                                   { return }
func (rel *TxnRelation) DeleteByPhyAddrKeys(containers.Vector, containers.Vector) (err error) { return }
func (rel *TxnRelation) RangeDelete(*common.ID, uint32, uint32, handle.DeleteType) (err error) {
	return
}
func (rel *TxnRelation) TryDeleteByDeltaloc(id *common.ID, deltaloc objectio.Location) (ok bool, err error) {
	return
}
func (rel *TxnRelation) GetByFilter(context.Context, *handle.Filter) (id *common.ID, offset uint32, err error) {
	return
}
func (rel *TxnRelation) GetValueByFilter(context.Context, *handle.Filter, int) (v any, isNull bool, err error) {
	return
}
func (rel *TxnRelation) UpdateByFilter(ctx context.Context, filter *handle.Filter, col uint16, v any, isNull bool) (err error) {
	return
}
func (rel *TxnRelation) DeleteByFilter(ctx context.Context, filter *handle.Filter) (err error) {
	return
}
func (rel *TxnRelation) LogTxnEntry(entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return
}
func (rel *TxnRelation) AlterTable(context.Context, *apipb.AlterTableReq) (err error) { return }
func (rel *TxnRelation) FillInWorkspaceDeletes(blkID types.Blockid, view **nulls.Nulls) error {
	return nil
}
func (obj *TxnObject) Reset() {
	obj.Txn = nil
	obj.Rel = nil
}
func (obj *TxnObject) GetMeta() any   { return nil }
func (obj *TxnObject) String() string { return "" }
func (obj *TxnObject) Close() error   { return nil }
func (obj *TxnObject) GetID() uint64  { return 0 }

// func (obj *TxnObject) GetByFilter(*handle.Filter) (id *common.ID, offset uint32, err error) {
// 	return
// }

func (obj *TxnObject) GetRelation() (rel handle.Relation)                                { return }
func (obj *TxnObject) Update(uint64, uint32, uint16, any) (err error)                    { return }
func (obj *TxnObject) RangeDelete(uint64, uint32, uint32, handle.DeleteType) (err error) { return }

func (obj *TxnObject) PushDeleteOp(handle.Filter) (err error)              { return }
func (obj *TxnObject) PushUpdateOp(handle.Filter, string, any) (err error) { return }
func (obj *TxnObject) SoftDeleteBlock(id types.Blockid) (err error)        { return }
func (obj *TxnObject) BatchDedup(containers.Vector) (err error)            { return }

// func (blk *TxnBlock) IsAppendable() bool                                   { return true }

func (blk *TxnBlock) Reset() {
	blk.Txn = nil
	blk.Seg = nil
}
func (blk *TxnBlock) GetTotalChanges() int                                  { return 0 }
func (blk *TxnBlock) IsAppendableBlock() bool                               { return true }
func (blk *TxnBlock) Fingerprint() *common.ID                               { return &common.ID{} }
func (blk *TxnBlock) Rows() int                                             { return 0 }
func (blk *TxnBlock) ID() uint64                                            { return 0 }
func (blk *TxnBlock) String() string                                        { return "" }
func (blk *TxnBlock) Close() error                                          { return nil }
func (blk *TxnBlock) GetMeta() any                                          { return nil }
func (blk *TxnBlock) GetByFilter(*handle.Filter) (offset uint32, err error) { return }

func (blk *TxnBlock) GetObject() (obj handle.Object) { return }

func (blk *TxnBlock) Append(*containers.Batch, uint32) (n uint32, err error)    { return }
func (blk *TxnBlock) Update(uint32, uint16, any) (err error)                    { return }
func (blk *TxnBlock) RangeDelete(uint32, uint32, handle.DeleteType) (err error) { return }
func (blk *TxnBlock) PushDeleteOp(handle.Filter) (err error)                    { return }
func (blk *TxnBlock) PushUpdateOp(handle.Filter, string, any) (err error)       { return }
