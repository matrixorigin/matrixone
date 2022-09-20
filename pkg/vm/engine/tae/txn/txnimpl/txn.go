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

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnImpl struct {
	*txnbase.Txn
	catalog *catalog.Catalog
}

var TxnFactory = func(catalog *catalog.Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, txnId uint64,
		start types.TS, info []byte) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, info)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore,
	txnId uint64, start types.TS, info []byte) *txnImpl {
	impl := &txnImpl{
		Txn:     txnbase.NewTxn(mgr, store, txnId, start, info),
		catalog: catalog,
	}
	return impl
}

func (txn *txnImpl) CreateDatabase(name string) (db handle.Database, err error) {
	return txn.Store.CreateDatabase(name)
}

func (txn *txnImpl) DropDatabase(name string) (db handle.Database, err error) {
	return txn.Store.DropDatabase(name)
}

func (txn *txnImpl) GetDatabase(name string) (db handle.Database, err error) {
	return txn.Store.GetDatabase(name)
}

func (txn *txnImpl) DatabaseNames() (names []string) {
	return txn.Store.DatabaseNames()
}

func (txn *txnImpl) LogTxnEntry(dbId, tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return txn.Store.LogTxnEntry(dbId, tableId, entry, readed)
}

// TODO::ref to MOToVectorTmp in moengine/utils.go
func MOToTAEVector(v *vector.Vector, nullable bool) containers.Vector {
	return nil
}

// TODO::
func PBToMOVector(v *api.Vector, nullable bool) *vector.Vector {
	return nil
}

func MOToTAEBatch(bat *batch.Batch, schema *catalog.Schema) *containers.Batch {
	//schema := rel.handle.GetMeta().(*catalog.TableEntry).GetSchema()
	allNullables := schema.AllNullables()
	taeBatch := containers.NewEmptyBatch()
	defer taeBatch.Close()
	for i, vec := range bat.Vecs {
		v := MOToTAEVector(vec, allNullables[i])
		taeBatch.AddVector(bat.Attrs[i], v)
	}
	return taeBatch
}

// TODO::
func PBToMOBatch(bat *api.Batch, schema *catalog.Schema) *batch.Batch {
	return nil
}

func (txn *txnImpl) HandleCmd(cmd *api.Entry) (err error) {
	db, err := txn.GetDatabase(cmd.DatabaseName)
	if err != nil {
		return err
	}
	//Handle DDL
	if cmd.DatabaseId == catalog.SystemDBID {
		switch cmd.TableId {
		case catalog.SystemTable_DB_ID:
			if cmd.EntryType == api.Entry_Insert {
				//TODO:: build schema of database.
				//dbDef := BuildDbDefFrom(cmd.Bat)
				//TODO::need to add interface:CreateDatabase(def Any)
				//_, err = txn.CreateDatabase(dbDef)
			} else {
				//TODO::parse name from cmd.Batch.
				_, err = txn.DropDatabase("")
			}
		case catalog.SystemTable_Table_ID:
			if cmd.EntryType == api.Entry_Insert {
				//TODO::build schema of table
				//tbDef := BuildTbDefFrom(cmd.Bat)
				//_, err = db.CreateRelation(tbDef)
			} else {
				//TODO::parse table name from cmd.Batch
				_, err = db.DropRelationByName("")
			}
		case catalog.SystemBlock_Columns_ID:

		}
		return err
	}
	//Handle DML
	tb, err := db.GetRelationByName(cmd.TableName)
	if err != nil {
		return err
	}
	if cmd.EntryType == api.Entry_Insert {
		//Append a block had been bulk-loaded into S3
		if cmd.FileName != "" {
			//TODO::Precommit a block from S3
			//tb.PrecommitAppendBlock()
			return
		}
		//Add a batch into table
		moBat := PBToMOBatch(cmd.Bat, tb.GetMeta().(*catalog.TableEntry).GetSchema())
		taeBat := MOToTAEBatch(moBat, tb.GetMeta().(*catalog.TableEntry).GetSchema())
		//TODO::use PrecommitAppend, instead of Append.
		err = tb.Append(taeBat)
		return
	}

	// Handle Delete
	//TODO:: handle delete rows of block had been bulk-loaded into S3.
	//if ...{

	//return
	//}

	//Delete a batch
	moV := PBToMOVector(cmd.Bat.Vecs[0], false)
	taeV := MOToTAEVector(moV, false)
	//TODO::use PrecommitDeleteByPhyAddrKeys,instead of DeleteByPhyAddrKeys
	err = tb.DeleteByPhyAddrKeys(taeV)
	return
}
