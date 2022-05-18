package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var sysTableNames map[string]bool

func init() {
	sysTableNames = make(map[string]bool)
	sysTableNames[catalog.SystemTable_Columns_Name] = true
	sysTableNames[catalog.SystemTable_Table_Name] = true
	sysTableNames[catalog.SystemTable_DB_Name] = true
}

func buildDB(txn txnif.AsyncTxn, meta *catalog.DBEntry) handle.Database {
	if meta.IsSystemDB() {
		return newSysDB(txn, meta)
	}
	return newDatabase(txn, meta)
}

type txnSysDB struct {
	*txnDatabase
}

func newSysDB(txn txnif.AsyncTxn, meta *catalog.DBEntry) *txnSysDB {
	sysDB := &txnSysDB{
		txnDatabase: newDatabase(txn, meta),
	}
	return sysDB
}

func (db *txnSysDB) DropRelationByName(name string) (rel handle.Relation, err error) {
	if isSys := sysTableNames[name]; isSys {
		err = catalog.ErrNotPermitted
		return
	}
	return db.txnDatabase.DropRelationByName(name)
}
