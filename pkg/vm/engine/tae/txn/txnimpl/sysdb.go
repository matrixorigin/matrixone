package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
)

var sysTableNames map[string]bool

func init() {
	sysTableNames = make(map[string]bool)
	sysTableNames[catalog.SystemTable_Columns_Name] = true
	sysTableNames[catalog.SystemTable_Table_Name] = true
	sysTableNames[catalog.SystemTable_DB_Name] = true
}

func buildDB(db *txnDB) handle.Database {
	if db.entry.IsSystemDB() {
		return newSysDB(db)
	}
	return newDatabase(db)
}

type txnSysDB struct {
	*txnDatabase
}

func newSysDB(db *txnDB) *txnSysDB {
	sysDB := &txnSysDB{
		txnDatabase: newDatabase(db),
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

func (db *txnSysDB) TruncateByName(name string) (rel handle.Relation, err error) {
	if isSys := sysTableNames[name]; isSys {
		err = catalog.ErrNotPermitted
		return
	}
	return db.txnDatabase.TruncateByName(name)
}
