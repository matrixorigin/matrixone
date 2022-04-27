package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type txnImpl struct {
	*txnbase.Txn
	catalog *catalog.Catalog
}

var TxnFactory = func(catalog *catalog.Catalog) txnbase.TxnFactory {
	return func(mgr *txnbase.TxnManager, store txnif.TxnStore, txnId, start uint64, info []byte) txnif.AsyncTxn {
		return newTxnImpl(catalog, mgr, store, txnId, start, info)
	}
}

func newTxnImpl(catalog *catalog.Catalog, mgr *txnbase.TxnManager, store txnif.TxnStore, txnId, start uint64, info []byte) *txnImpl {
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

func (txn *txnImpl) UseDatabase(name string) (err error) {
	return txn.Store.UseDatabase(name)
}

func (txn *txnImpl) CurrentDatabase() (db handle.Database) {
	return txn.Store.CurrentDatabase()
}

func (txn *txnImpl) LogTxnEntry(tableId uint64, entry txnif.TxnEntry, readed []*common.ID) (err error) {
	return txn.Store.LogTxnEntry(tableId, entry, readed)
}
