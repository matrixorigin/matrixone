package logtail

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func DecideTableScope(tableID uint64) Scope {
	var scope Scope
	switch tableID {
	case pkgcatalog.MO_DATABASE_ID:
		scope = ScopeDatabases
	case pkgcatalog.MO_TABLES_ID:
		scope = ScopeTables
	case pkgcatalog.MO_COLUMNS_ID:
		scope = ScopeColumns
	default:
		scope = ScopeUserTables
	}
	return scope
}

type Manager struct {
	txnbase.NoopCommitListener
	table *TxnTable
}

func NewManager(blockSize int, c clock.Clock) *Manager {
	mgr := &Manager{}
	mgr.table = NewTxnTable(
		blockSize,
		types.NewTsAlloctor(c),
	)
	return mgr
}

func (mgr *Manager) OnEndPrePrepare(txn txnif.AsyncTxn) {
	mgr.table.AddTxn(txn)
}

func (mgr *Manager) GetReader(from, to types.TS) *Reader {
	return &Reader{
		from:  from,
		to:    to,
		table: mgr.table,
	}
}

func (mgr *Manager) GetTableOperator(
	from, to types.TS,
	catalog *catalog.Catalog,
	dbID, tableID uint64,
	scope Scope,
	visitor catalog.Processor,
) *BoundTableOperator {
	reader := mgr.GetReader(from, to)
	return NewBoundTableOperator(
		catalog,
		reader,
		scope,
		dbID,
		tableID,
		visitor,
	)
}
