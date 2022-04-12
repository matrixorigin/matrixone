package moengine

import (
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var (
	_ engine.Engine = (*txnEngine)(nil)
)

func NewEngine(txn txnif.AsyncTxn) *txnEngine {
	return &txnEngine{
		txn: txn,
	}
}

func (e *txnEngine) Delete(_ uint64, name string) (err error) {
	_, err = e.txn.DropDatabase(name)
	return
}

func (e *txnEngine) Create(_ uint64, name string, _ int) (err error) {
	_, err = e.txn.CreateDatabase(name)
	return
}

func (e *txnEngine) Databases() (dbs []string) {
	// TODO
	return
}

func (e *txnEngine) Database(name string) (db engine.Database, err error) {
	h, err := e.txn.GetDatabase(name)
	if err != nil {
		return nil, err
	}
	db = newDatabase(h)
	return db, err
}

func (e *txnEngine) Node(ip string) *engine.NodeInfo {
	return &engine.NodeInfo{Mcpu: runtime.NumCPU()}
}
