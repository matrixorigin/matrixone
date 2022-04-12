package iface

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type StateMachine interface {
	Append(id uint64, data *batch.Batch, txn txnif.AsyncTxn) error
	BatchDedup(id uint64, col *vector.Vector, txn txnif.AsyncTxn) error
}
