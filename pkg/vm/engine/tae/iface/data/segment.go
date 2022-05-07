package data

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment interface {
	CheckpointUnit
	GetID() uint64
	GetSegmentFile() file.Segment
	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
}
