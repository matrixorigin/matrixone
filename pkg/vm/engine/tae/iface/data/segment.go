package data

import (
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type Segment interface {
	GetID() uint64
	GetSegmentFile() dataio.SegmentFile
	BatchDedup(txn txnif.AsyncTxn, pks *vector.Vector) error
}
