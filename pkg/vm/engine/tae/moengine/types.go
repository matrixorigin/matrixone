package moengine

import (
	"bytes"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type txnEngine struct {
	txn txnif.AsyncTxn
}

type txnDatabase struct {
	handle handle.Database
}

type txnRelation struct {
	handle handle.Relation
}

type txnSegment struct {
	handle handle.Segment
}

type txnBlock struct {
	handle handle.Block
}

type txnReaderIt struct {
	*sync.RWMutex
	segmentIt handle.SegmentIt
	blockIt   handle.BlockIt
}

type txnReader struct {
	handle       handle.Relation
	it           *txnReaderIt
	compressed   []*bytes.Buffer
	decompressed []*bytes.Buffer
}
