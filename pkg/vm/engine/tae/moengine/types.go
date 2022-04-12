package moengine

import (
	"bytes"

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

type txnReader struct {
	handle       handle.Relation
	it           handle.BlockIt
	compressed   []*bytes.Buffer
	decompressed []*bytes.Buffer
}
