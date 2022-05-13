package db

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func (db *DB) ReplayDDL() {
	db.Wal.Replay(db.replayHandle)
}

func (db *DB) replayHandle(group uint32, commitId uint64, payload []byte, typ uint16, info interface{}) (err error) {
	r := bytes.NewBuffer(payload)
	txnCmd, _, err := txnbase.BuildCommandFrom(r)
	if err != nil {
		return err
	}
	switch txnCmd.(type) {
	case *catalog.EntryCommand:
		err = db.Catalog.ReplayCmd(txnCmd)

	default:
		//	TODO
	}
	return
}
