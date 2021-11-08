package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

func OpenWithWalBroker(dirname string, opts *storage.Options) (inst *DB, err error) {
	if opts.Wal != nil && opts.Wal.GetRole() != wal.BrokerRole {
		return nil, db.ErrUnexpectedWalRole
	}
	opts.WalRole = wal.BrokerRole
	return Open(dirname, opts)
}

func Open(dir string, opts *storage.Options) (inst *DB, err error) {
	impl, err := db.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	inst = &DB{Impl: *impl}
	return
}
