package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
)

func Open(dirname string, opts *storage.Options) (inst *DB, err error) {
	impl, err := db.Open(dirname, opts)
	if err != nil {
		return nil, err
	}
	inst = &DB{Impl: *impl}
	return
}
