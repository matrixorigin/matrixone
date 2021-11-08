package aoedb

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
)

func Open(dir string, opts *storage.Options) (inst *DB, err error) {
	impl, err := db.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &DB{Impl: *impl}, err
}
