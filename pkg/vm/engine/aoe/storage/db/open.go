package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
)

// type Reader interface {
// }

func Open(dirname string, opts *e.Options) (db *DB, err error) {
	db = &DB{
		Dir:  dirname,
		Opts: opts,
	}
	return db, err
}
