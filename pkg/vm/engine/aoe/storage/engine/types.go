package engine

import (
	base "matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

const (
	DefaultDatabase = "DEFAULTDB"
)

type Engine struct {
	DBImpl *db.DB
}

func (e *Engine) Type() int {
	return 0
}

func (e *Engine) Create(name string) error {
	return db.ErrUnsupported
}

func (e *Engine) Delete(name string) error {
	return db.ErrUnsupported
}

func (e *Engine) Databases() []string {
	return []string{DefaultDatabase}
}

func (e *Engine) Database(name string) (base.Database, error) {
	if name != DefaultDatabase {
		return nil, db.ErrNotFound
	}
	return NewDatabase(e.DBImpl), nil
}
