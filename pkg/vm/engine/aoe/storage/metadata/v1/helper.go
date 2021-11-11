package metadata

import (
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type Cleaner struct {
	Catalog *Catalog
}

func NewCleaner(catalog *Catalog) *Cleaner {
	return &Cleaner{
		Catalog: catalog,
	}
}

func (cleaner *Cleaner) TryCompactDB(db *Database, dropTableFn func(*Table) error) error {
	if !db.IsDeleted() {
		return errors.New("Cannot compact active database")
	}
	tables := make([]*Table, 0)
	processor := new(LoopProcessor)
	processor.TableFn = func(t *Table) error {
		var err error
		if t.IsHardDeleted() {
			return err
		}
		tables = append(tables, t)
		return err
	}

	db.RLock()
	db.LoopLocked(processor)
	db.RUnlock()

	if len(tables) == 0 {
		return db.SimpleHardDelete()
	}
	var err error
	for _, t := range tables {
		if t.IsDeleted() {
			continue
		}
		if err = dropTableFn(t); err != nil {
			logutil.Warn(err.Error())
			break
		}
	}
	return err
}
