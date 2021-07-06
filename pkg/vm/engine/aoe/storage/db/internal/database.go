package internal

import (
	"matrixone/pkg/vm/engine"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"sync/atomic"
)

type Database struct {
	Closed *atomic.Value
	Opts   *e.Options
	Tables *table.Tables
}

func NewDatabase(opts *e.Options, tables *table.Tables, closed *atomic.Value) engine.Database {
	d := &Database{
		Closed: closed,
		Opts:   opts,
		Tables: tables,
	}
	return d
}

func (d *Database) Relations() []string {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Opts.Meta.Info.TableNames()
}

func (d *Database) Relation(name string) (engine.Relation, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	tblMeta, err := d.Opts.Meta.Info.ReferenceTableByName(name)
	if err != nil {
		return nil, err
	}

	tblData, err := d.Tables.StrongRefTable(tblMeta.ID)
	if err != nil {
		return nil, err
	}
	return NewRelation(tblData), nil
}

func (d *Database) Delete(name string) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return nil
}

func (d *Database) Create(name string, tbl []engine.TableDef, pby *engine.PartitionBy, dpy *engine.DistributionBy) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return nil
}
