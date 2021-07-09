package engine

import (
	"fmt"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
	mdops "matrixone/pkg/vm/engine/aoe/storage/ops/memdata/v2"
)

type Database struct {
	DBImpl *db.DB
}

func NewDatabase(impl *db.DB) engine.Database {
	d := &Database{
		DBImpl: impl,
	}
	return d
}

func (d *Database) Relations() []string {
	d.DBImpl.EnsureNotClosed()
	return d.DBImpl.Opts.Meta.Info.TableNames()
}

func (d *Database) Relation(name string) (engine.Relation, error) {
	d.DBImpl.EnsureNotClosed()
	tblMeta, err := d.DBImpl.Opts.Meta.Info.ReferenceTableByName(name)
	if err != nil {
		return nil, err
	}

	tblData, err := d.DBImpl.Store.DataTables.StrongRefTable(tblMeta.ID)
	if err != nil {
		opCtx := &mdops.OpCtx{
			Opts:        d.DBImpl.Opts,
			MTManager:   d.DBImpl.MemTableMgr,
			TableMeta:   tblMeta,
			IndexBufMgr: d.DBImpl.IndexBufMgr,
			MTBufMgr:    d.DBImpl.MTBufMgr,
			SSTBufMgr:   d.DBImpl.SSTBufMgr,
			FsMgr:       d.DBImpl.FsMgr,
			Tables:      d.DBImpl.Store.DataTables,
		}
		op := mdops.NewCreateTableOp(opCtx)
		op.Push()
		err = op.WaitDone()
		if err != nil {
			panic(fmt.Sprintf("logic error: %s", err))
		}
		collection := op.Collection
		if tblData, err = d.DBImpl.Store.DataTables.StrongRefTable(tblMeta.ID); err != nil {
			collection.Unref()
			return nil, err
		}
		collection.Unref()
	}
	return NewRelation(d.DBImpl, tblData, tblMeta), nil
}

func (d *Database) Delete(name string) error {
	_, err := d.DBImpl.DropTable(name)
	return err
}

func (d *Database) Create(name string, attrs []engine.TableDef, pby *engine.PartitionBy, dpy *engine.DistributionBy) error {
	return nil
}
