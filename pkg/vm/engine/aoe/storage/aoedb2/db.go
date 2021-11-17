package aoedb2

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type Impl = db.DB

type DB struct {
	Impl
}

func (d *DB) CreateDatabase(ctx *CreateDBCtx) (*metadata.Database, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	return d.Store.Catalog.SimpleCreateDatabase(ctx.DB, nil)
}

func (d *DB) DropDatabase(ctx *DropDBCtx) (*metadata.Database, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.GetDatabaseByName(ctx.DB)
	if err != nil {
		return nil, err
	}
	if database.IsDeleted() {
		return nil, db.ErrResourceDeleted
	}

	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return database, err
	}
	defer d.Wal.Checkpoint(index)
	if err = database.SimpleSoftDelete(index); err != nil {
		return database, err
	}
	d.ScheduleGCDatabase(database)
	return database, nil
}

func (d *DB) CreateTable(ctx *CreateTableCtx) (*metadata.Table, error) {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	database, err := d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return nil, err
	}
	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return nil, err
	}
	defer d.Wal.Checkpoint(index)

	return database.SimpleCreateTable(ctx.Schema, index)
}

func (d *DB) CreateSnapshot(ctx *CreateSnapshotCtx) (uint64, error) {
	return d.Impl.CreateSnapshot(ctx.DB, ctx.Path, ctx.Sync)
}

func (d *DB) ApplySnapshot(ctx *ApplySnapshotCtx) error {
	return d.Impl.ApplySnapshot(ctx.DB, ctx.Path)
}

func (d *DB) PrepareSplitDatabase(ctx *PrepareSplitCtx) (uint64, uint64, [][]byte, []byte, error) {
	return d.Impl.SpliteDatabaseCheck(ctx.DB, ctx.Size)
}

func (d *DB) ExecSplitDatabase(ctx *ExecSplitCtx) error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}
	// TODO: validate parameters
	var (
		err      error
		database *metadata.Database
	)
	database, err = d.Store.Catalog.SimpleGetDatabaseByName(ctx.DB)
	if err != nil {
		return err
	}

	index := ctx.ToLogIndex(database)
	if err = d.Wal.SyncLog(index); err != nil {
		return err
	}
	defer d.Wal.Checkpoint(index)

	splitter := db.NewSplitter(database, ctx.NewNames, ctx.RenameTable, ctx.SplitKeys, ctx.SplitCtx, index, &d.Impl)
	defer splitter.Close()
	if err = splitter.Prepare(); err != nil {
		return err
	}
	if err = splitter.Commit(); err != nil {
		return err
	}
	d.ScheduleGCDatabase(database)
	return err
}
