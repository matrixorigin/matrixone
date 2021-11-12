package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type SSWriter = metadata.SSWriter
type SSLoader = metadata.SSLoader
type Snapshoter = metadata.Snapshoter

var (
	CopyTableFn func(t iface.ITableData, destDir string) error
)

func init() {
	CopyTableFn = func(t iface.ITableData, destDir string) error {
		return t.LinkTo(destDir)
	}
	// CopyTableFn = func(t iface.ITableData, destDir string) error {
	// 	return t.CopyTo(destDir)
	// }
}

type dbSnapshoter struct {
	metaw  metadata.SSWriter
	metar  metadata.SSLoader
	meta   *metadata.Database
	data   map[uint64]iface.ITableData
	tables *table.Tables
	dir    string
}

func NewDBSSWriter(database *metadata.Database, dir string, tables *table.Tables) *dbSnapshoter {
	w := &dbSnapshoter{
		data:   make(map[uint64]iface.ITableData),
		tables: tables,
		dir:    dir,
		meta:   database,
	}
	return w
}

func (ss *dbSnapshoter) Close() error {
	for _, t := range ss.data {
		t.Unref()
	}
	return nil
}

func (ss *dbSnapshoter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == ss.meta {
		data.Ref()
		ss.data[data.GetID()] = data
	}
	return nil
}

func (ss *dbSnapshoter) validatePrepare() error {
	// TODO
	return nil
}

func (ss *dbSnapshoter) PrepareWrite() error {
	ss.tables.RLock()
	index := ss.meta.GetCheckpointId()
	err := ss.tables.ForTablesLocked(ss.onTableData)
	ss.tables.RUnlock()
	if err != nil {
		return err
	}
	ss.metaw = metadata.NewDBSSWriter(ss.meta, ss.dir, index)
	if err = ss.metaw.PrepareWrite(); err != nil {
		return err
	}
	return ss.validatePrepare()
}

func (ss *dbSnapshoter) CommitWrite() error {
	err := ss.metaw.CommitWrite()
	if err != nil {
		return err
	}
	for _, t := range ss.data {
		if err = CopyTableFn(t, ss.dir); err != nil {
			break
		}
	}
	return err
}
