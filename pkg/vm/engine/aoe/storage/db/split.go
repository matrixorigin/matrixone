package db

import (
	"io/ioutil"
	"os"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type Splitter struct {
	msplitter   *metadata.ShardSplitter
	database    *metadata.Database
	renameTable RenameTableFactory
	ctx         []byte
	keys        [][]byte
	index       *LogIndex
	dbSpecs     []*metadata.DBSpec
	dbImpl      *DB
	tables      map[uint64]iface.ITableData
	tempDir     string
}

func NewSplitter(database *metadata.Database, newDBNames []string, rename RenameTableFactory,
	keys [][]byte, ctx []byte, index *LogIndex, dbImpl *DB) *Splitter {
	splitter := &Splitter{
		database:    database,
		renameTable: rename,
		ctx:         ctx,
		keys:        keys,
		index:       index,
		dbImpl:      dbImpl,
		tables:      make(map[uint64]iface.ITableData),
	}
	splitter.dbSpecs = make([]*metadata.DBSpec, len(splitter.keys))
	for i, _ := range splitter.dbSpecs {
		dbSpec := new(metadata.DBSpec)
		dbSpec.Name = newDBNames[i]
		splitter.dbSpecs[i] = dbSpec
	}
	return splitter
}

func (splitter *Splitter) onTableData(data iface.ITableData) error {
	meta := data.GetMeta()
	if meta.Database == splitter.database {
		data.Ref()
		splitter.tables[data.GetID()] = data
	}
	return nil
}

// func (splitter *Splitter) prepareTableData(t iface.ITableData, path string) error {
// 	err = CopyTableFn(t, path)
// 	if err != nil {
// 		return
// 	}
// }

func (splitter *Splitter) prepareData() error {
	err := splitter.dbImpl.Store.DataTables.ForTables(splitter.onTableData)
	if err != nil {
		return err
	}
	if splitter.tempDir, err = ioutil.TempDir("/tmp", "aoesplit"); err != nil {
		return err
	}
	for _, t := range splitter.tables {
		if err = CopyTableFn(t, splitter.tempDir); err != nil {
			return err
		}
	}
	// files, err := ioutil.ReadDir(splitter.tempDir)
	// if err != nil {
	// 	return err
	// }
	// var (
	// 	blks, tblks, segs []string
	// )
	// for _, file := range files {
	// 	name := file.Name()
	// 	if common.IsSegmentFile(name) {
	// 		segs = append(segs, name)
	// 	} else if common.IsBlockFile(name) {
	// 		blks = append(blks, name)
	// 	} else if common.IsTBlockFile(name) {
	// 		tblks = append(tblks, name)
	// 	}
	// }
	return err
}

func (splitter *Splitter) Prepare() error {
	var err error
	spec := metadata.NewEmptyShardSplitSpec()
	if err = spec.Unmarshal(splitter.ctx); err != nil {
		return err
	}
	if err = splitter.dbImpl.DoFlushDatabase(splitter.database); err != nil {
		return err
	}
	splitter.msplitter = metadata.NewShardSplitter(splitter.database.Catalog, spec, splitter.dbSpecs, splitter.index, splitter.renameTable)
	if err = splitter.msplitter.Prepare(); err != nil {
		return err
	}
	if err = splitter.prepareData(); err != nil {
		return err
	}
	return err
}

func (splitter *Splitter) Commit() error {
	err := splitter.msplitter.Commit()
	return err
}

func (splitter *Splitter) Close() error {
	for _, t := range splitter.tables {
		t.Unref()
	}
	if splitter.tempDir != "" {
		os.RemoveAll(splitter.tempDir)
	}
	return nil
}
