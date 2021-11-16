package db

import (
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
	}
	splitter.dbSpecs = make([]*metadata.DBSpec, len(splitter.keys))
	for i, _ := range splitter.dbSpecs {
		dbSpec := new(metadata.DBSpec)
		dbSpec.Name = newDBNames[i]
		splitter.dbSpecs[i] = dbSpec
	}
	return splitter
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
	return err
}

func (splitter *Splitter) Commit() error {
	err := splitter.msplitter.Commit()
	return err
}
