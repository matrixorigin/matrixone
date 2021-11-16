package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type Splitter struct {
	msplitter *metadata.ShardSplitter
	database  *metadata.Database
	tables    *table.Tables
}
