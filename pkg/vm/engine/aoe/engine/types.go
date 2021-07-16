package engine

import (
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

// aoe engine
type aoeEngine struct {
	catalog *catalog.Catalog
}

type database struct {
	id uint64
	typ int
	catalog *catalog.Catalog
}

type relation struct {
	pid uint64
	tbl *aoe.TableInfo
	catalog *catalog.Catalog
	tablets []*db.Relation
}


