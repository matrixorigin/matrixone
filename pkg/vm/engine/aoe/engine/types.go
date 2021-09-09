package engine

import (
	catalog3 "matrixone/pkg/catalog"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

// aoe engine
type aoeEngine struct {
	catalog *catalog3.Catalog
}

type database struct {
	id      uint64
	typ     int
	catalog *catalog3.Catalog
}

type relation struct {
	pid      uint64
	tbl      *aoe.TableInfo
	catalog  *catalog3.Catalog
	segments []engine.SegmentInfo
	tablets  []aoe.TabletInfo
	mp       map[string]*db.Relation
}
