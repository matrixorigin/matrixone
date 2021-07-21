package engine

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe"
	"matrixone/pkg/vm/engine/aoe/catalog"
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
	segments []engine.SegmentInfo
	tablets []aoe.TabletInfo
}