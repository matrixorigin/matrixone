package engine

import (
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/dist"
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
	id uint64
	catalog *catalog.Catalog
	store *dist.Storage
}


