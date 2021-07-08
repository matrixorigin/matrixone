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
	name string
	catalog *catalog.Catalog
}

type relation struct {
	dbName string
	name string
	catalog *catalog.Catalog
	store *dist.Storage
}


