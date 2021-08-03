package laoe

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/storage/db"
)

type aoeEngine struct {
	db *db.DB
	mp map[string]map[string][]engine.SegmentInfo
}

type database struct {
	id string
	db *db.DB
	mp map[string][]engine.SegmentInfo
}

type relation struct {
	id string
	db string
	mp map[string]*db.Relation
}
