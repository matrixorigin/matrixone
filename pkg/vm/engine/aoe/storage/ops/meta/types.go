package meta

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	// log "github.com/sirupsen/logrus"
)

type OpCtx struct {
	Block  *md.Block
	Opts   *e.Options
	Schema *md.Schema
}

type Op struct {
	ops.Op
	Ctx *OpCtx
}
