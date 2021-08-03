package handler

import (
	"matrixone/pkg/vm/engine/aoe/storage/db"
	"matrixone/pkg/vm/process"
)

type Handler struct {
	db   *db.DB
	proc *process.Process
}
