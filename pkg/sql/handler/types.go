package handler

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"
)

type Handler struct {
	engine engine.Engine
	proc   *process.Process
}
