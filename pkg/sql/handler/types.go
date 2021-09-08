package handler

import (
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/process"

	"github.com/fagongzi/goetty"
)

type Handler struct {
	engine engine.Engine
	proc   *process.Process
}

type userdata struct {
	conn goetty.IOSession
}
