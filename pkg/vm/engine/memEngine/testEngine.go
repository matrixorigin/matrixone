package memEngine

import (
	"matrixone/pkg/vm/engine"
)

func NewTestEngine() engine.Engine {
	e := New()
	e.Create("test", 0)
	return e
}
