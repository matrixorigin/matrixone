package pipeline

import (
	"matrixbase/pkg/vm"
)

type Pipeline struct {
	cs    []uint64
	attrs []string
	ins   vm.Instructions
}
