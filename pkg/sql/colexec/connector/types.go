package connector

import (
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
)

// pipe connector
type Argument struct {
	Mmu  *guest.Mmu
	vecs []*vector.Vector
	Reg  *process.WaitRegister
}
