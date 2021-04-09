package transfer

import (
	"matrixone/pkg/vm/mmu/guest"
	"matrixone/pkg/vm/process"
)

type Argument struct {
	Mmu *guest.Mmu
	Reg *process.WaitRegister
}
