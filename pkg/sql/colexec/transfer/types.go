package transfer

import (
	"matrixbase/pkg/vm/mmu/guest"
	"matrixbase/pkg/vm/process"
)

type Argument struct {
	Mmu *guest.Mmu
	Reg *process.WaitRegister
}
