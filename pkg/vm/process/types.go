package process

import "matrixbase/pkg/vm/mmu/guest"

/*
type Process interface {
	Destroy()

	Size() int64
	Free(int64)
	Alloc(int64) error
}
*/

type Process struct {
	Gm *guest.Mmu
}
