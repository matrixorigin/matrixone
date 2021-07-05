package vector

import (
	"matrixone/pkg/vm/engine/aoe/storage/container"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

func (v *BaseVector) HasNull() bool {
	return atomic.LoadUint64(&v.StatMask)&container.HasNullMask != 0
}

func (v *BaseVector) NullCnt() int {
	if !v.HasNull() {
		return 0
	}

	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}

	return v.VMask.Length()
}

func (v *BaseVector) IsReadonly() bool {
	return atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask != 0
}

func (v *BaseVector) Length() int {
	return int(atomic.LoadUint64(&v.StatMask) & container.PosMask)
}

func (v *BaseVector) IsNull(idx int) bool {
	if idx >= v.Length() {
		panic(VecInvalidOffsetErr.Error())
	}
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	return v.VMask.Contains(uint64(idx))
}
