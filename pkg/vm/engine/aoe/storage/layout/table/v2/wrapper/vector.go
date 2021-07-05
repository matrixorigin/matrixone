package wrapper

import (
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	// log "github.com/sirupsen/logrus"
)

type vectorWrapper struct {
	vector.IVector
	handle nif.IBufferHandle
}

func NewVector(handle nif.IBufferHandle) vector.IVector {
	if handle == nil {
		return nil
	}
	v := &vectorWrapper{
		handle:  handle,
		IVector: handle.GetHandle().GetBuffer().GetDataNode().(vector.IVector),
	}
	return v
}

func (v *vectorWrapper) Close() error {
	return v.handle.Close()
}
