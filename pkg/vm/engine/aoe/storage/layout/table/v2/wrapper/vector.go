package wrapper

import (
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	// log "github.com/sirupsen/logrus"
)

type vectorWrapper struct {
	vector.IVectorNode
	handle nif.IBufferHandle
}

func NewVector(handle nif.IBufferHandle) vector.IVector {
	if handle == nil {
		return nil
	}
	v := &vectorWrapper{
		handle:      handle,
		IVectorNode: handle.GetHandle().GetBuffer().GetDataNode().(vector.IVectorNode),
	}
	return v
}

func (v *vectorWrapper) Close() error {
	return v.handle.Close()
}

func (v *vectorWrapper) GetLatestView() vector.IVector {
	vec := v.IVectorNode.GetLatestView()
	ret := &vectorWrapper{
		IVectorNode: vec.(vector.IVectorNode),
		handle:      v.handle,
	}
	return ret
}
