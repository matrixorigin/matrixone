// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
