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

package node

import (
	buf "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	// log "github.com/sirupsen/logrus"
)

var (
	_ buf.IBuffer       = (*NodeBuffer)(nil)
	_ iface.INodeBuffer = (*NodeBuffer)(nil)
)

func NewNodeBuffer(id uint64, node buf.IMemoryNode) iface.INodeBuffer {
	if node == nil {
		return nil
	}
	ibuf := buf.NewBuffer(node)
	nb := &NodeBuffer{
		IBuffer: ibuf,
		ID:      id,
	}
	return nb
}

func (nb *NodeBuffer) GetID() uint64 {
	return nb.ID
}
