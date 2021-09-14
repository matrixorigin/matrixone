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

package buf

var (
	_ IBuffer = (*Buffer)(nil)
)

func NewBuffer(node IMemoryNode) IBuffer {
	if node == nil {
		return nil
	}
	buf := &Buffer{
		Node: node,
	}
	return buf
}

func (b *Buffer) Close() error {
	b.Node.FreeMemory()
	b.Node = nil
	return nil
}

func (buf *Buffer) GetCapacity() uint64 {
	return buf.Node.GetMemoryCapacity()
}

func (buf *Buffer) GetDataNode() IMemoryNode {
	return buf.Node
}
