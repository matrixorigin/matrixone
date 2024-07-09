// Copyright 2021-2024 Matrix Origin
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

package morpc

import (
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

type Buffer struct {
	buf *buf.ByteBuf
}

// NewBuffer creates a new buffer
func NewBuffer() *Buffer {
	return reuse.Alloc[Buffer](nil)
}

func (b Buffer) TypeName() string {
	return "morpc.buffer"
}

func (b *Buffer) reset() {
	b.buf.Reset()
}

func (b *Buffer) Close() {
	reuse.Free(b, nil)
}

func (b *Buffer) EncodeUint64(
	v uint64,
) []byte {
	b.buf.Grow(8)
	idx := b.buf.GetWriteIndex()
	if err := b.buf.WriteUint64(v); err != nil {
		panic(err)
	}
	return b.buf.RawSlice(idx, b.buf.GetWriteIndex())
}
