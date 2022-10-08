// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package adaptors

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl/containers"
)

func NewBuffer(buf []byte) *Buffer {
	b := &Buffer{
		storage: containers.NewStdVector[byte](),
	}
	if len(buf) > 0 {
		bs := stl.NewFixedTypeBytes[byte]()
		bs.Storage = buf
		b.storage.ReadBytes(bs, true)
	}
	return b
}

func (b *Buffer) Reset()         { b.storage.Reset() }
func (b *Buffer) Close()         { b.storage.Close() }
func (b *Buffer) String() string { return b.storage.String() }

func (b *Buffer) Write(p []byte) (n int, err error) {
	n = len(p)
	b.storage.AppendMany(p...)
	return
}

// WriteString TODO: avoid string to []byte copy
func (b *Buffer) WriteString(s string) (n int, err error) {
	n = len(s)
	b.storage.AppendMany([]byte(s)...)
	return
}

func (b *Buffer) Bytes() []byte  { return b.storage.Data() }
func (b *Buffer) Len() int       { return b.storage.Length() }
func (b *Buffer) Cap() int       { return b.storage.Capacity() }
func (b *Buffer) Allocated() int { return b.storage.Allocated() }
