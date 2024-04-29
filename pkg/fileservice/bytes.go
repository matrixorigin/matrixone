// Copyright 2022 Matrix Origin
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

package fileservice

import (
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"
)

type Bytes struct {
	bytes  []byte
	handle *malloc.Handle
}

func (b Bytes) Size() int64 {
	return int64(len(b.bytes))
}

func (b Bytes) Bytes() []byte {
	return b.bytes
}

func (b Bytes) Slice(length int) memorycache.CacheData {
	return Bytes{
		bytes:  b.bytes[:length],
		handle: b.handle,
	}
}

func (b Bytes) Release() {
	if b.handle != nil {
		b.handle.Free()
	}
}

func (b Bytes) Retain() {
}

type bytesAllocator struct{}

var _ CacheDataAllocator = new(bytesAllocator)

func (b *bytesAllocator) Alloc(size int) memorycache.CacheData {
	var ret Bytes
	ret.handle = malloc.Alloc(size, &ret.bytes)
	return ret
}
