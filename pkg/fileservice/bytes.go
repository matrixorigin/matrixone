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

import "github.com/matrixorigin/matrixone/pkg/fileservice/memorycache"

type Bytes []byte

func (b Bytes) Size() int64 {
	return int64(len(b))
}

func (b Bytes) Bytes() []byte {
	return b
}

func (b Bytes) Slice(length int) memorycache.CacheData {
	return b[:length]
}

func (b Bytes) Release() {
}

func (b Bytes) Retain() {
}

type bytesAllocator struct{}

var _ CacheDataAllocator = new(bytesAllocator)

func (b *bytesAllocator) Alloc(size int) memorycache.CacheData {
	return make(Bytes, size)
}
