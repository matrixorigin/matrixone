// Copyright 2021 - 2023 Matrix Origin
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

package buffer

import "sync"

const (
	FULL                   = 0x01 // indicates that a chunk doesn't have any space left
	PointerSize            = 8
	DefaultChunkBufferSize = 1 << 20
)

var ChunkSize int

type chunk struct {
	sync.Mutex
	flag     uint32
	off      uint32
	numFree  uint64
	numAlloc uint64
	ptr      uintptr
	data     []byte
}

type Buffer struct {
	sync.Mutex
	chunks []*chunk
}
