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

package client

import (
	"sync"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/rogpeppe/fastuuid"
)

var _ TxnIDGenerator = (*uuidTxnIDGenerator)(nil)

var (
	bufferSize = 1024 * 1024 * 32 // 32MB
)

type uuidTxnIDGenerator struct {
	g *fastuuid.Generator

	mu struct {
		sync.Mutex
		buffer *buf.ByteBuf
	}
}

func newUUIDTxnIDGenerator() TxnIDGenerator {
	g, err := fastuuid.NewGenerator()
	if err != nil {
		panic(err)
	}
	v := &uuidTxnIDGenerator{
		g: g,
	}
	v.resetLocked()
	return v
}

func (gen *uuidTxnIDGenerator) Generate() []byte {
	v := gen.g.Next()
	id := v[:]
	return gen.get(id)
}

func (gen *uuidTxnIDGenerator) get(v []byte) []byte {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	if gen.mu.buffer.Writeable() < len(v) {
		gen.resetLocked()
	}

	idx := gen.mu.buffer.GetWriteIndex()
	gen.mu.buffer.MustWrite(v)
	return gen.mu.buffer.RawBuf()[idx : idx+len(v)]
}

func (gen *uuidTxnIDGenerator) resetLocked() {
	gen.mu.buffer = buf.NewByteBuf(bufferSize)
}
