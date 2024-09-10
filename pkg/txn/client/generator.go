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
	"encoding/binary"
	"hash/fnv"
	"sync/atomic"
	"time"
)

var _ TxnIDGenerator = (*uuidTxnIDGenerator)(nil)

type uuidTxnIDGenerator struct {
	hash uint64
	seq  uint64
}

func newUUIDTxnIDGenerator(sid string) TxnIDGenerator {
	h := fnv.New64()
	_, err := h.Write([]byte(sid))
	if err != nil {
		panic(err)
	}

	v := &uuidTxnIDGenerator{
		hash: h.Sum64(),
		seq:  uint64(time.Now().UnixNano()),
	}
	return v
}

func (gen *uuidTxnIDGenerator) Generate() []byte {
	var uuid [16]byte
	binary.BigEndian.PutUint64(uuid[:8], gen.hash)
	binary.BigEndian.PutUint64(uuid[8:], atomic.AddUint64(&gen.seq, 1))
	return uuid[:]
}
