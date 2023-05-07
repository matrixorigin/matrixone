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

package entry

import (
	"bytes"
	"math/rand"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

var buf []byte

func init() {
	var bs bytes.Buffer
	for i := 0; i < 3000; i++ {
		bs.WriteString("helloyou")
	}
	buf = bs.Bytes()
}

func MockEntry() *Entry {
	payloadSize := 100

	e := entry.GetBase()
	e.SetType(entry.IOET_WALEntry_Test)
	info := &entry.Info{GroupLSN: uint64(rand.Intn(1000))}
	e.SetInfo(info)
	payload := make([]byte, payloadSize)
	copy(payload, buf)
	err := e.SetPayload(payload)
	if err != nil {
		panic(err)
	}
	e.PrepareWrite()
	return NewEntry(e)
}
