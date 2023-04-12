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

package common

import (
	"bytes"
	"fmt"
	"testing"
	"unsafe"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestIDAllocate(t *testing.T) {
	sid := NewSegmentid()
	bid := NewBlockid(&sid, 1, 2)
	rid := NewRowid(&bid, 42)

	// ensure not copy
	sidSlice := sid[:]
	require.Equal(t, unsafe.Pointer(&sidSlice[0]), unsafe.Pointer(&sid[0]))

	// get block id from rowid without copy
	bidUnsafe := rid.GetBlockidUnsafe()
	require.Equal(t, unsafe.Pointer(&bidUnsafe[0]), unsafe.Pointer(&rid[0]))
	require.Zero(t, bid.Compare(*bidUnsafe))

	require.Equal(t, rid.GetObjectString(), fmt.Sprintf("%s-%d", sid.ToString(), 1))
}

func TestWriteID(t *testing.T) {
	sid := NewSegmentid()
	// bid := NewBlockid(&sid, 1, 2)
	// rid := NewRowid(&bid, 42)

	var b bytes.Buffer

	b.Write(sid[:])

	var desid types.Uuid
	b.Read(desid[:])

	require.True(t, sid.Eq(desid))
	require.Equal(t, sid.ToString(), desid.ToString())
}

var global interface{}

func BenchmarkUuid1(b *testing.B) {
	var local interface{}
	for i := 0; i < b.N; i++ {
		local = MustUuid1()
	}
	global = local
}

func BenchmarkUuid4(b *testing.B) {
	var local interface{}
	for i := 0; i < b.N; i++ {
		local = uuid.New()
	}
	global = local
}
