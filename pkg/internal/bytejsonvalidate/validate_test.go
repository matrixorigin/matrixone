// Copyright 2026 Matrix Origin
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

package bytejsonvalidate

import (
	"encoding/binary"
	"testing"
)

func testLiteralArray() []byte {
	data := make([]byte, 18)
	binary.LittleEndian.PutUint32(data, 2)
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)))
	data[8], data[13] = typeLiteral, typeLiteral
	return data
}

func testWrapArray(child []byte, copies int, alias bool) []byte {
	header := headerSize + copies*valEntrySize
	payloadCopies := copies
	if alias {
		payloadCopies = 1
	}
	data := make([]byte, header+payloadCopies*len(child))
	binary.LittleEndian.PutUint32(data, uint32(copies))
	binary.LittleEndian.PutUint32(data[4:], uint32(len(data)))
	for i := 0; i < copies; i++ {
		offset := header
		if !alias {
			offset += i * len(child)
		}
		entry := headerSize + i*valEntrySize
		data[entry] = typeArray
		binary.LittleEndian.PutUint32(data[entry+1:], uint32(offset))
		copy(data[offset:], child)
	}
	return data
}

func TestContainerBoundsAliasedDescendantWork(t *testing.T) {
	data := testLiteralArray()
	for i := 0; i < 18; i++ {
		data = testWrapArray(data, 2, true)
	}
	visits := 0
	valid := Container(typeArray, data, func(byte, []byte) bool { visits++; return true })
	if valid || visits > len(data) {
		t.Fatalf("aliased descendants accepted=%v scalar visits=%d encoded bytes=%d", valid, visits, len(data))
	}
	// Distinct encoded children remain valid; no time-based assertion is used.
	data = testLiteralArray()
	for i := 0; i < 8; i++ {
		data = testWrapArray(data, 2, false)
	}
	visits = 0
	if !Container(typeArray, data, func(byte, []byte) bool { visits++; return true }) || visits != 512 {
		t.Fatalf("canonical tree: scalar visits=%d", visits)
	}
}

func TestContainerBoundsNesting(t *testing.T) {
	data := testLiteralArray()
	for i := 1; i < 100; i++ {
		data = testWrapArray(data, 1, false)
	}
	accept := func(byte, []byte) bool { return true }
	if !Container(typeArray, data, accept) {
		t.Fatal("100 container levels must remain valid")
	}
	data = testWrapArray(data, 1, false)
	visits := 0
	if Container(typeArray, data, func(byte, []byte) bool { visits++; return true }) || visits != 0 {
		t.Fatalf("over-depth chain was traversed: visits=%d", visits)
	}
}
