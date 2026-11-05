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
	"bytes"
	"encoding/binary"
	"testing"
)

func TestUvarintPayloadFraming(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    []byte
		payload []byte
		valid   bool
	}{
		{"empty payload", []byte{0}, []byte{}, true},
		{"one byte", []byte{1, 'x'}, []byte{'x'}, true},
		{"opaque bytes", []byte{1, 0xff}, []byte{0xff}, true},
		{"127", append([]byte{127}, bytes.Repeat([]byte{'x'}, 127)...), bytes.Repeat([]byte{'x'}, 127), true},
		{"128", append([]byte{0x80, 1}, bytes.Repeat([]byte{'x'}, 128)...), bytes.Repeat([]byte{'x'}, 128), true},
		{"16383", append([]byte{0xff, 0x7f}, bytes.Repeat([]byte{'x'}, 16383)...), bytes.Repeat([]byte{'x'}, 16383), true},
		{"16384", append([]byte{0x80, 0x80, 1}, bytes.Repeat([]byte{'x'}, 16384)...), bytes.Repeat([]byte{'x'}, 16384), true},
		{"missing", nil, nil, false},
		{"extra payload", []byte{0, 'x'}, nil, false},
		{"short payload", []byte{2, 'x'}, nil, false},
		{"unterminated", []byte{0x80}, nil, false},
		{"overlong empty", []byte{0x80, 0}, nil, false},
		{"overlong one", []byte{0x81, 0, 'x'}, nil, false},
		{"overflow", append(bytes.Repeat([]byte{0xff}, 9), 2), nil, false},
		{"huge missing payload", append(bytes.Repeat([]byte{0xff}, 9), 1), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, ok := UvarintPayload(tc.data)
			if ok != tc.valid || !bytes.Equal(payload, tc.payload) || !ok && payload != nil {
				t.Fatalf("got payload=%x valid=%v; want payload=%x valid=%v", payload, ok, tc.payload, tc.valid)
			}
			if len(payload) > 0 && &payload[0] != &tc.data[len(tc.data)-len(payload)] {
				t.Fatal("payload must remain a view of the original input")
			}
		})
	}
	for width := 2; width <= binary.MaxVarintLen64; width++ {
		overlong := append(bytes.Repeat([]byte{0x80}, width-1), 0)
		if _, ok := UvarintPayload(overlong); ok {
			t.Fatalf("accepted overlong zero with prefix width%d", width)
		}
	}
}

func TestDecimalJSONPayloadFraming(t *testing.T) {
	for _, tc := range []struct {
		data  []byte
		valid bool
	}{
		{[]byte{1, '1'}, true},
		{[]byte{4, '-', '1', '.', '5'}, true},
		{append([]byte{0x80, 1}, bytes.Repeat([]byte{'1'}, 128)...), true},
		{[]byte{0}, false},
		{[]byte{1, 'x'}, false},
		{[]byte{2, '1', ' '}, false},
		{[]byte{0x81, 0, '1'}, false},
		{[]byte{2, '1'}, false},
	} {
		if got := DecimalJSONPayload(tc.data); got != tc.valid {
			t.Fatalf("data=%x valid=%v; want%v", tc.data, got, tc.valid)
		}
	}
}

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
