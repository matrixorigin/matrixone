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

package frontend

import (
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestBasicIOPackage_WriteUint8(t *testing.T) {
	var buffer = make([]byte, 256)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 256; i++ {
		prePos = pos
		pos = IO.WriteUint8(buffer, pos, uint8(i))
		if pos != prePos+1 {
			t.Errorf("WriteUint8 value %d failed.", i)
			break
		}
		if buffer[i] != uint8(i) {
			t.Errorf("WriteUint8 value %d failed.", i)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint16(t *testing.T) {
	var buffer = make([]byte, 65536*2)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint16(i)
		prePos = pos
		pos = IO.WriteUint16(buffer, pos, value)
		if pos != prePos+2 {
			t.Errorf("WriteUint16 value %d failed.", value)
			break
		}

		var b1, b2 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
		} else {
			b1 = uint8((value >> 8) & 0xff)
			b2 = uint8(value & 0xff)
		}

		p := 2 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2) {
			t.Errorf("WriteUint16 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint32(t *testing.T) {
	var buffer = make([]byte, 65536*4)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint32(0x01010101 + i)
		prePos = pos
		pos = IO.WriteUint32(buffer, pos, value)
		if pos != prePos+4 {
			t.Errorf("WriteUint32 value %d failed.", value)
			break
		}
		var b1, b2, b3, b4 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
			b3 = uint8((value >> 16) & 0xff)
			b4 = uint8((value >> 24) & 0xff)
		} else {
			b4 = uint8(value & 0xff)
			b3 = uint8((value >> 8) & 0xff)
			b2 = uint8((value >> 16) & 0xff)
			b1 = uint8((value >> 24) & 0xff)
		}

		p := 4 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2 && buffer[p+2] == b3 && buffer[p+3] == b4) {
			t.Errorf("WriteUint32 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_WriteUint64(t *testing.T) {
	var buffer = make([]byte, 65536*8)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint64(0x0101010101010101 + i)
		prePos = pos
		pos = IO.WriteUint64(buffer, pos, value)
		if pos != prePos+8 {
			t.Errorf("WriteUint64 value %d failed.", value)
			break
		}
		var b1, b2, b3, b4, b5, b6, b7, b8 uint8
		if IO.IsLittleEndian() {
			b1 = uint8(value & 0xff)
			b2 = uint8((value >> 8) & 0xff)
			b3 = uint8((value >> 16) & 0xff)
			b4 = uint8((value >> 24) & 0xff)
			b5 = uint8((value >> 32) & 0xff)
			b6 = uint8((value >> 40) & 0xff)
			b7 = uint8((value >> 48) & 0xff)
			b8 = uint8((value >> 56) & 0xff)
		} else {
			b8 = uint8(value & 0xff)
			b7 = uint8((value >> 8) & 0xff)
			b6 = uint8((value >> 16) & 0xff)
			b5 = uint8((value >> 24) & 0xff)
			b4 = uint8((value >> 32) & 0xff)
			b3 = uint8((value >> 40) & 0xff)
			b2 = uint8((value >> 48) & 0xff)
			b1 = uint8((value >> 56) & 0xff)
		}

		p := 8 * i
		if !(buffer[p] == b1 && buffer[p+1] == b2 && buffer[p+2] == b3 && buffer[p+3] == b4 &&
			buffer[p+4] == b5 && buffer[p+5] == b6 && buffer[p+6] == b7 && buffer[p+7] == b8) {
			t.Errorf("WriteUint64 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint8(t *testing.T) {
	var buffer = make([]byte, 256)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 256; i++ {
		prePos = pos
		IO.WriteUint8(buffer, pos, uint8(i))
		rValue, pos, ok := IO.ReadUint8(buffer, pos)
		if !ok || rValue != uint8(i) || pos != prePos+1 {
			t.Errorf("ReadUint8 value %d failed.", i)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint16(t *testing.T) {
	var buffer = make([]byte, 65536*2)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint16(i)
		prePos = pos
		IO.WriteUint16(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint16(buffer, pos)
		if !ok || pos != prePos+2 || rValue != value {
			t.Errorf("ReadUint16 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint32(t *testing.T) {
	var buffer = make([]byte, 65536*4)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint32(0x01010101 + i)
		prePos = pos
		IO.WriteUint32(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint32(buffer, pos)
		if !ok || pos != prePos+4 || rValue != value {
			t.Errorf("ReadUint32 value %d failed.", value)
			break
		}
	}
}

func TestBasicIOPackage_ReadUint64(t *testing.T) {
	var buffer = make([]byte, 65536*8)
	var pos = 0
	var IO IOPackageImpl
	var prePos int
	for i := 0; i < 65536; i++ {
		value := uint64(0x0101010101010101 + i)
		prePos = pos
		IO.WriteUint64(buffer, pos, value)
		rValue, pos, ok := IO.ReadUint64(buffer, pos)
		if !ok || pos != prePos+8 || rValue != value {
			t.Errorf("ReadUint64 value %d failed.", value)
			break
		}
	}
}

func Test_AppendUint(t *testing.T) {
	convey.Convey("AppendUint succ", t, func() {
		var io IOPackageImpl
		var data, data2 = []byte{'a'}, []byte{'a', 'b'}
		var value uint8 = 'b'
		data = io.AppendUint8(data, value)
		convey.So(data, convey.ShouldResemble, data2)

		var value2 uint16 = 'c'
		data = io.AppendUint16(data, value2)
		data2 = append(data2, []byte{0, 'c'}...)
		convey.So(data, convey.ShouldResemble, data2)

		var value3 uint32 = 'd'
		data = io.AppendUint32(data, value3)
		data2 = append(data2, []byte{0, 0, 0, 'd'}...)
		convey.So(data, convey.ShouldResemble, data2)

		var value4 uint64 = 'e'
		data = io.AppendUint64(data, value4)
		data2 = append(data2, []byte{0, 0, 0, 0, 0, 0, 0, 'e'}...)
		convey.So(data, convey.ShouldResemble, data2)

		var pos = 9
		u, i, b := io.ReadUint64(data, pos)
		convey.So(u, convey.ShouldEqual, 0)
		convey.So(i, convey.ShouldEqual, 0)
		convey.So(b, convey.ShouldEqual, false)

		io.endian = true
		pos = 0
		_, i, b = io.ReadUint64(data, pos)
		convey.So(i, convey.ShouldEqual, 8)
		convey.So(b, convey.ShouldEqual, true)
	})
}
