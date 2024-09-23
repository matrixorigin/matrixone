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
	"encoding/binary"
)

const (
	DefaultReadBufferSize  int = 512
	DefaultWriteBufferSize int = 512
)

type IOPackage interface {
	// IsLittleEndian the byte order
	//true - littleEndian; false - littleEndian
	IsLittleEndian() bool

	// WriteUint8 writes an uint8 into the buffer at the position
	// returns position + 1
	WriteUint8([]byte, int, uint8) int

	// WriteUint16 writes an uint16 into the buffer at the position
	// returns position + 2
	WriteUint16([]byte, int, uint16) int

	// WriteUint32 writes an uint32 into the buffer at the position
	// returns position + 4
	WriteUint32([]byte, int, uint32) int

	// WriteUint64 writes an uint64 into the buffer at the position
	// returns position + 8
	WriteUint64([]byte, int, uint64) int

	// AppendUint8 appends an uint8 to the buffer
	// returns the buffer
	AppendUint8([]byte, uint8) []byte

	// AppendUint16 appends an uint16 to the buffer
	// returns the buffer
	AppendUint16([]byte, uint16) []byte

	// AppendUint32 appends an uint32 to the buffer
	// returns the buffer
	AppendUint32([]byte, uint32) []byte

	// AppendUint64 appends an uint64 to the buffer
	// returns the buffer
	AppendUint64([]byte, uint64) []byte

	// ReadUint8 reads an uint8 from the buffer at the position
	// returns uint8 value ; pos+1 ; true - decoded successfully or false - failed
	ReadUint8([]byte, int) (uint8, int, bool)

	// ReadUint16 reads an uint16 from the buffer at the position
	// returns uint16 value ; pos+2 ; true - decoded successfully or false - failed
	ReadUint16([]byte, int) (uint16, int, bool)

	// ReadUint32 reads an uint32 from the buffer at the position
	// returns uint32 value ; pos+4 ; true - decoded successfully or false - failed
	ReadUint32([]byte, int) (uint32, int, bool)

	// ReadUint64 reads an uint64 from the buffer at the position
	// returns uint64 value ; pos+8 ; true - decoded successfully or false - failed
	ReadUint64([]byte, int) (uint64, int, bool)
}

// IOPackageImpl implements the IOPackage for the basic interaction in the connection
type IOPackageImpl struct {
	//true - littleEndian; false - bigEndian
	endian bool
}

func NewIOPackage(littleEndian bool) *IOPackageImpl {
	return &IOPackageImpl{
		endian: littleEndian,
	}
}

func (bio *IOPackageImpl) IsLittleEndian() bool {
	return bio.endian
}

func (bio *IOPackageImpl) WriteUint8(data []byte, pos int, value uint8) int {
	data[pos] = value
	return pos + 1
}

func (bio *IOPackageImpl) WriteUint16(data []byte, pos int, value uint16) int {
	if bio.endian {
		binary.LittleEndian.PutUint16(data[pos:], value)
	} else {
		binary.BigEndian.PutUint16(data[pos:], value)
	}
	return pos + 2
}

func (bio *IOPackageImpl) WriteUint32(data []byte, pos int, value uint32) int {
	if bio.endian {
		binary.LittleEndian.PutUint32(data[pos:], value)
	} else {
		binary.BigEndian.PutUint32(data[pos:], value)
	}
	return pos + 4
}

func (bio *IOPackageImpl) WriteUint64(data []byte, pos int, value uint64) int {
	if bio.endian {
		binary.LittleEndian.PutUint64(data[pos:], value)
	} else {
		binary.BigEndian.PutUint64(data[pos:], value)
	}
	return pos + 8
}

func (bio *IOPackageImpl) AppendUint8(data []byte, value uint8) []byte {
	return append(data, value)
}

func (bio *IOPackageImpl) AppendUint16(data []byte, value uint16) []byte {
	if bio.endian {
		return binary.LittleEndian.AppendUint16(data, value)
	} else {
		return binary.BigEndian.AppendUint16(data, value)
	}
}

func (bio *IOPackageImpl) AppendUint32(data []byte, value uint32) []byte {
	if bio.endian {
		return binary.LittleEndian.AppendUint32(data, value)
	} else {
		return binary.BigEndian.AppendUint32(data, value)
	}
}

func (bio *IOPackageImpl) AppendUint64(data []byte, value uint64) []byte {
	if bio.endian {
		return binary.LittleEndian.AppendUint64(data, value)
	} else {
		return binary.BigEndian.AppendUint64(data, value)
	}
}

func (bio *IOPackageImpl) ReadUint8(data []byte, pos int) (uint8, int, bool) {
	if pos >= len(data) {
		return 0, 0, false
	}
	return data[pos], pos + 1, true
}

func (bio *IOPackageImpl) ReadUint16(data []byte, pos int) (uint16, int, bool) {
	if pos+1 >= len(data) {
		return 0, 0, false
	}
	if bio.endian {
		return binary.LittleEndian.Uint16(data[pos : pos+2]), pos + 2, true
	} else {
		return binary.BigEndian.Uint16(data[pos : pos+2]), pos + 2, true
	}
}

func (bio *IOPackageImpl) ReadUint32(data []byte, pos int) (uint32, int, bool) {
	if pos+3 >= len(data) {
		return 0, 0, false
	}
	if bio.endian {
		return binary.LittleEndian.Uint32(data[pos : pos+4]), pos + 4, true
	} else {
		return binary.BigEndian.Uint32(data[pos : pos+4]), pos + 4, true
	}
}

func (bio *IOPackageImpl) ReadUint64(data []byte, pos int) (uint64, int, bool) {
	if pos+7 >= len(data) {
		return 0, 0, false
	}
	if bio.endian {
		return binary.LittleEndian.Uint64(data[pos : pos+8]), pos + 8, true
	} else {
		return binary.BigEndian.Uint64(data[pos : pos+8]), pos + 8, true
	}
}
