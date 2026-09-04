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

import "encoding/binary"

const (
	typeObject   byte = 0x01
	typeArray    byte = 0x03
	typeLiteral  byte = 0x04
	typeInt64    byte = 0x09
	typeUint64   byte = 0x0a
	typeFloat64  byte = 0x0b
	typeString   byte = 0x0c
	typeDecimal  byte = 0x0d
	typeDate     byte = 0x0e
	typeTime     byte = 0x0f
	typeDatetime byte = 0x10
	typeBlob     byte = 0x11
	typeOpaque   byte = 0x12
	typeBit      byte = 0x13

	headerSize   = 8
	docSizeOff   = 4
	keyEntrySize = 6
	keyOriginOff = 4
	valTypeSize  = 1
	valEntrySize = 5
	numberSize   = 8
)

// UvarintPayload accepts the exact, shortest uvarint-prefixed payload.
func UvarintPayload(data []byte) ([]byte, bool) {
	payloadLength, prefixLength := binary.Uvarint(data)
	if prefixLength <= 0 || prefixLength != uvarintSize(payloadLength) ||
		payloadLength != uint64(len(data)-prefixLength) {
		return nil, false
	}
	return data[prefixLength:], true
}

// Container validates the bounds and every descendant of one binary JSON
// array or object. Scalar semantics stay with the caller through validScalar.
func Container(tp byte, data []byte, validScalar func(byte, []byte) bool) bool {
	if tp != typeArray && tp != typeObject || len(data) < headerSize {
		return false
	}
	count := uint64(binary.LittleEndian.Uint32(data))
	tableEntrySize := uint64(valEntrySize)
	keyTableSize := uint64(0)
	if tp == typeObject {
		tableEntrySize += uint64(keyEntrySize)
		keyTableSize = count * uint64(keyEntrySize)
	}
	minimumSize := uint64(headerSize) + count*tableEntrySize
	documentSize := uint64(binary.LittleEndian.Uint32(data[docSizeOff:]))
	if minimumSize > documentSize || documentSize != uint64(len(data)) {
		return false
	}

	valueTableStart := uint64(headerSize) + keyTableSize
	payloadStart := valueTableStart + count*uint64(valEntrySize)
	if tp == typeObject {
		for i := uint64(0); i < count; i++ {
			entryOffset := uint64(headerSize) + i*uint64(keyEntrySize)
			keyOffset := uint64(binary.LittleEndian.Uint32(data[entryOffset:]))
			keyLength := uint64(binary.LittleEndian.Uint16(data[entryOffset+keyOriginOff:]))
			if keyOffset < payloadStart || keyOffset > documentSize || keyLength > documentSize-keyOffset {
				return false
			}
		}
	}

	for i := uint64(0); i < count; i++ {
		entryOffset := valueTableStart + i*uint64(valEntrySize)
		childType := data[entryOffset]
		if childType == typeLiteral {
			if !validScalar(childType, data[entryOffset+valTypeSize:entryOffset+valTypeSize+1]) {
				return false
			}
			continue
		}
		childOffset := uint64(binary.LittleEndian.Uint32(data[entryOffset+valTypeSize:]))
		if childOffset < payloadStart || childOffset >= documentSize {
			return false
		}
		childData, ok := childValue(childType, data[childOffset:])
		if !ok {
			return false
		}
		if childType == typeArray || childType == typeObject {
			if !Container(childType, childData, validScalar) {
				return false
			}
		} else if !validScalar(childType, childData) {
			return false
		}
	}
	return true
}

func childValue(tp byte, data []byte) ([]byte, bool) {
	switch tp {
	case typeInt64, typeUint64, typeFloat64:
		if len(data) < numberSize {
			return nil, false
		}
		return data[:numberSize], true
	case typeString, typeDecimal, typeDate, typeTime, typeDatetime,
		typeBlob, typeOpaque, typeBit:
		payloadLength, prefixLength := binary.Uvarint(data)
		if prefixLength <= 0 || prefixLength != uvarintSize(payloadLength) ||
			payloadLength > uint64(len(data)-prefixLength) {
			return nil, false
		}
		return data[:uint64(prefixLength)+payloadLength], true
	case typeObject, typeArray:
		if len(data) < headerSize {
			return nil, false
		}
		documentSize := uint64(binary.LittleEndian.Uint32(data[docSizeOff:]))
		if documentSize < headerSize || documentSize > uint64(len(data)) {
			return nil, false
		}
		return data[:documentSize], true
	default:
		return nil, false
	}
}

func uvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}
