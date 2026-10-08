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
	"unicode/utf8"
)

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

	// Match the document nesting policy without importing container packages.
	maxContainerDepth = 100
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
// Charge each visited table, key and scalar against the serialized byte size.
// This permits canonical nested encodings without charging a child's payload
// twice, while rejecting alias expansion before it exceeds linear input work.
func Container(tp byte, data []byte, validScalar func(byte, []byte) bool) bool {
	remaining := uint64(len(data))
	return container(tp, data, validScalar, 1, &remaining, nil, nil)
}

// StoredContainer adds canonical key/range checks to the same bounded walk.
// The scalar callback must include stored scalar semantics. workLimit also
// preserves the stored validator's node/entry budget, independently of the
// serialized-byte budget. No child list or width-dependent frame is retained.
func StoredContainer(tp byte, data []byte, validScalar func(byte, []byte) bool, workLimit uint64) (valid, depthExceeded bool) {
	remaining := uint64(len(data))
	valid = container(tp, data, validScalar, 1, &remaining, &workLimit, &depthExceeded)
	return
}

func container(tp byte, data []byte, validScalar func(byte, []byte) bool, depth int, remaining, storedWork *uint64, depthExceeded *bool) bool {
	if depth > maxContainerDepth {
		if depthExceeded != nil {
			*depthExceeded = true
		}
		return false
	}
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
	if count > (^uint64(0)-uint64(headerSize))/tableEntrySize {
		return false
	}
	minimumSize := uint64(headerSize) + count*tableEntrySize
	documentSize := uint64(binary.LittleEndian.Uint32(data[docSizeOff:]))
	if minimumSize > documentSize || documentSize != uint64(len(data)) {
		return false
	}
	if !charge(remaining, minimumSize) {
		return false
	}
	if storedWork != nil && !charge(storedWork, 1+count) {
		return false
	}

	valueTableStart := uint64(headerSize) + keyTableSize
	payloadStart := valueTableStart + count*uint64(valEntrySize)
	previousRangeEnd := payloadStart
	if tp == typeObject {
		var previousKey []byte
		for i := uint64(0); i < count; i++ {
			entryOffset := uint64(headerSize) + i*uint64(keyEntrySize)
			keyOffset := uint64(binary.LittleEndian.Uint32(data[entryOffset:]))
			keyLength := uint64(binary.LittleEndian.Uint16(data[entryOffset+keyOriginOff:]))
			if keyOffset < payloadStart || keyOffset > documentSize || keyLength > documentSize-keyOffset {
				return false
			}
			if !charge(remaining, keyLength) {
				return false
			}
			if storedWork != nil {
				key := data[keyOffset : keyOffset+keyLength]
				if keyOffset < previousRangeEnd || !utf8.Valid(key) ||
					i > 0 && bytes.Compare(previousKey, key) >= 0 {
					return false
				}
				previousKey = key
				previousRangeEnd = keyOffset + keyLength
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
		if storedWork != nil {
			if childOffset < previousRangeEnd {
				return false
			}
			previousRangeEnd = childOffset + uint64(len(childData))
		}
		if childType == typeArray || childType == typeObject {
			if !container(childType, childData, validScalar, depth+1, remaining, storedWork, depthExceeded) {
				return false
			}
		} else {
			if storedWork != nil && !charge(storedWork, 1) {
				return false
			}
			if !charge(remaining, uint64(len(childData))) || !validScalar(childType, childData) {
				return false
			}
		}
	}
	return true
}

func charge(remaining *uint64, size uint64) bool {
	if size > *remaining {
		return false
	}
	*remaining -= size
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
