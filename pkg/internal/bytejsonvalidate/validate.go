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
	"encoding/json"
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
	if len(data) > 0 && data[0] < 0x80 {
		if int(data[0]) != len(data)-1 {
			return nil, false
		}
		return data[1:], true
	}
	payloadLength, prefixLength := binary.Uvarint(data)
	// A successful multi-byte unsigned varint is shortest exactly when its
	// final group is nonzero. All one-byte encodings returned above.
	if prefixLength <= 0 || data[prefixLength-1] == 0 ||
		payloadLength != uint64(len(data)-prefixLength) {
		return nil, false
	}
	return data[prefixLength:], true
}

// DecimalJSONPayload validates one complete JSON number in the exact,
// uvarint-prefixed decimal representation. The scan is linear in input bytes;
// even an enormous exponent never allocates an unbounded numeric intermediate.
func DecimalJSONPayload(data []byte) bool {
	payload, ok := UvarintPayload(data)
	if !ok || len(payload) == 0 ||
		(payload[0] != '-' && (payload[0] < '0' || payload[0] > '9')) {
		return false
	}
	for _, ch := range payload {
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			return false
		}
	}
	return json.Valid(payload)
}

// Container validates the bounds and every descendant of one binary JSON
// array or object. Scalar semantics stay with the caller through validScalar.
// Charge each visited table, key and scalar against the serialized byte size.
// This permits canonical nested encodings without charging a child's payload
// twice, while rejecting alias expansion before it exceeds linear input work.
func Container(tp byte, data []byte, validScalar func(byte, []byte) bool) bool {
	remaining := uint64(len(data))
	return container(tp, data, validScalar, 1, &remaining, nil, nil, false, false)
}

// ContainerWithNumericEncoding validates the same complete bounded document as
// Container, but handles fixed-width numbers directly: every int64/uint64 bit
// pattern is valid and float64 must be finite. validScalar is called for all
// other scalar domains. Callers with different numeric semantics must use
// Container; this does not assert immutability or skip any descendant bounds.
func ContainerWithNumericEncoding(tp byte, data []byte, validScalar func(byte, []byte) bool) bool {
	remaining := uint64(len(data))
	return container(tp, data, validScalar, 1, &remaining, nil, nil, true, false)
}

// StoredContainer adds canonical key/range checks to the same bounded walk.
// The scalar callback must include stored scalar semantics. workLimit also
// preserves the stored validator's node/entry budget, independently of the
// serialized-byte budget. No child list or width-dependent frame is retained.
func StoredContainer(tp byte, data []byte, validScalar func(byte, []byte) bool, workLimit uint64) (valid, depthExceeded bool) {
	remaining := uint64(len(data))
	valid = container(tp, data, validScalar, 1, &remaining, &workLimit, &depthExceeded, false, false)
	return
}

func container(tp byte, data []byte, validScalar func(byte, []byte) bool, depth int, remaining, storedWork *uint64, depthExceeded *bool, numericEncoding, canonicalStrings bool) bool {
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
		var keyRunStart, keyRunEnd uint64
		var hasKeyRun bool
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
				if keyOffset < previousRangeEnd || len(key) > 0 && !utf8.RuneStart(key[0]) ||
					i > 0 && bytes.Compare(previousKey, key) >= 0 {
					return false
				}
				if hasKeyRun && keyOffset != keyRunEnd {
					if !utf8.Valid(data[keyRunStart:keyRunEnd]) {
						return false
					}
					hasKeyRun = false
				}
				if !hasKeyRun {
					keyRunStart, hasKeyRun = keyOffset, true
				}
				keyRunEnd = keyOffset + keyLength
				previousKey = key
				previousRangeEnd = keyRunEnd
			}
		}
		if hasKeyRun && !utf8.Valid(data[keyRunStart:keyRunEnd]) {
			return false
		}
	}

	// The checked table has exactly count entries. Keep its views borrowed so
	// callbacks still observe the original backing bytes.
	valueTable := data[valueTableStart:payloadStart]
	for entryOffset := 0; entryOffset < len(valueTable); entryOffset += valEntrySize {
		entry := valueTable[entryOffset : entryOffset+valEntrySize]
		childType := entry[0]
		if childType == typeLiteral {
			if !validScalar(childType, entry[valTypeSize:valTypeSize+1]) {
				return false
			}
			continue
		}
		childOffset := uint64(binary.LittleEndian.Uint32(entry[valTypeSize:]))
		if childType >= typeInt64 && childType <= typeFloat64 {
			if storedWork != nil && numericEncoding && entryOffset+valEntrySize < len(valueTable) {
				nextType := valueTable[entryOffset+valEntrySize]
				if nextType >= typeInt64 && nextType <= typeFloat64 {
					// Numeric entries have no scalar callback. Charge a contiguous
					// run once, before the next callback or recursive descent, but
					// still examine every descriptor and every float payload.
					budget := *remaining / numberSize
					if *storedWork < budget {
						budget = *storedWork
					}
					var visited uint64
					end := entryOffset
					for end < len(valueTable) {
						numericEntry := valueTable[end : end+valEntrySize]
						tp := numericEntry[0]
						if tp < typeInt64 || tp > typeFloat64 {
							break
						}
						if visited == budget {
							return false
						}
						offset := uint64(binary.LittleEndian.Uint32(numericEntry[valTypeSize:]))
						if offset < previousRangeEnd || offset > documentSize-numberSize {
							return false
						}
						if tp == typeFloat64 && binary.LittleEndian.Uint64(data[offset:offset+numberSize])&0x7ff0000000000000 == 0x7ff0000000000000 {
							return false
						}
						previousRangeEnd = offset + numberSize
						visited++
						end += valEntrySize
					}
					*remaining -= visited * numberSize
					*storedWork -= visited
					entryOffset = end - valEntrySize
					continue
				}
			}
			// Fixed-width scalars need neither a tail view nor container
			// dispatch. Read each descriptor only after the previous callback;
			// the exact payload view retains its original borrowed capacity.
			if childOffset < payloadStart || childOffset > documentSize-numberSize {
				return false
			}
			if storedWork != nil {
				if childOffset < previousRangeEnd {
					return false
				}
				previousRangeEnd = childOffset + numberSize
				if !charge(storedWork, 1) {
					return false
				}
			}
			if !charge(remaining, numberSize) {
				return false
			}
			if numericEncoding {
				// All exponent bits set identify exactly infinities and NaNs.
				if childType == typeFloat64 && binary.LittleEndian.Uint64(data[childOffset:childOffset+numberSize])&0x7ff0000000000000 == 0x7ff0000000000000 {
					return false
				}
			} else if !validScalar(childType, data[childOffset:childOffset+numberSize]) {
				return false
			}
			continue
		}
		if canonicalStrings && childType == typeString && entryOffset+valEntrySize < len(valueTable) && valueTable[entryOffset+valEntrySize] == typeString {
			// The canonical string rule has no caller callback. Charge an
			// adjacent run once, before another scalar callback or descent,
			// while checking each member against both budgets as it is read.
			byteBudget, workBudget := *remaining, *storedWork
			var used, visited uint64
			var utf8Start, utf8End uint64
			var hasUTF8Run bool
			end := entryOffset
			for end < len(valueTable) {
				stringEntry := valueTable[end : end+valEntrySize]
				if stringEntry[0] != typeString {
					break
				}
				offset := uint64(binary.LittleEndian.Uint32(stringEntry[valTypeSize:]))
				if offset < previousRangeEnd || offset >= documentSize {
					return false
				}
				child, ok := childValue(typeString, data[offset:])
				if !ok || visited >= workBudget || uint64(len(child)) > byteBudget-used {
					return false
				}
				previousRangeEnd = offset + uint64(len(child))
				if child[0] < 0x80 {
					// A one-byte length is ASCII and separates payloads, so no
					// UTF-8 rune can span the boundary between two strings.
					if hasUTF8Run && offset != utf8End {
						if !utf8.Valid(data[utf8Start:utf8End]) {
							return false
						}
						hasUTF8Run = false
					}
					if !hasUTF8Run {
						utf8Start, hasUTF8Run = offset, true
					}
					utf8End = previousRangeEnd
				} else {
					if hasUTF8Run && !utf8.Valid(data[utf8Start:utf8End]) {
						return false
					}
					hasUTF8Run = false
					if !validCanonicalStringPayload(child) {
						return false
					}
				}
				used += uint64(len(child))
				visited++
				end += valEntrySize
			}
			if hasUTF8Run && !utf8.Valid(data[utf8Start:utf8End]) {
				return false
			}
			*remaining -= used
			*storedWork -= visited
			entryOffset = end - valEntrySize
			continue
		}
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
			if !container(childType, childData, validScalar, depth+1, remaining, storedWork, depthExceeded, numericEncoding, canonicalStrings) {
				return false
			}
		} else {
			if storedWork != nil && !charge(storedWork, 1) {
				return false
			}
			if !charge(remaining, uint64(len(childData))) {
				return false
			}
			if canonicalStrings && childType == typeString {
				if !validCanonicalStringPayload(childData) {
					return false
				}
			} else if !validScalar(childType, childData) {
				return false
			}
		}
	}
	return true
}

// childValue has already checked the shortest length prefix and exact payload
// bounds. Only the string payload's UTF-8 rule remains here.
func validCanonicalStringPayload(data []byte) bool {
	prefix := 1
	if data[0] >= 0x80 {
		_, prefix = binary.Uvarint(data)
	}
	return utf8.Valid(data[prefix:])
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
		if len(data) == 0 {
			return nil, false
		}
		payloadLength, prefixLength := uint64(data[0]), 1
		if data[0] >= 0x80 {
			payloadLength, prefixLength = binary.Uvarint(data)
			if prefixLength <= 0 || data[prefixLength-1] == 0 {
				return nil, false
			}
		}
		if payloadLength > uint64(len(data)-prefixLength) {
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

// StoredContainerWithNumericEncoding validates the stored layout while
// handling fixed-width numeric encodings without a scalar callback.
// Its caller must use the numeric semantics of ContainerWithNumericEncoding.
func StoredContainerWithNumericEncoding(tp byte, data []byte, validScalar func(byte, []byte) bool, workLimit uint64) (valid, depthExceeded bool) {
	remaining := uint64(len(data))
	valid = container(tp, data, validScalar, 1, &remaining, &workLimit, &depthExceeded, true, false)
	return
}

// StoredContainerWithCanonicalStrings validates the same complete stored
// layout and budgets. Numeric encodings and UTF-8 strings use their fixed
// canonical semantics; validScalar handles literals and all other scalar types.
func StoredContainerWithCanonicalStrings(tp byte, data []byte, validScalar func(byte, []byte) bool, workLimit uint64) (valid, depthExceeded bool) {
	remaining := uint64(len(data))
	valid = container(tp, data, validScalar, 1, &remaining, &workLimit, &depthExceeded, true, true)
	return
}
