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

package bytejson

import (
	"bytes"
	"encoding/binary"
	"math"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const storedJSONValidationMinimumWork = 1024

type storedJSONValidationChild struct {
	value ByteJson
}

type storedJSONValidationFrame struct {
	value ByteJson
	depth int
}

// ValidateStoredJSONDocument validates a binary JSON document before any
// operation can follow its offsets. Storage decoding is intentionally
// zero-copy, so this validation owns the bounds, type, depth, and work
// guarantees required by consumers of persisted or remote JSON values.
func ValidateStoredJSONDocument(document ByteJson) error {
	workLimit := uint64(len(document.Data))
	if workLimit < storedJSONValidationMinimumWork {
		workLimit = storedJSONValidationMinimumWork
	}
	if workLimit <= ^uint64(0)/4 {
		workLimit *= 4
	} else {
		workLimit = ^uint64(0)
	}

	work := uint64(0)
	depth := 0
	if isStoredJSONContainer(document.Type) {
		depth = 1
	}
	stack := []storedJSONValidationFrame{{value: document, depth: depth}}
	for len(stack) > 0 {
		frame := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if frame.depth > JSONDocumentMaxNestingDepth {
			return newJSONDocumentDepthError(JSONDocumentMaxNestingDepth)
		}

		children, err := validateStoredJSONValue(frame.value, &work, workLimit)
		if err != nil {
			return err
		}
		for i := len(children) - 1; i >= 0; i-- {
			childDepth := frame.depth
			if isStoredJSONContainer(children[i].value.Type) {
				childDepth++
			}
			stack = append(stack, storedJSONValidationFrame{
				value: children[i].value,
				depth: childDepth,
			})
		}
	}
	return nil
}

func isStoredJSONContainer(typeCode TpCode) bool {
	return typeCode == TpCodeArray || typeCode == TpCodeObject
}

func validateStoredJSONValue(value ByteJson, work *uint64, workLimit uint64) ([]storedJSONValidationChild, error) {
	if err := chargeStoredJSONValidationWork(work, workLimit, 1); err != nil {
		return nil, err
	}

	switch value.Type {
	case TpCodeArray, TpCodeObject:
		return validateStoredJSONContainer(value, work, workLimit)
	case TpCodeLiteral:
		if len(value.Data) != 1 || value.Data[0] < LiteralNull || value.Data[0] > LiteralFalse {
			return nil, invalidStoredJSONDocument()
		}
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		if len(value.Data) != numberSize {
			return nil, invalidStoredJSONDocument()
		}
		if value.Type == TpCodeFloat64 {
			f := math.Float64frombits(endian.Uint64(value.Data))
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, invalidStoredJSONDocument()
			}
		}
	case TpCodeString:
		payload, ok := storedJSONStringPayload(value.Data)
		if !ok || !utf8.Valid(payload) {
			return nil, invalidStoredJSONDocument()
		}
	case TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob, TpCodeOpaque, TpCodeBit:
		if _, ok := storedJSONStringPayload(value.Data); !ok {
			return nil, invalidStoredJSONDocument()
		}
	default:
		return nil, invalidStoredJSONDocument()
	}
	return nil, nil
}

func validateStoredJSONContainer(value ByteJson, work *uint64, workLimit uint64) ([]storedJSONValidationChild, error) {
	if len(value.Data) < headerSize {
		return nil, invalidStoredJSONDocument()
	}
	count := uint64(endian.Uint32(value.Data))
	documentSize := uint64(endian.Uint32(value.Data[docSizeOff:]))
	if documentSize != uint64(len(value.Data)) {
		return nil, invalidStoredJSONDocument()
	}

	entrySize := uint64(valEntrySize)
	if value.Type == TpCodeObject {
		entrySize += keyEntrySize
	}
	if count > (^uint64(0)-uint64(headerSize))/entrySize {
		return nil, invalidStoredJSONDocument()
	}
	tableSize := uint64(headerSize) + count*entrySize
	if tableSize > documentSize {
		return nil, invalidStoredJSONDocument()
	}
	if err := chargeStoredJSONValidationWork(work, workLimit, count); err != nil {
		return nil, err
	}

	children := make([]storedJSONValidationChild, 0, int(count))
	payloadStart := int(tableSize)
	// Canonical stored JSON writes key and value payloads in table order. Keep
	// validation linear and reject non-canonical offsets before any lookup can
	// rely on them; this avoids sorting every document during extraction.
	previousPayloadEnd := tableSize
	if value.Type == TpCodeObject {
		var previousKey []byte
		for i := uint64(0); i < count; i++ {
			entryOffset := uint64(headerSize) + i*keyEntrySize
			keyOffset := uint64(endian.Uint32(value.Data[entryOffset:]))
			keyLength := uint64(endian.Uint16(value.Data[entryOffset+keyOriginOff:]))
			keyEnd := keyOffset + keyLength
			if keyEnd < keyOffset || keyOffset < previousPayloadEnd || keyOffset < uint64(payloadStart) || keyEnd > documentSize {
				return nil, invalidStoredJSONDocument()
			}
			key := value.Data[int(keyOffset):int(keyEnd)]
			if !utf8.Valid(key) {
				return nil, invalidStoredJSONDocument()
			}
			if i > 0 && bytes.Compare(previousKey, key) >= 0 {
				return nil, invalidStoredJSONDocument()
			}
			previousKey = key
			previousPayloadEnd = keyEnd
		}
	}

	valueTableOffset := uint64(headerSize)
	if value.Type == TpCodeObject {
		valueTableOffset += count * keyEntrySize
	}
	for i := uint64(0); i < count; i++ {
		entryOffset := valueTableOffset + i*valEntrySize
		entry := value.Data[entryOffset:]
		typeCode := TpCode(entry[0])
		if typeCode == TpCodeLiteral {
			literal := entry[valTypeSize]
			if literal < LiteralNull || literal > LiteralFalse {
				return nil, invalidStoredJSONDocument()
			}
			continue
		}
		valueOffset := uint64(endian.Uint32(entry[valTypeSize:]))
		valueLength, ok := storedJSONValueLength(typeCode, value.Data, valueOffset, documentSize)
		valueEnd := valueOffset + valueLength
		if !ok || valueOffset < previousPayloadEnd || valueOffset < uint64(payloadStart) || valueEnd < valueOffset || valueEnd > documentSize {
			return nil, invalidStoredJSONDocument()
		}
		previousPayloadEnd = valueEnd
		children = append(children, storedJSONValidationChild{
			value: ByteJson{Type: typeCode, Data: value.Data[int(valueOffset):int(valueEnd)]},
		})
	}
	return children, nil
}

func storedJSONValueLength(typeCode TpCode, data []byte, offset, documentSize uint64) (uint64, bool) {
	if offset >= documentSize {
		return 0, false
	}
	switch typeCode {
	case TpCodeArray, TpCodeObject:
		if documentSize-offset < headerSize {
			return 0, false
		}
		length := uint64(endian.Uint32(data[offset+docSizeOff:]))
		return length, length >= headerSize && length <= documentSize-offset
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		return numberSize, numberSize <= documentSize-offset
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob, TpCodeOpaque, TpCodeBit:
		length, prefix := binary.Uvarint(data[offset:documentSize])
		if prefix <= 0 || uint64(prefix) > documentSize-offset || length > documentSize-offset-uint64(prefix) {
			return 0, false
		}
		return uint64(prefix) + length, true
	default:
		return 0, false
	}
}

func storedJSONStringPayload(data []byte) ([]byte, bool) {
	length, prefix := binary.Uvarint(data)
	if prefix <= 0 || length > uint64(len(data)-prefix) || uint64(prefix)+length != uint64(len(data)) {
		return nil, false
	}
	return data[prefix:], true
}

func chargeStoredJSONValidationWork(work *uint64, limit, amount uint64) error {
	if *work > limit || amount > limit-*work {
		return invalidStoredJSONDocument()
	}
	*work += amount
	return nil
}

func invalidStoredJSONDocument() error {
	return moerr.NewInvalidInputNoCtx("invalid binary JSON document")
}
