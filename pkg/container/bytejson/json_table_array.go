// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bytejson

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrJSONTableCellLimit identifies a JSON_TABLE JSON result cell that would
// exceed its configured encoded-byte limit. This is a statement-level error;
// callers must not route it through a column ON ERROR action.
var ErrJSONTableCellLimit = errors.New("JSON_TABLE JSON cell exceeds limit")

// JSONTableArrayBuilder constructs one JSON array incrementally. It owns the
// output buffer and copies each value into the final binary-JSON layout, so it
// never retains a slice of all ByteJson matches. The encoded size includes the
// one-byte top-level type marker used by ByteJson.Marshal.
type JSONTableArrayBuilder struct {
	entries  []byte
	payload  []byte
	count    uint32
	maxBytes int
	done     bool
}

// NewJSONTableArrayBuilder creates an empty bounded array builder. maxBytes is
// the complete encoded cell size, including the top-level type byte.
func NewJSONTableArrayBuilder(maxBytes int) (*JSONTableArrayBuilder, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("invalid JSON_TABLE JSON cell limit %d", maxBytes)
	}
	// The array header plus the top-level type byte must fit before any value is
	// appended. Keeping this check here makes a limit smaller than the binary
	// JSON header deterministic rather than dependent on the first append.
	if maxBytes < 1+headerSize+valEntrySize {
		return nil, fmt.Errorf("JSON_TABLE JSON cell limit %d is too small", maxBytes)
	}
	return &JSONTableArrayBuilder{
		maxBytes: maxBytes,
	}, nil
}

// Append adds one value in path order. The value's bytes are copied; callers
// may therefore reuse or release the input document after Append returns.
func (b *JSONTableArrayBuilder) Append(value ByteJson) error {
	return b.AppendContext(context.Background(), value)
}

// AppendContext is the cancellation-aware form of Append. There is no wait in
// this operation; the check keeps a long sequence of bounded appends
// interruptible without introducing a worker or an unbounded queue.
func (b *JSONTableArrayBuilder) AppendContext(ctx context.Context, value ByteJson) error {
	if b == nil {
		return errors.New("nil JSON_TABLE array builder")
	}
	if b.done {
		return errors.New("JSON_TABLE array builder is closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.Type == TpCodeLiteral && len(value.Data) != 1 {
		return fmt.Errorf("invalid JSON literal payload length %d", len(value.Data))
	}
	if b.count == math.MaxUint32 {
		return fmt.Errorf("JSON_TABLE JSON array has too many elements")
	}
	stored, err := storageCompatibleJSONValue(value)
	if err != nil {
		return err
	}
	if stored.Type == TpCodeLiteral && len(stored.Data) != 1 {
		return fmt.Errorf("invalid JSON literal payload length %d", len(stored.Data))
	}

	payloadLen := 0
	if stored.Type != TpCodeLiteral {
		payloadLen = len(stored.Data)
	}
	const entrySize = valEntrySize
	if payloadLen > math.MaxInt-(headerSize+len(b.entries)+entrySize+len(b.payload)) {
		return fmt.Errorf("JSON_TABLE JSON cell size overflows int")
	}
	nextLen := headerSize + len(b.entries) + entrySize + len(b.payload) + payloadLen
	// ByteJson.Marshal adds one byte for the top-level type code.
	if nextLen > b.maxBytes-1 {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrJSONTableCellLimit, nextLen+1, b.maxBytes)
	}

	entryOffset := len(b.entries)
	b.entries = append(b.entries, make([]byte, entrySize)...)
	b.entries[entryOffset] = stored.Type
	if stored.Type == TpCodeLiteral {
		if stored.Data[0] != LiteralNull && stored.Data[0] != LiteralTrue && stored.Data[0] != LiteralFalse {
			b.entries = b.entries[:entryOffset]
			return fmt.Errorf("invalid JSON literal type %d", stored.Data[0])
		}
		b.entries[entryOffset+valTypeSize] = stored.Data[0]
	} else {
		if uint64(len(b.payload)) > math.MaxUint32 {
			b.entries = b.entries[:entryOffset]
			return fmt.Errorf("JSON_TABLE JSON cell offset exceeds uint32")
		}
		binary.LittleEndian.PutUint32(
			b.entries[entryOffset+valTypeSize:],
			uint32(len(b.payload)),
		)
		b.payload = append(b.payload, stored.Data...)
	}
	b.count++
	return nil
}

func storageCompatibleJSONValue(value ByteJson) (stored ByteJson, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			stored = ByteJson{}
			err = fmt.Errorf("invalid JSON value: %v", recovered)
		}
	}()
	return value.StorageCompatible()
}

// Build finalizes and transfers ownership of the encoded array. A builder
// cannot be appended to after Build; Close may still be called by cleanup code.
func (b *JSONTableArrayBuilder) Build() (ByteJson, error) {
	if b == nil {
		return ByteJson{}, errors.New("nil JSON_TABLE array builder")
	}
	if b.done {
		return ByteJson{}, errors.New("JSON_TABLE array builder is closed")
	}
	if b.count == 0 {
		return ByteJson{}, errors.New("cannot build an empty JSON_TABLE array")
	}
	dataLen := headerSize + len(b.entries) + len(b.payload)
	if dataLen > math.MaxUint32 {
		return ByteJson{}, errors.New("JSON_TABLE JSON cell exceeds uint32 storage size")
	}
	data := make([]byte, dataLen)
	binary.LittleEndian.PutUint32(data[:4], b.count)
	binary.LittleEndian.PutUint32(data[docSizeOff:headerSize], uint32(len(data)))
	copy(data[headerSize:], b.entries)
	payloadOffset := headerSize + len(b.entries)
	copy(data[payloadOffset:], b.payload)
	// Entries held relative payload offsets while the number of entries was
	// still unknown. Convert them to offsets relative to the final document.
	for offset := 0; offset < len(b.entries); offset += valEntrySize {
		if TpCode(b.entries[offset]) == TpCodeLiteral {
			continue
		}
		relative := binary.LittleEndian.Uint32(data[headerSize+offset+valTypeSize:])
		binary.LittleEndian.PutUint32(
			data[headerSize+offset+valTypeSize:],
			uint32(payloadOffset)+relative,
		)
	}
	b.entries = nil
	b.payload = nil
	b.done = true
	return ByteJson{Type: TpCodeArray, Data: data}, nil
}

// Count reports the number of values currently owned by the builder.
func (b *JSONTableArrayBuilder) Count() int {
	if b == nil {
		return 0
	}
	return int(b.count)
}

// Bytes reports the complete encoded cell size currently reserved by the
// builder, including the top-level type marker.
func (b *JSONTableArrayBuilder) Bytes() int {
	if b == nil {
		return 0
	}
	return headerSize + len(b.entries) + len(b.payload) + 1
}

// Close releases an unfinished builder. A finalized builder is already
// detached from its output buffer, so Close remains idempotent.
func (b *JSONTableArrayBuilder) Close() {
	if b == nil {
		return
	}
	b.entries = nil
	b.payload = nil
	b.count = 0
	b.done = true
}
