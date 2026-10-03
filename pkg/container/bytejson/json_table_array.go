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
	"encoding/base64"
	"encoding/binary"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// ErrJSONTableCellLimit identifies a JSON_TABLE JSON result cell that would
// exceed its configured encoded-byte limit. This is a statement-level error;
// callers must not route it through a column ON ERROR action.
var ErrJSONTableCellLimit = moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds limit")

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

const (
	// Container document sizes and value offsets are uint32. The complete
	// ByteJson cell has one additional byte for its top-level type marker.
	maxJSONTableArrayDataBytes = uint64(math.MaxUint32)
	maxJSONTableArrayCellBytes = maxJSONTableArrayDataBytes + 1
)

// NewJSONTableArrayBuilder creates an empty bounded array builder. maxBytes is
// the complete encoded cell size, including the top-level type byte.
func NewJSONTableArrayBuilder(maxBytes int) (*JSONTableArrayBuilder, error) {
	if maxBytes <= 0 {
		return nil, moerr.NewInvalidInputNoCtxf("invalid JSON_TABLE JSON cell limit %d", maxBytes)
	}
	if uint64(maxBytes) > maxJSONTableArrayCellBytes {
		return nil, moerr.NewInvalidInputNoCtxf("JSON_TABLE JSON cell limit %d exceeds uint32 storage size", maxBytes)
	}
	// The array header plus the top-level type byte must fit before any value is
	// appended. Keeping this check here makes a limit smaller than the binary
	// JSON header deterministic rather than dependent on the first append.
	if maxBytes < 1+headerSize+valEntrySize {
		return nil, moerr.NewInvalidInputNoCtxf("JSON_TABLE JSON cell limit %d is too small", maxBytes)
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
		return moerr.NewInvalidStateNoCtx("nil JSON_TABLE array builder")
	}
	if b.done {
		return moerr.NewInvalidStateNoCtx("JSON_TABLE array builder is closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if value.Type == TpCodeLiteral && len(value.Data) != 1 {
		return moerr.NewInvalidInputNoCtxf("invalid JSON literal payload length %d", len(value.Data))
	}
	if b.count == math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("JSON_TABLE JSON array has too many elements")
	}
	// Admission must happen before StorageCompatible can materialize an
	// Opaque/Bit base64 payload or rebuild a nested container. Literal values
	// stay inline in the value table, so their one-byte data is not part of the
	// payload budget below.
	availablePayload, err := jsonTableArrayPayloadBudget(b.maxBytes, len(b.entries), len(b.payload))
	if err != nil {
		return err
	}
	storageLimit := availablePayload
	if value.Type == TpCodeLiteral {
		storageLimit = math.MaxInt
	}
	storedDataLen, err := StorageCompatibleDataSizeWithLimit(ctx, value, storageLimit)
	if err != nil {
		return err
	}
	if value.Type != TpCodeLiteral && storedDataLen > availablePayload {
		return jsonTableCellLimitError()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	stored, err := storageCompatibleJSONValue(value)
	if err != nil {
		return err
	}
	if stored.Type == TpCodeLiteral && len(stored.Data) != 1 {
		return moerr.NewInvalidInputNoCtxf("invalid JSON literal payload length %d", len(stored.Data))
	}

	payloadLen := 0
	if stored.Type != TpCodeLiteral {
		payloadLen = len(stored.Data)
	}
	if stored.Type != TpCodeLiteral && len(stored.Data) != storedDataLen {
		return moerr.NewInternalErrorNoCtxf("JSON_TABLE storage size changed during admission: measured %d, encoded %d", storedDataLen, len(stored.Data))
	}
	const entrySize = valEntrySize
	if payloadLen > math.MaxInt-(headerSize+len(b.entries)+entrySize+len(b.payload)) {
		return moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
	}
	nextLen := headerSize + len(b.entries) + entrySize + len(b.payload) + payloadLen
	// ByteJson.Marshal adds one byte for the top-level type code.
	if nextLen > b.maxBytes-1 {
		return jsonTableCellLimitError()
	}
	if uint64(nextLen) > math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
	}

	entryOffset := len(b.entries)
	b.entries = append(b.entries, make([]byte, entrySize)...)
	b.entries[entryOffset] = stored.Type
	if stored.Type == TpCodeLiteral {
		if stored.Data[0] != LiteralNull && stored.Data[0] != LiteralTrue && stored.Data[0] != LiteralFalse {
			b.entries = b.entries[:entryOffset]
			return moerr.NewInvalidInputNoCtxf("invalid JSON literal type %d", stored.Data[0])
		}
		b.entries[entryOffset+valTypeSize] = stored.Data[0]
	} else {
		if uint64(len(b.payload)) > math.MaxUint32 {
			b.entries = b.entries[:entryOffset]
			return moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell offset exceeds uint32")
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
			err = moerr.NewInvalidInputNoCtxf("invalid JSON value: %v", recovered)
		}
	}()
	return value.StorageCompatible()
}

// StorageCompatibleDataSizeWithLimit computes the number of bytes in the
// storage-compatible Data representation without allocating that
// representation. maxDataBytes is an admission bound; a value larger than it
// returns ErrJSONTableCellLimit before any base64 buffer or rebuilt container
// can be created. The one-byte type marker returned by ByteJson.Marshal is not
// included in this size.
func StorageCompatibleDataSizeWithLimit(ctx context.Context, value ByteJson, maxDataBytes int) (int, error) {
	if maxDataBytes < 0 {
		return 0, moerr.NewInvalidInputNoCtx("invalid JSON_TABLE storage data limit")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	remaining := uint64(len(value.Data))
	size, _, err := storageCompatibleDataSizeWithBudget(ctx, value, maxDataBytes, 1, &remaining)
	return size, err
}

// MarshalStorageCompatibleWithLimit performs the same bounded preflight used
// by the JSON_TABLE array builder, then allocates the final storage value only
// after the encoded size is known to fit. It is the single-match admission
// boundary; multi-match cells call the builder above and this helper for the
// final detached cell.
func MarshalStorageCompatibleWithLimit(ctx context.Context, value ByteJson, maxBytes int) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, moerr.NewInvalidInputNoCtxf("invalid JSON_TABLE JSON cell limit %d", maxBytes)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	dataSize, err := StorageCompatibleDataSizeWithLimit(ctx, value, maxBytes-1)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encoded, err := value.Marshal()
	if err != nil {
		return nil, err
	}
	if len(encoded) != dataSize+1 {
		return nil, moerr.NewInternalErrorNoCtxf("JSON_TABLE storage size changed during admission: measured %d, encoded %d", dataSize+1, len(encoded))
	}
	if len(encoded) > maxBytes {
		return nil, jsonTableCellLimitError()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encoded, nil
}

func jsonTableCellLimitError() error {
	return ErrJSONTableCellLimit
}

// jsonTableArrayPayloadBudget computes the payload admitted by both the
// configured cell limit and the uint32 container format. It is intentionally
// expressed in uint64 arithmetic so malformed builder state cannot wrap an
// int before a value reaches StorageCompatible or a base64 encoder.
func jsonTableArrayPayloadBudget(maxBytes, entriesLen, payloadLen int) (int, error) {
	if maxBytes <= 0 {
		return 0, moerr.NewInvalidInputNoCtxf("invalid JSON_TABLE JSON cell limit %d", maxBytes)
	}
	if uint64(maxBytes) > maxJSONTableArrayCellBytes {
		return 0, moerr.NewInvalidInputNoCtxf("JSON_TABLE JSON cell limit %d exceeds uint32 storage size", maxBytes)
	}
	if entriesLen < 0 || payloadLen < 0 {
		return 0, moerr.NewInternalErrorNoCtx("invalid JSON_TABLE JSON array buffer length")
	}

	base := uint64(headerSize)
	if uint64(entriesLen) > maxJSONTableArrayDataBytes-base {
		return 0, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
	}
	base += uint64(entriesLen)
	if uint64(valEntrySize) > maxJSONTableArrayDataBytes-base {
		return 0, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
	}
	base += uint64(valEntrySize)
	if uint64(payloadLen) > maxJSONTableArrayDataBytes-base {
		return 0, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
	}
	base += uint64(payloadLen)

	cellDataLimit := uint64(maxBytes - 1)
	if base > cellDataLimit {
		return 0, jsonTableCellLimitError()
	}
	available := cellDataLimit - base
	formatAvailable := maxJSONTableArrayDataBytes - base
	if available > formatAvailable {
		available = formatAvailable
	}
	return int(available), nil
}

func storageCompatibleDataSizeWithBudget(ctx context.Context, value ByteJson, limit, depth int, remaining *uint64) (size int, expands bool, err error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	switch value.Type {
	case TpCodeLiteral:
		if len(value.Data) != 1 || value.Data[0] < LiteralNull || value.Data[0] > LiteralFalse {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON literal payload")
		}
		return storageSizeWithinLimit(1, limit, false)
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		if len(value.Data) != numberSize {
			return 0, false, moerr.NewInvalidInputNoCtxf("invalid JSON numeric payload length %d", len(value.Data))
		}
		if value.Type == TpCodeFloat64 {
			floating := math.Float64frombits(binary.LittleEndian.Uint64(value.Data))
			if math.IsNaN(floating) || math.IsInf(floating, 0) {
				return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON float payload")
			}
		}
		return storageSizeWithinLimit(numberSize, limit, false)
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob:
		if !validStorageStringData(value.Data) {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON string payload")
		}
		return storageSizeWithinLimit(len(value.Data), limit, false)
	case TpCodeOpaque, TpCodeBit:
		payload, ok := storageStringPayload(value.Data)
		if !ok {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON binary payload")
		}
		encodedLen, err := checkedBase64EncodedLen(len(payload))
		if err != nil {
			return 0, false, err
		}
		if value.Type == TpCodeBit {
			if len(persistedBitPrefix) > math.MaxInt-encodedLen {
				return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
			}
			encodedLen += len(persistedBitPrefix)
		}
		prefixLen := storageUvarintSize(uint64(encodedLen))
		if encodedLen > math.MaxInt-prefixLen {
			return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
		}
		return storageSizeWithinLimit(prefixLen+encodedLen, limit, true)
	case TpCodeArray, TpCodeObject:
		return storageCompatibleContainerDataSizeWithBudget(ctx, value, limit, depth, remaining)
	default:
		return 0, false, moerr.NewInvalidInputNoCtxf("invalid JSON value type %#x", value.Type)
	}
}

func storageCompatibleContainerDataSizeWithBudget(ctx context.Context, value ByteJson, limit, depth int, remaining *uint64) (int, bool, error) {
	if depth > JSONDocumentMaxNestingDepth {
		return 0, false, moerr.NewInvalidInputNoCtxf("json document nesting depth exceeds %d", JSONDocumentMaxNestingDepth)
	}
	data := value.Data
	if len(data) < headerSize {
		return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container header")
	}
	if uint64(len(data)) > math.MaxUint32 || binary.LittleEndian.Uint32(data[docSizeOff:]) != uint32(len(data)) {
		return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container size")
	}
	count := uint64(binary.LittleEndian.Uint32(data[:4]))
	keyTableSize := uint64(0)
	if value.Type == TpCodeObject {
		keyTableSize = count * uint64(keyEntrySize)
	}
	valueTableSize := count * uint64(valEntrySize)
	minimumSize := uint64(headerSize) + keyTableSize + valueTableSize
	if minimumSize > uint64(len(data)) || minimumSize > math.MaxInt {
		return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container table")
	}
	if len(data) > limit {
		// StorageCompatible only replaces descendants with larger legacy
		// encodings, so a valid container already larger than the remaining
		// budget cannot become admissible.
		return 0, false, jsonTableCellLimitError()
	}
	if !consumeStoragePreflightWork(remaining, minimumSize) {
		return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container serialized work")
	}
	valueTableStart := int(uint64(headerSize) + keyTableSize)
	payloadStart := int(minimumSize)
	keyBytes := 0
	if value.Type == TpCodeObject {
		for i := uint64(0); i < count; i++ {
			if err := ctx.Err(); err != nil {
				return 0, false, err
			}
			entryOffset := int(uint64(headerSize) + i*uint64(keyEntrySize))
			keyOffset := uint64(binary.LittleEndian.Uint32(data[entryOffset:]))
			keyLength := uint64(binary.LittleEndian.Uint16(data[entryOffset+keyOriginOff:]))
			if keyOffset < uint64(payloadStart) || keyOffset > uint64(len(data)) || keyLength > uint64(len(data))-keyOffset {
				return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON object key")
			}
			if keyLength > uint64(math.MaxInt-keyBytes) {
				return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
			}
			if !consumeStoragePreflightWork(remaining, keyLength) {
				return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container serialized work")
			}
			keyBytes += int(keyLength)
		}
	}

	if keyBytes > math.MaxInt-int(minimumSize) {
		return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
	}
	canonicalSize := int(minimumSize) + keyBytes
	expands := false
	for i := uint64(0); i < count; i++ {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		entryOffset := valueTableStart + int(i)*valEntrySize
		childType := TpCode(data[entryOffset])
		if childType == TpCodeLiteral {
			literal := data[entryOffset+valTypeSize]
			if literal < LiteralNull || literal > LiteralFalse {
				return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON literal type")
			}
			continue
		}
		childOffset := uint64(binary.LittleEndian.Uint32(data[entryOffset+valTypeSize:]))
		if childOffset < uint64(payloadStart) || childOffset >= uint64(len(data)) {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON value offset")
		}
		child, ok := storageCompatibleChild(childType, data[childOffset:])
		if !ok {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container value")
		}
		if childType != TpCodeArray && childType != TpCodeObject && !consumeStoragePreflightWork(remaining, uint64(len(child.Data))) {
			return 0, false, moerr.NewInvalidInputNoCtx("invalid JSON container serialized work")
		}
		childSize, childExpands, err := storageCompatibleDataSizeWithBudget(ctx, child, limit, depth+1, remaining)
		if err != nil {
			return 0, false, err
		}
		if childType != TpCodeLiteral {
			if childSize > math.MaxInt-canonicalSize {
				return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
			}
			canonicalSize += childSize
			if canonicalSize > limit {
				return 0, false, jsonTableCellLimitError()
			}
		}
		expands = expands || childExpands
	}
	if expands {
		if canonicalSize > math.MaxUint32 {
			return 0, false, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
		}
		return canonicalSize, true, nil
	}
	return len(data), false, nil
}

func consumeStoragePreflightWork(remaining *uint64, size uint64) bool {
	if remaining == nil || size > *remaining {
		return false
	}
	*remaining -= size
	return true
}

func storageCompatibleChild(tp TpCode, data []byte) (ByteJson, bool) {
	switch tp {
	case TpCodeInt64, TpCodeUint64, TpCodeFloat64:
		if len(data) < numberSize {
			return ByteJson{}, false
		}
		return ByteJson{Type: tp, Data: data[:numberSize]}, true
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob, TpCodeOpaque, TpCodeBit:
		payloadLen, prefixLen := binary.Uvarint(data)
		if prefixLen <= 0 || payloadLen > uint64(len(data)-prefixLen) || payloadLen > uint64(math.MaxInt-prefixLen) {
			return ByteJson{}, false
		}
		total := prefixLen + int(payloadLen)
		return ByteJson{Type: tp, Data: data[:total]}, true
	case TpCodeArray, TpCodeObject:
		if len(data) < headerSize {
			return ByteJson{}, false
		}
		documentSize := uint64(binary.LittleEndian.Uint32(data[docSizeOff:]))
		if documentSize < headerSize || documentSize > uint64(len(data)) || documentSize > math.MaxInt {
			return ByteJson{}, false
		}
		return ByteJson{Type: tp, Data: data[:int(documentSize)]}, true
	default:
		return ByteJson{}, false
	}
}

func validStorageStringData(data []byte) bool {
	_, prefixLen := binary.Uvarint(data)
	if prefixLen <= 0 {
		return false
	}
	payloadLen, _ := binary.Uvarint(data)
	return prefixLen == storageUvarintSize(payloadLen) && payloadLen == uint64(len(data)-prefixLen)
}

func storageStringPayload(data []byte) ([]byte, bool) {
	payloadLen, prefixLen := binary.Uvarint(data)
	if prefixLen <= 0 || prefixLen != storageUvarintSize(payloadLen) || payloadLen != uint64(len(data)-prefixLen) {
		return nil, false
	}
	return data[prefixLen:], true
}

func checkedBase64EncodedLen(inputLen int) (int, error) {
	if inputLen > math.MaxInt-2 {
		return 0, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
	}
	groups := (inputLen + 2) / 3
	if groups > math.MaxInt/4 {
		return 0, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell size overflows int")
	}
	return base64.StdEncoding.EncodedLen(inputLen), nil
}

func storageUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func storageSizeWithinLimit(size, limit int, expands bool) (int, bool, error) {
	if size > limit {
		return 0, false, jsonTableCellLimitError()
	}
	return size, expands, nil
}

// Build finalizes and transfers ownership of the encoded array. A builder
// cannot be appended to after Build; Close may still be called by cleanup code.
func (b *JSONTableArrayBuilder) Build() (ByteJson, error) {
	if b == nil {
		return ByteJson{}, moerr.NewInvalidStateNoCtx("nil JSON_TABLE array builder")
	}
	if b.done {
		return ByteJson{}, moerr.NewInvalidStateNoCtx("JSON_TABLE array builder is closed")
	}
	if b.count == 0 {
		return ByteJson{}, moerr.NewInvalidStateNoCtx("cannot build an empty JSON_TABLE array")
	}
	dataLen := headerSize + len(b.entries) + len(b.payload)
	if dataLen > math.MaxUint32 {
		return ByteJson{}, moerr.NewInvalidInputNoCtx("JSON_TABLE JSON cell exceeds uint32 storage size")
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
