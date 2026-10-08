// Copyright 2022 Matrix Origin
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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/defines"
	json2 "github.com/segmentio/encoding/json"
)

func (bj ByteJson) String() string {
	ret, _ := bj.MarshalJSON()
	return string(ret)
}

func (bj ByteJson) Unquote() (string, error) {
	if bj.Type == TpCodeBlob {
		if _, _, ok := mysqlOpaqueValue(bj.GetString()); ok {
			return string(bj.GetString()), nil
		}
		if payload, ok := bj.persistedBitPayload(); ok {
			return base64.StdEncoding.EncodeToString(payload), nil
		}
		return string(bj.GetString()), nil
	}
	if bj.Type == TpCodeOpaque || bj.Type == TpCodeBit {
		return base64.StdEncoding.EncodeToString(bj.GetString()), nil
	}
	// Binary JSON stores string payload bytes without JSON representation
	// delimiters or escapes. Do not infer delimiters from the payload itself:
	// a valid string value may begin and end with a double quote.
	if bj.Type == TpCodeString {
		return string(bj.GetString()), nil
	}
	if bj.Type != TpCodeDate &&
		bj.Type != TpCodeTime &&
		bj.Type != TpCodeDatetime {
		return bj.String(), nil
	}
	str := bj.GetString()
	if len(str) < 2 || (str[0] != '"' || str[len(str)-1] != '"') {
		return string(str), nil
	}
	str = str[1 : len(str)-1]
	var sb strings.Builder
	for i := 0; i < len(str); i++ {
		if str[i] != '\\' {
			sb.WriteByte(str[i])
			continue
		}
		i++
		if trans, ok := escapedChars[str[i]]; ok {
			sb.WriteByte(trans)
			continue
		}
		if str[i] == 'u' { // transform unicode to utf8
			if i+4 > len(str) {
				return "", moerr.NewInvalidInputNoCtx("invalid unicode")
			}
			unicodeStr := string(str[i-1 : i+5])
			content := strings.Replace(strconv.Quote(unicodeStr), `\\u`, `\u`, -1)
			text, err := strconv.Unquote(content)
			if err != nil {
				return "", moerr.NewInvalidInputNoCtx("invalid unicode")
			}
			sb.WriteString(text)
			i += 4
			continue
		}
		sb.WriteByte(str[i])
	}
	return sb.String(), nil
}

// MarshalJSON transform bytejson to []byte,for visible
func (bj ByteJson) MarshalJSON() ([]byte, error) {
	ret := make([]byte, 0, len(bj.Data)*3/2)
	return bj.to(ret)
}

// Marshal transform bytejson to []byte,for storage
func (bj ByteJson) Marshal() ([]byte, error) {
	stored, err := bj.StorageCompatible()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(stored.Data)+1)
	buf[0] = byte(stored.Type)
	copy(buf[1:], stored.Data)
	return buf, nil
}

// StorageCompatible returns a representation that only uses type codes known
// before TpCodeOpaque and TpCodeBit were introduced. The receiver is returned
// unchanged when it is already safe to persist.
func (bj ByteJson) StorageCompatible() (ByteJson, error) {
	if !bj.requiresLegacyBinaryEncoding() {
		return bj, nil
	}
	tp, data, err := appendLegacyCompatibleJSON(nil, bj)
	if err != nil {
		return ByteJson{}, err
	}
	return ByteJson{Type: tp, Data: data}, nil
}

// Unmarshal transform storage []byte  to bytejson
func (bj *ByteJson) Unmarshal(buf []byte) error {
	//TODO add validate checker
	bj.Type = TpCode(buf[0])
	bj.Data = buf[1:]
	return nil
}

// UnmarshalJSON transform visible []byte to bytejson
func (bj *ByteJson) UnmarshalJSON(data []byte) error {
	bs, err := ParseJsonByte(data)
	if err != nil {
		return err
	}
	bj.Data = bs[1:]
	bj.Type = TpCode(bs[0])
	return nil
}

func (bj *ByteJson) unmarshalJSONWithDepthLimit(data []byte, maxDepth int) error {
	bs, err := parseJsonByte(data, maxDepth)
	if err != nil {
		return err
	}
	bj.Data = bs[1:]
	bj.Type = TpCode(bs[0])
	return nil
}

func (bj ByteJson) IsNull() bool {
	return bj.Type == TpCodeLiteral && bj.Data[0] == LiteralNull
}

func (bj ByteJson) GetElemCnt() int {
	return int(endian.Uint32(bj.Data))
}

func (bj ByteJson) GetInt64() int64 {
	return int64(bj.GetUint64())
}

func (bj ByteJson) GetUint64() uint64 {
	return endian.Uint64(bj.Data)
}

func (bj ByteJson) GetFloat64() float64 {
	switch bj.Type {
	case TpCodeInt64:
		return float64(bj.GetInt64())
	case TpCodeUint64:
		return float64(bj.GetUint64())
	default:
		return math.Float64frombits(bj.GetUint64())
	}
}

func (bj ByteJson) GetString() []byte {
	num, length := calStrLen(bj.Data)
	return bj.Data[length : length+num]
}

func (bj ByteJson) to(buf []byte) ([]byte, error) {
	var err error
	switch bj.Type {
	case TpCodeArray:
		buf, err = bj.toArray(buf)
	case TpCodeObject:
		buf, err = bj.toObject(buf)
	case TpCodeInt64:
		buf = bj.toInt64(buf)
	case TpCodeUint64:
		buf = bj.toUint64(buf)
	case TpCodeLiteral:
		buf = bj.toLiteral(buf)
	case TpCodeFloat64:
		buf, err = bj.toFloat64(buf)
	case TpCodeString:
		buf, err = bj.toString(buf)
	case TpCodeDecimal:
		data := bj.GetString()
		buf = append(buf, data...)
	case TpCodeDate, TpCodeTime, TpCodeDatetime:
		buf = append(buf, '"')
		data := bj.GetString()
		buf = append(buf, data...)
		buf = append(buf, '"')
	case TpCodeBlob:
		buf = append(buf, '"')
		data := bj.GetString()
		if _, _, ok := mysqlOpaqueValue(data); ok {
			buf = append(buf, data...)
		} else if payload, ok := bj.persistedBitPayload(); ok {
			start := len(buf)
			buf = append(buf, make([]byte, base64.StdEncoding.EncodedLen(len(payload)))...)
			base64.StdEncoding.Encode(buf[start:], payload)
		} else {
			buf = append(buf, data...)
		}
		buf = append(buf, '"')
	case TpCodeOpaque, TpCodeBit:
		buf = append(buf, '"')
		data := bj.GetString()
		start := len(buf)
		buf = append(buf, make([]byte, base64.StdEncoding.EncodedLen(len(data)))...)
		base64.StdEncoding.Encode(buf[start:], data)
		buf = append(buf, '"')
	default:
		err = moerr.NewInvalidInputNoCtxf("invalid json type '%v'", bj.Type)
	}
	return buf, err
}

func (bj ByteJson) toArray(buf []byte) ([]byte, error) {
	cnt := bj.GetElemCnt()
	buf = append(buf, '[')
	var err error
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		buf, err = bj.getArrayElem(i).to(buf)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, ']'), nil
}

func (bj ByteJson) toObject(buf []byte) ([]byte, error) {
	cnt := bj.GetElemCnt()
	buf = append(buf, '{')
	for i := 0; i < cnt; i++ {
		if i != 0 {
			buf = append(buf, ", "...)
		}
		var err error
		buf, err = toString(buf, bj.getObjectKey(i))
		if err != nil {
			return nil, err
		}
		buf = append(buf, ": "...)
		buf, err = bj.getObjectVal(i).to(buf)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, '}'), nil
}

func (bj ByteJson) toInt64(buf []byte) []byte {
	return strconv.AppendInt(buf, bj.GetInt64(), 10)
}

func (bj ByteJson) toUint64(buf []byte) []byte {
	return strconv.AppendUint(buf, bj.GetUint64(), 10)
}

func (bj ByteJson) toLiteral(buf []byte) []byte {
	litTp := bj.Data[0]
	switch litTp {
	case LiteralNull:
		buf = append(buf, "null"...)
	case LiteralTrue:
		buf = append(buf, "true"...)
	case LiteralFalse:
		buf = append(buf, "false"...)
	default:
		panic(fmt.Sprintf("invalid literal type:%d", litTp))
	}
	return buf
}

func (bj ByteJson) toFloat64(buf []byte) ([]byte, error) {
	f := bj.GetFloat64()
	err := checkFloat64(f)
	if err != nil {
		return nil, err
	}
	// https://github.com/golang/go/issues/14135
	var format byte
	abs := math.Abs(f)
	if abs == 0 || 1e-6 <= abs && abs < 1e21 {
		format = 'f'
	} else {
		format = 'e'
	}
	buf = strconv.AppendFloat(buf, f, format, -1, 64)
	return buf, nil
}

// transform byte string to visible string
func (bj ByteJson) toString(buf []byte) ([]byte, error) {
	data := bj.GetString()
	return toString(buf, data)
}

func (bj ByteJson) getObjectKey(i int) []byte {
	keyOff := int(endian.Uint32(bj.Data[headerSize+i*keyEntrySize:]))
	keyLen := int(endian.Uint16(bj.Data[headerSize+i*keyEntrySize+keyOriginOff:]))
	return bj.Data[keyOff : keyOff+keyLen]
}

// GetObjectKey returns the key at index i of a JSON object.
// Keys are stored in sorted order (binary search is used for lookups).
func (bj ByteJson) GetObjectKey(i int) []byte {
	return bj.getObjectKey(i)
}

// GetObjectVal returns the value at index i of a JSON object.
func (bj ByteJson) GetObjectVal(i int) ByteJson {
	return bj.getObjectVal(i)
}

// GetArrayElem returns the element at index i of a JSON array.
func (bj ByteJson) GetArrayElem(i int) ByteJson {
	return bj.getArrayElem(i)
}

func (bj ByteJson) getArrayElem(i int) ByteJson {
	return bj.getValEntry(headerSize + i*valEntrySize)
}

func (bj ByteJson) getObjectVal(i int) ByteJson {
	cnt := bj.GetElemCnt()
	return bj.getValEntry(headerSize + cnt*keyEntrySize + i*valEntrySize)
}

func (bj ByteJson) getValEntry(off int) ByteJson {
	tpCode := bj.Data[off]
	valOff := endian.Uint32(bj.Data[off+valTypeSize:])
	switch TpCode(tpCode) {
	case TpCodeLiteral:
		return ByteJson{Type: TpCodeLiteral, Data: bj.Data[off+valTypeSize : off+valTypeSize+1]}
	case TpCodeUint64, TpCodeInt64, TpCodeFloat64:
		return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+numberSize]}
	case TpCodeString, TpCodeDecimal, TpCodeDate, TpCodeTime, TpCodeDatetime, TpCodeBlob, TpCodeOpaque, TpCodeBit:
		num, length := calStrLen(bj.Data[valOff:])
		totalLen := uint32(num) + uint32(length)
		return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+totalLen]}
	}
	dataBytes := endian.Uint32(bj.Data[valOff+docSizeOff:])
	return ByteJson{Type: TpCode(tpCode), Data: bj.Data[valOff : valOff+dataBytes]}
}

const (
	persistedBitPrefix         = "~mo:json-bit:v1:"
	mysqlOpaquePrefix          = "base64:type"
	MySQLOpaqueProtocolVersion = defines.MORPCVersion52
)

// NewMySQLOpaque creates the JSON representation MySQL uses for binary SQL
// values after the cluster-wide protocol admission has completed. The caller
// must pass the minimum protocol version supported by every executing CN;
// older readers compare tagged and legacy payloads differently.
func NewMySQLOpaque(protocolVersion int64, fieldType uint8, payload []byte) (ByteJson, error) {
	if protocolVersion < MySQLOpaqueProtocolVersion {
		return ByteJson{}, moerr.NewNotSupportedNoCtxf(
			"MySQL binary JSON type tags require MORPC protocol version %d",
			MySQLOpaqueProtocolVersion)
	}
	return ByteJson{
		Type: TpCodeBlob,
		Data: appendBinaryString(nil, mysqlOpaqueText(fieldType, payload)),
	}, nil
}

func mysqlOpaqueText(fieldType uint8, payload []byte) string {
	encodedLen := base64.StdEncoding.EncodedLen(len(payload))
	buf := make([]byte, 0, len(mysqlOpaquePrefix)+3+1+encodedLen)
	buf = append(buf, mysqlOpaquePrefix...)
	buf = strconv.AppendUint(buf, uint64(fieldType), 10)
	buf = append(buf, ':')
	start := len(buf)
	buf = append(buf, make([]byte, encodedLen)...)
	base64.StdEncoding.Encode(buf[start:], payload)
	return string(buf)
}

// mysqlOpaqueValue parses only the bounded tag header. Consumers validate or
// decode the returned payload once for their own terminal operation.
func mysqlOpaqueValue(payload []byte) (uint8, []byte, bool) {
	if !bytes.HasPrefix(payload, []byte(mysqlOpaquePrefix)) {
		return 0, nil, false
	}
	rest := payload[len(mysqlOpaquePrefix):]
	fieldType := uint16(0)
	for i, digit := range rest {
		if digit == ':' {
			if i == 0 {
				return 0, nil, false
			}
			return uint8(fieldType), rest[i+1:], true
		}
		if i == 3 {
			return 0, nil, false
		}
		if digit < '0' || digit > '9' {
			return 0, nil, false
		}
		n := uint16(digit - '0')
		if fieldType > (255-n)/10 {
			return 0, nil, false
		}
		fieldType = fieldType*10 + n
	}
	return 0, nil, false
}

func (bj ByteJson) isPersistedBit() bool {
	if bj.Type != TpCodeBlob {
		return false
	}
	payload := bj.GetString()
	if fieldType, encoded, ok := mysqlOpaqueValue(payload); ok {
		if fieldType != 16 {
			return false
		}
		_, valid := base64DecodedLen(encoded)
		return valid
	}
	if !bytes.HasPrefix(payload, []byte(persistedBitPrefix)) {
		return false
	}
	_, valid := base64DecodedLen(payload[len(persistedBitPrefix):])
	return valid
}

func (bj ByteJson) persistedBitPayload() ([]byte, bool) {
	if bj.Type != TpCodeBlob {
		return nil, false
	}
	payload := bj.GetString()
	if fieldType, encoded, ok := mysqlOpaqueValue(payload); ok && fieldType == 16 {
		raw, err := base64.StdEncoding.DecodeString(string(encoded))
		return raw, err == nil
	}
	if !bytes.HasPrefix(payload, []byte(persistedBitPrefix)) {
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(string(payload[len(persistedBitPrefix):]))
	if err != nil {
		return nil, false
	}
	return raw, true
}

func (bj ByteJson) requiresLegacyBinaryEncoding() bool {
	switch bj.Type {
	case TpCodeOpaque, TpCodeBit:
		return true
	case TpCodeArray:
		for i := 0; i < bj.GetElemCnt(); i++ {
			if bj.getArrayElem(i).requiresLegacyBinaryEncoding() {
				return true
			}
		}
	case TpCodeObject:
		for i := 0; i < bj.GetElemCnt(); i++ {
			if bj.getObjectVal(i).requiresLegacyBinaryEncoding() {
				return true
			}
		}
	}
	return false
}

// appendLegacyCompatibleJSON writes only type codes understood by readers
// predating TpCodeOpaque and TpCodeBit. Opaque values use the legacy BLOB
// representation; BIT values keep the legacy sentinel so existing CAST AS JSON
// display remains unchanged. Constructors that require MySQL's type16 tag use
// NewMySQLOpaque directly and therefore do not enter this conversion.
func appendLegacyCompatibleJSON(buf []byte, bj ByteJson) (TpCode, []byte, error) {
	switch bj.Type {
	case TpCodeOpaque:
		encoded := base64.StdEncoding.EncodeToString(bj.GetString())
		return TpCodeBlob, appendBinaryString(buf, encoded), nil
	case TpCodeBit:
		encoded := persistedBitPrefix + base64.StdEncoding.EncodeToString(bj.GetString())
		return TpCodeBlob, appendBinaryString(buf, encoded), nil
	case TpCodeArray:
		data, err := appendLegacyCompatibleArray(buf, bj)
		return TpCodeArray, data, err
	case TpCodeObject:
		data, err := appendLegacyCompatibleObject(buf, bj)
		return TpCodeObject, data, err
	default:
		return bj.Type, append(buf, bj.Data...), nil
	}
}

func appendLegacyCompatibleArray(buf []byte, bj ByteJson) ([]byte, error) {
	docOff := len(buf)
	count := bj.GetElemCnt()
	buf = appendUint32(buf, uint32(count))
	buf = appendZero(buf, docSizeOff)
	valEntryBegin := len(buf)
	buf = appendZero(buf, count*valEntrySize)
	for i := 0; i < count; i++ {
		var err error
		buf, err = appendLegacyCompatibleValueEntry(buf, docOff, valEntryBegin+i*valEntrySize, bj.getArrayElem(i))
		if err != nil {
			return nil, err
		}
	}
	endian.PutUint32(buf[docOff+docSizeOff:], uint32(len(buf)-docOff))
	return buf, nil
}

func appendLegacyCompatibleObject(buf []byte, bj ByteJson) ([]byte, error) {
	docOff := len(buf)
	count := bj.GetElemCnt()
	buf = appendUint32(buf, uint32(count))
	buf = appendZero(buf, docSizeOff)
	keyEntryBegin := len(buf)
	buf = appendZero(buf, count*keyEntrySize)
	valEntryBegin := len(buf)
	buf = appendZero(buf, count*valEntrySize)
	for i := 0; i < count; i++ {
		key := bj.getObjectKey(i)
		keyEntryOff := keyEntryBegin + i*keyEntrySize
		endian.PutUint32(buf[keyEntryOff:], uint32(len(buf)-docOff))
		endian.PutUint16(buf[keyEntryOff+keyOriginOff:], uint16(len(key)))
		buf = append(buf, key...)
	}
	for i := 0; i < count; i++ {
		var err error
		buf, err = appendLegacyCompatibleValueEntry(buf, docOff, valEntryBegin+i*valEntrySize, bj.getObjectVal(i))
		if err != nil {
			return nil, err
		}
	}
	endian.PutUint32(buf[docOff+docSizeOff:], uint32(len(buf)-docOff))
	return buf, nil
}

func appendLegacyCompatibleValueEntry(buf []byte, docOff, valEntryOff int, bj ByteJson) ([]byte, error) {
	elemOff := len(buf)
	tp, buf, err := appendLegacyCompatibleJSON(buf, bj)
	if err != nil {
		return nil, err
	}
	buf[valEntryOff] = byte(tp)
	if tp == TpCodeLiteral {
		buf[valEntryOff+valTypeSize] = buf[elemOff]
		return buf[:elemOff], nil
	}
	endian.PutUint32(buf[valEntryOff+valTypeSize:], uint32(elemOff-docOff))
	return buf, nil
}

type binaryJSONSubtype uint8

const (
	binaryJSONBit  binaryJSONSubtype = 16
	binaryJSONBlob binaryJSONSubtype = 252
)

const (
	binaryJSONCompareEncodedChunkSize = 4 * 1024
	binaryJSONCompareDecodedChunkSize = binaryJSONCompareEncodedChunkSize / 4 * 3
)

type binaryJSONValueView struct {
	subtype         binaryJSONSubtype
	rawPayload      []byte
	legacyEncoded   []byte
	fallbackRaw     []byte
	needsValidation bool
}

func binaryJSONValue(bj ByteJson) (binaryJSONValueView, bool) {
	if !isValidByteJsonStringEncoding(bj.Data) {
		return binaryJSONValueView{}, false
	}
	switch bj.Type {
	case TpCodeBlob:
		payload := bj.GetString()
		if fieldType, encoded, ok := mysqlOpaqueValue(payload); ok {
			return binaryJSONValueView{
				subtype:         binaryJSONSubtype(fieldType),
				legacyEncoded:   encoded,
				fallbackRaw:     payload,
				needsValidation: true,
			}, true
		}
		if bytes.HasPrefix(payload, []byte(persistedBitPrefix)) {
			encoded := payload[len(persistedBitPrefix):]
			return binaryJSONValueView{
				subtype:         binaryJSONBit,
				legacyEncoded:   encoded,
				fallbackRaw:     payload,
				needsValidation: true,
			}, true
		}
		return binaryJSONValueView{
			subtype:       binaryJSONBlob,
			legacyEncoded: payload,
			fallbackRaw:   payload,
		}, true
	case TpCodeOpaque:
		return binaryJSONValueView{
			subtype:    binaryJSONBlob,
			rawPayload: bj.GetString(),
		}, true
	case TpCodeBit:
		return binaryJSONValueView{
			subtype:    binaryJSONBit,
			rawPayload: bj.GetString(),
		}, true
	default:
		return binaryJSONValueView{}, false
	}
}

func resolveBinaryJSONValue(value binaryJSONValueView) binaryJSONValueView {
	if !value.needsValidation {
		return value
	}
	if _, ok := base64DecodedLen(value.legacyEncoded); ok {
		value.needsValidation = false
		return value
	}
	return binaryJSONValueView{
		subtype:    binaryJSONBlob,
		rawPayload: value.fallbackRaw,
	}
}

func binaryJSONFallbackBytes(value binaryJSONValueView) []byte {
	if value.fallbackRaw != nil {
		return value.fallbackRaw
	}
	if value.legacyEncoded != nil {
		return value.legacyEncoded
	}
	return value.rawPayload
}

func compareBinaryJSONValues(left, right binaryJSONValueView) (int, bool) {
	if left.subtype != right.subtype {
		return int(left.subtype) - int(right.subtype), true
	}
	switch {
	case left.legacyEncoded != nil && right.legacyEncoded != nil:
		if cmp, ok := compareDecodedBase64Payloads(left.legacyEncoded, right.legacyEncoded); ok {
			return cmp, true
		}
	case left.legacyEncoded != nil:
		if cmp, ok := compareDecodedBase64WithRaw(left.legacyEncoded, right.rawPayload); ok {
			return cmp, true
		}
	case right.legacyEncoded != nil:
		if cmp, ok := compareDecodedBase64WithRaw(right.legacyEncoded, left.rawPayload); ok {
			return -cmp, true
		}
	default:
		return bytes.Compare(left.rawPayload, right.rawPayload), true
	}
	return bytes.Compare(binaryJSONFallbackBytes(left), binaryJSONFallbackBytes(right)), false
}

// CompareBinaryJSON compares opaque JSON values by their MySQL subtype and
// original bytes. TpCodeBlob is the legacy BLOB encoding and aliases Opaque.
func CompareBinaryJSON(left, right ByteJson) (int, bool) {
	leftValue, leftOK := binaryJSONValue(left)
	rightValue, rightOK := binaryJSONValue(right)
	if !leftOK || !rightOK {
		return 0, false
	}
	if leftValue.needsValidation || rightValue.needsValidation {
		if leftValue.subtype == rightValue.subtype {
			if cmp, ok := compareBinaryJSONValues(leftValue, rightValue); ok {
				return cmp, true
			}
		}
		leftValue = resolveBinaryJSONValue(leftValue)
		rightValue = resolveBinaryJSONValue(rightValue)
	}
	cmp, _ := compareBinaryJSONValues(leftValue, rightValue)
	return cmp, true
}

// BinaryJSONPayloadLen returns the decoded byte length of an opaque JSON
// value. It keeps legacy TpCodeBlob values compatible with new raw payloads.
func BinaryJSONPayloadLen(bj ByteJson) (int, bool) {
	value, ok := binaryJSONValue(bj)
	if !ok {
		return 0, false
	}
	if value.legacyEncoded == nil {
		return len(value.rawPayload), true
	}
	if n, ok := base64DecodedLen(value.legacyEncoded); ok {
		return n, true
	}
	return len(value.fallbackRaw), true
}

const canonicalBinaryMarker byte = 0x84

// CanonicalBinarySize returns the exact equality-key size for a binary JSON
// value. Legacy Base64 BLOB/BIT representations and their raw successors use
// one subtype-plus-payload domain.
func CanonicalBinarySize(bj ByteJson) (int, bool) {
	value, ok := binaryJSONValue(bj)
	if !ok {
		return 0, false
	}
	payloadSize := len(value.rawPayload)
	if value.legacyEncoded != nil {
		if decodedSize, valid := base64DecodedLen(value.legacyEncoded); valid {
			payloadSize = decodedSize
		} else {
			payloadSize = len(value.fallbackRaw)
		}
	}
	return 2 + payloadSize, true
}

// AppendCanonicalBinary appends the equality key for a binary JSON value.
// Valid legacy payloads are decoded directly into dst; malformed legacy data
// retains CompareBinaryJSON's raw fallback behavior.
func AppendCanonicalBinary(dst []byte, bj ByteJson) ([]byte, bool) {
	value, ok := binaryJSONValue(bj)
	if !ok {
		return dst, false
	}
	start := len(dst)
	dst = append(dst, canonicalBinaryMarker, byte(value.subtype))
	if value.legacyEncoded == nil {
		return append(dst, value.rawPayload...), true
	}
	var decodedBuffer [binaryJSONCompareDecodedChunkSize]byte
	for offset := 0; offset < len(value.legacyEncoded); {
		n, nextOffset, decoded := decodeBase64Chunk(
			value.legacyEncoded,
			offset,
			decodedBuffer[:],
		)
		if !decoded {
			dst = dst[:start]
			dst = append(dst, canonicalBinaryMarker, byte(binaryJSONBlob))
			return append(dst, value.fallbackRaw...), true
		}
		dst = append(dst, decodedBuffer[:n]...)
		offset = nextOffset
	}
	return dst, true
}

func compareDecodedBase64Payloads(leftEncoded, rightEncoded []byte) (int, bool) {
	var leftBuf [binaryJSONCompareDecodedChunkSize]byte
	var rightBuf [binaryJSONCompareDecodedChunkSize]byte
	var leftEncOff, rightEncOff int
	var leftN, rightN int
	var leftOff, rightOff int
	for {
		if leftOff == leftN && leftEncOff < len(leftEncoded) {
			n, nextOff, ok := decodeBase64Chunk(leftEncoded, leftEncOff, leftBuf[:])
			if !ok {
				return 0, false
			}
			leftEncOff, leftN, leftOff = nextOff, n, 0
		}
		if rightOff == rightN && rightEncOff < len(rightEncoded) {
			n, nextOff, ok := decodeBase64Chunk(rightEncoded, rightEncOff, rightBuf[:])
			if !ok {
				return 0, false
			}
			rightEncOff, rightN, rightOff = nextOff, n, 0
		}
		leftAvail := leftN - leftOff
		rightAvail := rightN - rightOff
		if leftAvail == 0 || rightAvail == 0 {
			switch {
			case leftAvail == 0 && rightAvail == 0:
				if leftEncOff == len(leftEncoded) && rightEncOff == len(rightEncoded) {
					return 0, true
				}
				continue
			case leftAvail == 0 && leftEncOff == len(leftEncoded):
				return -1, true
			case rightAvail == 0 && rightEncOff == len(rightEncoded):
				return 1, true
			default:
				continue
			}
		}

		chunkLen := leftAvail
		if rightAvail < chunkLen {
			chunkLen = rightAvail
		}
		if cmp := bytes.Compare(leftBuf[leftOff:leftOff+chunkLen], rightBuf[rightOff:rightOff+chunkLen]); cmp != 0 {
			return cmp, true
		}
		leftOff += chunkLen
		rightOff += chunkLen
	}
}

func compareDecodedBase64WithRaw(encoded, raw []byte) (int, bool) {
	var decodedBuf [binaryJSONCompareDecodedChunkSize]byte
	var encodedOff, rawOff int
	var decodedN, decodedOff int
	for {
		if decodedOff == decodedN && encodedOff < len(encoded) {
			n, nextOff, ok := decodeBase64Chunk(encoded, encodedOff, decodedBuf[:])
			if !ok {
				return 0, false
			}
			encodedOff, decodedN, decodedOff = nextOff, n, 0
		}
		decodedAvail := decodedN - decodedOff
		rawAvail := len(raw) - rawOff
		if decodedAvail == 0 || rawAvail == 0 {
			switch {
			case decodedAvail == 0 && rawAvail == 0:
				if encodedOff == len(encoded) {
					return 0, true
				}
				continue
			case decodedAvail == 0 && encodedOff == len(encoded):
				return -1, true
			case rawAvail == 0:
				return 1, true
			default:
				continue
			}
		}
		chunkLen := decodedAvail
		if rawAvail < chunkLen {
			chunkLen = rawAvail
		}
		if cmp := bytes.Compare(decodedBuf[decodedOff:decodedOff+chunkLen], raw[rawOff:rawOff+chunkLen]); cmp != 0 {
			return cmp, true
		}
		decodedOff += chunkLen
		rawOff += chunkLen
	}
}

func base64DecodedLen(encoded []byte) (int, bool) {
	var buf [binaryJSONCompareDecodedChunkSize]byte
	total := 0
	for off := 0; off < len(encoded); {
		n, nextOff, ok := decodeBase64Chunk(encoded, off, buf[:])
		if !ok {
			return 0, false
		}
		total += n
		off = nextOff
	}
	return total, true
}

func decodeBase64Chunk(encoded []byte, offset int, dst []byte) (int, int, bool) {
	if offset >= len(encoded) {
		return 0, offset, true
	}
	end := base64ChunkEnd(encoded, offset)
	n, err := base64.StdEncoding.Decode(dst, encoded[offset:end])
	if err != nil {
		return 0, offset, false
	}
	return n, end, true
}

// base64ChunkEnd selects a complete Base64 quantum. EncodeJson emits compact
// Base64, but DecodeString historically also accepted CR/LF in legacy values;
// retaining quantum alignment preserves that behavior while keeping decoding
// bounded by the fixed comparison buffer.
func base64ChunkEnd(encoded []byte, offset int) int {
	limit := offset + binaryJSONCompareEncodedChunkSize
	if limit >= len(encoded) {
		return len(encoded)
	}
	if bytes.IndexByte(encoded[offset:limit], '\r') == -1 &&
		bytes.IndexByte(encoded[offset:limit], '\n') == -1 {
		return limit
	}
	end := offset
	base64Chars := 0
	for i := offset; i < len(encoded); i++ {
		if encoded[i] != '\r' && encoded[i] != '\n' {
			base64Chars++
			if base64Chars%4 == 0 {
				end = i + 1
			}
		}
		if i+1 >= limit && end != offset {
			return end
		}
	}
	return len(encoded)
}

func (bj ByteJson) queryValByKey(key []byte) ByteJson {
	val, ok := bj.queryValByKeyExists(key)
	if !ok {
		return Null
	}
	return val
}

func (bj ByteJson) queryValByKeyExists(key []byte) (ByteJson, bool) {
	cnt := bj.GetElemCnt()
	var idx int
	if cnt < binarySearchCutoff {
		for i := 0; i < cnt; i++ {
			k := bj.getObjectKey(i)
			if bytes.Compare(k, key) >= 0 {
				idx = i
				break
			}
		}
	} else {
		idx = sort.Search(cnt, func(i int) bool {
			k := bj.getObjectKey(i)
			return bytes.Compare(k, key) >= 0
		})
	}

	if idx >= cnt || !bytes.Equal(bj.getObjectKey(idx), key) {
		return Null, false
	}
	return bj.getObjectVal(idx), true
}

func (bj ByteJson) query(cur []ByteJson, path *Path) []ByteJson {
	if path.empty() {
		cur = append(cur, bj)
		return cur
	}
	sub, nPath := path.step()

	if sub.tp == subPathDoubleStar {
		cur = bj.query(cur, &nPath)
		if bj.Type == TpCodeObject {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				cur = bj.getObjectVal(i).query(cur, path) // take care here, the argument is path,not nPath
			}
		} else if bj.Type == TpCodeArray {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				cur = bj.getArrayElem(i).query(cur, path) // take care here, the argument is path,not nPath
			}
		}
		return cur
	}

	if bj.Type == TpCodeObject {
		switch sub.tp {
		case subPathIdx:
			start, _, _ := sub.idx.genIndex(1)
			if start == 0 {
				cur = bj.query(cur, &nPath)
			}
		case subPathRange:
			if sub.iRange.matchesIndex(0, 1) {
				cur = bj.query(cur, &nPath)
			}
		case subPathKey:
			tmp, exists := bj.queryValByKeyExists(util.UnsafeStringToBytes(sub.key))
			if exists {
				cur = tmp.query(cur, &nPath)
			}
		case subPathKeyWildcard:
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				cur = bj.getObjectVal(i).query(cur, &nPath)
			}
		}
		return cur
	}

	if bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			idx, _, last := sub.idx.genIndex(cnt)
			if last && idx < 0 || cnt <= idx {
				return cur
			}
			if idx == subPathIdxALL {
				for i := 0; i < cnt; i++ {
					cur = bj.getArrayElem(i).query(cur, &nPath)
				}
			} else {
				cur = bj.getArrayElem(idx).query(cur, &nPath)
			}
		case subPathRange:
			if cnt == 0 {
				return cur
			}
			se := sub.iRange.genRange(cnt)
			if se[0] < 0 || se[1] < 0 {
				return cur
			}
			for i := se[0]; i <= se[1]; i++ {
				cur = bj.getArrayElem(i).query(cur, &nPath)
			}
		}
		return cur
	}
	if sub.tp == subPathIdx {
		idx, _, _ := sub.idx.genIndex(1)
		if idx == 0 {
			return bj.query(cur, &nPath)
		}
	}
	if sub.tp == subPathRange && sub.iRange.matchesIndex(0, 1) {
		return bj.query(cur, &nPath)
	}
	return cur
}

func (bj ByteJson) Query(paths []*Path) ByteJson {
	result, _ := bj.QueryWithExists(paths)
	return result
}

// QueryWithExists returns the selected JSON value and whether any path matched.
// A matched JSON literal null is a value and therefore returns exists=true.
func (bj ByteJson) QueryWithExists(paths []*Path) (ByteJson, bool) {
	out := make([]ByteJson, 0, len(paths))
	for _, path := range paths {
		tmp := bj.query(nil, path)
		if len(tmp) > 0 {
			out = append(out, tmp...)
		}
	}
	if len(out) == 0 {
		return Null, false
	}
	if len(out) == 1 && len(paths) == 1 && !paths[0].mayReturnMultiple() {
		return out[0], true
	}
	return mergeToArray(out), true
}

// PathExists reports whether path selects at least one JSON value. A selected
// JSON literal null counts as an existing path. Array index zero autowraps
// non-array values to match JSON_CONTAINS_PATH semantics.
func (bj ByteJson) PathExists(path *Path) bool {
	return bj.pathExists(path)
}

func (bj ByteJson) pathExists(path *Path) bool {
	if path.empty() {
		return true
	}

	sub, nextPath := path.step()
	if sub.tp == subPathDoubleStar {
		if bj.pathExists(&nextPath) {
			return true
		}
		if bj.Type == TpCodeObject {
			for i := 0; i < bj.GetElemCnt(); i++ {
				if bj.getObjectVal(i).pathExists(path) {
					return true
				}
			}
		} else if bj.Type == TpCodeArray {
			for i := 0; i < bj.GetElemCnt(); i++ {
				if bj.getArrayElem(i).pathExists(path) {
					return true
				}
			}
		}
		return false
	}

	switch bj.Type {
	case TpCodeObject:
		switch sub.tp {
		case subPathIdx:
			idx, _, _ := sub.idx.genIndex(1)
			return idx == 0 && bj.pathExists(&nextPath)
		case subPathRange:
			return sub.iRange.matchesIndex(0, 1) && bj.pathExists(&nextPath)
		case subPathKey:
			value, ok := bj.queryValByKeyExists(util.UnsafeStringToBytes(sub.key))
			return ok && value.pathExists(&nextPath)
		case subPathKeyWildcard:
			for i := 0; i < bj.GetElemCnt(); i++ {
				if bj.getObjectVal(i).pathExists(&nextPath) {
					return true
				}
			}
		}
	case TpCodeArray:
		count := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			idx, _, last := sub.idx.genIndex(count)
			if (last && idx < 0) || count <= idx {
				return false
			}
			if idx == subPathIdxALL {
				for i := 0; i < count; i++ {
					if bj.getArrayElem(i).pathExists(&nextPath) {
						return true
					}
				}
				return false
			}
			return bj.getArrayElem(idx).pathExists(&nextPath)
		case subPathRange:
			for i := 0; i < count; i++ {
				if sub.iRange.matchesIndex(i, count) && bj.getArrayElem(i).pathExists(&nextPath) {
					return true
				}
			}
		}
	default:
		if sub.tp == subPathIdx {
			idx, _, _ := sub.idx.genIndex(1)
			return idx == 0 && bj.pathExists(&nextPath)
		}
		if sub.tp == subPathRange {
			return sub.iRange.matchesIndex(0, 1) && bj.pathExists(&nextPath)
		}
	}

	return false
}

func (bj ByteJson) querySimple(path *Path) ByteJson {
	val, ok := bj.querySimpleExist(path, false)
	if !ok {
		return Null
	}
	return val
}

// QuerySimpleExist returns the value at a simple path and whether the path exists.
func (bj ByteJson) QuerySimpleExist(path *Path) (ByteJson, bool) {
	return bj.querySimpleExist(path, false)
}

// QuerySimpleContainPath returns the value at a simple path using JSON_CONTAINS
// scalar-[0] autowrap semantics for scalar array-index access.
func (bj ByteJson) QuerySimpleContainPath(path *Path) (ByteJson, bool) {
	return bj.querySimpleExist(path, true)
}

func (bj ByteJson) querySimpleExist(path *Path, autowrapScalarIndex bool) (ByteJson, bool) {
	cur := bj
	// don't go through th step(), recursive call route.  We know
	// we have a simple path, each step will bring us to ONE SINGLE next value.

	for _, sub := range path.paths {
		if cur.Type == TpCodeObject {
			switch sub.tp {
			case subPathIdx:
				// obj[0] is itself, continue
				start, _, _ := sub.idx.genIndex(1)
				if start != 0 {
					return Null, false
				}
			case subPathKey:
				var ok bool
				cur, ok = cur.queryValByKeyExists(util.UnsafeStringToBytes(sub.key))
				if !ok {
					return Null, false
				}
			default:
				return Null, false
			}
		} else if cur.Type == TpCodeArray {
			if sub.tp != subPathIdx {
				return Null, false
			}
			cnt := cur.GetElemCnt()
			idx, _, _ := sub.idx.genIndex(cnt)
			// don't bother checking last -- idx < 0 and not last means the path
			// is not valid, we should have caught this earlier.
			// if (last && idx < 0) || cnt <= idx {
			if idx < 0 || cnt <= idx {
				// out of range
				return Null, false
			} else {
				cur = cur.getArrayElem(idx)
			}
		} else {
			if autowrapScalarIndex && sub.tp == subPathIdx {
				idx, _, _ := sub.idx.genIndex(1)
				if idx == 0 {
					continue
				}
			}
			return Null, false
		}
	}
	return cur, true
}

func (bj ByteJson) QuerySimple(paths []*Path) ByteJson {
	result, _ := bj.QuerySimpleWithExists(paths)
	return result
}

// QuerySimpleWithExists is the simple-path variant of QueryWithExists.
func (bj ByteJson) QuerySimpleWithExists(paths []*Path) (ByteJson, bool) {
	if len(paths) == 0 {
		// not retrieve anything
		return Null, false
	} else if len(paths) == 1 {
		// only retrieve one path
		return bj.querySimpleExist(paths[0], true)
	} else {
		// retrieve multiple paths, merge them into an array
		out := make([]ByteJson, 0, len(paths))
		for _, path := range paths {
			tmp, exists := bj.querySimpleExist(path, true)
			if exists {
				out = append(out, tmp)
			}
		}
		if len(out) == 0 {
			return Null, false
		}
		return mergeToArray(out), true
	}
}

func (bj ByteJson) Modify(pathList []*Path, valList []ByteJson, modifyType JsonModifyType) (ByteJson, error) {
	var (
		err error
	)

	if len(pathList) != len(valList) {
		return Null, moerr.NewInvalidInputNoCtx("pathList and valList should have the same length")
	}

	if len(pathList) == 0 {
		return bj, nil
	}

	for _, path := range pathList {
		if path == nil || !path.IsSimple() {
			return Null, moerr.NewInvalidInputNoCtx("path expression is not simple")
		}
	}

	for i := 0; i < len(pathList); i++ {
		path := pathList[i]
		val := valList[i]

		modifier := &bytejsonModifier{bj: bj}

		switch modifyType {
		case JsonModifySet:
			bj, err = modifier.set(path, val)
		case JsonModifyInsert:
			bj, err = modifier.insert(path, val)
		case JsonModifyReplace:
			bj, err = modifier.replace(path, val)
		case JsonModifyArrayAppend:
			bj, err = modifier.arrayAppend(path, val)
		default:
			return Null, moerr.NewInvalidInputNoCtx("invalid modify type")
		}

		if err != nil {
			return Null, err
		}
	}
	return bj, nil
}

func (bj ByteJson) Remove(pathList []*Path) (ByteJson, error) {
	if len(pathList) == 0 {
		return bj, nil
	}

	for _, path := range pathList {
		if path == nil || path.empty() || !path.IsSimple() {
			return Null, moerr.NewInvalidInputNoCtx("path expression is not simple")
		}
	}

	var err error
	for _, path := range pathList {
		modifier := &bytejsonModifier{bj: bj}
		bj, err = modifier.remove(path)
		if err != nil {
			return Null, err
		}
	}
	return bj, nil
}

func (bj ByteJson) canUnnest() bool {
	return bj.Type == TpCodeArray || bj.Type == TpCodeObject
}

func (bj ByteJson) queryWithSubPath(keys []string, vals []ByteJson, path *Path, pathStr string) ([]string, []ByteJson) {
	if path.empty() {
		keys = append(keys, pathStr)
		vals = append(vals, bj)
		return keys, vals
	}
	sub, nPath := path.step()
	if sub.tp == subPathDoubleStar {
		keys, vals = bj.queryWithSubPath(keys, vals, &nPath, pathStr)
		if bj.Type == TpCodeObject {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s.%s", pathStr, bj.getObjectKey(i))
				keys, vals = bj.getObjectVal(i).queryWithSubPath(keys, vals, path, newPathStr) // take care here, the argument is path,not nPath
			}
		} else if bj.Type == TpCodeArray {
			cnt := bj.GetElemCnt()
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
				keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, path, newPathStr) // take care here, the argument is path,not nPath
			}
		}
		return keys, vals
	}
	if bj.Type == TpCodeObject {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			start, _, _ := sub.idx.genIndex(1)
			if start == 0 {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, start)
				keys, vals = bj.queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathRange:
			se := sub.iRange.genRange(cnt)
			if se[0] == 0 {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, se[0])
				keys, vals = bj.queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathKey:
			tmp := bj.queryValByKey(util.UnsafeStringToBytes(sub.key))
			newPathStr := fmt.Sprintf("%s.%s", pathStr, sub.key)
			keys, vals = tmp.queryWithSubPath(keys, vals, &nPath, newPathStr)
		case subPathKeyWildcard:
			for i := 0; i < cnt; i++ {
				newPathStr := fmt.Sprintf("%s.%s", pathStr, bj.getObjectKey(i))
				keys, vals = bj.getObjectVal(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		}
	}
	if bj.Type == TpCodeArray {
		cnt := bj.GetElemCnt()
		switch sub.tp {
		case subPathIdx:
			idx, _, last := sub.idx.genIndex(cnt)
			if last && idx < 0 {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, sub.idx.num)
				keys = append(keys, newPathStr)
				vals = append(vals, tmp)
				return keys, vals
			}
			if idx == subPathIdxALL {
				for i := 0; i < cnt; i++ {
					newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
					keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
				}
			} else {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, idx)
				keys, vals = bj.getArrayElem(idx).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		case subPathRange:
			se := sub.iRange.genRange(cnt)
			if se[0] == subPathIdxErr {
				tmp := ByteJson{Type: TpCodeLiteral, Data: []byte{LiteralNull}}
				newPathStr := fmt.Sprintf("%s[%d to %d]", pathStr, sub.iRange.start.num, sub.iRange.end.num)
				keys = append(keys, newPathStr)
				vals = append(vals, tmp)
				return keys, vals
			}
			for i := se[0]; i <= se[1]; i++ {
				newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
				keys, vals = bj.getArrayElem(i).queryWithSubPath(keys, vals, &nPath, newPathStr)
			}
		}
	}
	return keys, vals
}

func (bj ByteJson) unnestWithParams(out []UnnestResult, outer, recursive bool, mode string, pathStr string, this []byte, filterMap map[string]struct{}) []UnnestResult {
	if !bj.canUnnest() {
		index, key := genIndexOrKey(pathStr)
		tmp := UnnestResult{}
		genUnnestResult(tmp, index, key, util.UnsafeStringToBytes(pathStr), &bj, this, filterMap)
		out = append(out, tmp)
		return out
	}
	if bj.Type == TpCodeObject && mode != "array" {
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			key := bj.getObjectKey(i)
			val := bj.getObjectVal(i)
			newPathStr := fmt.Sprintf("%s.%s", pathStr, key)
			tmp := UnnestResult{}
			genUnnestResult(tmp, nil, key, util.UnsafeStringToBytes(newPathStr), &val, this, filterMap)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				dt, _ := val.Marshal()
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, dt, filterMap)
			}
		}
	}
	if bj.Type == TpCodeArray && mode != "object" {
		cnt := bj.GetElemCnt()
		for i := 0; i < cnt; i++ {
			val := bj.getArrayElem(i)
			newPathStr := fmt.Sprintf("%s[%d]", pathStr, i)
			tmp := UnnestResult{}
			genUnnestResult(tmp, util.UnsafeStringToBytes(strconv.Itoa(i)), nil, util.UnsafeStringToBytes(newPathStr), &val, this, filterMap)
			out = append(out, tmp)
			if val.canUnnest() && recursive {
				dt, _ := val.Marshal()
				out = val.unnestWithParams(out, outer, recursive, mode, newPathStr, dt, filterMap)
			}
		}
	}
	return out
}

func (bj ByteJson) unnest(out []UnnestResult, path *Path, outer, recursive bool, mode string, filterMap map[string]struct{}) ([]UnnestResult, int, error) {

	keys := make([]string, 0, 1)
	vals := make([]ByteJson, 0, 1)
	keys, vals = bj.queryWithSubPath(keys, vals, path, "$")
	if len(keys) != len(vals) {
		return nil, 0, moerr.NewInvalidInputNoCtxf("len(key) and len(val) are not equal, len(key)=%d, len(val)=%d", len(keys), len(vals))
	}
	for i := 0; i < len(keys); i++ {
		if vals[i].canUnnest() {
			dt, err := vals[i].Marshal()
			if err != nil {
				return nil, 0, err
			}
			out = vals[i].unnestWithParams(out, outer, recursive, mode, keys[i], dt, filterMap)
		}
	}
	if len(out) == 0 && outer {
		for i := 0; i < len(keys); i++ {
			tmp := UnnestResult{}
			out = append(out, tmp)
		}
		if _, ok := filterMap["path"]; ok {
			for i := 0; i < len(keys); i++ {
				out[i]["path"] = util.UnsafeStringToBytes(keys[i])
			}
		}
		if _, ok := filterMap["this"]; ok {
			for i := 0; i < len(vals); i++ {
				dt, err := vals[i].Marshal()
				if err != nil {
					return nil, 0, err
				}
				out[i]["this"] = dt
			}
		}

	}
	return out, len(keys), nil
}

// Unnest returns a slice of UnnestResult, each UnnestResult contains filtered data, if param filters is nil, return all fields.
func (bj ByteJson) Unnest(path *Path, outer, recursive bool, mode string, filterMap map[string]struct{}) ([]UnnestResult, int, error) {
	if !checkMode(mode) {
		return nil, 0, moerr.NewInvalidInputNoCtx("mode must be one of [object, array, both]")
	}
	out := make([]UnnestResult, 0, 1)
	out, thiscnt, err := bj.unnest(out, path, outer, recursive, mode, filterMap)
	return out, thiscnt, err
}

func genUnnestResult(res UnnestResult, index, key, path []byte, value *ByteJson, this []byte, filterMap map[string]struct{}) UnnestResult {
	if _, ok := filterMap["index"]; ok {
		res["index"] = index
	}
	if _, ok := filterMap["key"]; ok {
		res["key"] = key
	}
	if _, ok := filterMap["path"]; ok {
		res["path"] = path
	}
	if _, ok := filterMap["value"]; ok {
		dt, _ := value.Marshal()
		res["value"] = dt
	}
	if _, ok := filterMap["this"]; ok {
		res["this"] = this
	}
	return res
}

func ParseJsonByteFromString(s string) ([]byte, error) {
	return ParseJsonByte(util.UnsafeStringToBytes(s))
}

func ParseJsonByte(data []byte) ([]byte, error) {
	return parseJsonByte(data, 0)
}

func parseJsonByte(data []byte, maxDepth int) ([]byte, error) {
	n, err := parseNode(data, maxDepth)
	if err != nil {
		return nil, err
	}
	w := byteJsonWriter{
		buf: make([]byte, 0, len(data)*2),
	}
	_, _, err = w.writeNode(true, n)
	n.Free()
	if err != nil {
		return nil, err
	}
	return w.buf, nil
}

func ParseNodeString(s string) (Node, error) {
	return ParseNode(util.UnsafeStringToBytes(s))
}

func ParseNode(data []byte) (Node, error) {
	return parseNode(data, 0)
}

func parseNode(data []byte, maxDepth int) (Node, error) {
	p := parser{src: data, maxDepth: maxDepth}
	return p.do()
}

type parser struct {
	src      []byte
	stack    []*Group
	tz       *json2.Tokenizer
	state    func(*parser) int
	top      Node
	maxDepth int
	depthErr error
}

func (p *parser) do() (Node, error) {
	p.stack = make([]*Group, 0, 2)
	p.tz = json2.NewTokenizer(p.src)
	p.state = (*parser).stateBeginValue
	var z Node
	for {
		if !p.tz.Next() {
			for _, g := range p.stack {
				g.free()
			}
			return z, io.ErrUnexpectedEOF
		}
		switch p.state(p) {
		case scanError:
			for _, g := range p.stack {
				g.free()
			}
			if p.depthErr != nil {
				return z, p.depthErr
			}
			if errors.Is(p.tz.Err, io.EOF) {
				return z, io.ErrUnexpectedEOF
			}
			var se *json.SyntaxError
			if p.tz.Remaining() == 0 && errors.As(p.tz.Err, &se) {
				return z, io.ErrUnexpectedEOF
			}
			return z, moerr.NewInternalErrorNoCtxf("parse json: %v", p.tz.Err)
		case scanEnd:
			if p.tz.Next() {
				p.top.Free()
				p.top = Node{}
				return z, moerr.NewInvalidInputNoCtxf("invalid json: %s", p.src)
			}
			if p.tz.Err != nil {
				p.top.Free()
				p.top = Node{}
				return z, moerr.NewInternalErrorNoCtxf("parse json: %v", p.tz.Err)
			}
			return p.top, nil
		}
	}
}

const (
	scanContinue = iota
	scanEnd      // top-level value ended *before* this byte; known to be first "stop" result
	scanError    // hit an error, scanner.err.
)

func (p *parser) stateBeginValue() int {
	k := p.tz.Kind()

	switch k.Class() {
	case json2.Array:
		if !p.openGroup(k) {
			return scanError
		}
		p.state = (*parser).stateBeginValueOrEmpty
		return scanContinue
	case json2.Object:
		if !p.openGroup(k) {
			return scanError
		}
		p.state = (*parser).stateObjectKeyOrEmpty
		return scanContinue
	}

	var n Node
	switch k.Class() {
	case json2.String:
		n = Node{string(p.tz.String())}
	case json2.Num:
		n = Node{json.Number(p.tz.Value)}
	case json2.Bool:
		n = Node{p.tz.Bool()}
	case json2.Null:
		n = Node{nil}
	default:
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: looking for beginning of value")
		return scanError
	}
	if p.tz.Depth != 0 {
		p.appendToLastGroup(n)
		p.state = (*parser).stateEndValue
		return scanContinue
	}

	p.top = n
	p.state = nil
	return scanEnd
}

func (p *parser) stateBeginValueOrEmpty() int {
	if p.tz.Delim == ']' {
		return p.stateEndValue()
	}

	return p.stateBeginValue()
}

func (p *parser) stateObjectKey() int {
	if p.tz.Kind().Class() != json2.String || !p.tz.IsKey {
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: object key")
		return scanError
	}

	g := p.stack[len(p.stack)-1]
	g.Keys = append(g.Keys, string(p.tz.String()))
	p.state = (*parser).stateColon
	return scanContinue
}

func (p *parser) stateObjectKeyOrEmpty() int {
	if p.tz.Delim == '}' {
		return p.stateEndValue()
	}

	return p.stateObjectKey()
}

func (p *parser) stateColon() int {
	if p.tz.Delim != ':' {
		p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: after object key")
		return scanError
	}
	p.state = (*parser).stateBeginValue
	return scanContinue
}

func (p *parser) stateEndValue() int {
	if p.tz.Delim == ']' || p.tz.Delim == '}' {
		p.closeGroup()
		if p.tz.Depth == 0 {
			p.state = nil
			return scanEnd
		}
		p.state = (*parser).stateEndValue
		return scanContinue
	}

	if p.tz.Delim == ',' {
		g := p.stack[len(p.stack)-1]
		if g.Obj {
			p.state = (*parser).stateObjectKey
		} else {
			p.state = (*parser).stateBeginValue
		}
		return scanContinue
	}

	p.tz.Err = moerr.NewInvalidInputNoCtx("invalid json: end value")
	return scanError
}

func (p *parser) openGroup(k json2.Kind) bool {
	if p.maxDepth > 0 && len(p.stack) >= p.maxDepth {
		p.depthErr = newJSONDocumentDepthError(p.maxDepth)
		p.tz.Err = p.depthErr
		return false
	}
	g := reuse.Alloc[Group](nil)
	g.Obj = k == json2.Object
	p.stack = append(p.stack, g)
	return true
}

func (p *parser) closeGroup() {
	n := len(p.stack) - 1
	g := p.stack[n]
	p.stack = p.stack[:n]

	if g.Obj {
		g.sortKeys()
	}

	if len(p.stack) == 0 {
		p.top = Node{g}
		return
	}
	p.appendToLastGroup(Node{g})
}

func (p *parser) appendToLastGroup(n Node) {
	g := p.stack[len(p.stack)-1]
	if !g.Obj || len(g.Keys) <= 1 {
		g.Values = append(g.Values, n)
		return
	}

	last := len(g.Keys) - 1
	dupIdx := slices.Index(g.Keys[:last], g.Keys[last])
	if dupIdx < 0 {
		g.Values = append(g.Values, n)
		return
	}
	old := g.Values[dupIdx]
	old.Free()
	g.Keys = g.Keys[:last]
	g.Values[dupIdx] = n
}

func init() {
	reuse.CreatePool[Group](
		func() *Group {
			return &Group{}
		},
		func(g *Group) { g.reset() },
		reuse.DefaultOptions[Group](),
	)
}

type Group struct {
	Obj    bool
	Keys   []string
	Values []Node
}

func (g Group) TypeName() string {
	return "bytejson.group"
}

func (g *Group) reset() {
	g.Obj = false
	g.Keys = g.Keys[:0]
	g.Values = g.Values[:0]
}

func (g *Group) free() {
	g.Obj = false
	g.Keys = g.Keys[:0]
	for _, sub := range g.Values {
		sg, ok := sub.V.(*Group)
		if !ok {
			continue
		}
		sg.free()
	}
	g.Values = g.Values[:0]
	reuse.Free(g, nil)
}

func (g *Group) sortKeys() {
	sort.Sort((*groupSortKeys)(g))
}

type groupSortKeys Group

func (g *groupSortKeys) Len() int { return len(g.Keys) }

func (g *groupSortKeys) Less(i, j int) bool { return g.Keys[i] < g.Keys[j] }

func (g *groupSortKeys) Swap(i, j int) {
	g.Keys[i], g.Keys[j] = g.Keys[j], g.Keys[i]
	g.Values[i], g.Values[j] = g.Values[j], g.Values[i]
}

type byteJsonWriter struct {
	buf []byte
}

func (w *byteJsonWriter) writeNode(root bool, node Node) (TpCode, uint32, error) {
	start := len(w.buf)
	switch val := node.V.(type) {
	case *Group:
		if val.Obj {
			obj := val
			keys := obj.Keys
			n := len(keys)
			baseOffset := start
			if root {
				w.buf = append(w.buf, byte(TpCodeObject))
				baseOffset += valTypeSize
			}
			w.buf = endian.AppendUint32(w.buf, uint32(n))
			w.buf = endian.AppendUint32(w.buf, 0) // object buf length

			w.buf = extendByte(w.buf, n*(keyEntrySize+valEntrySize))

			loc := uint32(headerSize + n*(keyEntrySize+valEntrySize))
			for i, k := range keys {
				o := baseOffset + headerSize + i*keyEntrySize
				length := uint32(len(k))
				if length > math.MaxUint16 {
					return 0, 0, moerr.NewInvalidInputNoCtxf("json key %s", k)
				}
				endian.PutUint32(w.buf[o:], loc)
				endian.PutUint16(w.buf[o+keyOriginOff:], uint16(length))
				loc += length
				w.buf = append(w.buf, k...)
			}

			for i := range keys {
				tp, length, err := w.writeNode(false, obj.Values[i])
				if err != nil {
					return 0, 0, err
				}
				o := baseOffset + headerSize + n*keyEntrySize + i*valEntrySize
				w.buf[o] = byte(tp)
				if tp == TpCodeLiteral {
					endian.PutUint32(w.buf[o+valTypeSize:], length)
					continue
				}
				endian.PutUint32(w.buf[o+valTypeSize:], loc)
				loc += length
			}

			endian.PutUint32(w.buf[baseOffset+4:], loc) // object buf length
			return TpCodeObject, uint32(len(w.buf) - start), nil
		}

		arr := val
		n := len(arr.Values)
		baseOffset := start
		if root {
			w.buf = append(w.buf, byte(TpCodeArray))
			baseOffset++
		}
		w.buf = endian.AppendUint32(w.buf, uint32(n))
		w.buf = endian.AppendUint32(w.buf, 0) // array buf length
		w.buf = extendByte(w.buf, n*5)

		loc := uint32(headerSize + n*valEntrySize)
		for i := range arr.Values {
			tp, length, err := w.writeNode(false, arr.Values[i])
			if err != nil {
				return 0, 0, err
			}
			o := baseOffset + headerSize + i*valEntrySize
			w.buf[o] = byte(tp)
			if tp == TpCodeLiteral {
				endian.PutUint32(w.buf[o+valTypeSize:], length)
				continue
			}
			endian.PutUint32(w.buf[o+valTypeSize:], loc)
			loc += length
		}

		endian.PutUint32(w.buf[baseOffset+4:], loc) // array buf length
		return TpCodeArray, uint32(len(w.buf) - start), nil
	case bool:
		lit := LiteralFalse
		if val {
			lit = LiteralTrue
		}
		if root {
			w.buf = append(w.buf, byte(TpCodeLiteral), lit)
		}
		return TpCodeLiteral, uint32(lit), nil
	case nil:
		if root {
			w.buf = append(w.buf, byte(TpCodeLiteral), LiteralNull)
		}
		return TpCodeLiteral, uint32(LiteralNull), nil
	case json.Number:
		tp, data, err := w.parseNumber(val)
		if err != nil {
			return 0, 0, err
		}
		if root {
			w.buf = append(w.buf, byte(tp))
		}
		w.buf = append(w.buf, data...)
		return tp, uint32(len(w.buf) - start), nil
	case string:
		if root {
			w.buf = append(w.buf, byte(TpCodeString))
		}
		w.buf = addString(w.buf, val)
		return TpCodeString, uint32(len(w.buf) - start), nil
	default:
		return 0, 0, moerr.NewInvalidInputNoCtxf("unknown type %T", node)
	}
}

func (w *byteJsonWriter) parseNumber(in json.Number) (TpCode, []byte, error) {
	var data [8]byte
	//check if it is a float
	if strings.ContainsAny(string(in), "Ee.") {
		val, err := in.Float64()
		if err != nil {
			return TpCodeFloat64, nil, moerr.NewInvalidInputNoCtxf("json number %v", in)
		}
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		endian.PutUint64(data[:], math.Float64bits(val))
		return TpCodeFloat64, data[:], nil
	}
	if val, err := in.Int64(); err == nil { //check if it is an int
		endian.PutUint64(data[:], uint64(val))
		return TpCodeInt64, data[:], nil
	}
	if val, err := strconv.ParseUint(string(in), 10, 64); err == nil { //check if it is a uint
		endian.PutUint64(data[:], val)
		return TpCodeUint64, data[:], nil
	}
	if val, err := in.Float64(); err == nil { //check if it is a float
		if err = checkFloat64(val); err != nil {
			return TpCodeFloat64, nil, err
		}
		endian.PutUint64(data[:], math.Float64bits(val))
		return TpCodeFloat64, data[:], nil
	}
	var tpCode TpCode
	return tpCode, nil, moerr.NewInvalidInputNoCtxf("json number %v", in)
}

type Node struct {
	V any
}

func (n Node) Free() {
	g, ok := n.V.(*Group)
	if ok {
		g.free()
	}
}

func (n Node) ByteJson() (ByteJson, error) {
	buf, err := n.ByteJsonRaw()
	if err != nil {
		return ByteJson{}, err
	}
	return ByteJson{
		Data: buf[1:],
		Type: TpCode(buf[0]),
	}, nil
}

func (n Node) ByteJsonRaw() ([]byte, error) {
	w := byteJsonWriter{}
	_, _, err := w.writeNode(true, n)
	if err != nil {
		return nil, err
	}
	return w.buf, nil
}

func (n Node) String() string {
	switch v := n.V.(type) {
	case *Group:
		if !v.Obj {
			return fmt.Sprint(v.Values)
		}
		m := make(map[string]Node, len(v.Keys))
		for i, key := range v.Keys {
			m[key] = v.Values[i]
		}
		return fmt.Sprint(m)
	default:
		return fmt.Sprint(v)
	}
}
