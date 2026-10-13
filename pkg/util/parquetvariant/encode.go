// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquetvariant

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

// Encode emits unshredded VARIANT v1. It preserves the numerical value of
// uint64 through DECIMAL16 when INT64 is insufficient. Decimals must fit 38
// digits and scale 0..38; unsupported values fail instead of becoming strings.
// The integer width and decimal lexical spelling are not round-trip contracts.
func Encode(ctx context.Context, bj bytejson.ByteJson) (metadata, value []byte, err error) {
	if err = context.Cause(ctx); err != nil {
		return nil, nil, err
	}
	if len(bj.Data) > maxDecodedBytes || !bytejson.IsValidByteJson(bj) {
		return nil, nil, fmt.Errorf("invalid or oversized MO JSON for Parquet VARIANT")
	}
	keys := make(map[string]int)
	// Match the decoder's conservative expansion budget before allocating
	// output tables. Otherwise a wide, small-valued document could be exported
	// successfully but rejected when read back by the same codec.
	remaining := maxDecodedBytes
	reserve := func(n int) error {
		if n > remaining {
			return fmt.Errorf("Parquet VARIANT expansion exceeds 64 MiB limit")
		}
		remaining -= n
		return nil
	}
	var collect func(bytejson.ByteJson) error
	collect = func(v bytejson.ByteJson) error {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := reserve(10); err != nil {
			return err
		}
		switch v.Type {
		case bytejson.TpCodeObject:
			if err := reserve(v.GetElemCnt() * 64); err != nil {
				return err
			}
			prev := ""
			for i := 0; i < v.GetElemCnt(); i++ {
				key := string(v.GetObjectKey(i))
				if !utf8.ValidString(key) || i > 0 && prev >= key {
					return fmt.Errorf("Parquet VARIANT requires unique sorted UTF-8 object keys")
				}
				prev = key
				if err := reserve(len(key)); err != nil {
					return err
				}
				if _, exists := keys[key]; !exists {
					if err := reserve(16 + len(key)); err != nil {
						return err
					}
				}
				keys[key] = 0
				if err := reserve(max(len(v.GetObjectVal(i).Data), 64)); err != nil {
					return err
				}
				if err := collect(v.GetObjectVal(i)); err != nil {
					return err
				}
			}
		case bytejson.TpCodeArray:
			if err := reserve(v.GetElemCnt() * 64); err != nil {
				return err
			}
			for i := 0; i < v.GetElemCnt(); i++ {
				if err := reserve(max(len(v.GetArrayElem(i).Data), 64)); err != nil {
					return err
				}
				if err := collect(v.GetArrayElem(i)); err != nil {
					return err
				}
			}
		case bytejson.TpCodeString, bytejson.TpCodeBlob, bytejson.TpCodeOpaque:
			if err := reserve(len(v.GetString())); err != nil {
				return err
			}
		}
		return nil
	}
	if err = collect(bj); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(keys))
	for key := range keys {
		names = append(names, key)
	}
	slices.Sort(names)
	var dictionaryBytes uint32
	for _, key := range names {
		dictionaryBytes += uint32(len(key))
	}
	width := variantUintWidth(max(dictionaryBytes, uint32(len(names))))
	metadata = appendVariantUint([]byte{0x11 | byte(width-1)<<6}, uint32(len(names)), width)
	var offset uint32
	for id, key := range names {
		keys[key] = id
		metadata = appendVariantUint(metadata, offset, width)
		offset += uint32(len(key))
	}
	metadata = appendVariantUint(metadata, offset, width)
	for _, key := range names {
		metadata = append(metadata, key...)
	}
	value, err = encodeValue(ctx, nil, bj, keys)
	if err != nil {
		return nil, nil, err
	}
	if len(metadata) > maxDecodedBytes || len(value) > maxDecodedBytes {
		return nil, nil, fmt.Errorf("Parquet VARIANT encoding exceeds 64 MiB limit")
	}
	return metadata, value, nil
}

func encodeValue(ctx context.Context, out []byte, v bytejson.ByteJson, keys map[string]int) ([]byte, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if len(out) > maxDecodedBytes {
		return nil, fmt.Errorf("Parquet VARIANT encoding exceeds 64 MiB limit")
	}
	switch v.Type {
	case bytejson.TpCodeLiteral:
		switch v.Data[0] {
		case bytejson.LiteralNull:
			return append(out, 0), nil
		case bytejson.LiteralTrue:
			return append(out, 4), nil
		default:
			return append(out, 8), nil
		}
	case bytejson.TpCodeInt64:
		return appendVariantInt(out, v.GetInt64()), nil
	case bytejson.TpCodeUint64:
		n := v.GetUint64()
		if n <= math.MaxInt64 {
			return appendVariantInt(out, int64(n)), nil
		}
		return appendDecimal(out, strconv.FormatUint(n, 10))
	case bytejson.TpCodeFloat64:
		return binary.LittleEndian.AppendUint64(append(out, 7<<2), math.Float64bits(v.GetFloat64())), nil
	case bytejson.TpCodeDecimal:
		return appendDecimal(out, string(v.GetString()))
	case bytejson.TpCodeString:
		data := v.GetString()
		if !utf8.Valid(data) {
			return nil, fmt.Errorf("Parquet VARIANT requires UTF-8 strings")
		}
		if len(data) < 64 {
			return append(append(out, byte(len(data)<<2)|1), data...), nil
		}
		out = binary.LittleEndian.AppendUint32(append(out, 16<<2), uint32(len(data)))
		return append(out, data...), nil
	case bytejson.TpCodeBlob, bytejson.TpCodeOpaque, bytejson.TpCodeBit:
		canonical, ok := bytejson.AppendCanonicalBinary(nil, v)
		if !ok || canonical[1] != 252 {
			return nil, fmt.Errorf("MO JSON binary subtype has no lossless Parquet VARIANT mapping")
		}
		data := canonical[2:]
		out = binary.LittleEndian.AppendUint32(append(out, 15<<2), uint32(len(data)))
		return append(out, data...), nil
	case bytejson.TpCodeDate, bytejson.TpCodeDatetime, bytejson.TpCodeTime:
		layout := "2006-01-02"
		if v.Type == bytejson.TpCodeDatetime {
			layout = "2006-01-02 15:04:05.999999"
		} else if v.Type == bytejson.TpCodeTime {
			layout = "15:04:05.999999"
		}
		t, err := time.ParseInLocation(layout, string(v.GetString()), time.UTC)
		if err != nil || v.Type != bytejson.TpCodeTime && (t.Year() < 1 || t.Year() > 9999) || t.Nanosecond()%1000 != 0 {
			return nil, fmt.Errorf("MO JSON temporal value cannot be represented as Parquet VARIANT microseconds")
		}
		if v.Type == bytejson.TpCodeDate {
			return binary.LittleEndian.AppendUint32(append(out, 11<<2), uint32(t.Unix()/86400)), nil
		}
		if v.Type == bytejson.TpCodeTime {
			micros := int64(t.Hour()*3600+t.Minute()*60+t.Second())*1e6 + int64(t.Nanosecond()/1000)
			return binary.LittleEndian.AppendUint64(append(out, 17<<2), uint64(micros)), nil
		}
		return binary.LittleEndian.AppendUint64(append(out, 13<<2), uint64(t.UnixMicro())), nil
	case bytejson.TpCodeObject, bytejson.TpCodeArray:
		start := len(out)
		n := v.GetElemCnt()
		object := v.Type == bytejson.TpCodeObject
		header := byte(3 | (7 << 2)) // array, large count, four-byte offsets
		if object {
			header = 2 | (31 << 2) // object, large count, four-byte IDs/offsets
		}
		out = binary.LittleEndian.AppendUint32(append(out, header), uint32(n))
		if object {
			for i := 0; i < n; i++ {
				out = binary.LittleEndian.AppendUint32(out, uint32(keys[string(v.GetObjectKey(i))]))
			}
		}
		table := len(out)
		out = append(out, make([]byte, (n+1)*4)...)
		dataStart := len(out)
		for i := 0; i < n; i++ {
			binary.LittleEndian.PutUint32(out[table+i*4:], uint32(len(out)-dataStart))
			var child bytejson.ByteJson
			if object {
				child = v.GetObjectVal(i)
			} else {
				child = v.GetArrayElem(i)
			}
			var err error
			out, err = encodeValue(ctx, out, child, keys)
			if err != nil {
				return nil, err
			}
		}
		binary.LittleEndian.PutUint32(out[table+n*4:], uint32(len(out)-dataStart))
		return compactVariantContainer(out, start, table, dataStart, n, object), nil
	default:
		return nil, fmt.Errorf("unsupported MO JSON type %d for Parquet VARIANT", v.Type)
	}
}

// Standard VARIANT supports one through four byte unsigned offsets/IDs.
func variantUintWidth(n uint32) int {
	switch {
	case n <= math.MaxUint8:
		return 1
	case n <= math.MaxUint16:
		return 2
	case n <= 0xffffff:
		return 3
	default:
		return 4
	}
}

func appendVariantUint(out []byte, n uint32, width int) []byte {
	for i := 0; i < width; i++ {
		out = append(out, byte(n>>(8*i)))
	}
	return out
}

func appendVariantInt(out []byte, n int64) []byte {
	switch {
	case n >= math.MinInt8 && n <= math.MaxInt8:
		return append(out, 3<<2, byte(n))
	case n >= math.MinInt16 && n <= math.MaxInt16:
		return binary.LittleEndian.AppendUint16(append(out, 4<<2), uint16(n))
	case n >= math.MinInt32 && n <= math.MaxInt32:
		return binary.LittleEndian.AppendUint32(append(out, 5<<2), uint32(n))
	default:
		return binary.LittleEndian.AppendUint64(append(out, 6<<2), uint64(n))
	}
}

// Children are encoded after a temporary four-byte table. Compact that table
// in place once payload length is known. Every write is at/before its old
// table entry, and the payload moves only after all old entries are consumed.
func compactVariantContainer(out []byte, start, table, dataStart, count int, object bool) []byte {
	cw, iw := 1, 0
	if count > math.MaxUint8 {
		cw = 4
	}
	if object {
		var maxID uint32
		for i := 0; i < count; i++ {
			maxID = max(maxID, binary.LittleEndian.Uint32(out[start+5+i*4:]))
		}
		iw = variantUintWidth(maxID)
	}
	ow := variantUintWidth(uint32(len(out) - dataStart))
	header := byte(ow - 1)
	if object {
		header |= byte(iw-1) << 2
		if cw == 4 {
			header |= 16
		}
		out[start] = header<<2 | 2
	} else {
		if cw == 4 {
			header |= 4
		}
		out[start] = header<<2 | 3
	}
	pos := start + 1
	for i := 0; i < cw; i++ {
		out[pos+i] = byte(uint32(count) >> (8 * i))
	}
	pos += cw
	if object {
		for i := 0; i < count; i++ {
			id := binary.LittleEndian.Uint32(out[start+5+i*4:])
			for j := 0; j < iw; j++ {
				out[pos+j] = byte(id >> (8 * j))
			}
			pos += iw
		}
	}
	for i := 0; i <= count; i++ {
		offset := binary.LittleEndian.Uint32(out[table+i*4:])
		for j := 0; j < ow; j++ {
			out[pos+j] = byte(offset >> (8 * j))
		}
		pos += ow
	}
	payloadSize := copy(out[pos:], out[dataStart:])
	return out[:pos+payloadSize]
}

func appendDecimal(out []byte, text string) ([]byte, error) {
	negative := strings.HasPrefix(text, "-")
	if negative {
		text = text[1:]
	}
	exponent := 0
	if pos := strings.IndexAny(text, "Ee"); pos >= 0 {
		expText := text[pos+1:]
		text = text[:pos]
		// Never allocate big.Int powers for an unbounded JSON exponent.
		if len(strings.TrimLeft(expText, "+-0")) > 6 {
			return nil, fmt.Errorf("decimal exponent exceeds Parquet VARIANT range")
		}
		var err error
		exponent, err = strconv.Atoi(expText)
		if err != nil || exponent < -100000 || exponent > 100000 {
			return nil, fmt.Errorf("decimal exponent exceeds Parquet VARIANT range")
		}
	}
	scale := -exponent
	if pos := strings.IndexByte(text, '.'); pos >= 0 {
		scale += len(text) - pos - 1
		text = text[:pos] + text[pos+1:]
	}
	text = strings.TrimLeft(text, "0")
	if text == "" {
		text, scale = "0", max(0, min(scale, 38))
	} else {
		// Reduce trailing zeros only if needed to fit the maximum precision
		// or scale. Numerical equality is retained; lexical spelling is not.
		for (scale > 38 || len(text) > 38) && scale > 0 && strings.HasSuffix(text, "0") {
			text, scale = text[:len(text)-1], scale-1
		}
		if scale < 0 {
			if len(text)-scale > 38 {
				return nil, fmt.Errorf("decimal exceeds Parquet VARIANT precision 38")
			}
			text += strings.Repeat("0", -scale)
			scale = 0
		}
	}
	if len(text) > 38 || scale > 38 {
		return nil, fmt.Errorf("decimal exceeds Parquet VARIANT precision/scale 38")
	}
	n, ok := new(big.Int).SetString(text, 10)
	if !ok {
		return nil, fmt.Errorf("invalid MO JSON decimal")
	}
	width, tag := 16, byte(10)
	// Include scale when choosing width so consumers with a conventional
	// DECIMAL(precision,scale) representation can also represent tiny values.
	precision := max(len(text), scale)
	if precision <= 9 {
		width, tag = 4, 8
	} else if precision <= 18 {
		width, tag = 8, 9
	}
	if negative && n.Sign() != 0 {
		n.Sub(new(big.Int).Lsh(big.NewInt(1), uint(width*8)), n)
	}
	data := make([]byte, width)
	n.FillBytes(data)
	slices.Reverse(data)
	return append(append(out, tag<<2, byte(scale)), data...), nil
}
