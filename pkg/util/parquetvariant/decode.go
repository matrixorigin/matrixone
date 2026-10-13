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

// Package parquetvariant encodes and decodes Apache Parquet VARIANT v1.
// It intentionally rejects logical types without a lossless MO JSON mapping.
package parquetvariant

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
)

// Bound dictionary expansion and recursion independently of encoded offsets.
const maxDecodedBytes = 64 << 20

type decoder struct {
	ctx       context.Context
	keys      []string
	remaining int
}

// Decode converts one complete unshredded VARIANT into binary JSON. DECIMAL,
// DATE, local microsecond DATETIME/TIME and raw binary retain their JSON tags.
// UTC timestamps, nanosecond timestamps and UUID have no equivalent JSON tag
// and return an explicit error instead of silently becoming strings.
// Specification: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
func Decode(ctx context.Context, metadata, value []byte) (bytejson.ByteJson, error) {
	if err := context.Cause(ctx); err != nil {
		return bytejson.ByteJson{}, err
	}
	if len(metadata) > maxDecodedBytes || len(value) > maxDecodedBytes {
		return bytejson.ByteJson{}, fmt.Errorf("parquet VARIANT exceeds 64 MiB decoding limit")
	}
	d := decoder{ctx: ctx, remaining: maxDecodedBytes}
	if err := d.metadata(metadata); err != nil {
		return bytejson.ByteJson{}, err
	}
	return d.value(value, 0)
}

func invalid(reason string) error { return fmt.Errorf("malformed parquet VARIANT: %s", reason) }

func uintLE(b []byte, width int) (int, bool) {
	if len(b) < width || width < 1 || width > 4 {
		return 0, false
	}
	var n uint32
	for i := 0; i < width; i++ {
		n |= uint32(b[i]) << (8 * i)
	}
	return int(n), true
}

func (d *decoder) charge(n int) error {
	if n < 0 || n > d.remaining {
		return fmt.Errorf("parquet VARIANT expanded JSON exceeds 64 MiB decoding limit")
	}
	d.remaining -= n
	return nil
}

func (d *decoder) metadata(b []byte) error {
	if len(b) < 3 || b[0]&15 != 1 {
		return invalid("missing metadata or unsupported version")
	}
	w := int(b[0]>>6) + 1
	n, ok := uintLE(b[1:], w)
	if !ok || n > (len(b)-1-w)/w-1 {
		return invalid("truncated dictionary")
	}
	offsets := b[1+w : 1+w+(n+1)*w]
	data := b[1+w+(n+1)*w:]
	first, _ := uintLE(offsets, w)
	last, _ := uintLE(offsets[n*w:], w)
	if first != 0 || last != len(data) {
		return invalid("dictionary offsets do not cover dictionary bytes")
	}
	if err := d.charge(n*16 + len(data)); err != nil {
		return err
	}
	d.keys = make([]string, n)
	start := 0
	for i := range d.keys {
		if i%256 == 0 {
			if err := context.Cause(d.ctx); err != nil {
				return err
			}
		}
		end, _ := uintLE(offsets[(i+1)*w:], w)
		if end < start || end > len(data) || !utf8.Valid(data[start:end]) {
			return invalid("invalid dictionary string or offset")
		}
		d.keys[i] = string(data[start:end])
		if b[0]&16 != 0 && i > 0 && d.keys[i-1] >= d.keys[i] {
			return invalid("sorted dictionary is not sorted and unique")
		}
		start = end
	}
	return nil
}

func typed(tp bytejson.TpCode, text string) bytejson.ByteJson {
	data := binary.AppendUvarint(nil, uint64(len(text)))
	return bytejson.ByteJson{Type: tp, Data: append(data, text...)}
}

func (d *decoder) value(b []byte, depth int) (bytejson.ByteJson, error) {
	if err := context.Cause(d.ctx); err != nil {
		return bytejson.ByteJson{}, err
	}
	if len(b) == 0 {
		return bytejson.ByteJson{}, invalid("empty value")
	}
	if err := d.charge(10); err != nil {
		return bytejson.ByteJson{}, err
	}
	header := int(b[0] >> 2)
	switch b[0] & 3 {
	case 0:
		return d.primitive(header, b[1:])
	case 1:
		if len(b)-1 != header || !utf8.Valid(b[1:]) {
			return bytejson.ByteJson{}, invalid("invalid short string")
		}
		if err := d.charge(header); err != nil {
			return bytejson.ByteJson{}, err
		}
		return bytejson.CreateByteJSON(string(b[1:]))
	case 2, 3:
		if depth >= bytejson.JSONDocumentMaxNestingDepth {
			return bytejson.ByteJson{}, invalid("nesting exceeds 100 containers")
		}
		return d.container(b, depth)
	default:
		panic("unreachable")
	}
}

func (d *decoder) container(b []byte, depth int) (bytejson.ByteJson, error) {
	object := b[0]&3 == 2
	h := int(b[0] >> 2)
	ow, iw, cw := (h&3)+1, 0, 1
	if object {
		iw = ((h >> 2) & 3) + 1
		if h&16 != 0 {
			cw = 4
		}
	} else if h&4 != 0 {
		cw = 4
	}
	n, ok := uintLE(b[1:], cw)
	if !ok || len(b) < 1+cw+ow || n > (len(b)-1-cw-ow)/(iw+ow) {
		return bytejson.ByteJson{}, invalid("truncated container tables")
	}
	ids := b[1+cw : 1+cw+n*iw]
	table := b[1+cw+n*iw : 1+cw+n*iw+(n+1)*ow]
	data := b[1+cw+n*iw+(n+1)*ow:]
	last, _ := uintLE(table[n*ow:], ow)
	if last != len(data) {
		return bytejson.ByteJson{}, invalid("container offsets do not cover value bytes")
	}
	if err := d.charge(n * 64); err != nil {
		return bytejson.ByteJson{}, err
	}
	// Object fields are ordered by name, but their payload offsets may be
	// non-monotonic. Sorting a separate offset list also rejects aliasing,
	// overlap, gaps and unused bytes before any descendants are materialized.
	offsets := make([]int, n+1)
	for i := range offsets {
		offsets[i], _ = uintLE(table[i*ow:], ow)
	}
	physical := slices.Clone(offsets)
	if object {
		slices.Sort(physical)
	}
	if physical[0] != 0 || physical[n] != len(data) {
		return bytejson.ByteJson{}, invalid("invalid container offset")
	}
	ends := make(map[int]int, n)
	for i := 0; i < n; i++ {
		if physical[i] >= physical[i+1] {
			return bytejson.ByteJson{}, invalid("overlapping or out-of-order container values")
		}
		ends[physical[i]] = physical[i+1]
	}
	array := make([]any, 0)
	var fields map[string]any
	if object {
		fields = make(map[string]any, n)
	} else {
		array = make([]any, n)
	}
	prevKey := ""
	for i := 0; i < n; i++ {
		var key string
		if object {
			id, _ := uintLE(ids[i*iw:], iw)
			if id >= len(d.keys) {
				return bytejson.ByteJson{}, invalid("field ID outside dictionary")
			}
			key = d.keys[id]
			if i > 0 && key <= prevKey {
				return bytejson.ByteJson{}, invalid("object field names must be sorted and unique")
			}
			if len(key) > math.MaxUint16 {
				return bytejson.ByteJson{}, invalid("object key exceeds MO JSON key limit")
			}
			if err := d.charge(len(key)); err != nil {
				return bytejson.ByteJson{}, err
			}
			prevKey = key
		}
		start := offsets[i]
		end, ok := ends[start]
		if !ok {
			return bytejson.ByteJson{}, invalid("container offset points past payload")
		}
		child, err := d.value(data[start:end], depth+1)
		if err != nil {
			return bytejson.ByteJson{}, err
		}
		if err := d.charge(max(len(child.Data), 64)); err != nil {
			return bytejson.ByteJson{}, err
		}
		if object {
			fields[key] = child
		} else {
			array[i] = child
		}
	}
	if object {
		return bytejson.CreateByteJSON(fields)
	}
	return bytejson.CreateByteJSON(array)
}

func (d *decoder) primitive(tag int, b []byte) (bytejson.ByteJson, error) {
	var value any
	sizes := [...]int{0, 0, 0, 1, 2, 4, 8, 8, 5, 9, 17, 4, 8, 8, 4}
	if tag < len(sizes) && len(b) != sizes[tag] {
		return bytejson.ByteJson{}, invalid("primitive payload has wrong size")
	}
	switch tag {
	case 0:
		return bytejson.Null, nil
	case 1, 2:
		value = tag == 1
	case 3:
		value = int64(int8(b[0]))
	case 4:
		value = int64(int16(binary.LittleEndian.Uint16(b)))
	case 5:
		value = int64(int32(binary.LittleEndian.Uint32(b)))
	case 6:
		value = int64(binary.LittleEndian.Uint64(b))
	case 7, 14:
		var f float64
		if tag == 7 {
			f = math.Float64frombits(binary.LittleEndian.Uint64(b))
		} else {
			f = float64(math.Float32frombits(binary.LittleEndian.Uint32(b)))
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return bytejson.ByteJson{}, invalid("non-finite floating point has no JSON representation")
		}
		value = f
	case 8, 9, 10:
		if b[0] > 38 {
			return bytejson.ByteJson{}, invalid("decimal scale exceeds 38")
		}
		// VARIANT decimals use little-endian signed integers (unlike the
		// big-endian representation of Parquet DECIMAL leaf columns).
		bytes := slices.Clone(b[1:])
		slices.Reverse(bytes)
		n := new(big.Int).SetBytes(bytes)
		if bytes[0]&128 != 0 {
			n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(len(bytes)*8)))
		}
		negative := n.Sign() < 0
		n.Abs(n)
		text := n.String()
		precision := [...]int{9, 18, 38}[tag-8]
		if len(text) > precision {
			return bytejson.ByteJson{}, invalid("decimal exceeds encoded precision")
		}
		scale := int(b[0])
		if scale > 0 {
			if len(text) <= scale {
				text = strings.Repeat("0", scale+1-len(text)) + text
			}
			pos := len(text) - scale
			text = text[:pos] + "." + text[pos:]
		}
		if negative {
			text = "-" + text
		}
		return typed(bytejson.TpCodeDecimal, text), nil
	case 11, 13:
		var t time.Time
		tp, layout := bytejson.TpCodeDate, "2006-01-02"
		if tag == 11 {
			days := int64(int32(binary.LittleEndian.Uint32(b)))
			t = time.Unix(days*86400, 0).UTC()
		} else {
			micros := int64(binary.LittleEndian.Uint64(b))
			t = time.Unix(micros/1e6, (micros%1e6)*1000).UTC()
			tp, layout = bytejson.TpCodeDatetime, "2006-01-02 15:04:05.000000"
		}
		if t.Year() < 1 || t.Year() > 9999 {
			return bytejson.ByteJson{}, fmt.Errorf("parquet VARIANT temporal value outside supported years 1..9999")
		}
		return typed(tp, t.Format(layout)), nil
	case 15, 16:
		n, ok := uintLE(b, 4)
		if !ok || n != len(b)-4 || tag == 16 && !utf8.Valid(b[4:]) {
			return bytejson.ByteJson{}, invalid("invalid binary or string length/payload")
		}
		if err := d.charge(n); err != nil {
			return bytejson.ByteJson{}, err
		}
		if tag == 15 {
			return typed(bytejson.TpCodeOpaque, string(b[4:])), nil
		}
		value = string(b[4:])
	case 17:
		if len(b) != 8 {
			return bytejson.ByteJson{}, invalid("time payload has wrong size")
		}
		micros := int64(binary.LittleEndian.Uint64(b))
		if micros < 0 || micros >= 86400*1e6 {
			return bytejson.ByteJson{}, invalid("time outside a calendar day")
		}
		return typed(bytejson.TpCodeTime, fmt.Sprintf("%02d:%02d:%02d.%06d", micros/3600e6, micros/60e6%60, micros/1e6%60, micros%1e6)), nil
	case 12, 18, 19, 20:
		return bytejson.ByteJson{}, fmt.Errorf("parquet VARIANT primitive type %d has no lossless MO JSON mapping", tag)
	default:
		return bytejson.ByteJson{}, fmt.Errorf("unsupported parquet VARIANT primitive type %d", tag)
	}
	return bytejson.CreateByteJSON(value)
}
