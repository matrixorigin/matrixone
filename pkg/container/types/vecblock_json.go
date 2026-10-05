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

package types

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/bytedance/sonic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// The exact text of a vecf8/vecf4 cell is a JSON object with its blocks as stored:
//
//	{"g": 0.0032617, "b": [{"s": 0.5, "v": [1, 3, 4, 5, ...]}, {"s": 2, "v": [2, 3, ...]}]}
//
// "s" is a block's scale value (E8M0 for vecf8, UE4M3 for vecf4), "v" its element values
// (E4M3 for vecf8, E2M1 for vecf4), 32 (vecf8) or 16 (vecf4) per block and fewer in the
// last block, and "g" the vecf4 global scale; vecf8's global is 1 and "g" is omitted. Each
// element decodes as v * s * g. Every number is the shortest float32 text, so parsing the
// text rebuilds the same cell bytes, without quantization.

// IsBlockScaledJSON reports whether s is the exact text form (a JSON object) rather than a
// "[...]" list of values.
func IsBlockScaledJSON(s string) bool {
	return strings.HasPrefix(strings.TrimLeft(s, " \t\r\n"), "{")
}

// BlockScaledToJSON returns the exact text of a cell.
func BlockScaledToJSON(cell []byte) (string, error) {
	c, err := ParseBlockScaledCell(cell)
	if err != nil {
		return "", err
	}
	bs := c.Format.BlockSize()
	out := make([]byte, 0, 16+len(c.Scales)*12+c.Dim*6)
	out = append(out, '{')
	if c.Format == BlockScaledNVFP4 {
		out = append(out, `"g":`...)
		out = strconv.AppendFloat(out, float64(c.Global), 'g', -1, 32)
		out = append(out, ',')
	}
	out = append(out, `"b":[`...)
	for b := range c.Scales {
		if b > 0 {
			out = append(out, ',')
		}
		out = append(out, `{"s":`...)
		out = strconv.AppendFloat(out, float64(blockScaleValue(c.Format, c.Scales[b])), 'g', -1, 32)
		out = append(out, `,"v":[`...)
		for i := b * bs; i < min((b+1)*bs, c.Dim); i++ {
			if i > b*bs {
				out = append(out, ',')
			}
			out = strconv.AppendFloat(out, float64(blockScaledElemValue(&c, i)), 'g', -1, 32)
		}
		out = append(out, "]}"...)
	}
	return string(append(out, "]}"...)), nil
}

// BlockScaledDim returns the dimension in a cell's header, 0 for a cell too short to hold one.
func BlockScaledDim(cell []byte) int {
	if len(cell) < BlockScaledHeaderSize {
		return 0
	}
	return int(binary.LittleEndian.Uint32(cell[4:8]))
}

// blockScaledElemValue is element i's code value, before the block and global scales.
func blockScaledElemValue(c *BlockScaledCell, i int) float32 {
	if c.Format == BlockScaledMXFP8 {
		return f8e4m3Values[c.Elems[i]]
	}
	return f4e2m1Pairs[c.Elems[i/2]][i%2]
}

// blockScaledJSONAPI decodes the exact text; an unknown key is an error.
var blockScaledJSONAPI = sonic.Config{DisallowUnknownFields: true}.Froze()

type blockScaledJSONBlock struct {
	S json.RawMessage   `json:"s"`
	V []json.RawMessage `json:"v"`
}

type blockScaledJSON struct {
	G json.RawMessage        `json:"g"`
	B []blockScaledJSONBlock `json:"b"`
}

// BlockScaledFromJSON builds the cell of format f from its exact text: every scale and
// element must be a value of its code, so nothing is rounded.
func BlockScaledFromJSON(f BlockScaledFormat, s string) ([]byte, error) {
	invalid := func(format string, args ...any) error {
		return moerr.NewInvalidInputNoCtxf("%s exact text: "+format, append([]any{f}, args...)...)
	}
	var doc blockScaledJSON
	if err := blockScaledJSONAPI.UnmarshalFromString(s, &doc); err != nil {
		return nil, invalid("%v", err)
	}
	if len(doc.B) == 0 {
		return nil, invalid(`"b" must list at least one block`)
	}
	global := float32(1)
	switch {
	case f == BlockScaledNVFP4 && doc.G == nil:
		return nil, invalid(`"g" is required`)
	case doc.G != nil:
		g, err := parseBlockScaledNumber(doc.G)
		if err != nil {
			return nil, invalid(`"g": %v`, err)
		}
		if f == BlockScaledMXFP8 && g != 1 {
			return nil, invalid(`"g" is %v, vecf8's global scale is 1`, g)
		}
		global = g
	}
	bs := f.BlockSize()
	dim := 0
	for i, b := range doc.B {
		if b.S == nil {
			return nil, invalid(`block %d has no "s"`, i)
		}
		if len(b.V) == 0 || len(b.V) > bs || (i < len(doc.B)-1 && len(b.V) != bs) {
			return nil, invalid("block %d has %d values, want %d (fewer only in the last block)", i, len(b.V), bs)
		}
		dim += len(b.V)
	}
	if dim > MaxArrayDimension {
		return nil, invalid("dimension %d exceeds %d", dim, MaxArrayDimension)
	}

	cell := make([]byte, BlockScaledCellSize(f, dim))
	cell[0] = blockScaledVersion
	cell[1] = byte(f)
	binary.LittleEndian.PutUint32(cell[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(cell[8:12], math.Float32bits(global))
	scales := cell[BlockScaledHeaderSize : BlockScaledHeaderSize+len(doc.B)]
	elems := cell[BlockScaledHeaderSize+len(doc.B):]
	for b, blk := range doc.B {
		sv, err := parseBlockScaledNumber(blk.S)
		if err != nil {
			return nil, invalid("block %d scale: %v", b, err)
		}
		code, ok := blockScaleCode(f, sv)
		if !ok {
			return nil, invalid("block %d scale %v is not a %s", b, sv, map[BlockScaledFormat]string{BlockScaledMXFP8: "power of two in [2^-127, 2^127]", BlockScaledNVFP4: "non-negative E4M3 value"}[f])
		}
		scales[b] = code
		for k, num := range blk.V {
			v, err := parseBlockScaledNumber(num)
			if err != nil {
				return nil, invalid("block %d value %d: %v", b, k, err)
			}
			c, ok := blockElemCode(f, v)
			if !ok {
				return nil, invalid("block %d value %v is not an %s value", b, v, map[BlockScaledFormat]string{BlockScaledMXFP8: "E4M3", BlockScaledNVFP4: "E2M1"}[f])
			}
			i := b*bs + k
			if f == BlockScaledMXFP8 {
				elems[i] = c
			} else {
				elems[i/2] |= c << (4 * (i % 2))
			}
		}
	}
	// the cell rules of a stored cell: scales, global, decoded values finite
	if _, err := ParseBlockScaledCell(cell); err != nil {
		return nil, err
	}
	return cell, nil
}

// parseBlockScaledNumber reads a JSON number (not a string) as a finite float32.
func parseBlockScaledNumber(n json.RawMessage) (float32, error) {
	if len(n) == 0 || !(n[0] == '-' || (n[0] >= '0' && n[0] <= '9')) {
		return 0, moerr.NewInvalidInputNoCtxf("%s is not a number", string(n))
	}
	v, err := strconv.ParseFloat(string(n), 32)
	if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, moerr.NewInvalidInputNoCtxf("%s is not a finite float32", string(n))
	}
	return float32(v), nil
}

// Code lookups by value bits: the finite E4M3, E2M1 and E8M0 codes; +0 is code 0.
var (
	f8e4m3CodeOf = map[uint32]uint8{}
	f4e2m1CodeOf = map[uint32]uint8{}
	e8m0CodeOf   = map[uint32]uint8{}
)

func init() {
	for c := 255; c >= 0; c-- {
		if c&0x7f != f8e4m3NaN {
			f8e4m3CodeOf[math.Float32bits(f8e4m3Values[c])] = uint8(c)
		}
		if c != 255 { // 255 is the E8M0 NaN
			e8m0CodeOf[math.Float32bits(e8m0Values[c])] = uint8(c)
		}
	}
	for c := 15; c >= 0; c-- {
		f4e2m1CodeOf[math.Float32bits(Float4(uint8(c)).ToFloat32())] = uint8(c)
	}
}

// blockScaleCode returns the scale code whose value is v: E8M0 for vecf8, a non-negative
// E4M3 value for vecf4.
func blockScaleCode(f BlockScaledFormat, v float32) (uint8, bool) {
	if f == BlockScaledMXFP8 {
		c, ok := e8m0CodeOf[math.Float32bits(v)]
		return c, ok
	}
	c, ok := f8e4m3CodeOf[math.Float32bits(v)]
	return c, ok && c <= f8e4m3MaxBits
}

// blockElemCode returns the element code whose value is v; a zero of either sign is code 0,
// as the encoder stores it.
func blockElemCode(f BlockScaledFormat, v float32) (uint8, bool) {
	if v == 0 {
		return 0, true
	}
	if f == BlockScaledMXFP8 {
		c, ok := f8e4m3CodeOf[math.Float32bits(v)]
		return c, ok
	}
	c, ok := f4e2m1CodeOf[math.Float32bits(v)]
	return c, ok
}

// BlockScaledFromBinary builds the f cell of a binary value: the stored cell
// (vecblock_binary) as is, or little-endian float32 elements quantized. dim is the declared
// dimension, MaxArrayDimension or 0 when unsized. With a declared dimension the value is a
// cell when its length is the cell size of dim, which is never 4*dim; without one, when its
// header names f and its length is the cell size of its dimension. The returned cell aliases
// b when b is a cell.
func BlockScaledFromBinary(f BlockScaledFormat, dim int, b []byte) ([]byte, error) {
	sized := dim > 0 && dim != MaxArrayDimension
	isCell := false
	if sized {
		isCell = len(b) == BlockScaledCellSize(f, dim)
	} else if len(b) >= BlockScaledHeaderSize && b[0] == blockScaledVersion && b[1] == byte(f) && b[2] == 0 && b[3] == 0 {
		d := BlockScaledDim(b)
		isCell = d > 0 && d <= MaxArrayDimension && len(b) == BlockScaledCellSize(f, d)
	}
	if isCell {
		c, err := ParseBlockScaledCell(b)
		if err != nil {
			return nil, err
		}
		if c.Format != f {
			return nil, moerr.NewInvalidInputNoCtxf("%s cell is not a %s cell", c.Format, f)
		}
		if sized && c.Dim != dim {
			return nil, moerr.NewArrayDefMismatchNoCtx(dim, c.Dim)
		}
		return b, nil
	}
	if sized && len(b) != 4*dim {
		return nil, moerr.NewInvalidInputNoCtxf("%d-byte value is neither a %s(%d) cell (%d bytes) nor %d float32 elements (%d bytes)",
			len(b), f, dim, BlockScaledCellSize(f, dim), dim, 4*dim)
	}
	if len(b)%4 != 0 {
		return nil, moerr.NewInvalidInputNoCtx("vector payload is not aligned to its element size")
	}
	if len(b)/4 > MaxArrayDimension {
		return nil, moerr.NewInvalidInputNoCtx("vector dimension exceeds maximum dimension")
	}
	return AppendBlockScaled(nil, f, BytesToArray[float32](b))
}
