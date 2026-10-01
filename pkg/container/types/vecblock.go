// Copyright 2021 - 2024 Matrix Origin
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
	"bytes"
	"encoding/binary"
	"math"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Block-scaled vector cells (vecf8 = MXFP8, vecf4 = NVFP4). A cell is
//
//	[ header (12 B) | one scale byte per block | packed elements ]
//
// header: version (1 B), format (1 B), reserved zero (2 B), dimension (uint32 LE),
// global scale g (float32 LE).
// vecf8 elements are e4m3 bytes (Float8) with an E8M0 scale per 32 elements; g = 1.
// vecf4 elements are e2m1 nibbles (Float4), element 2i in the low nibble, with an
// unsigned E4M3 scale per 16 elements; g = amax/(6*448), 0 for an all-zero vector.
// Element i of block b is g * scale[b] * element[i].
// See docs/design/20260930-low-precision-vector-storage.md.

// BlockScaledFormat identifies the element and scale encoding of a cell.
type BlockScaledFormat uint8

const (
	BlockScaledMXFP8 BlockScaledFormat = 1
	BlockScaledNVFP4 BlockScaledFormat = 2
)

const (
	BlockScaledHeaderSize = 12
	blockScaledVersion    = 1

	// Largest vecf4 block value before the global scale: e2m1 max 6 times E4M3 max 448.
	blockScaledNVFP4Span = 6 * f8e4m3MaxNorm
)

// BlockSize returns the number of elements sharing one scale byte.
func (f BlockScaledFormat) BlockSize() int {
	if f == BlockScaledMXFP8 {
		return 32
	}
	return 16
}

func (f BlockScaledFormat) valid() bool {
	return f == BlockScaledMXFP8 || f == BlockScaledNVFP4
}

func (f BlockScaledFormat) String() string {
	switch f {
	case BlockScaledMXFP8:
		return "vecf8"
	case BlockScaledNVFP4:
		return "vecf4"
	}
	return "unknown(" + strconv.Itoa(int(f)) + ")"
}

// BlockScaledScaleCount returns the number of scale bytes for dim elements.
func BlockScaledScaleCount(f BlockScaledFormat, dim int) int {
	bs := f.BlockSize()
	return (dim + bs - 1) / bs
}

// BlockScaledElemBytes returns the packed element byte count for dim elements.
func BlockScaledElemBytes(f BlockScaledFormat, dim int) int {
	if f == BlockScaledMXFP8 {
		return dim
	}
	return (dim + 1) / 2
}

// BlockScaledCellSize returns the total cell byte length for dim elements.
func BlockScaledCellSize(f BlockScaledFormat, dim int) int {
	return BlockScaledHeaderSize + BlockScaledScaleCount(f, dim) + BlockScaledElemBytes(f, dim)
}

// BlockScaledCell is a validated view of a cell; Scales and Elems alias the cell bytes.
type BlockScaledCell struct {
	Format BlockScaledFormat
	Dim    int
	Global float32
	Scales []byte
	Elems  []byte
}

// E8M0ToFloat32 decodes an E8M0 scale byte, 2^(b-127). 0xff is NaN.
func E8M0ToFloat32(b uint8) float32 {
	if b == 0xff {
		return float32(math.NaN())
	}
	return float32(math.Ldexp(1, int(b)-127))
}

// e8m0ScaleFor returns the smallest E8M0 byte whose value times 448 covers amax.
func e8m0ScaleFor(amax float32) uint8 {
	if amax == 0 {
		return 0
	}
	target := float64(amax)
	e := int(math.Ceil(math.Log2(target / f8e4m3MaxNorm)))
	for e > -127 && math.Ldexp(f8e4m3MaxNorm, e-1) >= target {
		e--
	}
	for math.Ldexp(f8e4m3MaxNorm, e) < target {
		e++
	}
	if e < -127 {
		e = -127
	}
	return uint8(e + 127)
}

// ue4m3ScaleFor returns the smallest unsigned E4M3 byte covering target, capped at 448.
func ue4m3ScaleFor(target float64) uint8 {
	if target <= 0 {
		return 0
	}
	if target >= f8e4m3MaxNorm {
		return f8e4m3MaxBits
	}
	c := Float8FromFloat32(float32(target))
	if float64(c.ToFloat32()) < target {
		c++
	}
	return uint8(c)
}

func blockScaleValue(f BlockScaledFormat, b uint8) float32 {
	if f == BlockScaledMXFP8 {
		return E8M0ToFloat32(b)
	}
	return Float8(b).ToFloat32()
}

// AppendBlockScaled quantizes v into a cell appended to dst. It rejects NaN/Inf, an
// empty vector and a dimension above MaxArrayDimension.
func AppendBlockScaled(dst []byte, f BlockScaledFormat, v []float32) ([]byte, error) {
	if !f.valid() {
		return nil, moerr.NewInvalidInputNoCtxf("invalid block-scaled vector format %d", f)
	}
	dim := len(v)
	if dim == 0 || dim > MaxArrayDimension {
		return nil, moerr.NewInvalidInputNoCtxf("%s dimension %d out of range [1, %d]", f, dim, MaxArrayDimension)
	}
	var vmax float32
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return nil, moerr.NewInvalidInputNoCtxf("%s element %d is not finite", f, i)
		}
		if a := float32(math.Abs(float64(x))); a > vmax {
			vmax = a
		}
	}
	global := float32(1)
	if f == BlockScaledNVFP4 {
		global = float32(float64(vmax) / blockScaledNVFP4Span)
		if vmax != 0 && global == 0 {
			// vmax/2688 underflows float32: use the smallest subnormal.
			global = math.SmallestNonzeroFloat32
		}
	}

	start := len(dst)
	dst = append(dst, make([]byte, BlockScaledCellSize(f, dim))...)
	cell := dst[start:]
	cell[0] = blockScaledVersion
	cell[1] = byte(f)
	binary.LittleEndian.PutUint32(cell[4:8], uint32(dim))
	binary.LittleEndian.PutUint32(cell[8:12], math.Float32bits(global))
	nScales := BlockScaledScaleCount(f, dim)
	scales := cell[BlockScaledHeaderSize : BlockScaledHeaderSize+nScales]
	elems := cell[BlockScaledHeaderSize+nScales:]

	bs := f.BlockSize()
	for b := 0; b < nScales; b++ {
		lo, hi := b*bs, min((b+1)*bs, dim)
		var amax float32
		for _, x := range v[lo:hi] {
			if a := float32(math.Abs(float64(x))); a > amax {
				amax = a
			}
		}
		if amax == 0 {
			continue
		}
		if f == BlockScaledMXFP8 {
			scales[b] = e8m0ScaleFor(amax)
		} else {
			scales[b] = ue4m3ScaleFor(float64(amax) / (float64(f4e2m1Mags[7]) * float64(global)))
		}
		div := float64(global) * float64(blockScaleValue(f, scales[b]))
		for i := lo; i < hi; i++ {
			q := float32(float64(v[i]) / div)
			if f == BlockScaledMXFP8 {
				elems[i] = uint8(Float8FromFloat32(q))
			} else {
				elems[i/2] |= uint8(Float4FromFloat32(q)) << (4 * (i % 2))
			}
		}
	}
	return dst, nil
}

// ParseBlockScaledCell validates cell and returns a view of it. It rejects a wrong
// length, version, format or dimension, non-zero reserved or padding bits, a global
// scale that is not finite and non-negative (or not 1 for vecf8), NaN scale or element
// codes, and signed vecf4 scales.
func ParseBlockScaledCell(cell []byte) (BlockScaledCell, error) {
	if len(cell) < BlockScaledHeaderSize {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("block-scaled vector cell too short: %d bytes", len(cell))
	}
	if cell[0] != blockScaledVersion {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("unsupported block-scaled vector version %d", cell[0])
	}
	f := BlockScaledFormat(cell[1])
	if !f.valid() {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("invalid block-scaled vector format %d", cell[1])
	}
	if cell[2] != 0 || cell[3] != 0 {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtx("block-scaled vector header reserved bytes are not zero")
	}
	dim := int(binary.LittleEndian.Uint32(cell[4:8]))
	if dim == 0 || dim > MaxArrayDimension {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("%s dimension %d out of range [1, %d]", f, dim, MaxArrayDimension)
	}
	if want := BlockScaledCellSize(f, dim); len(cell) != want {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("%s(%d) cell is %d bytes, want %d", f, dim, len(cell), want)
	}
	global := math.Float32frombits(binary.LittleEndian.Uint32(cell[8:12]))
	if f == BlockScaledMXFP8 && global != 1 {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("vecf8 cell global scale is %v, want 1", global)
	}
	if !(global >= 0) || math.IsInf(float64(global), 0) || math.Signbit(float64(global)) {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("%s cell global scale %v is not finite and non-negative", f, global)
	}
	nScales := BlockScaledScaleCount(f, dim)
	c := BlockScaledCell{
		Format: f,
		Dim:    dim,
		Global: global,
		Scales: cell[BlockScaledHeaderSize : BlockScaledHeaderSize+nScales],
		Elems:  cell[BlockScaledHeaderSize+nScales:],
	}
	for _, s := range c.Scales {
		if f == BlockScaledMXFP8 && s == 0xff {
			return BlockScaledCell{}, moerr.NewInvalidInputNoCtx("vecf8 cell has a NaN scale")
		}
		if f == BlockScaledNVFP4 && (s&0x80 != 0 || s == f8e4m3NaN) {
			return BlockScaledCell{}, moerr.NewInvalidInputNoCtxf("vecf4 cell has an invalid scale 0x%02x", s)
		}
	}
	if f == BlockScaledMXFP8 {
		if hasF8E4M3NaN(c.Elems) {
			return BlockScaledCell{}, moerr.NewInvalidInputNoCtx("vecf8 cell has a NaN element")
		}
	} else if dim%2 == 1 && c.Elems[len(c.Elems)-1]>>4 != 0 {
		return BlockScaledCell{}, moerr.NewInvalidInputNoCtx("vecf4 cell has a non-zero padding nibble")
	}
	return c, nil
}

// hasF8E4M3NaN reports whether any byte is an E4M3 NaN (low 7 bits all set), 8 bytes per step.
func hasF8E4M3NaN(b []byte) bool {
	const lo7, one, hi = 0x7f7f7f7f7f7f7f7f, 0x0101010101010101, 0x8080808080808080
	i := 0
	for ; i+8 <= len(b); i += 8 {
		if (binary.LittleEndian.Uint64(b[i:])&lo7+one)&hi != 0 {
			return true
		}
	}
	for ; i < len(b); i++ {
		if b[i]&0x7f == f8e4m3NaN {
			return true
		}
	}
	return false
}

var (
	f8e4m3Values [256]float32
	e8m0Values   [256]float32
	f4e2m1Pairs  [256][2]float32
)

func init() {
	for i := 0; i < 256; i++ {
		f8e4m3Values[i] = Float8(uint8(i)).ToFloat32()
		e8m0Values[i] = E8M0ToFloat32(uint8(i))
		f4e2m1Pairs[i] = [2]float32{Float4(uint8(i)).ToFloat32(), Float4(uint8(i) >> 4).ToFloat32()}
	}
}

// BlockScaledTables returns the decode tables: E4M3 code to value, E8M0 code to value, and an
// E2M1 byte to its low and high nibble values.
func BlockScaledTables() (f8 *[256]float32, e8 *[256]float32, f4 *[256][2]float32) {
	return &f8e4m3Values, &e8m0Values, &f4e2m1Pairs
}

// At returns the dequantized element i.
func (c *BlockScaledCell) At(i int) float32 {
	if c.Format == BlockScaledMXFP8 {
		return f8e4m3Values[c.Elems[i]] * (c.Global * e8m0Values[c.Scales[i/32]])
	}
	return f4e2m1Pairs[c.Elems[i/2]][i%2] * (c.Global * f8e4m3Values[c.Scales[i/16]])
}

// Dequantize writes the dequantized elements into dst, which must hold Dim values.
func (c *BlockScaledCell) Dequantize(dst []float32) {
	c.DequantizeRange(0, dst[:c.Dim])
}

// DequantizeRange writes the dequantized elements [off, off+len(dst)) into dst. off must be a
// multiple of 32 and off+len(dst) at most Dim.
func (c *BlockScaledCell) DequantizeRange(off int, dst []float32) {
	end := off + len(dst)
	if c.Format == BlockScaledMXFP8 {
		for lo := off; lo < end; lo += 32 {
			scale := c.Global * e8m0Values[c.Scales[lo/32]]
			d := dst[lo-off:]
			if lo+32 <= end {
				e := (*[32]byte)(c.Elems[lo : lo+32])
				d := (*[32]float32)(d[:32])
				for i := 0; i < 32; i += 4 {
					d[i] = f8e4m3Values[e[i]] * scale
					d[i+1] = f8e4m3Values[e[i+1]] * scale
					d[i+2] = f8e4m3Values[e[i+2]] * scale
					d[i+3] = f8e4m3Values[e[i+3]] * scale
				}
				continue
			}
			for i, b := range c.Elems[lo:end] {
				d[i] = f8e4m3Values[b] * scale
			}
		}
		return
	}
	for lo := off; lo < end; lo += 16 {
		scale := c.Global * f8e4m3Values[c.Scales[lo/16]]
		d := dst[lo-off:]
		if lo+16 <= end {
			e := (*[8]byte)(c.Elems[lo/2 : lo/2+8])
			d := (*[16]float32)(d[:16])
			for i := 0; i < 8; i += 2 {
				p, q := &f4e2m1Pairs[e[i]], &f4e2m1Pairs[e[i+1]]
				d[2*i] = p[0] * scale
				d[2*i+1] = p[1] * scale
				d[2*i+2] = q[0] * scale
				d[2*i+3] = q[1] * scale
			}
			continue
		}
		for i := 0; i < end-lo; i++ {
			d[i] = f4e2m1Pairs[c.Elems[(lo+i)/2]][(lo+i)%2] * scale
		}
	}
}

// BlockScaledToFloat32 validates cell and returns its dequantized elements.
func BlockScaledToFloat32(cell []byte) ([]float32, error) {
	c, err := ParseBlockScaledCell(cell)
	if err != nil {
		return nil, err
	}
	out := make([]float32, c.Dim)
	c.Dequantize(out)
	return out, nil
}

// BlockScaledToString renders cell as "[v1, v2, ...]" with the vecf32 text format.
func BlockScaledToString(cell []byte) (string, error) {
	v, err := BlockScaledToFloat32(cell)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := WriteArrayTo(&buf, v); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// StringToBlockScaled parses "[v1, v2, ...]" with the vecf32 text format and quantizes it.
func StringToBlockScaled(f BlockScaledFormat, s string) ([]byte, error) {
	v, err := StringToArray[float32](s)
	if err != nil {
		return nil, err
	}
	return AppendBlockScaled(nil, f, v)
}

// CompareBlockScaledFromBytes orders two cells by their dequantized values with the
// vecf32 ordering (ArrayCompare). A cell that fails to parse orders by its bytes.
func CompareBlockScaledFromBytes(x, y []byte, desc bool) int {
	vx, errx := BlockScaledToFloat32(x)
	vy, erry := BlockScaledToFloat32(y)
	var c int
	if errx != nil || erry != nil {
		c = bytes.Compare(x, y)
	} else {
		c = ArrayCompare(vx, vy)
	}
	if desc {
		return -c
	}
	return c
}
