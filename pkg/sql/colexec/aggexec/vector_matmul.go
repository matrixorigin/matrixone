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

package aggexec

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// vector_matmul(topk, id, vec, queries [, options]): per group, the `topk` rows nearest to
// each query under the options' metric, as JSON [[["id", distance], ...], ...] with one
// inner array per query. topk, queries and options
// are compile-time configuration; the executor receives [id, vec].

const (
	vectorMatmulMaxTopK    = 16384
	vectorMatmulMaxQueries = 4096
	vectorMatmulMaxEntries = 1 << 22
	vectorMatmulStateV1    = 1
	// vectorMatmulMaxFixedIDLen bounds the text of a non-string id (int64, uint64, uuid).
	vectorMatmulMaxFixedIDLen = 36
)

// VectorMatmulIDSupported reports the id column types vector_matmul accepts.
func VectorMatmulIDSupported(oid types.T) bool {
	switch oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_char, types.T_varchar, types.T_text, types.T_uuid:
		return true
	}
	return false
}

// VectorMatmulVecSupported reports the vector column types vector_matmul accepts.
func VectorMatmulVecSupported(oid types.T) bool {
	switch oid {
	case types.T_array_float8, types.T_array_float4, types.T_array_float32,
		types.T_array_float16, types.T_array_bf16, types.T_array_int8, types.T_array_uint8:
		return true
	}
	return false
}

// GPU engine formats, as in cgo/cuvs/blockscaled_matmul_c.h.
const (
	vectorMatmulEngineMXFP8 = 1
	vectorMatmulEngineNVFP4 = 2
	vectorMatmulEngineF32   = 3
	vectorMatmulEngineF16   = 4
	vectorMatmulEngineI8    = 5
	vectorMatmulEngineU8    = 6
	vectorMatmulEngineBF16  = 7
)

// VectorMatmulReturnType is the JSON result type.
func VectorMatmulReturnType(_ []types.Type) types.Type {
	return types.T_json.ToType()
}

// EncodeVectorMatmulConfig packs topk, the queries as a JSON array, the options JSON
// string and whether the session allows the GPU (gpu_mode).
func EncodeVectorMatmulConfig(topk int64, queries, options string, gpu bool) []byte {
	return encodeVectorMatmulConfig(topk, queries, options, gpu, false)
}

// EncodeVectorMatmulBinaryConfig packs topk, the queries as little-endian float32 values
// back to back (a BLOB argument), the options JSON string and whether the session allows
// the GPU (gpu_mode).
func EncodeVectorMatmulBinaryConfig(topk int64, queries []byte, options string, gpu bool) []byte {
	return encodeVectorMatmulConfig(topk, string(queries), options, gpu, true)
}

const (
	vectorMatmulFlagGPU byte = 1 << iota
	vectorMatmulFlagBinaryQueries
)

func encodeVectorMatmulConfig(topk int64, queries, options string, gpu, binaryQueries bool) []byte {
	out := make([]byte, 0, 17+len(queries)+len(options))
	out = binary.LittleEndian.AppendUint64(out, uint64(topk))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(queries)))
	out = append(out, queries...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(options)))
	out = append(out, options...)
	var flags byte
	if gpu {
		flags |= vectorMatmulFlagGPU
	}
	if binaryQueries {
		flags |= vectorMatmulFlagBinaryQueries
	}
	return append(out, flags)
}

func decodeVectorMatmulConfig(b []byte) (topk int64, queries, options string, gpu, binaryQueries bool, err error) {
	return decodeVectorMatmulConfigFields(b, func(v []byte) string { return string(v) })
}

// decodeVectorMatmulConfigView is decodeVectorMatmulConfig with queries and options viewing b.
func decodeVectorMatmulConfigView(b []byte) (topk int64, queries, options string, gpu, binaryQueries bool, err error) {
	return decodeVectorMatmulConfigFields(b, util.UnsafeBytesToString)
}

func decodeVectorMatmulConfigFields(b []byte, str func([]byte) string) (topk int64, queries, options string, gpu, binaryQueries bool, err error) {
	read := func() (string, bool) {
		if len(b) < 4 {
			return "", false
		}
		n := binary.LittleEndian.Uint32(b)
		if uint64(len(b)-4) < uint64(n) {
			return "", false
		}
		s := str(b[4 : 4+n])
		b = b[4+n:]
		return s, true
	}
	malformed := moerr.NewInternalErrorNoCtx("vector_matmul: malformed configuration")
	if len(b) < 8 {
		return 0, "", "", false, false, malformed
	}
	topk = int64(binary.LittleEndian.Uint64(b))
	b = b[8:]
	var ok1, ok2 bool
	queries, ok1 = read()
	options, ok2 = read()
	if !ok1 || !ok2 || len(b) != 1 || b[0]&^(vectorMatmulFlagGPU|vectorMatmulFlagBinaryQueries) != 0 {
		return 0, "", "", false, false, malformed
	}
	return topk, queries, options, b[0]&vectorMatmulFlagGPU != 0, b[0]&vectorMatmulFlagBinaryQueries != 0, nil
}

// vectorMatmulConfig is the parsed configuration shared by all groups and, through
// vectorMatmulConfigs, by the executors of a query; it is read only.
type vectorMatmulConfig struct {
	topk int
	nq   int
	// queryCells holds the queries back to back in the column's cell format; the CPU scorer
	// reads them through views of the same storage.
	queryCells []byte
	// retained is the byte size the configuration holds: the query storage and the views.
	retained int
	// cellBytes is the byte length of one cell.
	cellBytes int
	// engineFormat is the GPU engine format of the column type.
	engineFormat int
	// score validates a cell and writes its dot product with each query into out; row is the
	// caller's scratch for the cell's operand.
	score func(cell []byte, row *metric.VecBlockOperand, out []float64) error
	// gpu reports that the session allows the GPU.
	gpu bool
	// metric is the distance: vectorMatmulInnerProduct, vectorMatmulCosine or
	// vectorMatmulL2sq.
	metric int
}

// Metrics of vector_matmul. The executors rank by the negated distance (largest is
// nearest) and report the distance: -dot, 1 - cos or the squared L2 distance, as the
// inner_product, cosine_distance and l2_distance_sq functions.
const (
	vectorMatmulInnerProduct = iota
	vectorMatmulCosine
	vectorMatmulL2sq
)

// parseVectorMatmulOptions reads the options JSON object: "metric" (inner_product, the
// default, cosine or l2sq) and "query_format" (float32, the default: float values; vecblock:
// vecf8/vecf4 cells as stored). Other keys are ignored.
func parseVectorMatmulOptions(options string) (metric int, vecblockQueries bool, err error) {
	if options == "" {
		return vectorMatmulInnerProduct, false, nil
	}
	var fields map[string]json.RawMessage
	if err := sonic.UnmarshalString(options, &fields); err != nil || fields == nil {
		return 0, false, moerr.NewInvalidInputNoCtx("vector_matmul: options must be a JSON object")
	}
	metric = vectorMatmulInnerProduct
	if raw, ok := fields["metric"]; ok {
		var name string
		_ = sonic.Unmarshal(raw, &name)
		switch name {
		case "inner_product":
		case "cosine":
			metric = vectorMatmulCosine
		case "l2sq":
			metric = vectorMatmulL2sq
		default:
			return 0, false, moerr.NewInvalidInputNoCtxf("vector_matmul: metric %s is not inner_product, cosine or l2sq", raw)
		}
	}
	if raw, ok := fields["query_format"]; ok {
		var name string
		_ = sonic.Unmarshal(raw, &name)
		switch name {
		case "float32":
		case "vecblock":
			vecblockQueries = true
		default:
			return 0, false, moerr.NewInvalidInputNoCtxf("vector_matmul: query_format %s is not float32 or vecblock", raw)
		}
	}
	return metric, vecblockQueries, nil
}

// vectorMatmulConfigs shares a parsed configuration between the executors holding the same
// configuration bytes for the same column type: a query's pipeline executors and the
// executors that receive its partial states for the merge. An entry lives while an executor
// holds it.
var vectorMatmulConfigs = struct {
	sync.Mutex
	m map[vectorMatmulConfigKey]*vectorMatmulConfigEntry
}{m: make(map[vectorMatmulConfigKey]*vectorMatmulConfigEntry)}

type vectorMatmulConfigKey struct {
	sum   [sha256.Size]byte
	oid   types.T
	width int32
}

type vectorMatmulConfigEntry struct {
	once sync.Once
	cfg  *vectorMatmulConfig
	err  error
	refs int
}

// acquireVectorMatmulConfig returns the parsed configuration of raw for vecType, parsing it
// once for all executors holding it; release drops the executor's hold.
func acquireVectorMatmulConfig(raw []byte, vecType types.Type) (cfg *vectorMatmulConfig, release func(), err error) {
	key := vectorMatmulConfigKey{sum: sha256.Sum256(raw), oid: vecType.Oid, width: vecType.Width}
	c := &vectorMatmulConfigs
	c.Lock()
	e := c.m[key]
	if e == nil {
		e = &vectorMatmulConfigEntry{}
		c.m[key] = e
	}
	e.refs++
	c.Unlock()
	release = func() {
		c.Lock()
		if e.refs--; e.refs == 0 && c.m[key] == e {
			delete(c.m, key)
		}
		c.Unlock()
	}
	e.once.Do(func() { e.cfg, e.err = parseVectorMatmulConfig(raw, vecType) })
	if e.err != nil {
		release()
		return nil, nil, e.err
	}
	return e.cfg, release, nil
}

// vectorMatmulParseBound returns an upper bound of the bytes parseVectorMatmulConfig allocates
// for raw and vecType, the transient decoding and the retained configuration together, plus
// the executor's score scratch. It decodes no query: it rejects a topk, query count or
// dimension outside the limits from the configuration's lengths and a scan of JSON queries,
// so the account admits the bound before parsing allocates.
func vectorMatmulParseBound(raw []byte, vecType types.Type) (uint64, error) {
	topk, queries, options, _, binaryQueries, err := decodeVectorMatmulConfigView(raw)
	if err != nil {
		return 0, err
	}
	if topk < 1 || topk > vectorMatmulMaxTopK {
		return 0, moerr.NewInvalidInputNoCtxf("vector_matmul: topk %d out of range [1, %d]", topk, vectorMatmulMaxTopK)
	}
	dim := int(vecType.Width)
	if dim < 1 || dim > types.MaxArrayDimension {
		return 0, moerr.NewInvalidInputNoCtxf("vector_matmul: dimension %d out of range [1, %d]", dim, types.MaxArrayDimension)
	}
	_, vecblockQueries, err := parseVectorMatmulOptions(options)
	if err != nil {
		// parsing reports the options
		return 0, err
	}
	cellSize := 4 * dim
	if f, ok := vecType.Oid.BlockScaledFormat(); ok {
		cellSize = types.BlockScaledCellSize(f, dim)
	}
	// the copies of queries and options, and the options map
	bound := 2*uint64(len(queries)) + 4*uint64(len(options)) + 4096
	var nq int
	switch {
	case binaryQueries:
		width := 4 * dim
		if vecblockQueries {
			width = cellSize
		}
		if len(queries) == 0 || len(queries)%width != 0 {
			// parsing reports the length
			return bound, nil
		}
		nq = len(queries) / width
		// the decoded values and their slice headers
		bound += uint64(len(queries)) + uint64(nq)*24
	case vecblockQueries:
		rows, ok := vectorMatmulJSONRows(queries, nil)
		if !ok {
			return bound, nil
		}
		nq = rows
		// the element texts, their JSON decoding and the cells built from them
		bound += uint64(len(queries)) + uint64(nq)*(2048+uint64(cellSize)+256*uint64(dim))
	default:
		badRow, badLen := -1, 0
		rows, ok := vectorMatmulJSONRows(queries, func(row, n, _ int) {
			if badRow < 0 && n != dim {
				badRow, badLen = row, n
			}
		})
		if !ok {
			return bound, nil
		}
		nq = rows
		if nq >= 1 && nq <= vectorMatmulMaxQueries && nq*int(topk) <= vectorMatmulMaxEntries && badRow >= 0 {
			return 0, moerr.NewArrayInvalidOpNoCtx(dim, badLen)
		}
		// the decoded vectors with append growth
		bound += uint64(nq) * (128 + 24*uint64(dim))
	}
	if nq < 1 || nq > vectorMatmulMaxQueries {
		return 0, moerr.NewInvalidInputNoCtxf("vector_matmul: query count %d out of range [1, %d]", nq, vectorMatmulMaxQueries)
	}
	if nq*int(topk) > vectorMatmulMaxEntries {
		return 0, moerr.NewInvalidInputNoCtxf("vector_matmul: queries x topk exceeds %d", vectorMatmulMaxEntries)
	}
	// the converted storage (cells built from vectors, then copied back to back), the
	// scorer views and the score scratch
	opSize := uint64(reflect.TypeFor[metric.VecBlockOperand]().Size())
	bound += uint64(nq) * (64 + 4*uint64(cellSize) + 8*uint64(dim) + opSize + 8)
	// a margin for the decoder's own allocation; the commit returns what is not retained
	return 2 * bound, nil
}

// vectorMatmulJSONRows scans text as a JSON array of arrays without decoding it: it returns
// the number of rows and calls row, when not nil, with each row's element count and the
// index of its first null element (-1 when none). ok is false when text is not well-formed
// JSON of that shape; the decoder then reports the error. An element of the outer array that
// is not an array counts as a row of one element; a null one has null at -2.
func vectorMatmulJSONRows(text string, row func(i, n, null int)) (rows int, ok bool) {
	i := 0
	ws := func() {
		for i < len(text) && (text[i] == ' ' || text[i] == '\t' || text[i] == '\n' || text[i] == '\r') {
			i++
		}
	}
	// skipString moves i past the string starting at text[i].
	skipString := func() bool {
		for i++; i < len(text); i++ {
			switch text[i] {
			case '\\':
				i++
			case '"':
				i++
				return true
			}
		}
		return false
	}
	// isNull reports whether the value at text[i] is the literal null.
	isNull := func() bool {
		ws()
		return strings.HasPrefix(text[i:], "null") &&
			(i+4 == len(text) || strings.IndexByte(",]} \t\n\r", text[i+4]) >= 0)
	}
	// firstNull is the index of the first null element of the array skipValue last left.
	firstNull := -1
	// skipValue moves i past one value, counting the elements of an array.
	var skipValue func() (elems int, ok bool)
	skipValue = func() (int, bool) {
		ws()
		if i >= len(text) {
			return 0, false
		}
		switch text[i] {
		case '"':
			return 1, skipString()
		case '[', '{':
			open, closer := text[i], byte(']')
			if open == '{' {
				closer = '}'
			}
			i++
			ws()
			if i < len(text) && text[i] == closer {
				i++
				firstNull = -1
				return 0, true
			}
			n, null := 0, -1
			for {
				if open == '{' {
					ws()
					if i >= len(text) || text[i] != '"' || !skipString() {
						return 0, false
					}
					ws()
					if i >= len(text) || text[i] != ':' {
						return 0, false
					}
					i++
				}
				if null < 0 && open == '[' && isNull() {
					null = n
				}
				if _, ok := skipValue(); !ok {
					return 0, false
				}
				n++
				ws()
				if i >= len(text) {
					return 0, false
				}
				if text[i] == closer {
					i++
					firstNull = -1
					if open == '{' {
						return 1, true
					}
					firstNull = null
					return n, true
				}
				if text[i] != ',' {
					return 0, false
				}
				i++
			}
		default:
			start := i
			for i < len(text) && text[i] != ',' && text[i] != ']' && text[i] != '}' &&
				text[i] != ' ' && text[i] != '\t' && text[i] != '\n' && text[i] != '\r' {
				i++
			}
			return 1, i > start
		}
	}
	ws()
	if i >= len(text) || text[i] != '[' {
		return 0, false
	}
	i++
	ws()
	if i < len(text) && text[i] == ']' {
		i++
		ws()
		return 0, i == len(text)
	}
	for {
		ws()
		isArray := i < len(text) && text[i] == '['
		null := -1
		if !isArray && isNull() {
			null = -2
		}
		n, ok := skipValue()
		if !ok {
			return 0, false
		}
		if isArray {
			null = firstNull
		} else {
			n = 1
		}
		if row != nil {
			row(rows, n, null)
		}
		rows++
		ws()
		if i >= len(text) {
			return 0, false
		}
		if text[i] == ']' {
			i++
			ws()
			return rows, i == len(text)
		}
		if text[i] != ',' {
			return 0, false
		}
		i++
	}
}

func parseVectorMatmulConfig(raw []byte, vecType types.Type) (*vectorMatmulConfig, error) {
	topk, queriesText, options, gpu, binaryQueries, err := decodeVectorMatmulConfig(raw)
	if err != nil {
		return nil, err
	}
	metric, vecblockQueries, err := parseVectorMatmulOptions(options)
	if err != nil {
		return nil, err
	}
	if topk < 1 || topk > vectorMatmulMaxTopK {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: topk %d out of range [1, %d]", topk, vectorMatmulMaxTopK)
	}
	if vecblockQueries {
		cells, err := decodeVectorMatmulCells(queriesText, binaryQueries, vecType)
		if err != nil {
			return nil, err
		}
		if len(cells)*int(topk) > vectorMatmulMaxEntries {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: queries x topk exceeds %d", vectorMatmulMaxEntries)
		}
		cfg := &vectorMatmulConfig{topk: int(topk), nq: len(cells), gpu: gpu, metric: metric}
		if err := cfg.setBlockScaledCells(vecType.Oid, int(vecType.Width), cells); err != nil {
			return nil, err
		}
		return cfg, nil
	}
	var queries [][]float32
	if binaryQueries {
		if queries, err = decodeVectorMatmulBinaryQueries(queriesText, int(vecType.Width)); err != nil {
			return nil, err
		}
	} else if queries, err = decodeVectorMatmulJSONQueries(queriesText); err != nil {
		return nil, err
	}
	if len(queries) == 0 || len(queries) > vectorMatmulMaxQueries {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query count %d out of range [1, %d]", len(queries), vectorMatmulMaxQueries)
	}
	if len(queries)*int(topk) > vectorMatmulMaxEntries {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: queries x topk exceeds %d", vectorMatmulMaxEntries)
	}
	dim := int(vecType.Width)
	for _, q := range queries {
		if len(q) != dim {
			return nil, moerr.NewArrayInvalidOpNoCtx(dim, len(q))
		}
	}
	cfg := &vectorMatmulConfig{topk: int(topk), nq: len(queries), gpu: gpu, metric: metric}
	switch oid := vecType.Oid; oid {
	case types.T_array_float8, types.T_array_float4:
		err = cfg.setBlockScaled(oid, dim, queries)
	case types.T_array_float32:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineF32, queries, func(v float32) (float32, bool) { return v, true })
	case types.T_array_float16:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineF16, queries, func(v float32) (types.Float16, bool) {
			return types.Float16FromFloat32(v), v >= -types.Float16MaxFinite && v <= types.Float16MaxFinite
		})
	case types.T_array_bf16:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineBF16, queries, func(v float32) (types.BF16, bool) {
			return types.BF16FromFloat32(v), v >= -types.BF16MaxFinite && v <= types.BF16MaxFinite
		})
	case types.T_array_int8:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineI8, queries, func(v float32) (int8, bool) {
			return int8(v), v == float32(math.Trunc(float64(v))) && v >= math.MinInt8 && v <= math.MaxInt8
		})
	case types.T_array_uint8:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineU8, queries, func(v float32) (uint8, bool) {
			return uint8(v), v == float32(math.Trunc(float64(v))) && v >= 0 && v <= math.MaxUint8
		})
	default:
		err = moerr.NewInvalidInputNoCtxf("vector_matmul: unsupported vector type %s", oid)
	}
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

// decodeVectorMatmulBinaryQueries reads queries packed as little-endian float32 values,
// dim per query, back to back; every value must be finite.
func decodeVectorMatmulBinaryQueries(b string, dim int) ([][]float32, error) {
	if dim <= 0 || len(b) == 0 || len(b)%(4*dim) != 0 {
		return nil, moerr.NewInvalidInputNoCtxf(
			"vector_matmul: binary queries are %d bytes, want a non-zero multiple of %d (float32 x dimension %d)", len(b), 4*dim, dim)
	}
	nq := len(b) / (4 * dim)
	if nq > vectorMatmulMaxQueries {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query count %d out of range [1, %d]", nq, vectorMatmulMaxQueries)
	}
	values := make([]float32, nq*dim)
	for i := range values {
		j := 4 * i
		v := math.Float32frombits(uint32(b[j]) | uint32(b[j+1])<<8 | uint32(b[j+2])<<16 | uint32(b[j+3])<<24)
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query %d element %d is not finite", i/dim, i%dim)
		}
		values[i] = v
	}
	queries := make([][]float32, nq)
	for q := range queries {
		queries[q] = values[q*dim : (q+1)*dim : (q+1)*dim]
	}
	return queries, nil
}

// decodeVectorMatmulJSONQueries reads a JSON array of vectors. A null query or coordinate is
// an error: unmarshalling into float32 reads it as 0, so the row scan finds the first one.
func decodeVectorMatmulJSONQueries(text string) ([][]float32, error) {
	var queries [][]float32
	if err := sonic.UnmarshalString(text, &queries); err != nil {
		return nil, moerr.NewInvalidInputNoCtx("vector_matmul: queries must be a JSON array of vectors")
	}
	nullRow, nullAt := -1, -1
	vectorMatmulJSONRows(text, func(row, _, null int) {
		if nullRow < 0 && null != -1 {
			nullRow, nullAt = row, null
		}
	})
	switch {
	case nullRow < 0:
		return queries, nil
	case nullAt == -2:
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query %d is null", nullRow)
	default:
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query %d: coordinate %d is null", nullRow, nullAt)
	}
}

// decodeVectorMatmulCells reads query_format vecblock queries for a vecf8/vecf4 column of
// vecType: a BLOB of cells back to back, or a JSON array of vecblock JSON objects. Every
// cell must be a valid cell of the column's format and dimension.
func decodeVectorMatmulCells(queries string, binaryQueries bool, vecType types.Type) ([][]byte, error) {
	format, ok := vecType.Oid.BlockScaledFormat()
	if !ok {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query_format vecblock needs a vecf8 or vecf4 column, not %s", vecType.Oid)
	}
	dim := int(vecType.Width)
	size := types.BlockScaledCellSize(format, dim)
	var cells [][]byte
	if binaryQueries {
		if len(queries) == 0 || len(queries)%size != 0 {
			return nil, moerr.NewInvalidInputNoCtxf(
				"vector_matmul: vecblock queries are %d bytes, want a non-zero multiple of %d (a %s(%d) cell)", len(queries), size, vecType.Oid, dim)
		}
		if n := len(queries) / size; n > vectorMatmulMaxQueries {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query count %d out of range [1, %d]", n, vectorMatmulMaxQueries)
		}
		b := []byte(queries)
		for off := 0; off < len(b); off += size {
			cells = append(cells, b[off:off+size:off+size])
		}
	} else {
		var texts []json.RawMessage
		if err := sonic.UnmarshalString(queries, &texts); err != nil {
			return nil, moerr.NewInvalidInputNoCtx("vector_matmul: vecblock queries must be a JSON array of vecblock JSON objects")
		}
		if len(texts) == 0 || len(texts) > vectorMatmulMaxQueries {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query count %d out of range [1, %d]", len(texts), vectorMatmulMaxQueries)
		}
		for i, text := range texts {
			cell, err := types.BlockScaledFromJSON(format, string(text))
			if err != nil {
				return nil, vectorMatmulQueryError(i, err)
			}
			cells = append(cells, cell)
		}
	}
	for i, cell := range cells {
		c, err := types.ParseBlockScaledCell(cell)
		if err != nil {
			return nil, vectorMatmulQueryError(i, err)
		}
		if c.Format != format || c.Dim != dim {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: query %d is a %s(%d) cell, want %s(%d)", i, c.Format, c.Dim, vecType.Oid, dim)
		}
	}
	return cells, nil
}

// vectorMatmulQueryError reports the invalid input err of query i.
func vectorMatmulQueryError(i int, err error) error {
	return moerr.NewInvalidInputNoCtxf("vector_matmul: query %d: %s", i, strings.TrimPrefix(err.Error(), "invalid input: "))
}

// setBlockScaled quantizes the queries to the vecf8/vecf4 format and scores cells with the
// block-scaled CPU kernel.
func (cfg *vectorMatmulConfig) setBlockScaled(oid types.T, dim int, queries [][]float32) error {
	format, _ := oid.BlockScaledFormat()
	cells := make([][]byte, len(queries))
	for i, q := range queries {
		var err error
		if cells[i], err = types.AppendBlockScaled(nil, format, q); err != nil {
			return err
		}
	}
	return cfg.setBlockScaledCells(oid, dim, cells)
}

// setBlockScaledCells takes the queries as vecf8/vecf4 cells and scores cells with the
// block-scaled CPU kernel.
func (cfg *vectorMatmulConfig) setBlockScaledCells(oid types.T, dim int, cells [][]byte) error {
	format, _ := oid.BlockScaledFormat()
	cfg.engineFormat = vectorMatmulEngineMXFP8
	if format == types.BlockScaledNVFP4 {
		cfg.engineFormat = vectorMatmulEngineNVFP4
	}
	cfg.cellBytes = types.BlockScaledCellSize(format, dim)
	cfg.queryCells = make([]byte, 0, len(cells)*cfg.cellBytes)
	for _, c := range cells {
		cfg.queryCells = append(cfg.queryCells, c...)
	}
	ops := make([]metric.VecBlockOperand, len(cells))
	for i := range cells {
		q := cfg.queryCells[i*cfg.cellBytes : (i+1)*cfg.cellBytes : (i+1)*cfg.cellBytes]
		var err error
		if ops[i].Cell, err = types.ParseBlockScaledCell(q); err != nil {
			return err
		}
	}
	cfg.retained = cap(cfg.queryCells) + cap(ops)*int(reflect.TypeFor[metric.VecBlockOperand]().Size())
	// the distance as inner_product, cosine_distance and l2_distance_sq return it; an
	// inner product overflow NaN ranks last
	m := metric.Metric_InnerProduct
	switch cfg.metric {
	case vectorMatmulCosine:
		m = metric.Metric_CosineDistance
	case vectorMatmulL2sq:
		m = metric.Metric_L2sqDistance
	}
	distance, err := metric.VecBlockSQLDistance(m)
	if err != nil {
		return err
	}
	cfg.score = func(cell []byte, row *metric.VecBlockOperand, out []float64) error {
		c, err := types.ParseBlockScaledCell(cell)
		if err != nil {
			return err
		}
		*row = metric.VecBlockOperand{Cell: c}
		for j := range ops {
			dist, err := distance(row, &ops[j])
			if err != nil {
				return err
			}
			out[j] = -dist
		}
		return nil
	}
	return nil
}

// setVectorMatmulPlain converts the queries to the element type T and scores cells with the
// inner product kernel of T. conv reports whether a value is representable.
func setVectorMatmulPlain[T types.ArrayElement](cfg *vectorMatmulConfig, engineFormat int, queries [][]float32, conv func(float32) (T, bool)) error {
	// the distance kernel as inner_product, cosine_distance and l2_distance_sq resolve it
	m := metric.Metric_InnerProduct
	switch cfg.metric {
	case vectorMatmulCosine:
		m = metric.Metric_CosineDistance
	case vectorMatmulL2sq:
		m = metric.Metric_L2sqDistance
	}
	fn, err := metric.ResolveDistanceFn[T, float64](m)
	if err != nil {
		return err
	}
	// one backing of the queries: queryCells is its bytes, the scorer slices query j from it
	dim := len(queries[0])
	backing := make([]T, len(queries)*dim)
	for i, q := range queries {
		for k, v := range q {
			var ok bool
			if backing[i*dim+k], ok = conv(v); !ok {
				return moerr.NewInvalidInputNoCtxf("vector_matmul: query value %v is not representable in the column type", v)
			}
		}
	}
	cfg.queryCells = types.ArrayToBytes(backing)
	cfg.retained = cap(cfg.queryCells)
	cfg.engineFormat = engineFormat
	cfg.cellBytes = len(cfg.queryCells) / len(queries)
	cfg.score = func(cell []byte, _ *metric.VecBlockOperand, out []float64) error {
		if len(cell) != cfg.cellBytes {
			return moerr.NewInvalidInputNoCtxf("vector_matmul: cell is %d bytes, want %d", len(cell), cfg.cellBytes)
		}
		row := types.BytesToArray[T](cell)
		for j := range out {
			q := backing[j*dim : (j+1)*dim : (j+1)*dim]
			dist, err := fn(row, q)
			if err != nil {
				return err
			}
			out[j] = -dist
		}
		return nil
	}
	return nil
}

// vectorMatmulEntry is one hit; the id text lives in the state arena at [off, off+n).
type vectorMatmulEntry struct {
	score float64
	off   uint32
	n     uint32
}

// vectorMatmulSpan identifies the arena text of an entry. An empty id takes the offset the
// next id is stored at, so the offset alone does not identify a span.
type vectorMatmulSpan struct{ off, n uint32 }

func (e vectorMatmulEntry) span() vectorMatmulSpan { return vectorMatmulSpan{e.off, e.n} }

// vectorMatmulState is one group's state: per query a min-heap of at most k entries
// ordered worst first, and an arena holding the id text of the entries.
type vectorMatmulState struct {
	q, k    int
	counts  []uint32
	entries []vectorMatmulEntry // q*k; heap of query j at [j*k, j*k+counts[j])
	arena   []byte              // used prefix [0, used)
	used    int
	// order is q*k entry indices, sorted by arena span for compaction and merge;
	// allocated on first use.
	order []uint32
	// pending is the id text of this group's rows waiting in the GPU tile; preflight
	// reserves arena space for it.
	pending    int
	mp         *mpool.MPool
	allocation *AllocationAccount
}

func newVectorMatmulState(mp *mpool.MPool, allocation *AllocationAccount, q, k int) (*vectorMatmulState, error) {
	s := &vectorMatmulState{q: q, k: k, mp: mp, allocation: allocation}
	if q == 0 {
		return s, nil
	}
	var err error
	if s.counts, err = makeAccountedScratch[uint32](allocation, mp, q); err != nil {
		return nil, err
	}
	if s.entries, err = makeAccountedScratch[vectorMatmulEntry](allocation, mp, q*k); err != nil {
		s.Free()
		return nil, err
	}
	return s, nil
}

// hits returns query j's heap.
func (s *vectorMatmulState) hits(j int) []vectorMatmulEntry {
	if s.counts == nil || s.counts[j] == 0 {
		return nil
	}
	return s.entries[j*s.k : j*s.k+int(s.counts[j])]
}

func (s *vectorMatmulState) id(e vectorMatmulEntry) []byte {
	return s.arena[e.off : e.off+e.n]
}

// worse orders a before b when a ranks lower: smaller score, or equal score and larger id.
func (s *vectorMatmulState) worse(a, b vectorMatmulEntry, aID, bID []byte) bool {
	if a.score != b.score {
		return a.score < b.score
	}
	return bytes.Compare(aID, bID) > 0
}

// admits reports whether a hit with this score and id enters query j's heap.
func (s *vectorMatmulState) admits(j int, score float64, id []byte) bool {
	if int(s.counts[j]) < s.k {
		return true
	}
	root := s.entries[j*s.k]
	return s.worse(root, vectorMatmulEntry{score: score}, s.id(root), id)
}

// insert adds a hit already admitted by admits; e.off/e.n point into the arena.
func (s *vectorMatmulState) insert(j int, e vectorMatmulEntry) {
	h := s.entries[j*s.k : j*s.k+s.k]
	n := int(s.counts[j])
	if n < s.k {
		h[n] = e
		s.counts[j]++
		for i := n; i > 0; {
			p := (i - 1) / 2
			if !s.worse(h[i], h[p], s.id(h[i]), s.id(h[p])) {
				break
			}
			h[i], h[p] = h[p], h[i]
			i = p
		}
		return
	}
	h[0] = e
	for i := 0; ; {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && s.worse(h[l], h[m], s.id(h[l]), s.id(h[m])) {
			m = l
		}
		if r < n && s.worse(h[r], h[m], s.id(h[r]), s.id(h[m])) {
			m = r
		}
		if m == i {
			return
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
}

// ensureOrder allocates the order scratch used by compaction and merge.
func (s *vectorMatmulState) ensureOrder() error {
	if s.q*s.k == 0 || s.order != nil {
		return nil
	}
	order, err := makeAccountedScratch[uint32](s.allocation, s.mp, s.q*s.k)
	if err != nil {
		return err
	}
	s.order = order
	return nil
}

// spanOrder returns the indices of src's live entries sorted by arena span, in s.order.
func (s *vectorMatmulState) spanOrder(src *vectorMatmulState) ([]uint32, error) {
	if s.q*s.k == 0 {
		return nil, nil
	}
	if err := s.ensureOrder(); err != nil {
		return nil, err
	}
	order := s.order[:0]
	for j := 0; j < src.q; j++ {
		for i := 0; i < int(src.counts[j]); i++ {
			order = append(order, uint32(j*src.k+i))
		}
	}
	slices.SortFunc(order, func(a, b uint32) int {
		x, y := src.entries[a], src.entries[b]
		if c := cmp.Compare(x.off, y.off); c != 0 {
			return c
		}
		return cmp.Compare(x.n, y.n)
	})
	return order, nil
}

// compact moves the referenced id text to the front of the arena in offset order.
func (s *vectorMatmulState) compact() error {
	order, err := s.spanOrder(s)
	if err != nil {
		return err
	}
	w, off := 0, uint32(0)
	var last vectorMatmulSpan
	for i, idx := range order {
		e := &s.entries[idx]
		if sp := e.span(); i == 0 || sp != last {
			copy(s.arena[w:], s.arena[sp.off:sp.off+sp.n])
			last, off = sp, uint32(w)
			w += int(sp.n)
		}
		e.off = off
	}
	s.used = w
	return nil
}

// reserve makes room for extra more arena bytes, compacting before growing.
func (s *vectorMatmulState) reserve(extra int) error {
	if s.used+extra <= len(s.arena) {
		return nil
	}
	if s.used > 0 {
		if err := s.compact(); err != nil {
			return err
		}
	}
	need := s.used + extra
	if need <= len(s.arena) {
		return nil
	}
	if need > math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("vector_matmul: id text exceeds 4 GiB")
	}
	capacity := max(64, 2*len(s.arena))
	for capacity < need {
		capacity *= 2
	}
	arena, err := makeAccountedScratch[byte](s.allocation, s.mp, capacity)
	if err != nil {
		return err
	}
	copy(arena, s.arena[:s.used])
	if cap(s.arena) > 0 {
		mpool.FreeSlice(s.mp, s.arena)
	}
	s.arena = arena
	return nil
}

// appendID copies id into reserved arena space.
func (s *vectorMatmulState) appendID(id []byte) (uint32, error) {
	if s.used+len(id) > len(s.arena) {
		if err := s.reserve(len(id)); err != nil {
			return 0, err
		}
	}
	off := s.used
	copy(s.arena[off:], id)
	s.used += len(id)
	return uint32(off), nil
}

// offer scores a row against every query and records the hits it wins.
func (s *vectorMatmulState) offer(scores []float64, id []byte) error {
	off, stored := uint32(0), false
	for j, score := range scores {
		if !s.admits(j, score, id) {
			continue
		}
		if !stored {
			var err error
			if off, err = s.appendID(id); err != nil {
				return err
			}
			stored = true
		}
		s.insert(j, vectorMatmulEntry{score: score, off: off, n: uint32(len(id))})
	}
	return nil
}

// offerHit offers one row's score for query j. off caches the row's arena offset across
// queries, -1 until the id is stored; the caller reserves the arena space beforehand, so no
// compaction moves a cached offset.
func (s *vectorMatmulState) offerHit(j int, score float64, id []byte, off *int64) error {
	if !s.admits(j, score, id) {
		return nil
	}
	if *off < 0 {
		o, err := s.appendID(id)
		if err != nil {
			return err
		}
		*off = int64(o)
	}
	s.insert(j, vectorMatmulEntry{score: score, off: uint32(*off), n: uint32(len(id))})
	return nil
}

// Merge offers every hit of other.
func (s *vectorMatmulState) Merge(other *vectorMatmulState) error {
	if other == nil {
		return nil
	}
	if other.q != s.q || other.k != s.k {
		return moerr.NewInvalidInputNoCtx("vector_matmul: cannot merge different query configurations")
	}
	// reserve up front: a compaction inside the loop would move the cached offsets
	if err := s.reserve(other.used); err != nil {
		return err
	}
	order, err := s.spanOrder(other)
	if err != nil {
		return err
	}
	var last vectorMatmulSpan
	off := int64(-1)
	for i, idx := range order {
		e := other.entries[idx]
		if sp := e.span(); i == 0 || sp != last {
			last, off = sp, -1
		}
		j := int(idx) / other.k
		id := other.id(e)
		if !s.admits(j, e.score, id) {
			continue
		}
		if off < 0 {
			stored, err := s.appendID(id)
			if err != nil {
				return err
			}
			off = int64(stored)
		}
		s.insert(j, vectorMatmulEntry{score: e.score, off: uint32(off), n: e.n})
	}
	return nil
}

func (s *vectorMatmulState) Size() int64 {
	return int64(cap(s.counts))*4 + int64(cap(s.entries))*16 + int64(cap(s.arena)) + int64(cap(s.order))*4
}

func (s *vectorMatmulState) Free() {
	if cap(s.counts) > 0 {
		mpool.FreeSlice(s.mp, s.counts)
	}
	if cap(s.entries) > 0 {
		mpool.FreeSlice(s.mp, s.entries)
	}
	if cap(s.arena) > 0 {
		mpool.FreeSlice(s.mp, s.arena)
	}
	if cap(s.order) > 0 {
		mpool.FreeSlice(s.mp, s.order)
	}
	s.counts, s.entries, s.arena, s.order, s.used = nil, nil, nil, nil, 0
}

// Encoding: version byte, q uint32, k uint32, then per query a count uint32 and per hit
// score float64 bits, id length uint32 and the id bytes.

func (s *vectorMatmulState) MarshaledSize() int {
	size := 1 + 4 + 4
	for j := 0; j < s.q; j++ {
		size += 4
		for _, e := range s.hits(j) {
			size += 8 + 4 + int(e.n)
		}
	}
	return size
}

func (s *vectorMatmulState) MarshalTo(w io.Writer) error {
	var buf [12]byte
	buf[0] = vectorMatmulStateV1
	binary.LittleEndian.PutUint32(buf[1:], uint32(s.q))
	binary.LittleEndian.PutUint32(buf[5:], uint32(s.k))
	if _, err := w.Write(buf[:9]); err != nil {
		return err
	}
	for j := 0; j < s.q; j++ {
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(s.hits(j))))
		if _, err := w.Write(buf[:4]); err != nil {
			return err
		}
		for _, e := range s.hits(j) {
			binary.LittleEndian.PutUint64(buf[:8], math.Float64bits(e.score))
			binary.LittleEndian.PutUint32(buf[8:], e.n)
			if _, err := w.Write(buf[:12]); err != nil {
				return err
			}
			if _, err := w.Write(s.id(e)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *vectorMatmulState) MarshalBinary() ([]byte, error) {
	var b bytes.Buffer
	b.Grow(s.MarshaledSize())
	err := s.MarshalTo(&b)
	return b.Bytes(), err
}

func (s *vectorMatmulState) UnmarshalBinary(data []byte) error {
	return s.UnmarshalFromReader(bytes.NewReader(data))
}

func (s *vectorMatmulState) UnmarshalFromReader(r io.Reader) error {
	var buf [12]byte
	if _, err := io.ReadFull(r, buf[:9]); err != nil {
		return err
	}
	if buf[0] != vectorMatmulStateV1 {
		return moerr.NewInternalErrorNoCtxf("vector_matmul: unsupported state version %d", buf[0])
	}
	q, k := int(binary.LittleEndian.Uint32(buf[1:])), int(binary.LittleEndian.Uint32(buf[5:]))
	if q != s.q || k != s.k {
		if q > vectorMatmulMaxQueries || k > vectorMatmulMaxTopK || q*k > vectorMatmulMaxEntries {
			return moerr.NewInternalErrorNoCtx("vector_matmul: malformed state")
		}
		mp, allocation := s.mp, s.allocation
		s.Free()
		fresh, err := newVectorMatmulState(mp, allocation, q, k)
		if err != nil {
			return err
		}
		*s = *fresh
	}
	for j := 0; j < s.q; j++ {
		if _, err := io.ReadFull(r, buf[:4]); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(buf[:4])
		if int(n) > s.k {
			return moerr.NewInternalErrorNoCtx("vector_matmul: malformed state")
		}
		for i := uint32(0); i < n; i++ {
			if _, err := io.ReadFull(r, buf[:12]); err != nil {
				return err
			}
			score := math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
			idLen := binary.LittleEndian.Uint32(buf[8:])
			if idLen > math.MaxInt32 {
				return moerr.NewInternalErrorNoCtx("vector_matmul: malformed state")
			}
			if err := s.reserve(int(idLen)); err != nil {
				return err
			}
			off := s.used
			if _, err := io.ReadFull(r, s.arena[off:off+int(idLen)]); err != nil {
				return err
			}
			s.used += int(idLen)
			e := vectorMatmulEntry{score: score, off: uint32(off), n: idLen}
			if s.admits(j, score, s.id(e)) {
				s.insert(j, e)
			}
		}
	}
	return nil
}

// vectorMatmulJSON encodes one group's result, [[["id", distance], ...], ...] with one
// inner array per query, straight into the result vector as the ByteJson value
// ParseSliceToByteJson gives for the result's JSON text. sorted is scratch for one query's
// hits; it is allocated through the allocation account.
type vectorMatmulJSON struct {
	s      *vectorMatmulState
	sorted []vectorMatmulEntry
	size   uint32
}

var _ bytejson.ByteJsonDataValidator = (*vectorMatmulJSON)(nil)

// ByteJson array layout: element count and data size (uint32 each), one value entry per
// element (type code and uint32 offset from the array's start), then the element data.
const (
	vectorMatmulJSONHeader = 8
	vectorMatmulJSONEntry  = 5
	// a pair holds the id string and the distance, an 8-byte number
	vectorMatmulJSONPairFixed = vectorMatmulJSONHeader + 2*vectorMatmulJSONEntry + 8
)

func (e *vectorMatmulJSON) TypeCode() bytejson.TpCode { return bytejson.TpCodeArray }

func (e *vectorMatmulJSON) DataSize() uint32 { return e.size }

// ValidateData checks every distance is finite and computes the data size.
func (e *vectorMatmulJSON) ValidateData() error {
	s := e.s
	total := uint64(vectorMatmulJSONHeader + s.q*vectorMatmulJSONEntry)
	for j := 0; j < s.q; j++ {
		hits := s.hits(j)
		total += uint64(vectorMatmulJSONHeader + len(hits)*vectorMatmulJSONEntry)
		for _, h := range hits {
			if math.IsInf(h.score, 0) || math.IsNaN(h.score) {
				return moerr.NewInvalidInputNoCtx("vector_matmul: the distance overflows the float32 domain")
			}
			n := vectorMatmulJSONStringLen(s.id(h))
			total += uint64(vectorMatmulJSONPairFixed + vectorMatmulUvarintLen(n) + n)
		}
	}
	if total > math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("json value is too large")
	}
	e.size = uint32(total)
	return nil
}

func (e *vectorMatmulJSON) EncodeDataInto(dst []byte) (int, error) {
	s := e.s
	if len(dst) != int(e.size) {
		return 0, moerr.NewInternalErrorNoCtxf("vector_matmul: json size %d, want %d", len(dst), e.size)
	}
	off := vectorMatmulJSONArrayStart(dst, s.q)
	for j := 0; j < s.q; j++ {
		inner := dst[off:]
		vectorMatmulJSONSetEntry(dst, j, bytejson.TpCodeArray, off)
		sorted := e.sortedHits(j)
		in := vectorMatmulJSONArrayStart(inner, len(sorted))
		for i, h := range sorted {
			pair := inner[in:]
			vectorMatmulJSONSetEntry(inner, i, bytejson.TpCodeArray, in)
			p := vectorMatmulJSONArrayStart(pair, 2)
			vectorMatmulJSONSetEntry(pair, 0, bytejson.TpCodeString, p)
			id := s.id(h)
			p += binary.PutUvarint(pair[p:], uint64(vectorMatmulJSONStringLen(id)))
			p += vectorMatmulJSONPutString(pair[p:], id)
			vectorMatmulJSONSetEntry(pair, 1, vectorMatmulJSONPutDistance(pair[p:p+8], -h.score), p)
			p += 8
			binary.LittleEndian.PutUint32(pair[4:], uint32(p))
			in += p
		}
		binary.LittleEndian.PutUint32(inner[4:], uint32(in))
		off += in
	}
	binary.LittleEndian.PutUint32(dst[4:], uint32(off))
	if off != len(dst) {
		return 0, moerr.NewInternalErrorNoCtxf("vector_matmul: json size %d, wrote %d", len(dst), off)
	}
	return off, nil
}

// sortedHits returns query j's hits best first, in e.sorted.
func (e *vectorMatmulJSON) sortedHits(j int) []vectorMatmulEntry {
	s := e.s
	out := append(e.sorted[:0], s.hits(j)...)
	slices.SortFunc(out, func(a, b vectorMatmulEntry) int {
		if s.worse(b, a, s.id(b), s.id(a)) {
			return -1
		}
		if s.worse(a, b, s.id(a), s.id(b)) {
			return 1
		}
		return 0
	})
	return out
}

// vectorMatmulJSONArrayStart writes the element count of an array at dst and returns the
// offset its element data starts at.
func vectorMatmulJSONArrayStart(dst []byte, count int) int {
	binary.LittleEndian.PutUint32(dst, uint32(count))
	return vectorMatmulJSONHeader + count*vectorMatmulJSONEntry
}

// vectorMatmulJSONSetEntry writes value entry i of the array at dst.
func vectorMatmulJSONSetEntry(dst []byte, i int, tp bytejson.TpCode, off int) {
	entry := dst[vectorMatmulJSONHeader+i*vectorMatmulJSONEntry:]
	entry[0] = tp
	binary.LittleEndian.PutUint32(entry[1:], uint32(off))
}

// vectorMatmulJSONPutDistance writes dist as the 8-byte number its JSON text parses to, as
// bytejson.AppendBinaryNumber reads it: text that parses as an int64 or uint64 is that
// integer, other text a float64. The text is the shortest float32 text when dist is a float32
// value (GPU, float32 kernels), float64 text otherwise. Text with a '.' or an exponent goes
// to ParseFloat without the integer parsers.
func vectorMatmulJSONPutDistance(dst []byte, dist float64) bytejson.TpCode {
	if dist == 0 {
		dist = 0
	}
	bitSize := 64
	if float64(float32(dist)) == dist {
		bitSize = 32
	}
	var buf [32]byte
	text := util.UnsafeBytesToString(strconv.AppendFloat(buf[:0], dist, 'g', -1, bitSize))
	if !strings.ContainsAny(text, ".eE") {
		if v, err := strconv.ParseInt(text, 10, 64); err == nil {
			binary.LittleEndian.PutUint64(dst, uint64(v))
			return bytejson.TpCodeInt64
		}
		if v, err := strconv.ParseUint(text, 10, 64); err == nil {
			binary.LittleEndian.PutUint64(dst, v)
			return bytejson.TpCodeUint64
		}
	}
	v, _ := strconv.ParseFloat(text, 64)
	binary.LittleEndian.PutUint64(dst, math.Float64bits(v))
	return bytejson.TpCodeFloat64
}

// vectorMatmulJSONStringLen is the length of id as a JSON string value: JSON text replaces
// each byte that is not part of valid UTF-8 with U+FFFD.
func vectorMatmulJSONStringLen(id []byte) int {
	if utf8.Valid(id) {
		return len(id)
	}
	n := 0
	for i := 0; i < len(id); {
		r, size := utf8.DecodeRune(id[i:])
		if r == utf8.RuneError && size == 1 {
			n += utf8.RuneLen(utf8.RuneError)
		} else {
			n += size
		}
		i += size
	}
	return n
}

// vectorMatmulJSONPutString writes id as a JSON string value and returns its length.
func vectorMatmulJSONPutString(dst, id []byte) int {
	if utf8.Valid(id) {
		return copy(dst, id)
	}
	n := 0
	for i := 0; i < len(id); {
		r, size := utf8.DecodeRune(id[i:])
		if r == utf8.RuneError && size == 1 {
			n += utf8.EncodeRune(dst[n:], utf8.RuneError)
		} else {
			n += copy(dst[n:], id[i:i+size])
		}
		i += size
	}
	return n
}

func vectorMatmulUvarintLen(n int) int {
	var b [binary.MaxVarintLen64]byte
	return binary.PutUvarint(b[:], uint64(n))
}

// appendVectorMatmulID appends the JSON-string text of the id at row.
func appendVectorMatmulID(dst []byte, vec *vector.Vector, row int) []byte {
	switch vec.GetType().Oid {
	case types.T_int8:
		return strconv.AppendInt(dst, int64(vector.GetFixedAtNoTypeCheck[int8](vec, row)), 10)
	case types.T_int16:
		return strconv.AppendInt(dst, int64(vector.GetFixedAtNoTypeCheck[int16](vec, row)), 10)
	case types.T_int32:
		return strconv.AppendInt(dst, int64(vector.GetFixedAtNoTypeCheck[int32](vec, row)), 10)
	case types.T_int64:
		return strconv.AppendInt(dst, vector.GetFixedAtNoTypeCheck[int64](vec, row), 10)
	case types.T_uint8:
		return strconv.AppendUint(dst, uint64(vector.GetFixedAtNoTypeCheck[uint8](vec, row)), 10)
	case types.T_uint16:
		return strconv.AppendUint(dst, uint64(vector.GetFixedAtNoTypeCheck[uint16](vec, row)), 10)
	case types.T_uint32:
		return strconv.AppendUint(dst, uint64(vector.GetFixedAtNoTypeCheck[uint32](vec, row)), 10)
	case types.T_uint64:
		return strconv.AppendUint(dst, vector.GetFixedAtNoTypeCheck[uint64](vec, row), 10)
	case types.T_uuid:
		u := vector.GetFixedAtNoTypeCheck[types.Uuid](vec, row)
		var text [36]byte
		hex.Encode(text[0:8], u[0:4])
		text[8] = '-'
		hex.Encode(text[9:13], u[4:6])
		text[13] = '-'
		hex.Encode(text[14:18], u[6:8])
		text[18] = '-'
		hex.Encode(text[19:23], u[8:10])
		text[23] = '-'
		hex.Encode(text[24:], u[10:])
		return append(dst, text[:]...)
	default:
		return append(dst, vec.GetBytesAt(row)...)
	}
}

// vectorMatmulIDLenBound bounds the id text length at row.
func vectorMatmulIDLenBound(vec *vector.Vector, row int) int {
	if vec.GetType().IsVarlen() {
		return len(vec.GetBytesAt(row))
	}
	return vectorMatmulMaxFixedIDLen
}

// vectorMatmulEngine scores tiles of cells against the queries on a GPU.
type vectorMatmulEngine interface {
	MaxRows() int
	CellBytes() int
	Run(cells []byte, scores []float32) error
	// TopK and RunTopK: see cuvs.BlockScaledMatmul.
	TopK() int
	RunTopK(cells []byte, topScores []float32, topRows []int32, full []float32, tied []uint8) error
	Close()
}

// vectorMatmulGPUHooks create GPU engines: available reports a device meeting the
// engine's baseline, hostBytes the native host memory create allocates for a shape.
type vectorMatmulGPUHooks struct {
	available func() bool
	hostBytes func(format, dim, nq, maxRows int) uint64
	create    func(format, dim, nq int, queryCells []byte, cellBytes, maxRows, topk, metric int) (vectorMatmulEngine, error)
}

// vectorMatmulGPU is nil in builds without GPU support.
var vectorMatmulGPU *vectorMatmulGPUHooks

// vectorMatmulTileBytes bounds the host tile: cells plus scores.
var vectorMatmulTileBytes = 64 << 20

// vectorMatmulTile holds rows waiting for the GPU: their cells back to back, groups and ids.
// A tile whose rows all belong to one group is scored with the GPU top-k (top, rows, tied,
// scores for tied queries); a tile of several groups with the full scores.
type vectorMatmulTile struct {
	cells  []byte
	groups []uint64
	mixed  bool
	idEnds []int
	ids    []byte
	scores []float32
	top    []float32
	rows   []int32
	tied   []uint8
	offs   []int64
}

var _ GroupAggFuncExec = (*vectorMatmulExec)(nil)

type vectorMatmulExec struct {
	aggExec
	cfg *vectorMatmulConfig

	scores []float64
	// row is the operand of the cell being scored on the CPU.
	row metric.VecBlockOperand
	// idText holds the text of a fixed-size id while it is offered; a string id is offered
	// from the input vector.
	idText [vectorMatmulMaxFixedIDLen]byte

	// releaseCfg drops the hold on cfg in vectorMatmulConfigs.
	releaseCfg func()
	// cfgLease holds the account's charge for cfg's retained bytes and the score scratch.
	// Every executor holding a shared configuration is charged for all of it.
	cfgLease *mpool.CapacityLease

	engine vectorMatmulEngine
	// engineLease holds the account's charge for the engine's native host memory.
	engineLease *mpool.CapacityLease
	// engineTried is set once the engine is created or no device is in play; a failed
	// creation leaves it unset, so a retry creates the engine again.
	engineTried bool
	tile        vectorMatmulTile
}

func makeVectorMatmul(mp *mpool.MPool, id int64, isDistinct bool, params []types.Type) (AggFuncExec, error) {
	if isDistinct {
		return nil, moerr.NewNotSupportedNoCtx("vector_matmul in distinct mode")
	}
	if len(params) != 2 || !VectorMatmulIDSupported(params[0].Oid) || !VectorMatmulVecSupported(params[1].Oid) {
		return nil, moerr.NewInternalErrorNoCtxf("vector_matmul: unexpected argument types %v", params)
	}
	exec := &vectorMatmulExec{}
	exec.mp = mp
	exec.aggInfo = aggInfo{
		aggId:              id,
		argTypes:           slices.Clone(params),
		retType:            VectorMatmulReturnType(params),
		emptyNull:          false,
		boundedOpaqueState: true,
		makeMarshalerUnmarshaler: func(mp *mpool.MPool, allocation *AllocationAccount) (MarshalerUnmarshaler, error) {
			return exec.newState(mp, allocation)
		},
		stableEmptyOpaqueState: func(w io.Writer) error {
			empty := exec.emptyState()
			if err := types.WriteInt32(w, int32(empty.MarshaledSize())); err != nil {
				return err
			}
			return empty.MarshalTo(w)
		},
	}
	return exec, nil
}

// emptyState is a group with no rows; it owns no memory and is never freed.
func (exec *vectorMatmulExec) emptyState() *vectorMatmulState {
	empty := &vectorMatmulState{}
	if exec.cfg != nil {
		empty.q, empty.k = exec.cfg.nq, exec.cfg.topk
	}
	return empty
}

func (exec *vectorMatmulExec) newState(mp *mpool.MPool, allocation *AllocationAccount) (*vectorMatmulState, error) {
	if exec.cfg == nil {
		return nil, moerr.NewInternalErrorNoCtx("vector_matmul: configuration is not set")
	}
	return newVectorMatmulState(mp, allocation, exec.cfg.nq, exec.cfg.topk)
}

// SetExtraInformation parses the configuration. With an account attached, which the
// aggregate factory attaches first (admitsConfigParse), the account admits the bound of the
// parse before it allocates, and keeps the configuration's charge after it.
func (exec *vectorMatmulExec) SetExtraInformation(partialResult any, _ int) error {
	raw, ok := partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("vector_matmul: unexpected configuration %T", partialResult)
	}
	// a new configuration replaces the held one; its charge is returned first
	exec.releaseConfig()
	var reservation *mpool.CapacityReservation
	if exec.allocation != nil {
		bound, err := vectorMatmulParseBound(raw, exec.argTypes[1])
		if err != nil {
			return err
		}
		if reservation, err = exec.allocation.reserveCapacity(bound); err != nil {
			return err
		}
	}
	cfg, release, err := acquireVectorMatmulConfig(raw, exec.argTypes[1])
	if err != nil {
		reservation.Abort()
		return err
	}
	exec.cfg, exec.releaseCfg = cfg, release
	exec.scores = make([]float64, cfg.nq)
	if reservation != nil {
		lease, err := reservation.Commit(exec.configBytes())
		if err != nil {
			reservation.Abort()
			exec.releaseConfig()
			return err
		}
		exec.cfgLease = lease
		return nil
	}
	if err := exec.chargeConfig(); err != nil {
		exec.releaseConfig()
		return err
	}
	return nil
}

// admitsConfigParse makes the aggregate factory attach the account before the configuration.
func (exec *vectorMatmulExec) admitsConfigParse() {}

// SetAllocationAccount attaches the account and charges it for a configuration set before.
func (exec *vectorMatmulExec) SetAllocationAccount(allocation *AllocationAccount) error {
	if err := exec.aggExec.SetAllocationAccount(allocation); err != nil {
		return err
	}
	if err := exec.chargeConfig(); err != nil {
		exec.releaseConfig()
		return err
	}
	return nil
}

// ClearAllocationAccount detaches the account and returns the configuration's charge.
func (exec *vectorMatmulExec) ClearAllocationAccount(allocation *AllocationAccount) error {
	if err := exec.aggExec.ClearAllocationAccount(allocation); err != nil {
		return err
	}
	exec.cfgLease.Release()
	exec.cfgLease = nil
	return nil
}

// configBytes is the charge of the configuration: its retained bytes and the score scratch.
func (exec *vectorMatmulExec) configBytes() uint64 {
	return uint64(exec.cfg.retained) + uint64(cap(exec.scores))*8
}

// chargeConfig reserves the configuration's bytes from the account once both are set; a
// denial fails the caller before any row is scored.
func (exec *vectorMatmulExec) chargeConfig() error {
	if exec.allocation == nil || exec.cfg == nil || exec.cfgLease != nil {
		return nil
	}
	n := exec.configBytes()
	reservation, err := exec.allocation.reserveCapacity(n)
	if err != nil {
		return err
	}
	lease, err := reservation.Commit(n)
	if err != nil {
		reservation.Abort()
		return err
	}
	exec.cfgLease = lease
	return nil
}

// releaseConfig returns the configuration's charge and drops the hold on it.
func (exec *vectorMatmulExec) releaseConfig() {
	exec.cfgLease.Release()
	exec.cfgLease = nil
	if exec.releaseCfg != nil {
		exec.releaseCfg()
		exec.releaseCfg = nil
	}
	exec.cfg = nil
}

func (exec *vectorMatmulExec) stateAt(group uint64) (*vectorMatmulState, error) {
	x, y := exec.getXY(group)
	if exec.state[x].mobs[y] == nil {
		s, err := exec.newState(exec.mp, exec.allocation)
		if err != nil {
			return nil, err
		}
		exec.state[x].mobs[y] = s
	}
	return exec.state[x].mobs[y].(*vectorMatmulState), nil
}

func (exec *vectorMatmulExec) preflightState(group uint64) (*vectorMatmulState, error) {
	x, y := exec.getXY(group)
	state := exec.preflightStateAt(x)
	if state == nil || int(y) >= len(state.mobs) {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	if state.mobs[y] == nil {
		s, err := exec.newState(exec.mp, exec.allocation)
		if err != nil {
			return nil, err
		}
		state.mobs[y] = s
	}
	s, ok := state.mobs[y].(*vectorMatmulState)
	if !ok {
		return nil, mpool.ErrAllocationAccountInvariant
	}
	return s, nil
}

// fillRow scores one row and offers it to the group's state.
func (exec *vectorMatmulExec) fillRow(group uint64, row int, vectors []*vector.Vector) error {
	ids, vecs := vectors[0], vectors[1]
	idRow, vecRow := row, row
	if ids.IsConst() {
		idRow = 0
	}
	if vecs.IsConst() {
		vecRow = 0
	}
	if ids.IsNull(uint64(idRow)) || vecs.IsNull(uint64(vecRow)) {
		return nil
	}
	raw := vecs.GetBytesAt(vecRow)
	if err := exec.ensureEngine(); err != nil {
		return err
	}
	if exec.engine != nil {
		// a vecf8/vecf4 cell is validated where it is produced (cast, LOAD, CDC, user
		// variables) and when its column is loaded from an object
		// (objectio.validateVectorCacheData), so the GPU path checks its length only
		if len(raw) != exec.cfg.cellBytes {
			return moerr.NewInvalidInputNoCtxf("vector_matmul: cell is %d bytes, want %d", len(raw), exec.cfg.cellBytes)
		}
		return exec.enqueue(group, raw, ids, idRow)
	}
	if err := exec.cfg.score(raw, &exec.row, exec.scores); err != nil {
		return err
	}
	for j, score := range exec.scores {
		// an overflow NaN ranks last
		if math.IsNaN(score) {
			exec.scores[j] = math.Inf(-1)
		}
	}
	s, err := exec.stateAt(group)
	if err != nil {
		return err
	}
	var id []byte
	if ids.GetType().IsVarlen() {
		id = ids.GetBytesAt(idRow)
	} else {
		id = appendVectorMatmulID(exec.idText[:0], ids, idRow)
	}
	return s.offer(exec.scores, id)
}

// ensureEngine creates the GPU engine on first use when the session allows the GPU and
// the build has a device meeting the baseline; otherwise rows are scored on the CPU. With a
// device in play the GPU scores every row: the engine's native host memory and the tile
// buffers are charged to the allocation account before they are allocated, and a denial
// or an engine error (including a shape the device has no algorithm for) fails the query.
// No row scored on the GPU is scored again on the CPU: the GPU result follows the fp32 GEMM
// contract of the design doc's Decisions.
//
// WARNING: near-zero recomputation is POISON. DO NOT add a CPU fallback, a CPU re-score of
// near-zero distances, or a GPU result check against the CPU kernel. GPU cosine and l2sq
// differ from the CPU near 0 by fp32 rounding relative to the squared norms, by decision of
// cpegeric ("GPU cosine and squared L2 are the GEMM expansion" in
// docs/design/20260930-low-precision-vector-storage.md); an exact near-zero pass measured
// 7x slower and was removed.
func (exec *vectorMatmulExec) ensureEngine() error {
	if exec.engineTried || exec.cfg == nil {
		return nil
	}
	gpu := vectorMatmulGPU
	if !exec.cfg.gpu || gpu == nil || !gpu.available() {
		exec.engineTried = true
		return nil
	}
	dim := int(exec.argTypes[1].Width)
	nq := exec.cfg.nq
	cellBytes := exec.cfg.cellBytes
	rows := max(1, min(65536, vectorMatmulTileBytes/(cellBytes+4*nq)))
	hostBytes := gpu.hostBytes(exec.cfg.engineFormat, dim, nq, rows)
	reservation, err := exec.allocation.reserveCapacity(hostBytes)
	if err != nil {
		return err
	}
	engine, err := gpu.create(exec.cfg.engineFormat, dim, nq, exec.cfg.queryCells, cellBytes, rows, exec.cfg.topk, exec.cfg.metric)
	if err != nil {
		reservation.Abort()
		return err
	}
	var lease *mpool.CapacityLease
	if reservation != nil {
		if lease, err = reservation.Commit(hostBytes); err != nil {
			reservation.Abort()
			engine.Close()
			return err
		}
	}
	k := 0
	if engine.TopK() == exec.cfg.topk {
		k = exec.cfg.topk
	}
	if err := exec.allocTile(engine.MaxRows(), engine.CellBytes(), nq, k); err != nil {
		engine.Close()
		lease.Release()
		return err
	}
	exec.engine, exec.engineLease = engine, lease
	exec.engineTried = true
	return nil
}

// allocTile allocates the tile buffers for n rows from the allocation account; k > 0 adds
// the GPU top-k buffers.
func (exec *vectorMatmulExec) allocTile(n, cellBytes, nq, k int) (err error) {
	a, mp, t := exec.allocation, exec.mp, &exec.tile
	defer func() {
		if err != nil {
			exec.freeTile()
		}
	}()
	if t.cells, err = makeAccountedScratch[byte](a, mp, n*cellBytes); err != nil {
		return err
	}
	if t.groups, err = makeAccountedScratch[uint64](a, mp, n); err != nil {
		return err
	}
	if t.idEnds, err = makeAccountedScratch[int](a, mp, n); err != nil {
		return err
	}
	if t.ids, err = makeAccountedScratch[byte](a, mp, n*8); err != nil {
		return err
	}
	if t.scores, err = makeAccountedScratch[float32](a, mp, n*nq); err != nil {
		return err
	}
	t.cells, t.groups, t.idEnds, t.ids = t.cells[:0], t.groups[:0], t.idEnds[:0], t.ids[:0]
	if k == 0 {
		return nil
	}
	if t.top, err = makeAccountedScratch[float32](a, mp, nq*k); err != nil {
		return err
	}
	if t.rows, err = makeAccountedScratch[int32](a, mp, nq*k); err != nil {
		return err
	}
	if t.tied, err = makeAccountedScratch[uint8](a, mp, nq); err != nil {
		return err
	}
	t.offs, err = makeAccountedScratch[int64](a, mp, n)
	return err
}

// growTileIDs makes room for extra more bytes of id text in the tile.
func (exec *vectorMatmulExec) growTileIDs(extra int) error {
	t := &exec.tile
	if cap(t.ids)-len(t.ids) >= extra {
		return nil
	}
	ids, err := makeAccountedScratch[byte](exec.allocation, exec.mp, max(2*cap(t.ids), len(t.ids)+extra))
	if err != nil {
		return err
	}
	ids = ids[:copy(ids, t.ids)]
	mpool.FreeSlice(exec.mp, t.ids)
	t.ids = ids
	return nil
}

// freeTile returns the tile buffers to the pool.
func (exec *vectorMatmulExec) freeTile() {
	t := &exec.tile
	mpool.FreeSlice(exec.mp, t.cells)
	mpool.FreeSlice(exec.mp, t.groups)
	mpool.FreeSlice(exec.mp, t.idEnds)
	mpool.FreeSlice(exec.mp, t.ids)
	mpool.FreeSlice(exec.mp, t.scores)
	mpool.FreeSlice(exec.mp, t.top)
	mpool.FreeSlice(exec.mp, t.rows)
	mpool.FreeSlice(exec.mp, t.tied)
	mpool.FreeSlice(exec.mp, t.offs)
	*t = vectorMatmulTile{}
}

// tileSize is the bytes held by the tile buffers and the engine's native host memory.
func (exec *vectorMatmulExec) tileSize() int64 {
	t := &exec.tile
	return int64(cap(t.cells)+cap(t.ids)+cap(t.tied)) + int64(cap(t.groups)+cap(t.idEnds)+cap(t.offs))*8 +
		int64(cap(t.scores)+cap(t.top)+cap(t.rows))*4 + int64(exec.engineLease.Capacity())
}

// enqueue appends a row to the GPU tile and scores the tile when it is full.
func (exec *vectorMatmulExec) enqueue(group uint64, cell []byte, ids *vector.Vector, idRow int) error {
	s, err := exec.stateAt(group)
	if err != nil {
		return err
	}
	t := &exec.tile
	if err := exec.growTileIDs(vectorMatmulIDLenBound(ids, idRow)); err != nil {
		return err
	}
	before := len(t.ids)
	t.ids = appendVectorMatmulID(t.ids, ids, idRow)
	s.pending += len(t.ids) - before
	t.idEnds = append(t.idEnds, len(t.ids))
	t.cells = append(t.cells, cell...)
	if len(t.groups) > 0 && t.groups[0] != group {
		t.mixed = true
	}
	t.groups = append(t.groups, group)
	if len(t.groups) == exec.engine.MaxRows() {
		return exec.drain()
	}
	return nil
}

// drain scores the rows waiting in the GPU tile and offers them to their groups.
func (exec *vectorMatmulExec) drain() error {
	t := &exec.tile
	n := len(t.groups)
	if exec.engine == nil || n == 0 {
		return nil
	}
	nq := exec.cfg.nq
	if !t.mixed && t.top != nil {
		if err := exec.drainTopK(); err != nil {
			return err
		}
		t.cells, t.groups, t.idEnds, t.ids = t.cells[:0], t.groups[:0], t.idEnds[:0], t.ids[:0]
		return nil
	}
	if err := exec.engine.Run(t.cells, t.scores[:n*nq]); err != nil {
		return err
	}
	start := 0
	for i, group := range t.groups {
		id := t.ids[start:t.idEnds[i]]
		start = t.idEnds[i]
		s, err := exec.stateAt(group)
		if err != nil {
			return err
		}
		s.pending -= len(id)
		for j := 0; j < nq; j++ {
			// an overflow NaN is the +Inf distance, which ranks last
			score := float64(t.scores[i*nq+j])
			if math.IsNaN(score) {
				score = math.Inf(-1)
			}
			exec.scores[j] = score
		}
		if err := s.offer(exec.scores, id); err != nil {
			return err
		}
	}
	t.cells, t.groups, t.idEnds, t.ids, t.mixed = t.cells[:0], t.groups[:0], t.idEnds[:0], t.ids[:0], false
	return nil
}

// drainTopK scores a one-group tile with the GPU top-k and offers the kept rows of each
// query, or every row of a query with rows tied at its k-th score left out.
func (exec *vectorMatmulExec) drainTopK() error {
	t := &exec.tile
	n, nq, k := len(t.groups), exec.cfg.nq, exec.engine.TopK()
	if err := exec.engine.RunTopK(t.cells, t.top, t.rows, t.scores[:n*nq], t.tied); err != nil {
		return err
	}
	s, err := exec.stateAt(t.groups[0])
	if err != nil {
		return err
	}
	s.pending -= len(t.ids)
	if err := s.reserve(len(t.ids)); err != nil {
		return err
	}
	offs := t.offs[:n]
	for i := range offs {
		offs[i] = -1
	}
	id := func(r int) []byte {
		if r == 0 {
			return t.ids[:t.idEnds[0]]
		}
		return t.ids[t.idEnds[r-1]:t.idEnds[r]]
	}
	for j := 0; j < nq; j++ {
		if t.tied[j] != 0 {
			for r, score := range t.scores[j*n : (j+1)*n] {
				if err := s.offerHit(j, float64(score), id(r), &offs[r]); err != nil {
					return err
				}
			}
			continue
		}
		for i := j * k; i < (j+1)*k; i++ {
			r := int(t.rows[i])
			if r < 0 {
				continue
			}
			if err := s.offerHit(j, float64(t.top[i]), id(r), &offs[r]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (exec *vectorMatmulExec) SaveIntermediateResult(cnt int64, flags [][]uint8, writer io.Writer) error {
	if err := exec.drain(); err != nil {
		return err
	}
	return exec.aggExec.SaveIntermediateResult(cnt, flags, writer)
}

func (exec *vectorMatmulExec) SaveIntermediateResultWithStringSource(cnt int64, flags [][]uint8, writer io.Writer, includeStringSource bool) error {
	if err := exec.drain(); err != nil {
		return err
	}
	return exec.aggExec.SaveIntermediateResultWithStringSource(cnt, flags, writer, includeStringSource)
}

func (exec *vectorMatmulExec) SaveIntermediateResultOfChunk(chunk int, writer io.Writer) error {
	if err := exec.drain(); err != nil {
		return err
	}
	return exec.aggExec.SaveIntermediateResultOfChunk(chunk, writer)
}

func (exec *vectorMatmulExec) SaveIntermediateResultOfChunkWithStringSource(chunk int, writer io.Writer, includeStringSource bool) error {
	if err := exec.drain(); err != nil {
		return err
	}
	return exec.aggExec.SaveIntermediateResultOfChunkWithStringSource(chunk, writer, includeStringSource)
}

func (exec *vectorMatmulExec) SaveSpillIntermediateRows(chunk int, rows []int32, writer io.Writer) error {
	if err := exec.drain(); err != nil {
		return err
	}
	return exec.aggExec.SaveSpillIntermediateRows(chunk, rows, writer)
}

func (exec *vectorMatmulExec) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	return exec.fillRow(uint64(groupIndex), row, vectors)
}

func (exec *vectorMatmulExec) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	for row := 0; row < vectors[1].Length(); row++ {
		if err := exec.fillRow(uint64(groupIndex), row, vectors); err != nil {
			return err
		}
	}
	return nil
}

func (exec *vectorMatmulExec) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.fillRow(group-1, offset+i, vectors); err != nil {
			return err
		}
	}
	return nil
}

// PreflightBatchFill creates the target states and reserves arena space for one id per row.
func (exec *vectorMatmulExec) PreflightBatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	if exec.allocation == nil {
		return nil
	}
	if err := validatePreflightVectors(vectors, offset, len(groups)); err != nil {
		return err
	}
	var targets [hashmap.UnitLimit]uint64
	var extra [hashmap.UnitLimit]int
	n := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		row := offset + i
		idRow, vecRow := row, row
		if vectors[0].IsConst() {
			idRow = 0
		}
		if vectors[1].IsConst() {
			vecRow = 0
		}
		if vectors[0].IsNull(uint64(idRow)) || vectors[1].IsNull(uint64(vecRow)) {
			continue
		}
		if _, _, _, err := exec.validatePreflightTarget(group); err != nil {
			return err
		}
		found := slices.Index(targets[:n], group)
		if found < 0 {
			found = n
			targets[n] = group
			n++
		}
		extra[found] += vectorMatmulIDLenBound(vectors[0], idRow)
	}
	for i := 0; i < n; i++ {
		s, err := exec.preflightState(targets[i] - 1)
		if err != nil {
			return err
		}
		if err := s.reserve(extra[i] + s.pending); err != nil {
			return err
		}
	}
	// the GPU engine and its tile are created and sized here, before the fill
	if err := exec.ensureEngine(); err != nil {
		return err
	}
	if exec.engine != nil {
		total := 0
		for i := 0; i < n; i++ {
			total += extra[i]
		}
		if err := exec.growTileIDs(total); err != nil {
			return err
		}
	}
	return nil
}

func (exec *vectorMatmulExec) mergeCompatible(other *vectorMatmulExec) bool {
	return other != nil && exec.aggId == other.aggId &&
		len(other.argTypes) == 2 &&
		exec.argTypes[0].Eq(other.argTypes[0]) && exec.argTypes[1].Eq(other.argTypes[1])
}

func (exec *vectorMatmulExec) Merge(next AggFuncExec, groupIdx1, groupIdx2 int) error {
	other, ok := next.(*vectorMatmulExec)
	if !ok || !exec.mergeCompatible(other) {
		return mpool.ErrAllocationAccountMismatch
	}
	if err := other.drain(); err != nil {
		return err
	}
	x2, y2 := other.getXY(uint64(groupIdx2))
	if other.state[x2].mobs[y2] == nil {
		return nil
	}
	target, err := exec.stateAt(uint64(groupIdx1))
	if err != nil {
		return err
	}
	return target.Merge(other.state[x2].mobs[y2].(*vectorMatmulState))
}

func (exec *vectorMatmulExec) BatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		if err := exec.Merge(next, int(group-1), offset+i); err != nil {
			return err
		}
	}
	return nil
}

// PreflightBatchMerge creates the target states, reserves each source's arena bytes and
// allocates each target's order scratch.
func (exec *vectorMatmulExec) PreflightBatchMerge(next AggFuncExec, offset int, groups []uint64) error {
	if exec.allocation == nil {
		return nil
	}
	other, ok := next.(*vectorMatmulExec)
	if !ok || !exec.mergeCompatible(other) || len(groups) > hashmap.UnitLimit ||
		offset < 0 || offset > other.GetNumGroups()-len(groups) {
		return mpool.ErrAllocationAccountInvalid
	}
	if err := other.drain(); err != nil {
		return err
	}
	var targets [hashmap.UnitLimit]uint64
	var extra [hashmap.UnitLimit]int
	n := 0
	for i, group := range groups {
		if group == GroupNotMatched {
			continue
		}
		sx, sy := other.getXY(uint64(offset + i))
		if sx >= len(other.state) || int(sy) >= len(other.state[sx].mobs) {
			return mpool.ErrAllocationAccountInvariant
		}
		source, _ := other.state[sx].mobs[sy].(*vectorMatmulState)
		if source == nil {
			continue
		}
		if _, _, _, err := exec.validatePreflightTarget(group); err != nil {
			return err
		}
		found := slices.Index(targets[:n], group)
		if found < 0 {
			found = n
			targets[n] = group
			n++
		}
		extra[found] += source.used
	}
	for i := 0; i < n; i++ {
		s, err := exec.preflightState(targets[i] - 1)
		if err != nil {
			return err
		}
		if err := s.reserve(extra[i]); err != nil {
			return err
		}
		if err := s.ensureOrder(); err != nil {
			return err
		}
	}
	return nil
}

func (exec *vectorMatmulExec) Flush() (_ []*vector.Vector, retErr error) {
	if exec.cfg == nil {
		return nil, moerr.NewInternalErrorNoCtx("vector_matmul: configuration is not set")
	}
	if err := exec.drain(); err != nil {
		return nil, err
	}
	results := make([]*vector.Vector, len(exec.state))
	defer func() {
		if retErr != nil {
			for _, result := range results {
				if result != nil {
					result.Free(exec.mp)
				}
			}
		}
	}()
	sorted, err := makeAccountedScratch[vectorMatmulEntry](exec.allocation, exec.mp, exec.cfg.topk)
	if err != nil {
		return nil, err
	}
	defer mpool.FreeSlice(exec.mp, sorted)
	enc := &vectorMatmulJSON{sorted: sorted}
	empty := exec.emptyState()
	for x, state := range exec.state {
		result, err := exec.allocation.newVector(exec.retType)
		if err != nil {
			return nil, err
		}
		results[x] = result
		if err := result.PreExtend(int(state.length), exec.mp); err != nil {
			return nil, err
		}
		for y := 0; y < int(state.length); y++ {
			enc.s = empty
			if state.mobs[y] != nil {
				enc.s = state.mobs[y].(*vectorMatmulState)
			}
			if err := vector.AppendByteJsonEncoded(result, enc, exec.mp); err != nil {
				return nil, err
			}
		}
	}
	return results, nil
}

func (exec *vectorMatmulExec) Size() int64 {
	var size int64
	for _, state := range exec.state {
		size += int64(cap(state.mobs)) * 8
		for _, mob := range state.mobs {
			if mob != nil {
				size += mob.(*vectorMatmulState).Size()
			}
		}
	}
	if exec.cfg != nil {
		size += int64(exec.configBytes())
	}
	return size + exec.tileSize()
}

func (exec *vectorMatmulExec) Free() {
	if exec.engine != nil {
		exec.engine.Close()
		exec.engine = nil
	}
	exec.freeTile()
	exec.engineLease.Release()
	exec.engineLease = nil
	exec.engineTried = false
	exec.aggExec.Free()
	exec.state = nil
	exec.releaseConfig()
}
