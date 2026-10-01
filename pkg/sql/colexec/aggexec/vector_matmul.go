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
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"slices"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// vector_matmul(topk, id, vec, queries [, options]): per group, the top `topk` dot
// products of each query against the group's vectors, as JSON
// [[["id", score], ...], ...] with one inner array per query. topk, queries and options
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

// EncodeVectorMatmulConfig packs topk, the queries and options JSON strings and whether
// the session allows the GPU (gpu_mode).
func EncodeVectorMatmulConfig(topk int64, queries, options string, gpu bool) []byte {
	out := make([]byte, 0, 17+len(queries)+len(options))
	out = binary.LittleEndian.AppendUint64(out, uint64(topk))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(queries)))
	out = append(out, queries...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(options)))
	out = append(out, options...)
	if gpu {
		return append(out, 1)
	}
	return append(out, 0)
}

func decodeVectorMatmulConfig(b []byte) (topk int64, queries, options string, gpu bool, err error) {
	read := func() (string, bool) {
		if len(b) < 4 {
			return "", false
		}
		n := binary.LittleEndian.Uint32(b)
		if uint64(len(b)-4) < uint64(n) {
			return "", false
		}
		s := string(b[4 : 4+n])
		b = b[4+n:]
		return s, true
	}
	if len(b) < 8 {
		return 0, "", "", false, moerr.NewInternalErrorNoCtx("vector_matmul: malformed configuration")
	}
	topk = int64(binary.LittleEndian.Uint64(b))
	b = b[8:]
	var ok1, ok2 bool
	queries, ok1 = read()
	options, ok2 = read()
	if !ok1 || !ok2 || len(b) != 1 || b[0] > 1 {
		return 0, "", "", false, moerr.NewInternalErrorNoCtx("vector_matmul: malformed configuration")
	}
	return topk, queries, options, b[0] == 1, nil
}

// vectorMatmulOptions is the optional fifth argument.
type vectorMatmulOptions struct {
	Mode      *string `json:"mode"`
	TileBytes *int64  `json:"tile_bytes"`
}

// vectorMatmulConfig is the parsed configuration shared by all groups.
type vectorMatmulConfig struct {
	topk int
	nq   int
	// queryCells holds the queries back to back in the column's cell format.
	queryCells []byte
	// cellBytes is the byte length of one cell.
	cellBytes int
	// engineFormat is the GPU engine format of the column type.
	engineFormat int
	// score validates a cell and writes its dot product with each query into out.
	score func(cell []byte, out []float64) error
	// gpu reports that the session allows the GPU.
	gpu bool
}

func parseVectorMatmulConfig(raw []byte, vecType types.Type) (*vectorMatmulConfig, error) {
	topk, queriesText, optionsText, gpu, err := decodeVectorMatmulConfig(raw)
	if err != nil {
		return nil, err
	}
	if topk < 1 || topk > vectorMatmulMaxTopK {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: topk %d out of range [1, %d]", topk, vectorMatmulMaxTopK)
	}
	if optionsText != "" {
		var options vectorMatmulOptions
		dec := json.NewDecoder(bytes.NewReader([]byte(optionsText)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&options); err != nil {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: invalid options %q: %v", optionsText, err)
		}
		if options.Mode != nil {
			switch *options.Mode {
			case "auto", "cpu":
			case "gpu":
				return nil, moerr.NewNotSupportedNoCtx("vector_matmul: gpu mode in this build")
			default:
				return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: invalid mode %q", *options.Mode)
			}
		}
		if options.TileBytes != nil && *options.TileBytes < 0 {
			return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: invalid tile_bytes %d", *options.TileBytes)
		}
	}

	var queries [][]float32
	if err := json.Unmarshal([]byte(queriesText), &queries); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("vector_matmul: queries must be a JSON array of vectors: %v", err)
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
	cfg := &vectorMatmulConfig{topk: int(topk), nq: len(queries), gpu: gpu}
	switch oid := vecType.Oid; oid {
	case types.T_array_float8, types.T_array_float4:
		err = cfg.setBlockScaled(oid, dim, queries)
	case types.T_array_float32:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineF32, queries, func(v float32) (float32, bool) { return v, true })
	case types.T_array_float16:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineF16, queries, func(v float32) (types.Float16, bool) { return types.Float16FromFloat32(v), true })
	case types.T_array_bf16:
		err = setVectorMatmulPlain(cfg, vectorMatmulEngineBF16, queries, func(v float32) (types.BF16, bool) { return types.BF16FromFloat32(v), true })
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

// setBlockScaled quantizes the queries to the vecf8/vecf4 format and scores cells with the
// block-scaled CPU kernel.
func (cfg *vectorMatmulConfig) setBlockScaled(oid types.T, dim int, queries [][]float32) error {
	format, _ := oid.BlockScaledFormat()
	cfg.engineFormat = vectorMatmulEngineMXFP8
	if format == types.BlockScaledNVFP4 {
		cfg.engineFormat = vectorMatmulEngineNVFP4
	}
	cfg.cellBytes = types.BlockScaledCellSize(format, dim)
	ops := make([]metric.VecBlockOperand, len(queries))
	for i, q := range queries {
		start := len(cfg.queryCells)
		var err error
		if cfg.queryCells, err = types.AppendBlockScaled(cfg.queryCells, format, q); err != nil {
			return err
		}
		if ops[i].Cell, err = types.ParseBlockScaledCell(cfg.queryCells[start:len(cfg.queryCells):len(cfg.queryCells)]); err != nil {
			return err
		}
	}
	var row metric.VecBlockOperand
	cfg.score = func(cell []byte, out []float64) error {
		c, err := types.ParseBlockScaledCell(cell)
		if err != nil {
			return err
		}
		row.Cell = c
		for j := range ops {
			// the inner product distance is -dot; an overflow NaN is +Inf, which ranks last
			dist, err := metric.VecBlockInnerProduct(&row, &ops[j])
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
	fn, err := metric.ResolveDistanceFn[T, float64](metric.Metric_InnerProduct)
	if err != nil {
		return err
	}
	typed := make([][]T, len(queries))
	for i, q := range queries {
		typed[i] = make([]T, len(q))
		for k, v := range q {
			var ok bool
			if typed[i][k], ok = conv(v); !ok {
				return moerr.NewInvalidInputNoCtxf("vector_matmul: query value %v is not representable in the column type", v)
			}
		}
		cfg.queryCells = append(cfg.queryCells, types.ArrayToBytes(typed[i])...)
	}
	cfg.engineFormat = engineFormat
	cfg.cellBytes = len(cfg.queryCells) / len(queries)
	cfg.score = func(cell []byte, out []float64) error {
		if len(cell) != cfg.cellBytes {
			return moerr.NewInvalidInputNoCtxf("vector_matmul: cell is %d bytes, want %d", len(cell), cfg.cellBytes)
		}
		row := types.BytesToArray[T](cell)
		for j, q := range typed {
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

// vectorMatmulState is one group's state: per query a min-heap of at most k entries
// ordered worst first, and an arena holding the id text of the entries.
type vectorMatmulState struct {
	q, k    int
	counts  []uint32
	entries []vectorMatmulEntry // q*k; heap of query j at [j*k, j*k+counts[j])
	arena   []byte              // used prefix [0, used)
	used    int
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
	if s.counts[j] == 0 {
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

// compact moves the referenced id text to the front of the arena in offset order.
func (s *vectorMatmulState) compact() {
	type ref struct{ off, n uint32 }
	var refs []ref
	seen := make(map[uint32]struct{})
	for j := 0; j < s.q; j++ {
		for _, e := range s.hits(j) {
			if _, ok := seen[e.off]; !ok {
				seen[e.off] = struct{}{}
				refs = append(refs, ref{e.off, e.n})
			}
		}
	}
	slices.SortFunc(refs, func(a, b ref) int { return int(a.off) - int(b.off) })
	moved := make(map[uint32]uint32, len(refs))
	w := 0
	for _, r := range refs {
		copy(s.arena[w:], s.arena[r.off:r.off+r.n])
		moved[r.off] = uint32(w)
		w += int(r.n)
	}
	for j := 0; j < s.q; j++ {
		h := s.hits(j)
		for i := range h {
			h[i].off = moved[h[i].off]
		}
	}
	s.used = w
}

// reserve makes room for extra more arena bytes, compacting before growing.
func (s *vectorMatmulState) reserve(extra int) error {
	if s.used+extra <= len(s.arena) {
		return nil
	}
	s.compact()
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

// Merge offers every hit of other.
func (s *vectorMatmulState) Merge(other *vectorMatmulState) error {
	if other == nil {
		return nil
	}
	if other.q != s.q || other.k != s.k {
		return moerr.NewInvalidInputNoCtx("vector_matmul: cannot merge different query configurations")
	}
	// reserve up front: a compaction inside the loop would move offsets cached in moved
	if err := s.reserve(other.used); err != nil {
		return err
	}
	moved := make(map[uint32]uint32)
	for j := 0; j < other.q; j++ {
		for _, e := range other.hits(j) {
			id := other.id(e)
			if !s.admits(j, e.score, id) {
				continue
			}
			off, ok := moved[e.off]
			if !ok {
				var err error
				if off, err = s.appendID(id); err != nil {
					return err
				}
				moved[e.off] = off
			}
			s.insert(j, vectorMatmulEntry{score: e.score, off: off, n: e.n})
		}
	}
	return nil
}

// sorted returns query j's hits best first.
func (s *vectorMatmulState) sorted(j int) []vectorMatmulEntry {
	out := slices.Clone(s.hits(j))
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

func (s *vectorMatmulState) Size() int64 {
	return int64(cap(s.counts))*4 + int64(cap(s.entries))*16 + int64(cap(s.arena))
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
	s.counts, s.entries, s.arena, s.used = nil, nil, nil, 0
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
		binary.LittleEndian.PutUint32(buf[:4], s.counts[j])
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

// appendJSON appends the group's result to out.
func (s *vectorMatmulState) appendJSON(out []byte) ([]byte, error) {
	out = append(out, '[')
	for j := 0; j < s.q; j++ {
		if j > 0 {
			out = append(out, ',')
		}
		out = append(out, '[')
		for i, e := range s.sorted(j) {
			if i > 0 {
				out = append(out, ',')
			}
			id, err := json.Marshal(string(s.id(e)))
			if err != nil {
				return nil, err
			}
			if math.IsInf(e.score, 0) || math.IsNaN(e.score) {
				return nil, moerr.NewInvalidInputNoCtx("vector_matmul: dot product overflows the float32 domain")
			}
			out = append(out, '[')
			out = append(out, id...)
			out = append(out, ',')
			out = strconv.AppendFloat(out, e.score, 'g', -1, 32)
			out = append(out, ']')
		}
		out = append(out, ']')
	}
	return append(out, ']'), nil
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
		return append(dst, vector.GetFixedAtNoTypeCheck[types.Uuid](vec, row).String()...)
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
	Close()
}

// newVectorMatmulEngine creates a GPU engine. It is nil in builds without GPU support and
// returns a nil engine when no device is visible.
var newVectorMatmulEngine func(format, dim, nq int, queryCells []byte, cellBytes, maxRows int) (vectorMatmulEngine, error)

// vectorMatmulTileBytes bounds the host tile: cells plus scores.
var vectorMatmulTileBytes = 64 << 20

// vectorMatmulTile holds rows waiting for the GPU: their cells back to back, groups and ids.
type vectorMatmulTile struct {
	cells  []byte
	groups []uint64
	idEnds []int
	ids    []byte
	scores []float32
}

var _ GroupAggFuncExec = (*vectorMatmulExec)(nil)

type vectorMatmulExec struct {
	aggExec
	cfg *vectorMatmulConfig

	scores []float64
	idBuf  []byte

	engine      vectorMatmulEngine
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

// emptyState is a group with no rows; it owns no pool memory and is never freed.
func (exec *vectorMatmulExec) emptyState() *vectorMatmulState {
	empty := &vectorMatmulState{}
	if exec.cfg != nil {
		empty.q, empty.k = exec.cfg.nq, exec.cfg.topk
		empty.counts = make([]uint32, empty.q)
	}
	return empty
}

func (exec *vectorMatmulExec) newState(mp *mpool.MPool, allocation *AllocationAccount) (*vectorMatmulState, error) {
	if exec.cfg == nil {
		return nil, moerr.NewInternalErrorNoCtx("vector_matmul: configuration is not set")
	}
	return newVectorMatmulState(mp, allocation, exec.cfg.nq, exec.cfg.topk)
}

func (exec *vectorMatmulExec) SetExtraInformation(partialResult any, _ int) error {
	raw, ok := partialResult.([]byte)
	if !ok {
		return moerr.NewInternalErrorNoCtxf("vector_matmul: unexpected configuration %T", partialResult)
	}
	cfg, err := parseVectorMatmulConfig(raw, exec.argTypes[1])
	if err != nil {
		return err
	}
	exec.cfg = cfg
	exec.scores = make([]float64, cfg.nq)
	return nil
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
		if len(raw) != exec.cfg.cellBytes {
			return moerr.NewInvalidInputNoCtxf("vector_matmul: cell is %d bytes, want %d", len(raw), exec.cfg.cellBytes)
		}
		if exec.argTypes[1].Oid.IsBlockScaledArray() {
			if _, err := types.ParseBlockScaledCell(raw); err != nil {
				return err
			}
		}
		return exec.enqueue(group, raw, ids, idRow)
	}
	if err := exec.cfg.score(raw, exec.scores); err != nil {
		return err
	}
	for j, score := range exec.scores {
		// an overflow NaN ranks last
		if math.IsNaN(score) {
			score = math.Inf(-1)
		}
		exec.scores[j] = float64(float32(score))
	}
	s, err := exec.stateAt(group)
	if err != nil {
		return err
	}
	exec.idBuf = appendVectorMatmulID(exec.idBuf[:0], ids, idRow)
	return s.offer(exec.scores, exec.idBuf)
}

// ensureEngine creates the GPU engine on first use when the session allows the GPU and
// the build has a device; otherwise rows are scored on the CPU.
func (exec *vectorMatmulExec) ensureEngine() error {
	if exec.engineTried {
		return nil
	}
	exec.engineTried = true
	if !exec.cfg.gpu || newVectorMatmulEngine == nil {
		return nil
	}
	dim := int(exec.argTypes[1].Width)
	nq := exec.cfg.nq
	cellBytes := exec.cfg.cellBytes
	rows := max(1, min(65536, vectorMatmulTileBytes/(cellBytes+4*nq)))
	engine, err := newVectorMatmulEngine(exec.cfg.engineFormat, dim, nq, exec.cfg.queryCells, cellBytes, rows)
	if err != nil || engine == nil {
		return err
	}
	exec.engine = engine
	n := engine.MaxRows()
	exec.tile = vectorMatmulTile{
		cells:  make([]byte, 0, n*engine.CellBytes()),
		groups: make([]uint64, 0, n),
		idEnds: make([]int, 0, n),
		scores: make([]float32, n*nq),
	}
	return nil
}

// enqueue appends a row to the GPU tile and scores the tile when it is full.
func (exec *vectorMatmulExec) enqueue(group uint64, cell []byte, ids *vector.Vector, idRow int) error {
	s, err := exec.stateAt(group)
	if err != nil {
		return err
	}
	t := &exec.tile
	before := len(t.ids)
	t.ids = appendVectorMatmulID(t.ids, ids, idRow)
	s.pending += len(t.ids) - before
	t.idEnds = append(t.idEnds, len(t.ids))
	t.cells = append(t.cells, cell...)
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
	t.cells, t.groups, t.idEnds, t.ids = t.cells[:0], t.groups[:0], t.idEnds[:0], t.ids[:0]
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

// PreflightBatchMerge creates the target states and reserves each source's arena bytes.
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
	empty := exec.emptyState()
	var buf []byte
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
			s := empty
			if state.mobs[y] != nil {
				s = state.mobs[y].(*vectorMatmulState)
			}
			if buf, err = s.appendJSON(buf[:0]); err != nil {
				return nil, err
			}
			bj, err := types.ParseSliceToByteJson(buf)
			if err != nil {
				return nil, err
			}
			if err := vector.AppendByteJson(result, bj, false, exec.mp); err != nil {
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
	return size
}

func (exec *vectorMatmulExec) Free() {
	if exec.engine != nil {
		exec.engine.Close()
		exec.engine = nil
	}
	exec.tile = vectorMatmulTile{}
	exec.aggExec.Free()
	exec.state = nil
}
