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

package objectio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// An additive V3 sub-meta section shares publication, identity and GC with its
// source object. Ordinary/older readers continue reading SchemaData unchanged.
const SchemaJSONSubcolumns DataMetaType = CkpMetaEnd + 1
const jsonSubcolumnMaxPaths = 32
const jsonSubcolumnMaxCandidates = 256
const jsonSubcolumnMaxBytes = 8 << 20
const JSONProjectionPrefix = "mo-json-subcolumns-v1:"
const jsonProjectionMaxColumns = 64
const jsonProjectionMaxBytes = 64 << 10

type JSONPathProjection map[uint16][]string
type jsonProjectionContextKey struct{}

func WithJSONPathProjection(ctx context.Context, p JSONPathProjection) context.Context {
	return context.WithValue(ctx, jsonProjectionContextKey{}, p)
}
func JSONPathProjectionFromContext(ctx context.Context) JSONPathProjection {
	p, _ := ctx.Value(jsonProjectionContextKey{}).(JSONPathProjection)
	return p
}
func EncodeJSONPathProjection(p JSONPathProjection) string {
	// This is an optional optimization. Only emit contracts the receiving scan
	// admits; an empty result leaves the original JSON read path in place.
	if len(p) == 0 || len(p) > jsonProjectionMaxColumns {
		return ""
	}
	b, err := json.Marshal(p)
	if err != nil || len(JSONProjectionPrefix)+len(b) > jsonProjectionMaxBytes {
		return ""
	}
	encoded := JSONProjectionPrefix + string(b)
	if _, err := DecodeJSONPathProjection(encoded); err != nil {
		return ""
	}
	return encoded
}
func DecodeJSONPathProjection(s string) (JSONPathProjection, error) {
	if s == "" {
		return nil, nil
	}
	if !strings.HasPrefix(s, JSONProjectionPrefix) || len(s) > jsonProjectionMaxBytes {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON projection contract")
	}
	var p JSONPathProjection
	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, JSONProjectionPrefix)), &p); err != nil {
		return nil, err
	}
	if len(p) > jsonProjectionMaxColumns {
		return nil, moerr.NewInvalidInputNoCtx("too many JSON projection columns")
	}
	for _, paths := range p {
		if len(paths) == 0 || len(paths) > jsonSubcolumnMaxPaths {
			return nil, moerr.NewInvalidInputNoCtx("invalid JSON projection path count")
		}
		for _, s := range paths {
			path, err := bytejson.ParseJsonPath(s)
			if err != nil {
				return nil, err
			}
			if _, ok := path.ObjectKeys(); !ok {
				return nil, moerr.NewInvalidInputNoCtx("JSON projection requires object-key paths")
			}
		}
	}
	return p, nil
}

type jsonSubcolumnPath struct {
	Keys   []string `json:"keys"`
	Column uint16   `json:"column"`
}
type jsonSubcolumnSource struct {
	Seqnum     uint16              `json:"seqnum"`
	NullColumn uint16              `json:"null_column"`
	Paths      []jsonSubcolumnPath `json:"paths"`
}
type jsonSubcolumnManifest struct {
	Version int                   `json:"version"`
	Block   uint16                `json:"block"`
	Rows    int                   `json:"rows"`
	Sources []jsonSubcolumnSource `json:"sources"`
}
type jsonPathCandidate struct {
	keys  []string
	count int
}

func jsonObjectKeyPath(keys []string) string {
	var b strings.Builder
	b.WriteByte('$')
	for _, key := range keys {
		b.WriteByte('.')
		quoted, _ := json.Marshal(key)
		b.Write(quoted)
	}
	return b.String()
}
func persistedJSON(vec *vector.Vector, row int) (bytejson.ByteJson, error) {
	data := vec.GetBytesAt(row)
	if len(data) < 2 {
		return bytejson.ByteJson{}, moerr.NewInvalidInputNoCtx("invalid stored JSON value")
	}
	bj := bytejson.ByteJson{Type: data[0], Data: data[1:]}
	if !bytejson.IsValidByteJson(bj) {
		return bj, moerr.NewInvalidInputNoCtx("invalid stored JSON value")
	}
	return bj, nil
}

// Build bounded, automatically chosen per-block scalar paths. Values retain
// their original ByteJson tags/spelling, including drift to containers. Long
// tails and paths which exceed the budget remain solely in the source column.
func buildJSONSubcolumns(bat *batch.Batch, seqnums *Seqnums, block uint16) (*batch.Batch, error) {
	n := 0
	if len(bat.Vecs) > 0 {
		n = bat.Vecs[0].Length()
	}
	hasJSON := false
	for _, v := range bat.Vecs {
		if v.GetType().Oid == types.T_json {
			hasJSON = true
		}
	}
	if !hasJSON {
		return nil, nil
	}
	mp := mpool.MustNewZero()
	out := batch.NewWithSize(0)
	defer func() { out.Clean(mp); mpool.DeleteMPool(mp) }()
	// Serialize the prepared batch into independent vector-owned bytes before
	// releasing this bounded builder's pool. The caller consumes a decoded view.
	manifest := jsonSubcolumnManifest{Version: 1, Block: block, Rows: n}
	out.Vecs = append(out.Vecs, nil)
	remaining := jsonSubcolumnMaxBytes - (64 << 10)
	pathCount := 0
	manifestBytes := 1024
	for ci, vec := range bat.Vecs {
		if vec.GetType().Oid != types.T_json || pathCount >= jsonSubcolumnMaxPaths || n > int(BlockMaxRows) || len(manifest.Sources) >= 64 {
			continue
		}
		documents := make([]bytejson.ByteJson, n)
		for row := 0; row < n; row++ {
			if vec.IsNull(uint64(row)) {
				continue
			}
			var e error
			documents[row], e = persistedJSON(vec, row)
			if e != nil {
				return nil, e
			}
		}
		candidates := make(map[string]*jsonPathCandidate)
		var visit func(bytejson.ByteJson, []string, int)
		visit = func(bj bytejson.ByteJson, keys []string, depth int) {
			if depth >= 16 || bj.Type != bytejson.TpCodeObject {
				return
			}
			for i := 0; i < bj.GetElemCnt(); i++ {
				key := string(bj.GetObjectKey(i))
				if len(key) > 512 {
					continue
				}
				child := bj.GetObjectVal(i)
				next := append(append([]string(nil), keys...), key)
				if child.Type == bytejson.TpCodeObject {
					visit(child, next, depth+1)
					continue
				}
				if child.Type == bytejson.TpCodeArray || child.IsNull() {
					continue
				}
				path := jsonObjectKeyPath(next)
				candidate := candidates[path]
				if candidate == nil {
					if len(candidates) >= jsonSubcolumnMaxCandidates {
						continue
					}
					candidate = &jsonPathCandidate{keys: next}
					candidates[path] = candidate
				}
				candidate.count++
			}
		}
		for row := 0; row < min(n, 1024); row++ {
			if vec.IsNull(uint64(row)) {
				continue
			}
			visit(documents[row], nil, 0)
		}
		ordered := make([]string, 0, len(candidates))
		for p := range candidates {
			ordered = append(ordered, p)
		}
		sort.Slice(ordered, func(i, j int) bool {
			a, b := candidates[ordered[i]], candidates[ordered[j]]
			if a.count != b.count {
				return a.count > b.count
			}
			return ordered[i] < ordered[j]
		})
		if len(ordered) == 0 || n+1024 > remaining {
			continue
		}
		remaining -= n + 1024
		source := jsonSubcolumnSource{Seqnum: seqnums.Seqs[ci], NullColumn: uint16(len(out.Vecs))}
		rootNulls := vector.NewVec(types.T_bool.ToType())
		out.Vecs = append(out.Vecs, rootNulls)
		for row := 0; row < n; row++ {
			if err := vector.AppendFixed(rootNulls, vec.IsNull(uint64(row)), false, mp); err != nil {
				return nil, err
			}
		}
		for _, p := range ordered {
			if pathCount >= jsonSubcolumnMaxPaths {
				break
			}
			candidate := candidates[p]
			keyBytes, _ := json.Marshal(candidate.keys)
			if manifestBytes+len(keyBytes)+64 > 48<<10 {
				continue
			}
			parsed, err := bytejson.ParseJsonPath(p)
			if err != nil {
				// A candidate the path parser cannot represent must not prevent
				// persistence of the valid source document.
				continue
			}
			values := vector.NewVec(types.T_json.ToType())
			used := n*(types.VarlenaSize+1) + 1024
			accepted := true
			for row := 0; row < n; row++ {
				if vec.IsNull(uint64(row)) {
					err = vector.AppendByteJson(values, bytejson.Null, true, mp)
				} else {
					bj := documents[row]
					var value bytejson.ByteJson
					var present bool
					value, present = bj.QuerySimpleExist(&parsed)
					used += len(value.Data) + 1
					if used > remaining {
						accepted = false
						break
					}
					err = vector.AppendByteJson(values, value, !present, mp)
				}
				if err != nil {
					values.Free(mp)
					return nil, err
				}
			}
			if !accepted {
				values.Free(mp)
				continue
			}
			remaining -= used
			manifestBytes += len(keyBytes) + 64
			source.Paths = append(source.Paths, jsonSubcolumnPath{Keys: candidate.keys, Column: uint16(len(out.Vecs))})
			out.Vecs = append(out.Vecs, values)
			pathCount++
		}
		manifest.Sources = append(manifest.Sources, source)
	}
	metadata, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	out.Vecs[0], err = vector.NewConstBytes(types.T_varchar.ToType(), metadata, n, mp)
	if err != nil {
		return nil, err
	}
	out.SetRowCount(n)
	data, err := out.MarshalBinary()
	if err != nil {
		return nil, err
	}
	prepared := batch.NewWithSize(0)
	if err = prepared.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return prepared, nil
}

func (w *objectWriterV1) SetJSONSubcolumns(enabled bool) {
	w.Lock()
	defer w.Unlock()
	w.jsonSubcolumns = enabled
}

func readJSONManifest(ctx context.Context, header ObjectMeta, name string, blk uint16, rows int, fs fileservice.FileService, mp *mpool.MPool, policy fileservice.Policy) (jsonSubcolumnManifest, ObjectDataMeta, error) {
	var manifest jsonSubcolumnManifest
	// Legacy objects have no section. Check count/index before SubMeta's slice.
	if header.SubMetaCount() <= uint16(SchemaJSONSubcolumns-2) {
		return manifest, nil, nil
	}
	body := header[IOEntryHeaderSize:]
	directoryEnd := int(metaHeaderLen+schemaCountLen) + int(header.SubMetaCount())*int(typePosLen)
	if directoryEnd > len(body) {
		return manifest, nil, moerr.NewInvalidInputNoCtx("truncated JSON subcolumn section directory")
	}
	entry := body[int(metaHeaderLen+schemaCountLen)+int(SchemaJSONSubcolumns-2)*int(typePosLen):]
	if types.DecodeUint16(entry[:schemaType]) != uint16(SchemaJSONSubcolumns-2) {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn section identity")
	}
	offset := int(types.DecodeUint32(entry[schemaType+schemaBlockCount : typePosLen]))
	if offset < directoryEnd || offset > len(body)-int(headerLen) {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn section offset")
	}
	sub := ObjectDataMeta(body[offset:])
	indexEnd := uint64(sub.Length()) + uint64(blockCountLen) + uint64(sub.BlockCount())*uint64(posLen)
	if indexEnd > uint64(len(sub)) || sub.BlockCount() != uint32(types.DecodeUint16(entry[schemaType:schemaType+schemaBlockCount])) {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn block directory")
	}
	if sub.BlockCount() == 0 {
		return manifest, nil, nil
	}
	if uint32(blk) >= sub.BlockCount() {
		return manifest, nil, moerr.NewInvalidInputNoCtx("JSON subcolumn block count mismatch")
	}
	off, length := sub.BlockIndex().BlockMetaPos(uint32(blk))
	if uint64(off) < indexEnd || uint64(off)+uint64(length) > uint64(len(sub)) || length < headerLen {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn block extent")
	}
	bm := BlockObject(sub[off : off+length])
	if uint64(headerLen)+uint64(bm.GetMetaColumnCount())*uint64(colMetaLen) > uint64(length) || bm.GetColumnCount() == 0 || bm.GetColumnCount() > 97 || bm.GetMetaColumnCount() != bm.GetColumnCount() || bm.GetRows() != uint32(rows) || bm.ColumnMeta(0).DataType() != uint8(types.T_varchar) {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn block shape")
	}
	ioVec, err := ReadOneBlock(ctx, &sub, name, sub.BlockHeader().StartID()+blk, []uint16{0}, []types.Type{types.T_varchar.ToType()}, mp, fs, policy)
	if err != nil {
		return manifest, nil, err
	}
	defer ioVec.Release()
	decoded, err := DecodeCached(ioVec.Entries[0].CachedData)
	if err != nil {
		return manifest, nil, err
	}
	metaVec := decoded.(*vector.Vector)
	defer metaVec.Free(nil)
	if rows <= 0 || metaVec.GetType().Oid != types.T_varchar || !metaVec.IsConst() || metaVec.IsConstNull() || metaVec.Length() != rows || len(metaVec.GetBytesAt(0)) > 64<<10 {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn manifest vector")
	}
	dec := json.NewDecoder(bytes.NewReader(metaVec.GetBytesAt(0)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&manifest); err != nil {
		return manifest, nil, err
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return manifest, nil, moerr.NewInvalidInputNoCtx("trailing JSON subcolumn manifest data")
	}
	if manifest.Version != 1 || manifest.Block != blk || manifest.Rows != rows || len(manifest.Sources) > 64 {
		return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn manifest identity")
	}
	seen := make(map[uint16]bool)
	columns := map[uint16]bool{0: true}
	count := 0

	for _, source := range manifest.Sources {
		if source.Seqnum > header.MustDataMeta().GetBlockMeta(uint32(blk)).GetMaxSeqnum() || types.T(header.MustDataMeta().GetBlockMeta(uint32(blk)).ColumnMeta(source.Seqnum).DataType()) != types.T_json || seen[source.Seqnum] || columns[source.NullColumn] || source.NullColumn >= bm.GetColumnCount() {
			return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn source")
		}
		seen[source.Seqnum] = true
		columns[source.NullColumn] = true
		if bm.ColumnMeta(source.NullColumn).DataType() != uint8(types.T_bool) {
			return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON root null column type")
		}
		keys := make(map[string]bool)
		for _, p := range source.Paths {
			count++
			key := jsonObjectKeyPath(p.Keys)
			if len(p.Keys) == 0 || len(p.Keys) > 16 || keys[key] || columns[p.Column] || p.Column >= bm.GetColumnCount() || count > jsonSubcolumnMaxPaths {
				return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn path")
			}
			for _, k := range p.Keys {
				if len(k) > 512 || !utf8.ValidString(k) {
					return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON subcolumn object key")
				}
			}
			if bm.ColumnMeta(p.Column).DataType() != uint8(types.T_json) {
				return manifest, nil, moerr.NewInvalidInputNoCtx("invalid JSON fragment column type")
			}
			keys[key] = true
			columns[p.Column] = true
		}
	}
	return manifest, sub, nil
}

func insertJSONProjection(fields map[string]any, keys []string, value bytejson.ByteJson) {
	for _, key := range keys[:len(keys)-1] {
		existing, ok := fields[key]
		if ok {
			if m, ok := existing.(map[string]any); ok {
				fields = m
				continue
			}
			return
		}
		nested := make(map[string]any)
		fields[key] = nested
		fields = nested
	}
	fields[keys[len(keys)-1]] = value
}

// ReadProjectedJSONColumns reads only independently stored requested paths
// when every path is available. Otherwise it reads the entire original column.
// The synthesized documents are private read results, never cached under the
// original JSON column's offset or used for full-document consumers.
func ReadProjectedJSONColumns(ctx context.Context, header ObjectMeta, name string, blk uint16, cols []uint16, typs []types.Type, mp *mpool.MPool, fs fileservice.FileService, policy fileservice.Policy) (result fileservice.IOVector, err error) {
	if len(cols) != len(typs) {
		return result, moerr.NewInvalidInputNoCtx("JSON read column/type count mismatch")
	}
	if err = context.Cause(ctx); err != nil {
		return result, err
	}
	projection := JSONPathProjectionFromContext(ctx)
	data := header.MustDataMeta()
	eligible := false
	for i, seq := range cols {
		if len(projection[seq]) > 0 && typs[i].Oid == types.T_json {
			eligible = true
		}
	}
	if !eligible {
		return ReadOneBlock(ctx, &data, name, blk, cols, typs, mp, fs, policy)
	}
	rows := int(data.GetBlockMeta(uint32(blk)).GetRows())
	if rows == 0 {
		return ReadOneBlock(ctx, &data, name, blk, cols, typs, mp, fs, policy)
	}
	manifest, sub, err := readJSONManifest(ctx, header, name, blk, rows, fs, mp, policy)
	if err != nil {
		return result, err
	}
	result.FilePath = name
	result.Entries = make([]fileservice.IOEntry, len(cols))
	var rawCols []uint16
	var rawTypes []types.Type
	var rawPositions []int
	defer func() {
		if err != nil {
			result.Release()
		}
	}()
	for ci, seq := range cols {
		paths := projection[seq]
		var source *jsonSubcolumnSource
		if len(paths) > 0 && typs[ci].Oid == types.T_json {
			for i := range manifest.Sources {
				if manifest.Sources[i].Seqnum == seq {
					source = &manifest.Sources[i]
					break
				}
			}
		}
		matched := make([]jsonSubcolumnPath, 0, len(paths))
		if source != nil {
			for _, p := range paths {
				parsed, e := bytejson.ParseJsonPath(p)
				if e != nil {
					return result, e
				}
				keys, ok := parsed.ObjectKeys()
				if !ok {
					break
				}
				canonical := jsonObjectKeyPath(keys)
				for _, candidate := range source.Paths {
					if jsonObjectKeyPath(candidate.Keys) == canonical {
						matched = append(matched, candidate)
						break
					}
				}
			}
		}
		if source == nil || len(matched) != len(paths) {
			rawCols = append(rawCols, seq)
			rawTypes = append(rawTypes, typs[ci])
			rawPositions = append(rawPositions, ci)
			continue
		}
		indexes := []uint16{source.NullColumn}
		subTypes := []types.Type{types.T_bool.ToType()}
		for _, p := range matched {
			indexes = append(indexes, p.Column)
			subTypes = append(subTypes, types.T_json.ToType())
		}
		values, e := ReadOneBlock(ctx, &sub, name, sub.BlockHeader().StartID()+blk, indexes, subTypes, mp, fs, policy)
		if e != nil {
			return result, e
		}
		vectors := make([]*vector.Vector, len(indexes))
		for i, entry := range values.Entries {
			obj, e := DecodeCached(entry.CachedData)
			if e != nil {
				for _, v := range vectors {
					if v != nil {
						v.Free(nil)
					}
				}
				values.Release()
				return result, e
			}
			vectors[i] = obj.(*vector.Vector)
		}
		output := vector.NewVec(types.T_json.ToType())
		materializeErr := func() error {
			// Fixed-width root flags borrow the IOVector's backing storage.
			// Release the decoded views first, then their owner, on every exit.
			defer values.Release()
			defer func() {
				for _, v := range vectors {
					v.Free(nil)
				}
			}()
			for i, v := range vectors {
				if v.Length() != rows || *v.GetType() != subTypes[i] {
					return moerr.NewInvalidInputNoCtx("JSON subcolumn payload shape mismatch")
				}
			}

			for row := 0; row < rows; row++ {
				if row%256 == 0 {
					if e := context.Cause(ctx); e != nil {
						return e
					}
				}
				if vectors[0].IsNull(uint64(row)) {
					return moerr.NewInvalidInputNoCtx("NULL JSON subcolumn root flag")
				}
				if vector.GetFixedAtNoTypeCheck[bool](vectors[0], row) {
					if e := vector.AppendByteJson(output, bytejson.Null, true, mp); e != nil {
						return e
					}
					continue
				}
				fields := make(map[string]any, len(matched))
				// Ancestors are inserted last and keep the exact stored container value.
				order := make([]int, len(matched))
				for i := range order {
					order[i] = i
				}
				sort.SliceStable(order, func(i, j int) bool { return len(matched[order[i]].Keys) > len(matched[order[j]].Keys) })
				for _, i := range order {
					v := vectors[i+1]
					if v.IsNull(uint64(row)) {
						continue
					}
					bj, e := persistedJSON(v, row)
					if e != nil {
						return e
					}
					insertJSONProjection(fields, matched[i].Keys, bj)
				}
				bj, e := bytejson.CreateByteJSON(fields)
				if e != nil {
					return e
				}
				if e = vector.AppendByteJson(output, bj, false, mp); e != nil {
					return e
				}
			}
			return nil
		}()
		if materializeErr != nil {
			output.Free(mp)
			return result, materializeErr
		}
		var encoded bytes.Buffer
		h := IOEntryHeader{Type: IOET_ColData, Version: IOET_ColumnData_CurrVer}
		encoded.Write(EncodeIOEntryHeader(&h))
		e = output.MarshalBinaryWithBuffer(&encoded)
		output.Free(mp)
		if e != nil {
			return result, e
		}
		result.Entries[ci] = fileservice.IOEntry{CachedData: fileservice.NewBytes(bytes.Clone(encoded.Bytes()))}
	}
	if len(rawCols) > 0 {
		raw, e := ReadOneBlock(ctx, &data, name, blk, rawCols, rawTypes, mp, fs, policy)
		if e != nil {
			return result, e
		}
		for i, pos := range rawPositions {
			result.Entries[pos] = raw.Entries[i]
		}
		raw.Entries = nil
	}
	return result, nil
}
