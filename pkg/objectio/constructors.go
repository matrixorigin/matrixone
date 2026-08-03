// Copyright 2021 Matrix Origin
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

package objectio

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

var eventVectorDestinationNotEmpty = logutil.Event{
	Name:    "objectio.vector.destination-not-empty",
	Message: "ObjectIO destination vector must be readonly or empty",
}

type CacheConstructor = func(ctx context.Context, r io.Reader, buf []byte, allocator fileservice.CacheDataAllocator) (fscache.Data, error)
type CacheConstructorFactory = func(size int64, algo uint8) CacheConstructor

func newColumnIOEntry(ext Extent, factory CacheConstructorFactory) fileservice.IOEntry {
	return fileservice.IOEntry{
		Offset:            int64(ext.Offset()),
		Size:              int64(ext.Length()),
		CachedDataSize:    int64(ext.OriginSize()),
		ToCacheData:       factory(int64(ext.OriginSize()), ext.Alg()),
		ValidateCacheData: validateVectorCacheData,
	}
}

// validatedVectorCacheData owns bytes that passed the full V2 vector validator
// after decompression. The backing Data is deliberately not embedded or
// exposed: every consumer receives an independent snapshot, so a decoded
// Vector cannot mutate the sealed representation retained by the cache.
type validatedVectorCacheData struct {
	data fscache.Data
}

var _ fscache.Data = (*validatedVectorCacheData)(nil)

func (d *validatedVectorCacheData) Bytes() []byte {
	return d.validatedVectorSnapshot()
}

func (d *validatedVectorCacheData) Size() int64 {
	return int64(len(d.data.Bytes()))
}

func (d *validatedVectorCacheData) Slice(length int) fscache.Data {
	buf := d.data.Bytes()
	if length == len(buf) {
		return d
	}
	// A changed range gets independent storage and no validation capability.
	// In particular, do not call Data.Slice: fileservice.Bytes slices in place,
	// which would mutate this sealed owner and every existing alias.
	return fileservice.NewBytes(bytes.Clone(buf[:length]))
}

func (d *validatedVectorCacheData) Retain() {
	d.data.Retain()
}

func (d *validatedVectorCacheData) Release() {
	d.data.Release()
}

func (d *validatedVectorCacheData) validatedVectorSnapshot() []byte {
	return bytes.Clone(d.data.Bytes())
}

// validatedVectorBackingForScope may only be consumed synchronously by an
// ObjectIO operation that returns owned data or scalar/row-offset results. The
// returned slice, and any Vector bound to it, must never escape to a caller.
func (d *validatedVectorCacheData) validatedVectorBackingForScope() []byte {
	return d.data.Bytes()
}

type validatedVectorCacheDataMarker interface {
	validatedVectorSnapshot() []byte
	validatedVectorBackingForScope() []byte
}

func isValidatedVectorCacheData(data fscache.Data) bool {
	_, ok := data.(validatedVectorCacheDataMarker)
	return ok
}

func vectorCacheDataBytes(data fscache.Data) (buf []byte, trusted bool) {
	if marked, ok := data.(validatedVectorCacheDataMarker); ok {
		return marked.validatedVectorSnapshot(), true
	}
	return data.Bytes(), false
}

// use this to replace all other constructors
func constructorFactory(size int64, algo uint8) CacheConstructor {
	return func(ctx context.Context, reader io.Reader, data []byte, allocator fileservice.CacheDataAllocator) (cacheData fscache.Data, err error) {
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return
			}
		}

		// no compress
		if algo == compress.None {
			cacheData = allocator.CopyToCacheData(ctx, data)
			return cacheData, nil
		}

		// lz4 compress
		decompressedData := allocator.AllocateCacheDataWithHint(ctx, int(size), malloc.NoClear)
		bs, err := compress.Decompress(data, decompressedData.Bytes(), compress.Lz4)
		if err != nil {
			decompressedData.Release()
			return
		}
		decompressedData = decompressedData.Slice(len(bs))
		return decompressedData, nil
	}
}

// columnCacheConstructorFactory validates V2 column data once, after
// decompression and before it can enter the memory cache. Varlen cache hits can
// then bind an isolated snapshot without repeating linear value scans. Fixed
// vectors stay unmarked because they do not perform the per-value scan, and
// copying their payload would make the common empty-bitmap path O(bytes).
func columnCacheConstructorFactory(size int64, algo uint8) CacheConstructor {
	construct := constructorFactory(size, algo)
	return func(
		ctx context.Context,
		reader io.Reader,
		data []byte,
		allocator fileservice.CacheDataAllocator,
	) (fscache.Data, error) {
		cacheData, err := construct(ctx, reader, data, allocator)
		if err != nil {
			return nil, err
		}
		validated, err := validateVectorCacheData(cacheData)
		if err != nil {
			if cacheData != nil {
				cacheData.Release()
			}
			return nil, err
		}
		return validated, nil
	}
}

func validateVectorCacheData(data fscache.Data) (fscache.Data, error) {
	if data == nil {
		return nil, moerr.NewInvalidInputNoCtx("nil object column cache data")
	}
	if isValidatedVectorCacheData(data) {
		return data, nil
	}
	buf := data.Bytes()
	if len(buf) < IOEntryHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	if header.Type != IOET_ColData {
		return nil, moerr.NewInvalidInputNoCtx("invalid object column data type")
	}
	if header.Version == IOET_ColumnData_V1 {
		// V1 null bitmaps compute their count while decoding, so V1 cannot
		// provide the constant-time trusted contract. Keep it on the legacy
		// path without granting the marker.
		return data, nil
	}
	if header.Version != IOET_ColumnData_V2 {
		return nil, moerr.NewInvalidInputNoCtx("invalid object column data version")
	}
	var vec vector.Vector
	if err := vec.UnmarshalBinary(buf[IOEntryHeaderSize:]); err != nil {
		return nil, err
	}
	if !vec.GetType().IsVarlen() {
		return data, nil
	}
	return &validatedVectorCacheData{data: data}, nil
}

func Decode(buf []byte) (any, error) {
	return decode(buf, false)
}

// DecodeCached uses the trusted V2 bind only for FileService cache data that
// objectio itself validated before cache admission. The trusted decoder binds
// an independent snapshot, not the sealed cache backing. Unmarked data uses
// the normal versioned decoder; V2 therefore keeps its full validation.
func DecodeCached(data fscache.Data) (any, error) {
	if data == nil {
		return nil, moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	buf, trusted := vectorCacheDataBytes(data)
	return decode(buf, trusted)
}

func decode(buf []byte, trusted bool) (any, error) {
	if len(buf) < IOEntryHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	codec := GetIOEntryCodec(*header)
	if codec.NoUnmarshal() {
		return buf[IOEntryHeaderSize:], nil
	}
	if trusted && header.Type == IOET_ColData && header.Version == IOET_ColumnData_V2 {
		vec := vector.NewVec(types.Type{})
		if err := vec.UnmarshalBinaryTrusted(buf[IOEntryHeaderSize:]); err != nil {
			return nil, err
		}
		return vec, nil
	}
	v, err := codec.Decode(buf[IOEntryHeaderSize:])
	if err != nil {
		return nil, err
	}
	return v, nil
}

// NOTE: hack way to get vector
func MustVectorTo(toVec *vector.Vector, buf []byte) (err error) {
	return mustVectorTo(toVec, buf, false)
}

// MustVectorToCached binds cache-backed column data to toVec. Only data with
// objectio's private validation marker uses the trusted path.
func MustVectorToCached(toVec *vector.Vector, data fscache.Data) error {
	if data == nil {
		return moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	buf, trusted := vectorCacheDataBytes(data)
	return mustVectorTo(toVec, buf, trusted)
}

// MustVectorToCachedWithMpool is the owned hot-path variant of
// MustVectorToCached. A validated varlen Vector is duplicated into mp before it
// is exposed; fixed and unmarked data keep the checked zero-copy path.
func MustVectorToCachedWithMpool(toVec *vector.Vector, data fscache.Data, mp *mpool.MPool) error {
	if data == nil {
		return moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	marked, ok := data.(validatedVectorCacheDataMarker)
	if !ok || mp == nil {
		return MustVectorToCached(toVec, data)
	}
	buf := marked.validatedVectorBackingForScope()
	warnVectorDestinationNotEmpty(toVec, len(buf))
	var borrowed vector.Vector
	if err := mustVectorTo(&borrowed, buf, true); err != nil {
		return err
	}
	owned, err := borrowed.Dup(mp)
	if err != nil {
		return err
	}
	*toVec = *owned
	return nil
}

// CopyCachedVectorRows materializes selected cache-backed rows into toVec
// without exposing a writable alias to the cached representation. The source
// is bound only for the duration of this call, while its FileService cache
// lease is held by the caller.
func CopyCachedVectorRows(toVec *vector.Vector, data fscache.Data, sels []int64, mp *mpool.MPool) error {
	return copyCachedVector(toVec, data, sels, false, mp)
}

// CopyCachedVectorAll materializes a complete cache-backed Vector into toVec
// without exposing a writable alias to the cached representation.
func CopyCachedVectorAll(toVec *vector.Vector, data fscache.Data, mp *mpool.MPool) error {
	return copyCachedVector(toVec, data, nil, true, mp)
}

func copyCachedVector(
	toVec *vector.Vector,
	data fscache.Data,
	sels []int64,
	allRows bool,
	mp *mpool.MPool,
) error {
	if toVec == nil {
		return moerr.NewInvalidInputNoCtx("nil object column destination")
	}
	if mp == nil {
		return moerr.NewInvalidInputNoCtx("nil mpool for object column materialization")
	}
	var source vector.Vector
	if err := bindCachedVectorForScope(&source, data); err != nil {
		return err
	}
	defer source.Free(nil)

	// Width, scale, and other logical metadata may legitimately differ across
	// schema versions while the physical Vector representation remains the same.
	// The OID is the compatibility boundary used by the existing Union paths.
	if toVec.GetType().Oid != source.GetType().Oid {
		return moerr.NewInvalidInputNoCtxf(
			"object column type %s does not match destination type %s",
			source.GetType().String(),
			toVec.GetType().String(),
		)
	}
	if allRows {
		return toVec.UnionBatch(&source, 0, source.Length(), nil, mp)
	}
	for _, sel := range sels {
		if sel < 0 || sel >= int64(source.Length()) {
			return moerr.NewInvalidInputNoCtxf(
				"object column row %d out of range [0, %d)",
				sel,
				source.Length(),
			)
		}
	}
	return toVec.Union(&source, sels, mp)
}

// SearchCachedVector executes a fixed supported varlen search while the cache
// entry is pinned. The borrowed Vector never crosses the ObjectIO boundary.
func SearchCachedVector(
	entry fileservice.IOEntry,
	search *ReadFilterSearch,
	sorted bool,
) ([]int64, error) {
	if search == nil {
		return nil, moerr.NewInvalidInputNoCtx("nil object column search")
	}
	var source vector.Vector
	if err := bindCachedVectorForScope(&source, entry.CachedData); err != nil {
		return nil, err
	}
	defer source.Free(nil)
	return search.search(&source, sorted), nil
}

func (s *ReadFilterSearch) search(source *vector.Vector, sorted bool) []int64 {
	if source.GetType().Oid != s.oid {
		return allReadFilterRows(source.Length(), false)
	}
	if source.Length() == 0 || len(s.terms) == 0 {
		return nil
	}
	if source.IsConstNull() || source.GetNulls().Any() {
		// Primary-key columns are non-null. Treat a malformed/unavailable null
		// column as unknown and fail open; persisted tombstone checks must never
		// turn it into a false negative.
		return allReadFilterRows(source.Length(), false)
	}
	if len(s.terms) == 1 {
		return s.terms[0].search(source, sorted)
	}
	marks := make([]int64, source.Length())
	for i := range s.terms {
		for _, row := range s.terms[i].search(source, sorted) {
			if row >= 0 && row < int64(len(marks)) {
				marks[row] = 1
			}
		}
	}
	rows := marks[:0]
	for row, matched := range marks {
		if matched != 0 {
			rows = append(rows, int64(row))
		}
	}
	return rows
}

func (t *readFilterSearchTerm) search(source *vector.Vector, sorted bool) []int64 {
	if source.IsConst() {
		value := source.GetBytesAt(0)
		if !t.matches(value, sorted) {
			return nil
		}
		return allReadFilterRows(
			source.Length(),
			t.kind == readFilterSearchGreater,
		)
	}
	switch t.kind {
	case readFilterSearchExact:
		if len(t.values) == 0 {
			return nil
		}
		if sorted {
			return vector.VarlenBinarySearchOffsetByValFactory(t.values)(source)
		}
		return vector.VarlenLinearSearchOffsetByValFactory(t.values)(source)
	case readFilterSearchPrefix:
		if len(t.values) == 0 {
			return nil
		}
		if len(t.values) == 1 {
			if sorted {
				return vector.CollectOffsetsByPrefixEqFactory(t.values[0])(source)
			}
			return vector.LinearCollectOffsetsByPrefixEqFactory(t.values[0])(source)
		}
		if sorted {
			return searchSortedReadFilterPrefixes(source, t.values)
		}
		col, area := vector.MustVarlenaRawData(source)
		rows := make([]int64, 0, len(t.values))
		for row := 0; row < source.Length(); row++ {
			value := col[row].GetByteSlice(area)
			for i := range t.values {
				if bytes.HasPrefix(value, t.values[i]) {
					rows = append(rows, int64(row))
					break
				}
			}
		}
		return rows
	case readFilterSearchLess:
		return vector.VarlenSearchOffsetByLess(t.ub, t.closed, sorted)(source)
	case readFilterSearchGreater:
		return vector.VarlenSearchOffsetByGreat(t.lb, t.closed, sorted)(source)
	case readFilterSearchBetween:
		if sorted {
			return vector.CollectOffsetsByBetweenString(string(t.lb), string(t.ub), t.hint)(source)
		}
		return vector.LinearCollectOffsetsByBetweenString(string(t.lb), string(t.ub), t.hint)(source)
	case readFilterSearchPrefixBetween:
		if t.hint == 0 {
			if sorted {
				return vector.CollectOffsetsByPrefixBetweenFactory(t.lb, t.ub)(source)
			}
			return vector.LinearCollectOffsetsByPrefixBetweenFactory(t.lb, t.ub)(source)
		}
		if sorted {
			return vector.CollectOffsetsByPrefixInRangeFactory(t.lb, t.ub, t.hint)(source)
		}
		return vector.LinearCollectOffsetsByPrefixInRangeFactory(t.lb, t.ub, t.hint)(source)
	default:
		return nil
	}
}

func searchSortedReadFilterPrefixes(source *vector.Vector, values [][]byte) []int64 {
	col, area := vector.MustVarlenaRawData(source)
	rows := make([]int64, 0, len(values))
	valuePos := 0
	value := values[0]
	row := 0
	for row < source.Length() {
		rowValue := col[row].GetByteSlice(area)
		cmp := types.PrefixCompare(rowValue, value)
		if cmp > 0 {
			valuePos++
			if valuePos == len(values) {
				break
			}
			value = values[valuePos]
			continue
		}
		if cmp == 0 {
			rows = append(rows, int64(row))
			row++
			continue
		}
		row = gallopReadFilterPrefixGE(col, area, value, row+1, source.Length())
	}
	return rows
}

func gallopReadFilterPrefixGE(
	col []types.Varlena,
	area, value []byte,
	low, high int,
) int {
	previous, current, step := low, low, 1
	for current < high &&
		types.PrefixCompare(col[current].GetByteSlice(area), value) < 0 {
		previous = current + 1
		current += step
		step <<= 1
	}
	if current > high {
		current = high
	}
	for previous < current {
		middle := int(uint(previous+current) >> 1)
		if types.PrefixCompare(col[middle].GetByteSlice(area), value) < 0 {
			previous = middle + 1
		} else {
			current = middle
		}
	}
	return previous
}

func (t *readFilterSearchTerm) matches(value []byte, sorted bool) bool {
	switch t.kind {
	case readFilterSearchExact:
		for i := range t.values {
			if bytes.Equal(value, t.values[i]) {
				return true
			}
		}
		return false
	case readFilterSearchPrefix:
		for i := range t.values {
			if bytes.HasPrefix(value, t.values[i]) {
				return true
			}
		}
		return false
	case readFilterSearchLess:
		cmp := bytes.Compare(value, t.ub)
		return cmp < 0 || t.closed && cmp == 0
	case readFilterSearchGreater:
		cmp := bytes.Compare(value, t.lb)
		return cmp > 0 || t.closed && cmp == 0
	case readFilterSearchBetween:
		return readFilterRangeMatches(
			bytes.Compare(value, t.lb),
			bytes.Compare(value, t.ub),
			t.hint,
		)
	case readFilterSearchPrefixBetween:
		leftCmp := types.PrefixCompare(value, t.lb)
		if sorted && t.hint == 0 {
			// Preserve CollectOffsetsByPrefixBetweenFactory's sorted lower
			// bound semantics, which use the full byte comparison.
			leftCmp = bytes.Compare(value, t.lb)
		}
		return readFilterRangeMatches(
			leftCmp,
			types.PrefixCompare(value, t.ub),
			t.hint,
		)
	default:
		return false
	}
}

func readFilterRangeMatches(leftCmp, rightCmp int, hint uint8) bool {
	switch hint {
	case 0:
		return leftCmp >= 0 && rightCmp <= 0
	case 1:
		return leftCmp > 0 && rightCmp <= 0
	case 2:
		return leftCmp >= 0 && rightCmp < 0
	case 3:
		return leftCmp > 0 && rightCmp < 0
	default:
		return false
	}
}

func allReadFilterRows(length int, reverse bool) []int64 {
	rows := make([]int64, length)
	for i := range rows {
		if reverse {
			rows[i] = int64(length - i - 1)
		} else {
			rows[i] = int64(i)
		}
	}
	return rows
}

// FilterCachedRowsByCommitTS removes rows newer than snapshot from sels while
// the cache entry is pinned. It never exposes the commit-ts Vector.
func FilterCachedRowsByCommitTS(
	data fscache.Data,
	sels []int64,
	snapshot types.TS,
) ([]int64, error) {
	var commits vector.Vector
	if err := bindCachedVectorForScope(&commits, data); err != nil {
		return nil, err
	}
	defer commits.Free(nil)
	if commits.GetType().Oid != types.T_TS || commits.IsConstNull() {
		return nil, moerr.NewInvalidInputNoCtx("object commit-ts column is unavailable")
	}

	filtered := sels[:0]
	for _, sel := range sels {
		if sel < 0 || sel >= int64(commits.Length()) {
			return nil, moerr.NewInvalidInputNoCtxf(
				"object commit-ts row %d out of range [0, %d)",
				sel,
				commits.Length(),
			)
		}
		if commits.IsNull(uint64(sel)) {
			return nil, moerr.NewInvalidInputNoCtxf("object commit-ts row %d is null", sel)
		}
		commit := vector.GetFixedAtNoTypeCheck[types.TS](&commits, int(sel))
		if !commit.GT(&snapshot) {
			filtered = append(filtered, sel)
		}
	}
	return filtered, nil
}

// AnyCachedTSInRange checks selected commit timestamps without returning a
// borrowed Vector. usable is false when the column cannot provide row-level
// commit timestamps, preserving the caller's conservative fallback.
func AnyCachedTSInRange(
	data fscache.Data,
	sels []int64,
	from, to types.TS,
) (matched bool, usable bool, err error) {
	return AnyCachedTSInRangeWithAbort(data, nil, sels, from, to)
}

// AnyCachedTSInRangeWithAbort checks selected commit timestamps while ignoring
// rows marked aborted. A nil or const-null abort vector represents the legacy
// commitTS-only object format.
func AnyCachedTSInRangeWithAbort(
	data fscache.Data,
	abortData fscache.Data,
	sels []int64,
	from, to types.TS,
) (matched bool, usable bool, err error) {
	var commits vector.Vector
	if err = bindCachedVectorForScope(&commits, data); err != nil {
		return
	}
	defer commits.Free(nil)
	if commits.GetType().Oid != types.T_TS || commits.IsConstNull() {
		return false, false, nil
	}
	var aborts vector.Vector
	hasAborts := abortData != nil
	if hasAborts {
		if err = bindCachedVectorForScope(&aborts, abortData); err != nil {
			return
		}
		defer aborts.Free(nil)
		if aborts.IsConstNull() {
			hasAborts = false
		} else if aborts.GetType().Oid != types.T_bool || aborts.Length() != commits.Length() {
			return false, false, nil
		}
	}
	for _, sel := range sels {
		if sel < 0 || sel >= int64(commits.Length()) || commits.IsNull(uint64(sel)) {
			return false, false, nil
		}
		if hasAborts {
			if aborts.IsNull(uint64(sel)) {
				return false, false, nil
			}
			if vector.GetFixedAtNoTypeCheck[bool](&aborts, int(sel)) {
				continue
			}
		}
		commit := vector.GetFixedAtNoTypeCheck[types.TS](&commits, int(sel))
		if commit.GT(&from) && commit.LE(&to) {
			return true, true, nil
		}
	}
	return false, true, nil
}

func bindCachedVectorForScope(toVec *vector.Vector, data fscache.Data) error {
	if toVec == nil {
		return moerr.NewInvalidInputNoCtx("nil object vector destination")
	}
	if data == nil {
		return moerr.NewInvalidInputNoCtx("nil object cache data")
	}
	var bound vector.Vector
	var err error
	if marked, ok := data.(validatedVectorCacheDataMarker); ok {
		err = mustVectorTo(&bound, marked.validatedVectorBackingForScope(), true)
	} else {
		err = mustVectorTo(&bound, data.Bytes(), false)
	}
	if err != nil {
		return err
	}
	*toVec = bound
	return nil
}

func mustVectorTo(toVec *vector.Vector, buf []byte, trusted bool) (err error) {
	warnVectorDestinationNotEmpty(toVec, len(buf))
	if len(buf) < IOEntryHeaderSize {
		return io.ErrUnexpectedEOF
	}
	header := DecodeIOEntryHeader(buf)
	if header.Type != IOET_ColData {
		return moerr.NewInternalError(context.Background(), fmt.Sprintf("invalid object meta: %s", header.String()))
	}
	if header.Version == IOET_ColumnData_V2 {
		if trusted {
			err = toVec.UnmarshalBinaryTrusted(buf[IOEntryHeaderSize:])
		} else {
			err = toVec.UnmarshalBinary(buf[IOEntryHeaderSize:])
		}
		return
	} else if header.Version == IOET_ColumnData_V1 {
		err = toVec.UnmarshalBinaryV1(buf[IOEntryHeaderSize:])
		return
	}
	panic(fmt.Sprintf("invalid column data: %s", header.String()))
}

func warnVectorDestinationNotEmpty(toVec *vector.Vector, inputBytes int) {
	if !toVec.NeedDup() && toVec.Allocated() > 0 {
		eventVectorDestinationNotEmpty.WarnLazy(func() []zap.Field {
			return []zap.Field{
				zap.Bool("need-dup", toVec.NeedDup()),
				zap.Int("allocated-bytes", toVec.Allocated()),
				zap.Int("input-bytes", inputBytes),
			}
		})
	}
}

func MustObjectMeta(buffer []byte) ObjectMeta {
	header := DecodeIOEntryHeader(buffer)
	if header.Type != IOET_ObjMeta {
		panic(fmt.Sprintf("invalid object meta: %s", header.String()))
	}
	return ObjectMeta(buffer)
}
