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

package objectio

import (
	"io"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
	"github.com/matrixorigin/matrixone/pkg/internal/jsonread"
)

var (
	cacheJSONVectorSize = reflect.TypeFor[vector.Vector]().Size()
	cacheJSONLeaseSize  = reflect.TypeFor[vector.RefCountedBufferLease]().Size()
	cacheJSONReaderSize = reflect.TypeFor[cacheJSONReader]().Size()
	cacheJSONResultSize = reflect.TypeFor[cacheJSONQueryResult]().Size()
	cacheJSONCursorSize = reflect.TypeFor[cacheJSONCursor]().Size()
)

// cacheJSONReader borrows only objectio's already validated sealed cache
// representation. Its private Vector cannot escape; all output operations copy.
// A lease pins the cache entry across readers, windows and selected results.
type cacheJSONReader struct {
	rows        *vector.Vector
	column      *vector.Vector
	lease       *vector.RefCountedBufferLease
	selection   *vector.AllocationAccountSelection
	start       int
	length      int
	repeated    bool
	repeatedRow int
	admission   vector.JSONReaderMemoryAdmission
	releaseMeta func()
	scratch     [128]byte // fixed formatting space, included in handle admission
}

// IsPrivateJSONCacheReader recognizes only a live private cache-backed owner.
// Implementing or wrapping JSONReader cannot confer this identity.
func IsPrivateJSONCacheReader(reader vector.JSONReader) bool {
	private, ok := reader.(*cacheJSONReader)
	return ok && private != nil && private.lease != nil
}

func newCacheJSONReader(data fscache.Data, marked validatedVectorCacheDataMarker, selection *vector.AllocationAccountSelection, capacity int64, releasePin func(), admission vector.JSONReaderMemoryAdmission) (vector.JSONReader, error) {
	ownerMeta, err := vector.ReserveJSONReaderMetadata(selection, admission, vector.JSONReaderMetadataCapacity(cacheJSONVectorSize+cacheJSONLeaseSize))
	if err != nil {
		releasePin()
		return nil, err
	}
	selection = vector.JSONReaderAllocationSelection(selection, admission)
	releaseMeta, err := vector.ReserveJSONReaderMetadata(selection, admission, vector.JSONReaderMetadataCapacity(cacheJSONReaderSize))
	if err != nil {
		if ownerMeta != nil {
			ownerMeta()
		}
		releasePin()
		return nil, err
	}
	data.Retain()
	column := new(vector.Vector)
	release := func() {
		column.Free(nil)
		data.Release()
		releasePin()
		if ownerMeta != nil {
			ownerMeta()
		}
	}
	if err := mustVectorTo(column, marked.validatedVectorBackingForScope(), true); err != nil {
		release()
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, err
	}
	if column.GetType().Oid != types.T_json {
		release()
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, moerr.NewInvalidInputNoCtx("JSON cache reader requires a JSON column")
	}
	lease, err := vector.NewRefCountedBufferLease(nil, capacity, release)
	if err != nil {
		release()
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, err
	}
	return &cacheJSONReader{column: column, lease: lease, selection: selection, length: column.Length(), admission: admission, releaseMeta: releaseMeta}, nil
}

func (r *cacheJSONReader) Length() int {
	if r == nil || r.lease == nil {
		return 0
	}
	return r.length
}

// Window retains the complete owner while narrowing the logical row domain.
// It does not duplicate descriptors or manufacture a mutable vector.Vector view.
func (r *cacheJSONReader) Window(start, end int) (vector.JSONReader, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if start < 0 || end < start || end > r.length {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader window")
	}
	releaseMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONReaderSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, io.ErrClosedPipe
	}
	return &cacheJSONReader{selection: r.selection, column: r.column, lease: r.lease, start: r.start + start, length: end - start, rows: r.rows, repeated: r.repeated, repeatedRow: r.repeatedRow, admission: r.admission, releaseMeta: releaseMeta}, nil
}

// Repeat retains one physical row without constructing a batch-sized mapping.
func (r *cacheJSONReader) Repeat(row, length int) (vector.JSONReader, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if row < 0 || row >= r.length || length < 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader repeated row")
	}
	physicalRow := r.physicalRow(row)
	releaseMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONReaderSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, io.ErrClosedPipe
	}
	return &cacheJSONReader{selection: r.selection, column: r.column, lease: r.lease, length: length, repeated: true, repeatedRow: physicalRow, admission: r.admission, releaseMeta: releaseMeta}, nil
}

// Select copies row indices into accounted private storage. It keeps no alias
// to the caller's selection, and composes nested mappings to physical rows.
func (r *cacheJSONReader) Select(rows []int64, mp *mpool.MPool) (vector.JSONReader, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON selection requires a pool")
	}
	for _, row := range rows {
		if row < 0 || row >= int64(r.length) {
			return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader selection")
		}
	}
	ownerMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONVectorSize+cacheJSONLeaseSize))
	if err != nil {
		return nil, err
	}
	defer func() {
		if ownerMeta != nil {
			ownerMeta()
		}
	}()
	releaseMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONReaderSize))
	if err != nil {
		return nil, err
	}
	defer func() {
		if releaseMeta != nil {
			releaseMeta()
		}
	}()
	mapping, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), r.selection)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err = vector.AppendFixed(mapping, int64(r.physicalRow(int(row))), false, mp); err != nil {
			mapping.Free(mp)
			return nil, err
		}
	}
	parent := r.lease
	if !parent.Retain() {
		mapping.Free(mp)
		return nil, io.ErrClosedPipe
	}
	ownedMeta := ownerMeta
	lease, err := vector.NewRefCountedBufferLease(nil, int64(mapping.Allocated()), func() {
		mapping.Free(mp)
		parent.Release()
		if ownedMeta != nil {
			ownedMeta()
		}
	})
	if err != nil {
		mapping.Free(mp)
		parent.Release()
		return nil, err
	}
	reader := &cacheJSONReader{selection: r.selection, column: r.column, rows: mapping, lease: lease, length: len(rows), admission: r.admission, releaseMeta: releaseMeta}
	ownerMeta = nil
	releaseMeta = nil
	return reader, nil
}

func (r *cacheJSONReader) physicalRow(row int) int {
	if r.repeated {
		return r.repeatedRow
	}
	if r.rows != nil {
		return int(vector.MustFixedColNoTypeCheck[int64](r.rows)[r.start+row])
	}
	return r.start + row
}

// Materialize returns an independent mutable vector. It is failure-atomic:
// the reader remains usable if allocation or output admission fails.
func (r *cacheJSONReader) Materialize(mp *mpool.MPool) (*vector.Vector, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON materialization requires a pool")
	}
	destination, err := vector.NewOffHeapVecWithTypeAndAllocation(*r.column.GetType(), r.selection)
	if err != nil {
		return nil, err
	}
	for row := 0; row < r.length; row++ {
		physicalRow := r.physicalRow(row)
		null := r.column.IsNull(uint64(physicalRow))
		var raw []byte
		if !null {
			raw = r.column.GetBytesAt(physicalRow)
		}
		if err = vector.AppendBytes(destination, raw, null, mp); err != nil {
			destination.Free(mp)
			return nil, err
		}
	}
	return destination, nil
}

func (r *cacheJSONReader) Close() {
	if r != nil && r.lease != nil {
		lease := r.lease
		releaseMeta := r.releaseMeta
		r.lease = nil
		r.selection = nil
		r.admission = nil
		r.releaseMeta = nil
		// Row coordinates remain immutable for outstanding cursors. The
		// final lease frees all column and mapping backing.
		lease.Release()
		if releaseMeta != nil {
			releaseMeta()
		}
	}
}

// Query returns an opaque selected value. It holds its own owner reference,
// so it survives the reader's Close. SQL NULL and path absence are separate.
// The caller must Close every successful result, including misses and NULLs.
func (r *cacheJSONReader) Query(row int, path *bytejson.Path) (vector.JSONQueryResult, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	releaseMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONResultSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, io.ErrClosedPipe
	}
	result := &cacheJSONQueryResult{lease: r.lease, releaseMeta: releaseMeta, selection: r.selection, admission: r.admission}
	if err := r.fillQuery(row, path, result); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

func (r *cacheJSONReader) loadQueryRow(row int, result *cacheJSONQueryResult) (bytejson.ByteJson, error) {
	result.clearValue()
	if r == nil || result.lease == nil {
		return bytejson.ByteJson{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	result.sqlNull = isNull
	return value, err
}

func (r *cacheJSONReader) loadValue(row int) (bytejson.ByteJson, bool, error) {
	if r == nil || r.column == nil {
		return bytejson.ByteJson{}, false, io.ErrClosedPipe
	}
	if row < 0 || row >= r.length {
		return bytejson.ByteJson{}, false, moerr.NewInvalidInputNoCtx("invalid JSON reader query")
	}
	physicalRow := r.physicalRow(row)
	if r.column.IsNull(uint64(physicalRow)) {
		return bytejson.ByteJson{}, true, nil
	}
	raw := r.column.GetBytesAt(physicalRow)
	value := bytejson.ByteJson{Type: raw[0], Data: raw[1:]}
	return value, false, nil
}

func (r *cacheJSONReader) fillQuery(row int, path *bytejson.Path, result *cacheJSONQueryResult) error {
	value, err := r.loadQueryRow(row, result)
	if err != nil {
		return err
	}
	if path == nil {
		return moerr.NewInvalidInputNoCtx("invalid JSON reader query")
	}
	if result.sqlNull {
		return nil
	}
	result.value, result.exists, result.releaseValue, err = value.QueryWithMemory(path, func(size uint64) (func(), error) {
		return vector.ReserveJSONReaderBacking(r.selection, r.admission, size)
	})
	return err
}

func (r *cacheJSONReader) OpenCursor() (vector.JSONCursor, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	releaseMeta, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONCursorSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		if releaseMeta != nil {
			releaseMeta()
		}
		return nil, io.ErrClosedPipe
	}
	return &cacheJSONCursor{
		reader:               cacheJSONReader{rows: r.rows, column: r.column, start: r.start, length: r.length, repeated: r.repeated, repeatedRow: r.repeatedRow, selection: r.selection, admission: r.admission},
		cacheJSONQueryResult: cacheJSONQueryResult{lease: r.lease, releaseMeta: releaseMeta, selection: r.selection, admission: r.admission},
	}, nil
}

type cacheJSONCursor struct {
	cacheJSONQueryResult
	reader cacheJSONReader
}

func (cursor *cacheJSONCursor) ResetQuery(row int, path bytejson.Path) error {
	if cursor == nil || cursor.cacheJSONQueryResult.lease == nil {
		return io.ErrClosedPipe
	}
	return cursor.reader.fillQuery(row, &path, &cursor.cacheJSONQueryResult)
}

func (cursor *cacheJSONCursor) Close() {
	if cursor != nil {
		cursor.reader = cacheJSONReader{}
		cursor.cacheJSONQueryResult.Close()
	}
}

// vector.JSONQueryResult keeps the borrowed ByteJson inside this package. Output
// methods return owned text or copy into an explicitly supplied destination.
// Separate query results may be consumed concurrently; Close is sequential.
type cacheJSONQueryResult struct {
	value        bytejson.ByteJson
	exists       bool
	sqlNull      bool
	lease        *vector.RefCountedBufferLease
	releaseMeta  func()
	releaseValue func()
	selection    *vector.AllocationAccountSelection
	admission    vector.JSONReaderMemoryAdmission
}

func (r *cacheJSONQueryResult) clearValue() {
	if r.releaseValue != nil {
		r.releaseValue()
		r.releaseValue = nil
	}
	r.value = bytejson.ByteJson{}
	r.exists, r.sqlNull = false, false
}

func (r *cacheJSONQueryResult) Exists() bool  { return r != nil && r.lease != nil && r.exists }
func (r *cacheJSONQueryResult) SQLNull() bool { return r != nil && r.lease != nil && r.sqlNull }
func (r *cacheJSONQueryResult) Text() (vector.JSONOwnedText, error) {
	if r == nil || r.lease == nil {
		return vector.JSONOwnedText{}, io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return vector.JSONOwnedText{}, moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return r.copyText(r.value, false)
}
func (r *cacheJSONQueryResult) AppendJSON(destination *vector.Vector, mp *mpool.MPool) error {
	if r == nil || r.lease == nil {
		return io.ErrClosedPipe
	}
	if destination == nil || destination.GetType().Oid != types.T_json {
		return moerr.NewInvalidInputNoCtx("JSON query requires a JSON destination")
	}
	if r.sqlNull || !r.exists {
		return vector.AppendBytes(destination, nil, true, mp)
	}
	return vector.AppendByteJson(destination, r.value, false, mp)
}
func (r *cacheJSONQueryResult) Close() {
	if r != nil && r.lease != nil {
		lease := r.lease
		releaseMeta := r.releaseMeta
		r.clearValue()
		r.lease = nil
		r.releaseMeta = nil
		r.selection = nil
		r.admission = nil
		lease.Release()
		if releaseMeta != nil {
			releaseMeta()
		}
	}
}

// Type and Numeric conversions inspect the selected value without exporting
// the backing. A closed result has no value.
func (r *cacheJSONQueryResult) Type() byte {
	if r == nil || r.lease == nil {
		return 0
	}
	return r.value.Type
}
func (r *cacheJSONQueryResult) IsJSONNull() bool {
	return r != nil && r.lease != nil && r.exists && r.value.IsNull()
}
func (r *cacheJSONQueryResult) Literal() (byte, bool) {
	if r.Type() != bytejson.TpCodeLiteral || len(r.value.Data) == 0 {
		return 0, false
	}
	return r.value.Data[0], true
}
func (r *cacheJSONQueryResult) Int64() (int64, bool) {
	if r == nil || r.lease == nil {
		return 0, false
	}
	return bytejson.NumericToInt64(r.value)
}
func (r *cacheJSONQueryResult) Uint64() (uint64, bool) {
	if r == nil || r.lease == nil {
		return 0, false
	}
	return bytejson.NumericToUint64(r.value)
}

// SelectSingle unwraps the array produced by a potentially multi-valued path.
// It returns its cardinality. The result stays opaque and retains its owner.
func (r *cacheJSONQueryResult) SelectSingle() int {
	if r == nil || r.lease == nil || !r.exists {
		return 0
	}
	if r.value.Type != bytejson.TpCodeArray {
		return 2
	}
	count := r.value.GetElemCnt()
	if count == 1 {
		r.value = r.value.GetArrayElem(0)
	}
	return count
}

func (r *cacheJSONQueryResult) TextWithLegacyFloat() (vector.JSONOwnedText, error) {
	if r == nil || r.lease == nil {
		return vector.JSONOwnedText{}, io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return vector.JSONOwnedText{}, moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return r.copyText(r.value, true)
}

func (r *cacheJSONQueryResult) copyText(value bytejson.ByteJson, legacyFloat bool) (vector.JSONOwnedText, error) {
	text, release, err := jsonread.CopyText(value, legacyFloat, func(size uint64) (func(), error) {
		return vector.ReserveJSONReaderBacking(r.selection, r.admission, size)
	})
	if err != nil {
		return vector.JSONOwnedText{}, err
	}
	return vector.NewJSONOwnedText(text, release), nil
}

// AppendResult copies through the ordinary function-result writer so its
// result length, allocation failures and result metadata remain authoritative.
func (r *cacheJSONQueryResult) AppendResult(result vector.FunctionResultWrapper) error {
	if r == nil || r.lease == nil {
		return io.ErrClosedPipe
	}
	writer := vector.MustFunctionResult[types.Varlena](result)
	if r.sqlNull || !r.exists {
		return writer.AppendMustNullForBytesResult()
	}
	return writer.AppendByteJson(r.value, false)
}

func (r *cacheJSONReader) IsNull(row int) (bool, error) {
	if r == nil || r.lease == nil {
		return false, io.ErrClosedPipe
	}
	if row < 0 || row >= r.length {
		return false, moerr.NewInvalidInputNoCtx("invalid JSON reader row")
	}
	return r.column.IsNull(uint64(r.physicalRow(row))), nil
}

// SelectText combines selection metadata and owned text without exposing the
// selected ByteJson. It leaves the current result unchanged.
func (r *cacheJSONQueryResult) selectText(simple, legacyFloat bool) (vector.JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return vector.JSONTextSelection{}, io.ErrClosedPipe
	}
	if r.sqlNull {
		return vector.JSONTextSelection{InputNull: true}, nil
	}
	if !r.exists {
		return vector.JSONTextSelection{}, nil
	}
	value := r.value
	if !simple {
		if value.Type != bytejson.TpCodeArray {
			return vector.JSONTextSelection{Exists: true, Multiple: true}, nil
		}
		count := value.GetElemCnt()
		if count == 0 {
			return vector.JSONTextSelection{}, nil
		}
		if count > 1 {
			return vector.JSONTextSelection{Exists: true, Multiple: true}, nil
		}
		value = value.GetArrayElem(0)
	}
	if value.IsNull() {
		return vector.JSONTextSelection{Exists: true, ValueNull: true}, nil
	}
	text, err := r.copyText(value, legacyFloat)
	if err != nil {
		return vector.JSONTextSelection{TextError: vector.IsJSONTextConversionError(err)}, err
	}
	return vector.NewJSONTextSelection(text, byte(value.Type)), nil
}

// QueryText borrows the live reader only for this call and returns owned text.
// Like the retained cursor, it checks a NULL document before parsing the path.
func (r *cacheJSONReader) QueryText(row int, pathBytes []byte, legacyFloat bool) (vector.JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return vector.JSONTextSelection{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return vector.JSONTextSelection{}, err
	}
	if isNull {
		return vector.JSONTextSelection{InputNull: true}, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return vector.JSONTextSelection{InvalidPath: true}, err
	}
	return r.queryTextFromValue(value, path, legacyFloat)
}

// QueryTextAtPath accepts an already parsed path and borrows the reader only
// until its owned result is returned. It does not create or retain a cursor.
func (r *cacheJSONReader) QueryTextAtPath(row int, path bytejson.Path, legacyFloat bool) (vector.JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return vector.JSONTextSelection{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return vector.JSONTextSelection{}, err
	}
	if isNull {
		return vector.JSONTextSelection{InputNull: true}, nil
	}
	return r.queryTextFromValue(value, path, legacyFloat)
}

func (r *cacheJSONReader) queryTextFromValue(value bytejson.ByteJson, path bytejson.Path, legacyFloat bool) (vector.JSONTextSelection, error) {
	release, err := vector.ReserveJSONReaderMetadata(r.selection, r.admission, vector.JSONReaderMetadataCapacity(cacheJSONResultSize))
	if err != nil {
		return vector.JSONTextSelection{}, err
	}
	defer func() {
		if release != nil {
			release()
		}
	}()
	result := cacheJSONQueryResult{lease: r.lease, selection: r.selection, admission: r.admission}
	defer result.clearValue()
	return queryCacheTextFromValue(value, path, &result, legacyFloat)
}

func queryCacheTextFromValue(value bytejson.ByteJson, path bytejson.Path, result *cacheJSONQueryResult, legacyFloat bool) (vector.JSONTextSelection, error) {
	var err error
	result.value, result.exists, result.releaseValue, err = value.QueryWithMemory(&path, func(size uint64) (func(), error) {
		return vector.ReserveJSONReaderBacking(result.selection, result.admission, size)
	})
	if err != nil {
		return vector.JSONTextSelection{}, err
	}
	return result.selectText(path.IsSimple(), legacyFloat)
}

func (r *cacheJSONReader) QueryTextTo(row int, pathBytes []byte, legacyFloat bool, limit int, target *vector.FunctionResult[types.Varlena]) (vector.JSONTextSelection, bool, error) {
	if r == nil || r.lease == nil {
		return vector.JSONTextSelection{}, false, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return vector.JSONTextSelection{}, false, err
	}
	if isNull {
		return vector.JSONTextSelection{InputNull: true}, false, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return vector.JSONTextSelection{InvalidPath: true}, false, err
	}
	return r.queryTextTo(value, path, legacyFloat, limit, target)
}

func (r *cacheJSONReader) QueryTextAtPathTo(row int, path bytejson.Path, legacyFloat bool, limit int, target *vector.FunctionResult[types.Varlena]) (vector.JSONTextSelection, bool, error) {
	if r == nil || r.lease == nil {
		return vector.JSONTextSelection{}, false, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return vector.JSONTextSelection{}, false, err
	}
	if isNull {
		return vector.JSONTextSelection{InputNull: true}, false, nil
	}
	return r.queryTextTo(value, path, legacyFloat, limit, target)
}

func (r *cacheJSONReader) queryTextTo(value bytejson.ByteJson, path bytejson.Path, legacyFloat bool, limit int, target *vector.FunctionResult[types.Varlena]) (vector.JSONTextSelection, bool, error) {
	if target != nil && path.IsSimple() {
		selected, exists, _, err := value.QueryWithMemory(&path, nil)
		if err != nil {
			return vector.JSONTextSelection{}, false, err
		}
		if !exists {
			return vector.JSONTextSelection{}, false, nil
		}
		if selected.IsNull() {
			return vector.JSONTextSelection{Exists: true, ValueNull: true}, false, nil
		}
		text, direct, formatErr := jsonread.DirectText(selected, legacyFloat, limit, r.scratch[:0])
		if formatErr != nil {
			return vector.JSONTextSelection{TextError: true}, false, formatErr
		}
		if direct {
			return vector.JSONTextSelection{}, true, target.AppendBytes(text, false)
		}
	}
	selection, err := r.queryTextFromValue(value, path, legacyFloat)
	return selection, false, err
}

// QueryText loads a frozen row and uses the existing path parser and query.
// An absent input has no path evaluation; invalid non-NULL paths are tagged
// separately so callers retain their own error text and response policy.
func (cursor *cacheJSONCursor) QueryText(row int, pathBytes []byte, legacyFloat bool) (vector.JSONTextSelection, error) {
	if cursor == nil || cursor.cacheJSONQueryResult.lease == nil {
		return vector.JSONTextSelection{}, io.ErrClosedPipe
	}
	value, err := cursor.reader.loadQueryRow(row, &cursor.cacheJSONQueryResult)
	if err != nil {
		return vector.JSONTextSelection{}, err
	}
	if cursor.sqlNull {
		return vector.JSONTextSelection{InputNull: true}, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return vector.JSONTextSelection{InvalidPath: true}, err
	}
	return queryCacheTextFromValue(value, path, &cursor.cacheJSONQueryResult, legacyFloat)
}
