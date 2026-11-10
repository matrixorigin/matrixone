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

package vector

import (
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// JSONReader is an immutable column version. Implementations keep their
// backing private; exported operations return owned data or opaque results.
// Consumers own each returned handle and close it exactly once. Implementations
// must reject malformed documents before publication and preserve backing
// lifetime for all retained windows and query results.
type JSONReader interface {
	OpenCursor() (JSONCursor, error)
	Length() int
	Window(int, int) (JSONReader, error)
	Select([]int64, *mpool.MPool) (JSONReader, error)
	Materialize(*mpool.MPool) (*Vector, error)
	Query(int, *bytejson.Path) (JSONQueryResult, error)
	IsNull(int) (bool, error)
	Close()
}

// JSONQueryResult never exposes borrowed bytes. Each result owns an independent
// lifetime reference and may outlive the reader from which it was selected.
type JSONQueryResult interface {
	Exists() bool
	SQLNull() bool
	Type() byte
	IsJSONNull() bool
	Literal() (byte, bool)
	Int64() (int64, bool)
	Uint64() (uint64, bool)
	SelectSingle() int
	Text() (string, error)
	TextWithLegacyFloat() (string, error)
	AppendJSON(*Vector, *mpool.MPool) error
	AppendResult(FunctionResultWrapper) error
	Close()
}

// JSONTextSelection contains only owned text and scalar selection metadata.
// It never exposes document bytes or extends a cursor's result lifetime.
type JSONTextSelection struct {
	Text        string
	Exists      bool
	InputNull   bool
	ValueNull   bool
	Multiple    bool
	InvalidPath bool
}

// JSONCursor retains one immutable version and reuses its result storage for
// sequential row queries. A result is valid until ResetQuery or Close; callers
// must copy output before advancing. Each cursor owns an independent reference.
type JSONCursor interface {
	JSONQueryResult
	QueryText(int, []byte, bool) (JSONTextSelection, error)
	ResetQuery(int, bytejson.Path) error
}

// jsonReader owns one reference to a fully validated, private JSON column.
// It never exposes a Vector, descriptor, or payload alias. Separate retained
// readers may query concurrently. Close must not race with operations on the
// same reader. Sources must remain stable while a snapshot is being copied.
// The zero value is closed. A reader must not be copied by value.
type jsonReader struct {
	rows   *Vector // private absolute physical row map, nil for contiguous views
	column *Vector
	lease  *RefCountedBufferLease
	start  int
	length int
}

// NewJSONReaderSnapshot copies both descriptors and payloads, then validates
// the private copy before publication. A borrowed lease on source is not proof
// of JSON validity or of exclusive ownership. All backing allocations use the
// source's statement account; source may be changed or freed after return.
func NewJSONReaderSnapshot(source *Vector, mp *mpool.MPool) (JSONReader, error) {
	var selection *AllocationAccountSelection
	if source != nil {
		selection = source.allocationAccount
	}
	return NewJSONReaderSnapshotWithAllocation(source, mp, selection)
}

// NewJSONReaderSnapshotWithAllocation explicitly selects the receiving owner's
// account, including when the source is a cache-owned borrowed vector.
func NewJSONReaderSnapshotWithAllocation(source *Vector, mp *mpool.MPool, selection *AllocationAccountSelection) (JSONReader, error) {
	rows := 0
	if source != nil {
		rows = source.length
	}
	return NewJSONReaderSnapshotForRows(source, rows, mp, selection)
}

// NewJSONReaderSnapshotForRows binds an explicit batch row domain. Constants
// have one physical value and can span that domain without payload repetition.
// The source is still copied and fully validated; its metadata is not changed.
func NewJSONReaderSnapshotForRows(source *Vector, logicalRows int, mp *mpool.MPool, selection *AllocationAccountSelection) (JSONReader, error) {
	if source == nil || mp == nil || source.typ.Oid != types.T_json || source.length < 0 || uint64(source.length) > math.MaxUint32 || logicalRows < 0 || uint64(logicalRows) > math.MaxUint32 {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader source")
	}
	// Dup uses TypeSize for slicing; reject corrupt representation metadata
	// before it can read beyond the source descriptor buffer.
	if _, err := canonicalVectorTypeSize(source.typ); err != nil {
		return nil, err
	}
	if !source.IsConst() && source.length != logicalRows {
		return nil, moerr.NewInvalidInputNoCtx("JSON input row domain mismatch")
	}
	rows := source.length
	if source.IsConst() {
		rows = 1
	}
	if !source.IsConstNull() && (rows > len(source.data)/types.VarlenaSize) {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader descriptors")
	}
	private, err := source.dup(mp, true, true, selection)
	if err != nil {
		return nil, err
	}
	private.length = logicalRows
	if !private.IsConstNull() {
		data := private.data[:rows*types.VarlenaSize]
		if err = validateVectorBinary(byte(private.class), private.typ, uint32(private.length), data, private.area, &private.nsp, true); err != nil {
			private.Free(mp)
			return nil, err
		}
	}
	reader, err := publishJSONReader(private, mp)
	if err != nil {
		private.Free(mp)
		return nil, err
	}
	return reader, nil
}

// JSONReaderBuilder owns an unpublished column. Append copies and validates
// each document through the existing raw admission boundary. No mutable view
// of its column is exported, so Finish can transfer ownership without a scan.
// Close releases unfinished storage; calls are sequential.
type JSONReaderBuilder struct {
	column *Vector
	mp     *mpool.MPool
}

func NewJSONReaderBuilder(mp *mpool.MPool, selection *AllocationAccountSelection) (*JSONReaderBuilder, error) {
	if mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader builder requires a pool")
	}
	column, err := NewOffHeapVecWithTypeAndAllocation(types.T_json.ToType(), selection)
	if err != nil {
		return nil, err
	}
	return &JSONReaderBuilder{column: column, mp: mp}, nil
}

func (b *JSONReaderBuilder) Append(raw []byte, null bool) error {
	if b == nil || b.column == nil {
		return io.ErrClosedPipe
	}
	return AppendBytes(b.column, raw, null, b.mp)
}

func (b *JSONReaderBuilder) Finish() (JSONReader, error) {
	if b == nil || b.column == nil {
		return nil, io.ErrClosedPipe
	}
	reader, err := publishJSONReader(b.column, b.mp)
	if err != nil {
		return nil, err
	}
	b.column = nil
	return reader, nil
}

func (b *JSONReaderBuilder) Close() {
	if b != nil && b.column != nil {
		b.column.Free(b.mp)
		b.column = nil
	}
}

func publishJSONReader(column *Vector, mp *mpool.MPool) (JSONReader, error) {
	lease, err := NewRefCountedBufferLease(nil, int64(column.Allocated()), func() { column.Free(mp) })
	if err != nil {
		return nil, err
	}
	return &jsonReader{column: column, lease: lease, length: column.length}, nil
}

func (r *jsonReader) Length() int {
	if r == nil || r.lease == nil {
		return 0
	}
	return r.length
}

// Window retains the complete owner while narrowing the logical row domain.
// It does not duplicate descriptors or manufacture a mutable Vector view.
func (r *jsonReader) Window(start, end int) (JSONReader, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if start < 0 || end < start || end > r.length {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader window")
	}
	if !r.lease.Retain() {
		return nil, io.ErrClosedPipe
	}
	return &jsonReader{column: r.column, lease: r.lease, start: r.start + start, length: end - start, rows: r.rows}, nil
}

// Select copies row indices into accounted private storage. It keeps no alias
// to the caller's selection, and composes nested mappings to physical rows.
func (r *jsonReader) Select(rows []int64, mp *mpool.MPool) (JSONReader, error) {
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
	mapping, err := NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), r.column.allocationAccount)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err = AppendFixed(mapping, int64(r.physicalRow(int(row))), false, mp); err != nil {
			mapping.Free(mp)
			return nil, err
		}
	}
	parent := r.lease
	if !parent.Retain() {
		mapping.Free(mp)
		return nil, io.ErrClosedPipe
	}
	lease, err := NewRefCountedBufferLease(nil, int64(mapping.Allocated()), func() { mapping.Free(mp); parent.Release() })
	if err != nil {
		mapping.Free(mp)
		parent.Release()
		return nil, err
	}
	return &jsonReader{column: r.column, rows: mapping, lease: lease, length: len(rows)}, nil
}

func (r *jsonReader) physicalRow(row int) int {
	if r.rows != nil {
		return int(MustFixedColNoTypeCheck[int64](r.rows)[r.start+row])
	}
	return r.start + row
}

// Materialize returns an independent mutable vector. It is failure-atomic:
// the reader remains usable if allocation or output admission fails.
func (r *jsonReader) Materialize(mp *mpool.MPool) (*Vector, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON materialization requires a pool")
	}
	destination, err := NewOffHeapVecWithTypeAndAllocation(r.column.typ, r.column.allocationAccount)
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
		if err = AppendBytes(destination, raw, null, mp); err != nil {
			destination.Free(mp)
			return nil, err
		}
	}
	return destination, nil
}

func (r *jsonReader) Close() {
	if r != nil && r.lease != nil {
		lease := r.lease
		r.lease = nil
		// Row coordinates remain immutable for outstanding cursors. The
		// final lease frees all column and mapping backing.
		lease.Release()
	}
}

// Query returns an opaque selected value. It holds its own owner reference,
// so it survives the reader's Close. SQL NULL and path absence are separate.
// The caller must Close every successful result, including misses and NULLs.
func (r *jsonReader) Query(row int, path *bytejson.Path) (JSONQueryResult, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if !r.lease.Retain() {
		return nil, io.ErrClosedPipe
	}
	result := &jsonQueryResult{lease: r.lease}
	if err := r.fillQuery(row, path, result); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

func (r *jsonReader) loadQueryRow(row int, result *jsonQueryResult) (bytejson.ByteJson, error) {
	result.value = bytejson.ByteJson{}
	result.exists, result.sqlNull = false, false
	if r == nil || result.lease == nil {
		return bytejson.ByteJson{}, io.ErrClosedPipe
	}
	if row < 0 || row >= r.length {
		return bytejson.ByteJson{}, moerr.NewInvalidInputNoCtx("invalid JSON reader query")
	}
	physicalRow := r.physicalRow(row)
	if r.column.IsNull(uint64(physicalRow)) {
		result.sqlNull = true
		return bytejson.ByteJson{}, nil
	}
	raw := r.column.GetBytesAt(physicalRow)
	value := bytejson.ByteJson{Type: raw[0], Data: raw[1:]}
	return value, nil
}

func (r *jsonReader) fillQuery(row int, path *bytejson.Path, result *jsonQueryResult) error {
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
	result.value, result.exists = value.QueryWithExists([]*bytejson.Path{path})
	return nil
}

func (r *jsonReader) OpenCursor() (JSONCursor, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if !r.lease.Retain() {
		return nil, io.ErrClosedPipe
	}
	return &jsonCursor{reader: r, jsonQueryResult: jsonQueryResult{lease: r.lease}}, nil
}

type jsonCursor struct {
	jsonQueryResult
	reader *jsonReader
}

func (cursor *jsonCursor) ResetQuery(row int, path bytejson.Path) error {
	if cursor.reader == nil || cursor.jsonQueryResult.lease == nil {
		return io.ErrClosedPipe
	}
	return cursor.reader.fillQuery(row, &path, &cursor.jsonQueryResult)
}

func (cursor *jsonCursor) Close() {
	cursor.reader = nil
	cursor.jsonQueryResult.Close()
}

// jsonQueryResult keeps the borrowed ByteJson inside this package. Output
// methods return owned text or copy into an explicitly supplied destination.
// Separate query results may be consumed concurrently; Close is sequential.
type jsonQueryResult struct {
	value   bytejson.ByteJson
	exists  bool
	sqlNull bool
	lease   *RefCountedBufferLease
}

func (r *jsonQueryResult) Exists() bool  { return r != nil && r.lease != nil && r.exists }
func (r *jsonQueryResult) SQLNull() bool { return r != nil && r.lease != nil && r.sqlNull }
func (r *jsonQueryResult) Text() (string, error) {
	if r == nil || r.lease == nil {
		return "", io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return "", moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return r.value.Unquote()
}
func (r *jsonQueryResult) AppendJSON(destination *Vector, mp *mpool.MPool) error {
	if r == nil || r.lease == nil {
		return io.ErrClosedPipe
	}
	if destination == nil || destination.typ.Oid != types.T_json {
		return moerr.NewInvalidInputNoCtx("JSON query requires a JSON destination")
	}
	if r.sqlNull || !r.exists {
		return AppendBytes(destination, nil, true, mp)
	}
	return AppendByteJson(destination, r.value, false, mp)
}
func (r *jsonQueryResult) Close() {
	if r != nil && r.lease != nil {
		lease := r.lease
		r.lease = nil
		r.value = bytejson.ByteJson{}
		lease.Release()
	}
}

// Type and Numeric conversions inspect the selected value without exporting
// the backing. A closed result has no value.
func (r *jsonQueryResult) Type() byte {
	if r == nil || r.lease == nil {
		return 0
	}
	return r.value.Type
}
func (r *jsonQueryResult) IsJSONNull() bool {
	return r != nil && r.lease != nil && r.exists && r.value.IsNull()
}
func (r *jsonQueryResult) Literal() (byte, bool) {
	if r.Type() != bytejson.TpCodeLiteral || len(r.value.Data) == 0 {
		return 0, false
	}
	return r.value.Data[0], true
}
func (r *jsonQueryResult) Int64() (int64, bool) {
	if r == nil || r.lease == nil {
		return 0, false
	}
	return bytejson.NumericToInt64(r.value)
}
func (r *jsonQueryResult) Uint64() (uint64, bool) {
	if r == nil || r.lease == nil {
		return 0, false
	}
	return bytejson.NumericToUint64(r.value)
}

// SelectSingle unwraps the array produced by a potentially multi-valued path.
// It returns its cardinality. The result stays opaque and retains its owner.
func (r *jsonQueryResult) SelectSingle() int {
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

func (r *jsonQueryResult) TextWithLegacyFloat() (string, error) {
	if r == nil || r.lease == nil {
		return "", io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return "", moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return r.value.UnquoteWithLegacyFloat()
}

// AppendResult copies through the ordinary function-result writer so its
// result length, allocation failures and result metadata remain authoritative.
func (r *jsonQueryResult) AppendResult(result FunctionResultWrapper) error {
	if r == nil || r.lease == nil {
		return io.ErrClosedPipe
	}
	writer := MustFunctionResult[types.Varlena](result)
	if r.sqlNull || !r.exists {
		return writer.AppendMustNullForBytesResult()
	}
	return writer.AppendByteJson(r.value, false)
}

func (r *jsonReader) IsNull(row int) (bool, error) {
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
func (r *jsonQueryResult) selectText(simple, legacyFloat bool) (JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return JSONTextSelection{}, io.ErrClosedPipe
	}
	if r.sqlNull {
		return JSONTextSelection{InputNull: true}, nil
	}
	if !r.exists {
		return JSONTextSelection{}, nil
	}
	value := r.value
	if !simple {
		if value.Type != bytejson.TpCodeArray {
			return JSONTextSelection{Exists: true, Multiple: true}, nil
		}
		count := value.GetElemCnt()
		if count == 0 {
			return JSONTextSelection{}, nil
		}
		if count > 1 {
			return JSONTextSelection{Exists: true, Multiple: true}, nil
		}
		value = value.GetArrayElem(0)
	}
	if value.IsNull() {
		return JSONTextSelection{Exists: true, ValueNull: true}, nil
	}
	var text string
	var err error
	if legacyFloat {
		text, err = value.UnquoteWithLegacyFloat()
	} else {
		text, err = value.Unquote()
	}
	if err != nil {
		return JSONTextSelection{}, err
	}
	return JSONTextSelection{Exists: true, Text: text}, nil
}

// QueryText loads a frozen row and uses the existing path parser and query.
// An absent input has no path evaluation; invalid non-NULL paths are tagged
// separately so callers retain their own error text and response policy.
func (cursor *jsonCursor) QueryText(row int, pathBytes []byte, legacyFloat bool) (JSONTextSelection, error) {
	if cursor == nil || cursor.reader == nil {
		return JSONTextSelection{}, io.ErrClosedPipe
	}
	value, err := cursor.reader.loadQueryRow(row, &cursor.jsonQueryResult)
	if err != nil {
		return JSONTextSelection{}, err
	}
	if cursor.sqlNull {
		return JSONTextSelection{InputNull: true}, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return JSONTextSelection{InvalidPath: true}, err
	}
	cursor.value, cursor.exists = value.QueryWithExists([]*bytejson.Path{&path})
	return cursor.jsonQueryResult.selectText(path.IsSimple(), legacyFloat)
}
