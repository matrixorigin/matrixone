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
	"unsafe"

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
	Repeat(int, int) (JSONReader, error)
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
	rows        *Vector // private absolute physical row map, nil for contiguous views
	column      *Vector
	lease       *RefCountedBufferLease
	start       int
	length      int
	repeated    bool
	repeatedRow int
	admission   JSONReaderMemoryAdmission
	releaseMeta func()
}

// IsPrivateJSONReader recognizes this package's live immutable owner without
// granting admission to another implementation of the JSONReader interface.
func IsPrivateJSONReader(reader JSONReader) bool {
	private, ok := reader.(*jsonReader)
	return ok && private != nil && private.lease != nil
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
func NewJSONReaderSnapshotWithAllocation(source *Vector, mp *mpool.MPool, selection *AllocationAccountSelection, admissions ...JSONReaderMemoryAdmission) (JSONReader, error) {
	rows := 0
	if source != nil {
		rows = source.length
	}
	return NewJSONReaderSnapshotForRows(source, rows, mp, selection, admissions...)
}

// NewJSONReaderSnapshotForRows binds an explicit batch row domain. Constants
// have one physical value and can span that domain without payload repetition.
// The source is still copied and fully validated; its metadata is not changed.
func NewJSONReaderSnapshotForRows(source *Vector, logicalRows int, mp *mpool.MPool, selection *AllocationAccountSelection, admissions ...JSONReaderMemoryAdmission) (JSONReader, error) {
	reader, _, err := TryNewJSONReaderSnapshotForRows(source, logicalRows, mp, selection, admissions...)
	return reader, err
}

// TryNewJSONReaderSnapshotForRows reports invalidInput only when descriptor
// preflight or complete validation of the private copy failed. Such a failure never returns a
// reader. Callers that preserve expression short-circuit order may defer this
// particular error to the ordinary evaluator; allocation and other failures
// remain terminal, and every document actually read must still be validated.
func TryNewJSONReaderSnapshotForRows(source *Vector, logicalRows int, mp *mpool.MPool, selection *AllocationAccountSelection, admissions ...JSONReaderMemoryAdmission) (reader JSONReader, invalidInput bool, err error) {
	if source == nil || mp == nil || source.typ.Oid != types.T_json || source.length < 0 || uint64(source.length) > math.MaxUint32 || logicalRows < 0 || uint64(logicalRows) > math.MaxUint32 {
		return nil, false, moerr.NewInvalidInputNoCtx("invalid JSON reader source")
	}
	// Dup uses TypeSize for slicing; reject corrupt representation metadata
	// before it can read beyond the source descriptor buffer.
	if _, err := canonicalVectorTypeSize(source.typ); err != nil {
		return nil, false, err
	}
	if !source.IsConst() && source.length != logicalRows {
		return nil, false, moerr.NewInvalidInputNoCtx("JSON input row domain mismatch")
	}
	rows := source.length
	if source.IsConst() {
		rows = 1
	}
	if !source.IsConstNull() && (rows > len(source.data)/types.VarlenaSize) {
		return nil, true, moerr.NewInvalidInputNoCtx("invalid JSON reader descriptors")
	}
	admission := firstJSONReaderMemoryAdmission(admissions)
	ownerMeta, err := ReserveJSONReaderMetadata(selection, admission, JSONReaderMetadataCapacity(unsafe.Sizeof(Vector{})+unsafe.Sizeof(RefCountedBufferLease{})))
	if err != nil {
		return nil, false, err
	}
	defer func() { releaseJSONReaderMetadata(ownerMeta) }()
	private, err := source.dup(mp, true, true, selection)
	if err != nil {
		return nil, false, err
	}
	private.length = logicalRows
	if !private.IsConstNull() {
		data := private.data[:rows*types.VarlenaSize]
		if err = validateVectorBinary(byte(private.class), private.typ, uint32(private.length), data, private.area, &private.nsp, true); err != nil {
			private.Free(mp)
			return nil, true, err
		}
	}
	reader, err = publishJSONReader(private, mp, admission, ownerMeta)
	if err != nil {
		private.Free(mp)
		return nil, false, err
	}
	ownerMeta = nil
	return reader, false, nil
}

// JSONReaderBuilder owns an unpublished column. Append copies and validates
// each document through the existing raw admission boundary. No mutable view
// of its column is exported, so Finish can transfer ownership without a scan.
// Close releases unfinished storage; calls are sequential.
type JSONReaderBuilder struct {
	column    *Vector
	mp        *mpool.MPool
	admission JSONReaderMemoryAdmission
	ownerMeta func()
}

func NewJSONReaderBuilder(mp *mpool.MPool, selection *AllocationAccountSelection, admissions ...JSONReaderMemoryAdmission) (*JSONReaderBuilder, error) {
	if mp == nil {
		return nil, moerr.NewInvalidInputNoCtx("JSON reader builder requires a pool")
	}
	admission := firstJSONReaderMemoryAdmission(admissions)
	ownerMeta, err := ReserveJSONReaderMetadata(selection, admission, JSONReaderMetadataCapacity(unsafe.Sizeof(Vector{})+unsafe.Sizeof(RefCountedBufferLease{})+unsafe.Sizeof(JSONReaderBuilder{})))
	if err != nil {
		return nil, err
	}
	column, err := NewOffHeapVecWithTypeAndAllocation(types.T_json.ToType(), selection)
	if err != nil {
		releaseJSONReaderMetadata(ownerMeta)
		return nil, err
	}
	return &JSONReaderBuilder{column: column, mp: mp, admission: admission, ownerMeta: ownerMeta}, nil
}

func (b *JSONReaderBuilder) Append(raw []byte, null bool) error {
	if b == nil || b.column == nil {
		return io.ErrClosedPipe
	}
	return AppendBytes(b.column, raw, null, b.mp)
}

// AppendByteJson uses the same complete admission and direct encoded copy as
// ordinary vector construction, without an intermediate Marshal allocation.
// The caller keeps value stable during this call; no writable alias is kept.
func (b *JSONReaderBuilder) AppendByteJson(value bytejson.ByteJson, null bool) error {
	if b == nil || b.column == nil {
		return io.ErrClosedPipe
	}
	return AppendByteJson(b.column, value, null, b.mp)
}

func (b *JSONReaderBuilder) Finish() (JSONReader, error) {
	if b == nil || b.column == nil {
		return nil, io.ErrClosedPipe
	}
	reader, err := publishJSONReader(b.column, b.mp, b.admission, b.ownerMeta)
	if err != nil {
		return nil, err
	}
	b.column = nil
	b.ownerMeta = nil
	b.admission = nil
	return reader, nil
}

func (b *JSONReaderBuilder) Close() {
	if b != nil && b.column != nil {
		b.column.Free(b.mp)
		b.column = nil
		releaseJSONReaderMetadata(b.ownerMeta)
		b.ownerMeta = nil
		b.admission = nil
	}
}

func publishJSONReader(column *Vector, mp *mpool.MPool, admission JSONReaderMemoryAdmission, ownerMeta func()) (JSONReader, error) {
	releaseMeta, err := ReserveJSONReaderMetadata(column.allocationAccount, admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonReader{})))
	if err != nil {
		return nil, err
	}
	lease, err := NewRefCountedBufferLease(nil, int64(column.Allocated()), func() { column.Free(mp); releaseJSONReaderMetadata(ownerMeta) })
	if err != nil {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, err
	}
	return &jsonReader{column: column, lease: lease, length: column.length, admission: admission, releaseMeta: releaseMeta}, nil
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
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonReader{})))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	return &jsonReader{column: r.column, lease: r.lease, start: r.start + start, length: end - start, rows: r.rows, repeated: r.repeated, repeatedRow: r.repeatedRow, admission: r.admission, releaseMeta: releaseMeta}, nil
}

// Repeat retains one physical row without constructing a batch-sized mapping.
func (r *jsonReader) Repeat(row, length int) (JSONReader, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	if row < 0 || row >= r.length || length < 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid JSON reader repeated row")
	}
	physicalRow := r.physicalRow(row)
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonReader{})))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	return &jsonReader{column: r.column, lease: r.lease, length: length, repeated: true, repeatedRow: physicalRow, admission: r.admission, releaseMeta: releaseMeta}, nil
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
	ownerMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(Vector{})+unsafe.Sizeof(RefCountedBufferLease{})))
	if err != nil {
		return nil, err
	}
	defer func() { releaseJSONReaderMetadata(ownerMeta) }()
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonReader{})))
	if err != nil {
		return nil, err
	}
	defer func() { releaseJSONReaderMetadata(releaseMeta) }()
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
	ownedMeta := ownerMeta
	lease, err := NewRefCountedBufferLease(nil, int64(mapping.Allocated()), func() { mapping.Free(mp); parent.Release(); releaseJSONReaderMetadata(ownedMeta) })
	if err != nil {
		mapping.Free(mp)
		parent.Release()
		return nil, err
	}
	reader := &jsonReader{column: r.column, rows: mapping, lease: lease, length: len(rows), admission: r.admission, releaseMeta: releaseMeta}
	ownerMeta = nil
	releaseMeta = nil
	return reader, nil
}

func (r *jsonReader) physicalRow(row int) int {
	if r.repeated {
		return r.repeatedRow
	}
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
		releaseMeta := r.releaseMeta
		r.lease = nil
		r.releaseMeta = nil
		r.admission = nil
		// Row coordinates remain immutable for outstanding cursors. The
		// final lease frees all column and mapping backing.
		lease.Release()
		releaseJSONReaderMetadata(releaseMeta)
	}
}

// Query returns an opaque selected value. It holds its own owner reference,
// so it survives the reader's Close. SQL NULL and path absence are separate.
// The caller must Close every successful result, including misses and NULLs.
func (r *jsonReader) Query(row int, path *bytejson.Path) (JSONQueryResult, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonQueryResult{})))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	result := &jsonQueryResult{lease: r.lease, releaseMeta: releaseMeta}
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
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(unsafe.Sizeof(jsonCursor{})))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	return &jsonCursor{
		reader:          jsonReader{rows: r.rows, column: r.column, start: r.start, length: r.length, repeated: r.repeated, repeatedRow: r.repeatedRow},
		jsonQueryResult: jsonQueryResult{lease: r.lease, releaseMeta: releaseMeta},
	}, nil
}

type jsonCursor struct {
	jsonQueryResult
	reader jsonReader
}

func (cursor *jsonCursor) ResetQuery(row int, path bytejson.Path) error {
	if cursor == nil || cursor.jsonQueryResult.lease == nil {
		return io.ErrClosedPipe
	}
	return cursor.reader.fillQuery(row, &path, &cursor.jsonQueryResult)
}

func (cursor *jsonCursor) Close() {
	if cursor != nil {
		cursor.reader = jsonReader{}
		cursor.jsonQueryResult.Close()
	}
}

// jsonQueryResult keeps the borrowed ByteJson inside this package. Output
// methods return owned text or copy into an explicitly supplied destination.
// Separate query results may be consumed concurrently; Close is sequential.
type jsonQueryResult struct {
	value       bytejson.ByteJson
	exists      bool
	sqlNull     bool
	lease       *RefCountedBufferLease
	releaseMeta func()
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
		releaseMeta := r.releaseMeta
		r.lease = nil
		r.releaseMeta = nil
		r.value = bytejson.ByteJson{}
		lease.Release()
		releaseJSONReaderMetadata(releaseMeta)
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
	if cursor == nil || cursor.jsonQueryResult.lease == nil {
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
