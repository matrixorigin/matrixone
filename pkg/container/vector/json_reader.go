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
	"reflect"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/internal/jsonread"
)

var (
	jsonReaderVectorSize      = reflect.TypeFor[Vector]().Size()
	jsonReaderLeaseSize       = reflect.TypeFor[RefCountedBufferLease]().Size()
	jsonReaderBuilderSize     = reflect.TypeFor[JSONReaderBuilder]().Size()
	jsonReaderHandleSize      = reflect.TypeFor[jsonReader]().Size()
	jsonReaderQueryResultSize = reflect.TypeFor[jsonQueryResult]().Size()
	jsonReaderCursorSize      = reflect.TypeFor[jsonCursor]().Size()
)

// JSONReader is an immutable column version. Implementations keep their
// backing private; exported operations return owned data or opaque results.
// Consumers own each returned handle and close it exactly once. Implementations
// must reject malformed documents before publication and preserve backing
// lifetime for all retained windows and query results. Independent readers may
// be used concurrently; operations on one reader are sequential.
type JSONReader interface {
	OpenCursor() (JSONCursor, error)
	QueryText(int, []byte, bool) (JSONTextSelection, error)
	QueryTextAtPath(int, bytejson.Path, bool) (JSONTextSelection, error)
	Length() int
	Window(int, int) (JSONReader, error)
	Repeat(int, int) (JSONReader, error)
	Select([]int64, *mpool.MPool) (JSONReader, error)
	Materialize(*mpool.MPool) (*Vector, error)
	Query(int, *bytejson.Path) (JSONQueryResult, error)
	IsNull(int) (bool, error)
	Close()
}

// JSONDirectTextReader copies eligible values directly into an accounted
// function result. Unsupported values fall back to the owned selection.
// The limit is a byte eligibility bound (negative means unlimited), not a
// character-width check; callers must handle the fallback's width semantics.
// No source bytes escape the reader, and append failures are returned.
type JSONDirectTextReader interface {
	QueryTextTo(int, []byte, bool, int, *FunctionResult[types.Varlena]) (JSONTextSelection, bool, error)
	QueryTextAtPathTo(int, bytejson.Path, bool, int, *FunctionResult[types.Varlena]) (JSONTextSelection, bool, error)
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
	Text() (JSONOwnedText, error)
	TextWithLegacyFloat() (JSONOwnedText, error)
	AppendJSON(*Vector, *mpool.MPool) error
	AppendResult(FunctionResultWrapper) error
	Close()
}

type jsonOwnedTextLease struct {
	once    sync.Once
	release func()
}

// JSONOwnedText is independent of the source reader. Its owner must Close it
// after use; copies share the same release, and their text must not be used
// after any copy is closed.
type JSONOwnedText struct {
	Text  string
	lease *jsonOwnedTextLease
}

// NewJSONOwnedText attaches a pre-existing allocation reservation to copied
// text. It does not validate input or confer JSON reader authority.
func NewJSONOwnedText(text string, release func()) JSONOwnedText {
	if release == nil {
		return JSONOwnedText{Text: text}
	}
	return JSONOwnedText{Text: text, lease: &jsonOwnedTextLease{release: release}}
}

func (text *JSONOwnedText) Close() {
	if text == nil {
		return
	}
	if text.lease != nil {
		text.lease.once.Do(text.lease.release)
		text.lease = nil
	}
	text.Text = ""
}

// IsJSONTextConversionError excludes allocation/admission failures from a
// consumer's value-conversion response policy.
func IsJSONTextConversionError(err error) bool { return jsonread.IsConversionError(err) }

func copyJSONOwnedText(value bytejson.ByteJson, legacyFloat bool, selection *AllocationAccountSelection, admission JSONReaderMemoryAdmission) (JSONOwnedText, error) {
	text, release, err := jsonread.CopyText(value, legacyFloat, func(size uint64) (func(), error) {
		return ReserveJSONReaderBacking(selection, admission, size)
	})
	if err != nil {
		return JSONOwnedText{}, err
	}
	return NewJSONOwnedText(text, release), nil
}

// JSONTextSelection owns copied text and scalar selection metadata. The
// caller must Close the selection; it never exposes document bytes.
type JSONTextSelection struct {
	Text        string
	Type        byte
	Exists      bool
	InputNull   bool
	ValueNull   bool
	Multiple    bool
	InvalidPath bool
	TextError   bool
	lease       *jsonOwnedTextLease
}

// NewJSONTextSelection transfers the copied text's reservation to a selection.
func NewJSONTextSelection(text JSONOwnedText, typ byte) JSONTextSelection {
	return JSONTextSelection{Text: text.Text, Type: typ, Exists: true, lease: text.lease}
}

func (selection *JSONTextSelection) Close() {
	if selection == nil {
		return
	}
	if selection.lease != nil {
		selection.lease.once.Do(selection.lease.release)
		selection.lease = nil
	}
	selection.Text = ""
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
	scratch     [128]byte // fixed formatting space, included in handle admission
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
	if source.IsDictionary() {
		// Dictionary materialization reads the selected physical descriptors.
		// Check them before gathering payloads from an unknown source.
		for row := 0; row < rows; row++ {
			if !source.IsNull(uint64(row)) {
				if _, err := source.GetBytesAtChecked(row); err != nil {
					return nil, true, err
				}
			}
		}
	} else if !source.IsConstNull() && (rows > len(source.data)/types.VarlenaSize) {
		return nil, true, moerr.NewInvalidInputNoCtx("invalid JSON reader descriptors")
	}
	admission := firstJSONReaderMemoryAdmission(admissions)
	ownerMeta, err := ReserveJSONReaderMetadata(selection, admission, JSONReaderMetadataCapacity(jsonReaderVectorSize+jsonReaderLeaseSize))
	if err != nil {
		return nil, false, err
	}
	defer func() { releaseJSONReaderMetadata(ownerMeta) }()
	selection = JSONReaderAllocationSelection(selection, admission)
	var private *Vector
	if source.IsDictionary() {
		private, err = source.materializeDictionaryWithBuilder(mp, selection, buildPrivateJSONVarlena)
	} else {
		private, err = source.dup(mp, true, true, selection)
	}
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

// buildPrivateJSONVarlena only copies into an unpublished snapshot. Its caller
// validates every copied document before transferring ownership to a reader.
func buildPrivateJSONVarlena(dst *Vector, descriptor *types.Varlena, data *[]byte, mp *mpool.MPool) error {
	if len(*data) <= types.VarlenaInlineSize {
		clear(descriptor[:])
		descriptor[0] = byte(len(*data))
		copy(descriptor[1:], *data)
		return nil
	}
	return buildVarlenaNoInline(dst, descriptor, data, mp)
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
	ownerMeta, err := ReserveJSONReaderMetadata(selection, admission, JSONReaderMetadataCapacity(jsonReaderVectorSize+jsonReaderLeaseSize+jsonReaderBuilderSize))
	if err != nil {
		return nil, err
	}
	selection = JSONReaderAllocationSelection(selection, admission)
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
	releaseMeta, err := ReserveJSONReaderMetadata(column.allocationAccount, admission, JSONReaderMetadataCapacity(jsonReaderHandleSize))
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
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderHandleSize))
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
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderHandleSize))
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
	ownerMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderVectorSize+jsonReaderLeaseSize))
	if err != nil {
		return nil, err
	}
	defer func() { releaseJSONReaderMetadata(ownerMeta) }()
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderHandleSize))
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
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderQueryResultSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	result := &jsonQueryResult{lease: r.lease, releaseMeta: releaseMeta, selection: r.column.allocationAccount, admission: r.admission}
	if err := r.fillQuery(row, path, result); err != nil {
		result.Close()
		return nil, err
	}
	return result, nil
}

func (r *jsonReader) loadQueryRow(row int, result *jsonQueryResult) (bytejson.ByteJson, error) {
	result.clearValue()
	if r == nil || result.lease == nil {
		return bytejson.ByteJson{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	result.sqlNull = isNull
	return value, err
}

func (r *jsonReader) loadValue(row int) (bytejson.ByteJson, bool, error) {
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
	result.value, result.exists, result.releaseValue, err = value.QueryWithMemory(path, func(size uint64) (func(), error) {
		return ReserveJSONReaderBacking(r.column.allocationAccount, r.admission, size)
	})
	return err
}

func (r *jsonReader) OpenCursor() (JSONCursor, error) {
	if r == nil || r.lease == nil {
		return nil, io.ErrClosedPipe
	}
	releaseMeta, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderCursorSize))
	if err != nil {
		return nil, err
	}
	if !r.lease.Retain() {
		releaseJSONReaderMetadata(releaseMeta)
		return nil, io.ErrClosedPipe
	}
	return &jsonCursor{
		reader:          jsonReader{rows: r.rows, column: r.column, start: r.start, length: r.length, repeated: r.repeated, repeatedRow: r.repeatedRow, admission: r.admission},
		jsonQueryResult: jsonQueryResult{lease: r.lease, releaseMeta: releaseMeta, selection: r.column.allocationAccount, admission: r.admission},
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
	value        bytejson.ByteJson
	exists       bool
	sqlNull      bool
	lease        *RefCountedBufferLease
	releaseMeta  func()
	releaseValue func()
	selection    *AllocationAccountSelection
	admission    JSONReaderMemoryAdmission
}

func (r *jsonQueryResult) clearValue() {
	if r.releaseValue != nil {
		r.releaseValue()
		r.releaseValue = nil
	}
	r.value = bytejson.ByteJson{}
	r.exists, r.sqlNull = false, false
}

func (r *jsonQueryResult) Exists() bool  { return r != nil && r.lease != nil && r.exists }
func (r *jsonQueryResult) SQLNull() bool { return r != nil && r.lease != nil && r.sqlNull }
func (r *jsonQueryResult) Text() (JSONOwnedText, error) {
	if r == nil || r.lease == nil {
		return JSONOwnedText{}, io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return JSONOwnedText{}, moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return copyJSONOwnedText(r.value, false, r.selection, r.admission)
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
		r.clearValue()
		r.lease = nil
		r.releaseMeta = nil
		r.selection = nil
		r.admission = nil
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

func (r *jsonQueryResult) TextWithLegacyFloat() (JSONOwnedText, error) {
	if r == nil || r.lease == nil {
		return JSONOwnedText{}, io.ErrClosedPipe
	}
	if r.sqlNull || !r.exists {
		return JSONOwnedText{}, moerr.NewInvalidInputNoCtx("JSON query has no value")
	}
	return copyJSONOwnedText(r.value, true, r.selection, r.admission)
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
	text, err := copyJSONOwnedText(value, legacyFloat, r.selection, r.admission)
	if err != nil {
		return JSONTextSelection{TextError: IsJSONTextConversionError(err)}, err
	}
	return NewJSONTextSelection(text, byte(value.Type)), nil
}

// QueryText borrows the live reader only for this call and returns owned text.
// Like the retained cursor, it checks a NULL document before parsing the path.
func (r *jsonReader) QueryText(row int, pathBytes []byte, legacyFloat bool) (JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return JSONTextSelection{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return JSONTextSelection{}, err
	}
	if isNull {
		return JSONTextSelection{InputNull: true}, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return JSONTextSelection{InvalidPath: true}, err
	}
	return r.queryTextFromValue(value, path, legacyFloat)
}

// QueryTextAtPath accepts an already parsed path and borrows the reader only
// until its owned result is returned. It does not create or retain a cursor.
func (r *jsonReader) QueryTextAtPath(row int, path bytejson.Path, legacyFloat bool) (JSONTextSelection, error) {
	if r == nil || r.lease == nil {
		return JSONTextSelection{}, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return JSONTextSelection{}, err
	}
	if isNull {
		return JSONTextSelection{InputNull: true}, nil
	}
	return r.queryTextFromValue(value, path, legacyFloat)
}

func (r *jsonReader) queryTextFromValue(value bytejson.ByteJson, path bytejson.Path, legacyFloat bool) (JSONTextSelection, error) {
	release, err := ReserveJSONReaderMetadata(r.column.allocationAccount, r.admission, JSONReaderMetadataCapacity(jsonReaderQueryResultSize))
	if err != nil {
		return JSONTextSelection{}, err
	}
	defer releaseJSONReaderMetadata(release)
	result := jsonQueryResult{lease: r.lease, selection: r.column.allocationAccount, admission: r.admission}
	defer result.clearValue()
	return queryTextFromValue(value, path, &result, legacyFloat)
}

func queryTextFromValue(value bytejson.ByteJson, path bytejson.Path, result *jsonQueryResult, legacyFloat bool) (JSONTextSelection, error) {
	var err error
	result.value, result.exists, result.releaseValue, err = value.QueryWithMemory(&path, func(size uint64) (func(), error) {
		return ReserveJSONReaderBacking(result.selection, result.admission, size)
	})
	if err != nil {
		return JSONTextSelection{}, err
	}
	return result.selectText(path.IsSimple(), legacyFloat)
}

// QueryTextTo keeps the NULL-before-path ordering of QueryText. Eligible
// scalars are copied directly into the result; other cases retain the ordinary
// selection and its independent memory reservation.
func (r *jsonReader) QueryTextTo(row int, pathBytes []byte, legacyFloat bool, limit int, target *FunctionResult[types.Varlena]) (JSONTextSelection, bool, error) {
	if r == nil || r.lease == nil {
		return JSONTextSelection{}, false, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return JSONTextSelection{}, false, err
	}
	if isNull {
		return JSONTextSelection{InputNull: true}, false, nil
	}
	path, err := bytejson.ParseJsonPath(string(pathBytes))
	if err != nil {
		return JSONTextSelection{InvalidPath: true}, false, err
	}
	return r.queryTextTo(value, path, legacyFloat, limit, target)
}

func (r *jsonReader) QueryTextAtPathTo(row int, path bytejson.Path, legacyFloat bool, limit int, target *FunctionResult[types.Varlena]) (JSONTextSelection, bool, error) {
	if r == nil || r.lease == nil {
		return JSONTextSelection{}, false, io.ErrClosedPipe
	}
	value, isNull, err := r.loadValue(row)
	if err != nil {
		return JSONTextSelection{}, false, err
	}
	if isNull {
		return JSONTextSelection{InputNull: true}, false, nil
	}
	return r.queryTextTo(value, path, legacyFloat, limit, target)
}

func (r *jsonReader) queryTextTo(value bytejson.ByteJson, path bytejson.Path, legacyFloat bool, limit int, target *FunctionResult[types.Varlena]) (JSONTextSelection, bool, error) {
	if target != nil && path.IsSimple() {
		selected, exists, _, err := value.QueryWithMemory(&path, nil)
		if err != nil {
			return JSONTextSelection{}, false, err
		}
		if !exists {
			return JSONTextSelection{}, false, nil
		}
		if selected.IsNull() {
			return JSONTextSelection{Exists: true, ValueNull: true}, false, nil
		}
		text, direct, formatErr := jsonread.DirectText(selected, legacyFloat, limit, r.scratch[:0])
		if formatErr != nil {
			return JSONTextSelection{TextError: true}, false, formatErr
		}
		if direct {
			return JSONTextSelection{}, true, target.AppendBytes(text, false)
		}
	}
	selection, err := r.queryTextFromValue(value, path, legacyFloat)
	return selection, false, err
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
	return queryTextFromValue(value, path, &cursor.jsonQueryResult, legacyFloat)
}
