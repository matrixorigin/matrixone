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

package vector

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"sort"
	"strings"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
)

const (
	FLAT     = iota // flat vector represent a uncompressed vector
	CONSTANT        // const vector
)

// Vector represent a column
type Vector struct {
	// vector's class
	class int
	// type represent the type of column
	typ types.Type

	// data of fixed length element, in case of varlen, the Varlena
	data []byte

	// area for holding large strings.
	area []byte

	length int

	nsp nulls.Nulls // nulls list

	sorted bool // for some optimization

	// mp is the pool the vector was created with. Every allocation of the
	// vector comes from it, and Free returns everything to it.
	//
	// A nil mp marks a read-only reference vector: its data and area point
	// into memory owned by someone else (for example the FileService cache),
	// who keeps it alive. Such a vector is never mutated, never allocates and
	// never frees its backing.
	mp *mpool.MPool
}

func toSliceOfLengthNoTypeCheck[T any](vec *Vector, length int) []T {
	if length == 0 {
		return nil
	}
	return util.UnsafeSliceCastToLength[T](vec.data, length)
}

func ToSliceNoTypeCheck[T any](vec *Vector, ret *[]T) {
	if vec.IsConst() {
		*ret = toSliceOfLengthNoTypeCheck[T](vec, 1)
	} else {
		*ret = toSliceOfLengthNoTypeCheck[T](vec, vec.length)
	}
}

func ToSliceNoTypeCheck2[T any](vec *Vector) []T {
	if vec.IsConst() {
		return toSliceOfLengthNoTypeCheck[T](vec, 1)
	} else {
		return toSliceOfLengthNoTypeCheck[T](vec, vec.length)
	}
}

func ToSlice[T any](vec *Vector, ret *[]T) {
	checkType[T](vec)
	if vec.IsConst() {
		*ret = util.UnsafeSliceCastToLength[T](vec.data, 1)
	} else {
		*ret = util.UnsafeSliceCastToLength[T](vec.data, vec.length)
	}
}

func checkType[T any](vec *Vector) {
	if !typeCompatible[T](vec.typ) {
		panic(fmt.Sprintf("type mismatch: casting %v vector to %T", vec.typ.String(), []T{}))
	}
}

func (v *Vector) GetSorted() bool {
	return v.sorted
}

func (v *Vector) SetSorted(b bool) {
	v.sorted = b
}

// Reset update vector's fields with a specific type.
// we should redefine the value of capacity and values-ptr because of the possible change in type.
func (v *Vector) Reset(typ types.Type) {
	v.typ = typ

	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}

	v.length = 0
	v.clearNulls()
	v.sorted = false
}

func (v *Vector) ResetWithSameType() {
	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.length = 0
	// Keep an owned bitmap's words for reuse, like the data capacity.
	v.clearNulls()
	v.sorted = false
}

func (v *Vector) ResetArea() {
	v.area = v.area[:0]
}

// TODO: It is semantically same as Reset, need to merge them later.
func (v *Vector) ResetWithNewType(t *types.Type) {
	v.typ = *t
	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.clearNulls()
	v.length = 0
	v.sorted = false
}

// clearNulls empties the null rows: an owned bitmap keeps its words for
// reuse, and a read-only one drops the words it references.
func (v *Vector) clearNulls() {
	if v.nsp.IsReadOnly() {
		v.nsp.Free()
		return
	}
	v.nsp.Clear()
}

func (v *Vector) UnsafeGetRawData() []byte {
	length := 1
	if !v.IsConst() {
		length = v.length
	}
	return v.data[:length*v.typ.TypeSize()]
}

func (v *Vector) Length() int {
	return v.length
}

func (v *Vector) Capacity() int {
	typeSize := v.typ.TypeSize()
	if typeSize == 0 {
		return 0
	}
	return cap(v.data) / typeSize
}

// Allocated returns the total allocated memory size of the vector.
// it can be used to estimate the memory usage of the vector.
func (v *Vector) Allocated() int {
	// An owned null bitmap holds pool memory too, and CleanOnlyData keeps it
	// for reuse; a read-only one references words it does not own.
	retained := 0
	if !v.nsp.IsReadOnly() {
		retained = v.nsp.GetBitmap().CapWords() * 8
	}
	return retained + cap(v.data) + cap(v.area)
}

func (v *Vector) SetLength(n int) {
	if n < 0 {
		panic("negative vector length")
	}
	v.setLengthAfterExtend(n)
}

// AppendCheckpoint captures the logical state changed by append operations.
// Capacity growth is deliberately not rolled back: it remains owned by the
// vector and can be reused by a later append.
type AppendCheckpoint struct {
	length     int
	areaLength int
	sorted     bool
}

// AppendCheckpointScratch interprets allocator-backed byte storage as append
// checkpoints. It keeps the checkpoint layout private to vector while letting
// wide batch operators account scratch by its real physical capacity.
func AppendCheckpointScratch(storage []byte, count int) ([]AppendCheckpoint, int, error) {
	if count < 0 {
		return nil, 0, mpool.ErrAllocationAccountInvalid
	}
	size := int(unsafe.Sizeof(AppendCheckpoint{}))
	if count != 0 && count > math.MaxInt/size {
		return nil, 0, mpool.ErrAllocationAllocatorLimit
	}
	required := count * size
	if len(storage) < required {
		return nil, required, nil
	}
	if count == 0 {
		return nil, 0, nil
	}
	if uintptr(unsafe.Pointer(&storage[0]))%unsafe.Alignof(AppendCheckpoint{}) != 0 {
		return nil, required, mpool.ErrAllocationAccountInvalid
	}
	return unsafe.Slice(
		(*AppendCheckpoint)(unsafe.Pointer(&storage[0])), count), required, nil
}

func (v *Vector) MakeAppendCheckpoint() AppendCheckpoint {
	return AppendCheckpoint{
		length:     v.length,
		areaLength: len(v.area),
		sorted:     v.sorted,
	}
}

// JoinAppendRollbackError preserves a successful rollback's original error
// identity, including direct MO errors used at protocol boundaries.
func JoinAppendRollbackError(cause, rollbackErr error) error {
	if rollbackErr == nil {
		return cause
	}
	return errors.Join(cause, rollbackErr)
}

// RollbackAppend restores an append checkpoint, retaining owned capacity.
// Invalid checkpoints return a terminal invariant error before any mutation.
// attemptedRows includes metadata published before a payload append failed.
func (v *Vector) RollbackAppend(checkpoint AppendCheckpoint, attemptedRows int) error {
	invalid := func(message string) error {
		return errors.Join(mpool.ErrAllocationAccountInvariant, moerr.NewInternalErrorNoCtx(message))
	}
	if v == nil || checkpoint.length < 0 || checkpoint.length > v.length ||
		checkpoint.areaLength < 0 || checkpoint.areaLength > len(v.area) ||
		attemptedRows < 0 || attemptedRows > math.MaxInt-checkpoint.length {
		return invalid("invalid vector append checkpoint")
	}
	end := max(v.length, checkpoint.length+attemptedRows)
	nulls.RemoveRange(&v.nsp, uint64(checkpoint.length), uint64(end))
	v.length = checkpoint.length
	v.area = v.area[:checkpoint.areaLength]
	v.sorted = checkpoint.sorted
	return nil
}

// Size of data, I think this function is inherently broken.  This
// Size is not meaningful other than used in (approximate) memory accounting.
func (v *Vector) Size() int {
	area := len(v.area)
	return v.length*v.typ.TypeSize() + area
}

func (v *Vector) GetType() *types.Type {
	return &v.typ
}

// Bug #23240
// This is very dangerous.   We changed vector type
// but did not change the underlying data.   So the length
// and capacity are all messed up.
// SetType changes representation metadata; it does not cast or admit payloads.
// A nonempty T_json result must already contain valid encoded JSON. Raw text
// must go through the JSON cast/parser and a checked vector write instead.
func (v *Vector) SetType(typ types.Type) {
	v.typ = typ
}

// SetTypeAndFixData changes a fixed-width result type and grows its owned data
// before publishing the new type. A failed growth leaves the original vector
// type, length, and backing allocation intact.
func (v *Vector) SetTypeAndFixData(
	typ types.Type,
	mp *mpool.MPool,
) error {
	if v.typ.IsVarlen() && typ.IsVarlen() {
		v.typ = typ
		return nil
	}

	if v.typ.IsVarlen() || typ.IsVarlen() {
		return moerr.NewInvalidInputNoCtx(
			"SetTypeAndFixData cannot change from or to a varlen type",
		)
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}

	oldType := v.typ
	v.typ = typ
	oldLength := v.length
	v.length = 0
	if err := extend(v, oldLength, mp); err != nil {
		v.typ = oldType
		v.length = oldLength
		return err
	}
	v.length = oldLength
	return nil
}

func (v *Vector) SetTypeScale(scale int32) {
	v.typ.Scale = scale
}

// GetNulls exposes the row validity bitmap.
func (v *Vector) GetNulls() *nulls.Nulls {
	return &v.nsp
}

// DetachNulls gives v an owned copy of its null bitmap allocated from mp, so
// that v stops referencing the bitmap of the vector it was windowed from. A
// read-only window that must outlive its source (its payload being kept
// alive by a lease) detaches its nulls first; v's payload is unchanged.
func (v *Vector) DetachNulls(mp *mpool.MPool) error {
	if !v.nsp.IsReadOnly() {
		return nil
	}
	if v.nsp.IsEmpty() {
		v.nsp.Free()
		return nil
	}
	var owned nulls.Nulls
	if err := owned.InitWith(&v.nsp, mp); err != nil {
		return err
	}
	v.nsp.Free()
	v.nsp = owned
	return nil
}

func (v *Vector) SetNulls(nsp *nulls.Nulls) {
	v.clearNulls()
	if nsp == nil {
		return
	}
	v.nsp.Or(nsp)
}

func (v *Vector) SetAllNulls(length int) {
	if err := v.nsp.InitWithSize(length, v.mp); err != nil {
		panic(err)
	}
	v.nsp.AddRange(0, uint64(length))
}

func (v *Vector) HasNull() bool {
	return v.IsConstNull() || !v.nsp.IsEmpty()
}

// NullCount counts the NULL rows; a const NULL vector is all NULL and any
// other const vector has none.
func (v *Vector) NullCount() int {
	if v.IsConstNull() {
		return v.length
	}
	if v.IsConst() {
		return 0
	}
	return v.nsp.Count()
}

func (v *Vector) AllNull() bool {
	if v.IsConstNull() {
		return true
	}
	if v.length == 0 || v.nsp.Count() < v.length {
		return false
	}
	if v.nsp.Len() <= int64(v.length) {
		return v.nsp.Count() == v.length
	}
	return v.nsp.CountRange(0, uint64(v.length)) == v.length
}

func (v *Vector) prepareOrdinaryAppendMetadata(rows int, mp *mpool.MPool) error {
	return nil
}

// setLengthAfterExtend publishes a length whose row-parallel capacities were
// already reserved. It cannot allocate.
func (v *Vector) setLengthAfterExtend(n int) {
	v.length = n
}

func (v *Vector) unionBatchPreflighted(
	w *Vector,
	offset int64,
	cnt int,
	flags []uint8,
	mp *mpool.MPool,
	selectedAreaBytes int,
	preflightRowCount int,
) error {
	return v.unionBatchFlat(w, offset, cnt, flags, mp, selectedAreaBytes, preflightRowCount, true)
}

// make sure the type check is done before calling this function
func GetFixedAtNoTypeCheck[T any](v *Vector, idx int) T {
	length := v.length
	if v.IsConst() {
		idx = 0
		length = 1
	}
	return toSliceOfLengthNoTypeCheck[T](v, length)[idx]
}

// Note:
// it is much inefficient than GetFixedAtNoTypeCheck
// if type check is done before calling this function, use GetFixedAtNoTypeCheck
func GetFixedAtWithTypeCheck[T any](v *Vector, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	var slice []T
	ToSlice(v, &slice)
	return slice[idx]
}

func (v *Vector) CloneBytesAt(i int) []byte {
	bs := v.GetBytesAt(i)
	ret := make([]byte, len(bs))
	copy(ret, bs)
	return ret
}

func (v *Vector) GetBytesAt(i int) []byte {
	length := v.length
	if v.IsConst() {
		i = 0
		length = 1
	}
	bs := toSliceOfLengthNoTypeCheck[types.Varlena](v, length)
	return bs[i].GetByteSlice(v.area)
}

// GetBytesAtChecked reads mutable varlen input without trusting descriptor
// offsets. The returned bytes still belong to v; this grants no read-only
// qualification and does not validate a JSON document's contents.
func (v *Vector) GetBytesAtChecked(i int) ([]byte, error) {
	if v == nil || !v.typ.IsVarlen() || i < 0 {
		return nil, moerr.NewInvalidInputNoCtx("invalid vector varlen row")
	}
	if v.IsConst() {
		i = 0
	} else if i >= v.length {
		return nil, moerr.NewInvalidInputNoCtx("invalid vector varlen row")
	}
	if i >= len(v.data)/types.VarlenaSize {
		return nil, moerr.NewInvalidInputNoCtx("invalid vector data size")
	}
	values := toSliceOfLengthNoTypeCheck[types.Varlena](v, i+1)
	descriptor := &values[i]
	if !descriptor.IsSmall() {
		offset, size := descriptor.OffsetLen()
		if uint64(offset) > uint64(len(v.area)) || uint64(size) > uint64(len(v.area))-uint64(offset) {
			return nil, moerr.NewInvalidInputNoCtx("invalid vector varlen offset")
		}
	}
	return descriptor.GetByteSlice(v.area), nil
}

func (v *Vector) GetBytesAt2(bs []types.Varlena, i int) []byte {
	if v.IsConst() {
		i = 0
	}
	return bs[i].GetByteSlice(v.area)
}

func (v *Vector) GetRawBytesAt(i int) []byte {
	if v.typ.IsVarlen() {
		return v.GetBytesAt(i)
	} else {
		if v.IsConst() {
			i = 0
		} else {
			i *= v.GetType().TypeSize()
		}
		return v.data[i : i+v.GetType().TypeSize()]
	}
}

func (v *Vector) CleanOnlyData() {
	// Constant NULLs have logical rows without a payload allocation.
	v.length = 0
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.clearNulls()
	v.sorted = false
}

// no copy. it is unsafe if the user cannot determine the vector's life
func (v *Vector) UnsafeGetStringAt(i int) string {
	if v.IsConst() {
		i = 0
	}
	// if !v.typ.Oid.IsFixedLen() {
	// 	panic(fmt.Sprintf("type mismatch: expect varlen type but actual %s", v.typ.String()))
	// }
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].UnsafeGetString(v.area)
}

// always copy
func (v *Vector) GetStringAt(i int) string {
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].GetString(v.area)
}

// GetArrayAt Returns []T at the specific index of the vector
func GetArrayAt[T types.ArrayElement](v *Vector, i int) []T {
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return types.GetArray[T](&bs[i], v.area)
}

func GetArrayAt2[T types.ArrayElement](v *Vector, bs []types.Varlena, i int) []T {
	if v.IsConst() {
		i = 0
	}
	return types.GetArray[T](&bs[i], v.area)
}

// WARNING: GetAny() return value with any type will cause memory escape to heap which will result in slow GC.
// If you know the actual type, better use the GetFixedAtWithTypeCheck() to get the values.
// Only use when you have no choice, e.g. you are dealing with column with any type that don't know in advanced.
func GetAny(vec *Vector, i int, deepCopy bool) any {
	if ops, ok := opsOf(vec); ok {
		return ops.getAny(vec, i, deepCopy)
	}
	return nil
}

// GetValue returns row i as a value, honoring NULL: nil for a const NULL
// vector, a nil []byte for a NULL varlena row, and a copy of a varlena value.
// A NULL fixed-size row returns whatever its slot holds.
//
// Like GetAny it boxes the value; it is for code that does not know the type.
func (v *Vector) GetValue(i int) any {
	if v.IsConstNull() {
		return nil
	}
	if v.typ.IsVarlen() && v.IsNull(uint64(i)) {
		return []byte(nil)
	}
	return GetAny(v, i, true)
}

// NewVec returns an empty flat vector of typ whose memory comes from mp. A
// nil mp makes a read-only reference vector (see Vector.mp), filled only by a
// no-copy decode or NewVecWithData.
func NewVec(typ types.Type, mp *mpool.MPool) *Vector {
	vec := new(Vector)
	vec.InitVec(typ, mp)
	return vec
}

// InitVec makes v an empty flat vector of typ bound to mp (nil for a
// read-only reference vector). It is for vectors held by value; v must not
// hold any memory.
func (v *Vector) InitVec(typ types.Type, mp *mpool.MPool) {
	if v.hasBackingStorage() {
		panic(moerr.NewInternalErrorNoCtx("InitVec on a vector that holds memory"))
	}
	*v = Vector{typ: typ, class: FLAT, mp: mp}
	v.nsp.Init(mp)
}

// MPool returns the pool the vector was created with; nil for a read-only
// reference vector.
func (v *Vector) MPool() *mpool.MPool {
	return v.mp
}

// IsReadOnly reports a reference vector that does not own its memory.
func (v *Vector) IsReadOnly() bool {
	return v.mp == nil
}

// NewVecWithData is an unchecked ownership transfer. Its descriptors and
// payload must come from a valid vector of typ, with no mutable aliases
// retained, and data/area must be allocations of mp. With a nil mp it builds a
// read-only reference vector over memory the caller keeps alive.
// External bytes must enter through NewVecWithDataCopy or checked unmarshal.
func NewVecWithData(
	typ types.Type,
	length int,
	data []byte,
	area []byte,
	mp *mpool.MPool,
) *Vector {
	vec := NewVec(typ, mp)
	vec.length = length
	vec.data = data
	vec.area = area
	return vec
}

// NewVecWithDataCopy copies external backing data into allocations owned by mp.
func NewVecWithDataCopy(
	typ types.Type,
	length int,
	data []byte,
	area []byte,
	mp *mpool.MPool,
) (*Vector, error) {
	if typ.Oid == types.T_json {
		if length < 0 || uint64(length) > math.MaxUint32 {
			return nil, moerr.NewInvalidInputNoCtx("invalid JSON vector length")
		}
		if err := validateVectorBinary(FLAT, typ, uint32(length), data, area, &nulls.Nulls{}, true); err != nil {
			return nil, err
		}
	}
	vec := NewVec(typ, mp)
	vec.length = length
	var err error
	if len(data) > 0 {
		vec.data, err = vec.alloc(mp, len(data))
		if err != nil {
			vec.Free(mp)
			return nil, err
		}
		copy(vec.data, data)
	}
	if len(area) > 0 {
		vec.area, err = vec.alloc(mp, len(area))
		if err != nil {
			vec.Free(mp)
			return nil, err
		}
		copy(vec.area, area)
	}
	return vec, nil
}

func NewConstNull(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := NewVec(typ, mp)
	vec.class = CONSTANT
	vec.length = length
	return vec
}

func NewConstFixed[T any](typ types.Type, val T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVec(typ, mp)
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstFixed(vec, val, length, mp)
	}

	return vec, err
}

func NewConstBytes(typ types.Type, val []byte, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVec(typ, mp)
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstBytes(vec, val, length, mp)
	}

	return vec, err
}

// NewConstArray Creates a Const_Array Vector
func NewConstArray[T types.ArrayElement](typ types.Type, val []T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVec(typ, mp)
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstArray[T](vec, val, length, mp)
	}

	return vec, err
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

func (v *Vector) SetClass(class int) {
	v.class = class
}

func (v *Vector) IsNull(i uint64) bool {
	if v.IsConstNull() {
		return true
	}
	if v.IsConst() {
		return false
	}
	return v.nsp.Contains(i)
}

func (v *Vector) SetNull(i uint64) {
	v.mustBeWritable()
	v.nsp.Add(i)
}

func (v *Vector) UnsetNull(i uint64) {
	v.mustBeWritable()
	v.nsp.Del(i)
}

// mustBeWritable panics on a read-only vector, for in-place mutators that
// have no error result.
func (v *Vector) mustBeWritable() {
	if v.mp == nil {
		panic(errVectorReadOnly())
	}
}

// call this function if type already checked
func SetFixedAtNoTypeCheck[T types.FixedSizeT](v *Vector, idx int, t T) error {
	if v.mp == nil {
		return errVectorReadOnly()
	}
	vacol := MustFixedColNoTypeCheck[T](v)
	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtxf("vector idx out of range: %d > %d", idx, len(vacol))
	}
	vacol[idx] = t
	return nil
}

// Note:
// it is 10x slower than SetFixedAtNoTypeCheck
func SetFixedAtWithTypeCheck[T types.FixedSizeT](v *Vector, idx int, t T) error {
	if v.mp == nil {
		return errVectorReadOnly()
	}
	// Let it panic if v is not a varlena vec
	vacol := MustFixedColWithTypeCheck[T](v)

	if idx < 0 {
		idx = len(vacol) + idx
	}
	if idx < 0 || idx >= len(vacol) {
		return moerr.NewInternalErrorNoCtxf("vector idx out of range: %d > %d", idx, len(vacol))
	}
	vacol[idx] = t
	return nil
}

func SetBytesAt(v *Vector, idx int, bs []byte, mp *mpool.MPool) error {
	if v.bytesAlias(bs) {
		return v.withClonedBytes(bs, mp, func(source []byte) error { return SetBytesAt(v, idx, source, mp) })
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	var va types.Varlena
	err := BuildVarlenaFromByteSlice(v, &va, &bs, mp)
	if err != nil {
		return err
	}
	if err = SetFixedAtWithTypeCheck(v, idx, va); err != nil {
		return err
	}
	// SetBytesAt appends a fresh area range before replacing the descriptor;
	// it cannot introduce an alias into a previously disjoint vector.
	return nil
}

// SetBytesAtFrom replaces a varlen payload and its dynamic binary-string
// provenance as one logical row state. Capacity is admitted before the payload
// changes, so a metadata allocation failure cannot leave a partial overwrite.
func SetBytesAtFrom(v *Vector, idx int, source *Vector, sourceRow int, mp *mpool.MPool) error {
	if v.dataAliases(source) {
		return v.withClonedSource(source, sourceRow, sourceRow+1, mp, func(w *Vector) error { return SetBytesAtFrom(v, idx, w, 0, mp) })
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if err := SetBytesAt(v, idx, source.GetBytesAt(sourceRow), mp); err != nil {
		return err
	}
	return nil
}

func (v *Vector) SetRawBytesAtFrom(idx int, source *Vector, sourceRow int, mp *mpool.MPool) error {
	if v.dataAliases(source) {
		return v.withClonedSource(source, sourceRow, sourceRow+1, mp, func(w *Vector) error { return v.SetRawBytesAtFrom(idx, w, 0, mp) })
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if err := v.SetRawBytesAt(idx, source.GetRawBytesAt(sourceRow), mp); err != nil {
		return err
	}
	return nil
}

// SetRawBytesAtFromAndUnsetNull replaces a row's payload, string source
// and dynamic binary provenance, then publishes it as non-NULL. All
// fallible capacity work happens before the NULL bit changes, so callers never
// expose a partial row on error.
func (v *Vector) SetRawBytesAtFromAndUnsetNull(idx int, source *Vector, sourceRow int, mp *mpool.MPool) error {
	if v.dataAliases(source) {
		return v.withClonedSource(source, sourceRow, sourceRow+1, mp, func(w *Vector) error { return v.SetRawBytesAtFromAndUnsetNull(idx, w, 0, mp) })
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if err := v.SetRawBytesAt(idx, source.GetRawBytesAt(sourceRow), mp); err != nil {
		return err
	}
	v.UnsetNull(uint64(idx))
	return nil
}

func SetStringAt(v *Vector, idx int, bs string, mp *mpool.MPool) error {
	return SetBytesAt(v, idx, []byte(bs), mp)
}

func (v *Vector) SetRawBytesAt(i int, bs []byte, mp *mpool.MPool) error {
	if v.typ.IsVarlen() {
		return SetBytesAt(v, i, bs, mp)
	} else {
		if err := v.checkMP(mp); err != nil {
			return err
		}
		copy(v.data[i*v.typ.TypeSize():i*v.typ.TypeSize()+v.typ.TypeSize()], bs)
		return nil
	}
}

// IsConstNull return true if the vector means a scalar Null.
// e.g.
//
//	a + Null, and the vector of right part will return true
func (v *Vector) IsConstNull() bool {
	if !v.IsConst() {
		return false
	}
	if len(v.data) == 0 {
		return true
	}

	return v.nsp.Count() > 0 && v.nsp.Contains(0)
}

func (v *Vector) GetArea() []byte {
	return v.area
}

// VarlenaAreaIsDisjoint reports whether every live (non-NULL) non-inline
// varlena descriptor lies inside the area and no two of them share bytes, so
// the logical non-inline payload is bounded by len(area). It is computed from
// the descriptors: one pass for append-built (monotone) layouts, a sort of the
// ranges otherwise. Const vectors never report it, because a broadcast
// descriptor changes the materialized multiplicity.
func (v *Vector) VarlenaAreaIsDisjoint() bool {
	if v == nil || !v.typ.IsVarlen() || v.IsConst() {
		return false
	}
	return varlenaRangesDisjoint(
		toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length), v.area, &v.nsp)
}

func varlenaRangesDisjoint(values []types.Varlena, area []byte, nsp *nulls.Nulls) bool {
	hasNull := !nsp.IsEmpty()
	areaLen := uint64(len(area))
	end := uint64(0)
	var ranges [][2]uint64
	for i := range values {
		value := &values[i]
		if value.IsSmall() || hasNull && nsp.Contains(uint64(i)) {
			continue
		}
		offset, length := value.OffsetLen()
		start, stop := uint64(offset), uint64(offset)+uint64(length)
		if stop > areaLen {
			return false
		}
		if length == 0 {
			continue
		}
		if ranges == nil && start >= end {
			end = stop
			continue
		}
		if ranges == nil {
			// Not monotone: collect every live range and check them sorted.
			ranges = make([][2]uint64, 0, len(values))
			for j := 0; j < i; j++ {
				prior := &values[j]
				if prior.IsSmall() || hasNull && nsp.Contains(uint64(j)) {
					continue
				}
				o, l := prior.OffsetLen()
				if l != 0 {
					ranges = append(ranges, [2]uint64{uint64(o), uint64(o) + uint64(l)})
				}
			}
		}
		ranges = append(ranges, [2]uint64{start, stop})
	}
	if ranges == nil {
		return true
	}
	slices.SortFunc(ranges, func(a, b [2]uint64) int { return cmp.Compare(a[0], b[0]) })
	for i := 1; i < len(ranges); i++ {
		if ranges[i][0] < ranges[i-1][1] {
			return false
		}
	}
	return true
}

// VarlenaPayloadSize returns the external bytes a selected non-NULL value
// contributes when it is copied to a flat vector.
func VarlenaPayloadSize(v *Vector, row int) int64 {
	if v.IsNull(uint64(row)) {
		return 0
	}
	value := GetFixedAtNoTypeCheck[types.Varlena](v, row)
	if value.IsSmall() {
		return 0
	}
	_, size := value.OffsetLen()
	return int64(size)
}

// VarlenaPayloadExceeds reports whether any non-NULL value contributes more
// external bytes than limit when copied to a flat vector.
func VarlenaPayloadExceeds(v *Vector, limit int64) bool {
	if v == nil || !v.typ.IsVarlen() || v.Length() == 0 || v.IsConstNull() {
		return false
	}
	areaBytes := len(v.area)
	if int64(areaBytes) <= limit {
		return false
	}
	rows := v.Length()
	if v.IsConst() {
		rows = 1
	}
	for row := range rows {
		if VarlenaPayloadSize(v, row) > limit {
			return true
		}
	}
	return false
}

func (v *Vector) GetData() []byte {
	return v.data
}

func GetPtrAt[T any](v *Vector, idx int64) *T {
	if v.IsConst() {
		idx = 0
	} else {
		idx *= int64(v.GetType().TypeSize())
	}
	return (*T)(unsafe.Pointer(&v.data[idx]))
}

func (v *Vector) Free(mp *mpool.MPool) {
	if v == nil {
		return
	}
	// A read-only vector owns nothing: only drop its references.
	if v.mp != nil {
		v.mustCheckMP(mp)
		v.mp.Free(v.data)
		v.mp.Free(v.area)
	}
	v.class = FLAT
	v.data = nil
	v.area = nil
	v.length = 0

	// Free returns the bitmap's words to its pool (the vector's), or only
	// drops a read-only vector's reference.
	v.nsp.Free()
	v.sorted = false
}

func (v *Vector) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	err := v.MarshalBinaryWithBuffer(&buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (v *Vector) MarshalBinaryWithBuffer(buf *bytes.Buffer) error {
	return v.MarshalBinaryTo(buf)
}

// MarshalBinaryPlan is a validated snapshot of one Vector's wire layout. It
// lets batch writers size once and encode once.
type MarshalBinaryPlan struct {
	vector               *Vector
	size                 int
	dataLength           uint32
	areaLength           uint32
	nullLength           uint32
	canonicalVarlen      bool
	normalizedVarlenData []byte
	// zeroNullVarlen asks MarshalToBytes to clear the descriptors of null
	// rows in place of normalizedVarlenData, which trusted plans skip.
	zeroNullVarlen  bool
	canonicalOffset []uint32
	canonicalFirst  []bool
}

func (p MarshalBinaryPlan) Size() int {
	return p.size
}

func (v *Vector) PrepareMarshalBinary() (MarshalBinaryPlan, error) {
	return v.prepareMarshalBinary(false)
}

// PrepareMarshalBinaryTrusted is PrepareMarshalBinary for a vector known to
// be valid, such as one an engine built. It skips the linear check that
// decides whether the varlen area can be sent as is, and always sends it as
// is: a valid vector's descriptors all point inside its area. It allocates
// nothing; write the plan with MarshalToBytes.
func (v *Vector) PrepareMarshalBinaryTrusted() (MarshalBinaryPlan, error) {
	return v.prepareMarshalBinary(true)
}

func (v *Vector) prepareMarshalBinary(trusted bool) (MarshalBinaryPlan, error) {
	if v == nil || v.length < 0 {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx("invalid vector for marshal")
	}
	const maxWireBuffer = uint64(^uint32(0))
	if uint64(v.length) > maxWireBuffer {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector length exceeds marshal format",
		)
	}
	typeSize := v.typ.TypeSize()
	if typeSize < 0 {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector type has invalid marshal size",
		)
	}
	dataLength := uint64(typeSize)
	if !v.IsConst() {
		if v.length != 0 &&
			dataLength > ^uint64(0)/uint64(v.length) {
			return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
				"vector data exceeds marshal format",
			)
		}
		dataLength *= uint64(v.length)
	} else if v.IsConstNull() {
		dataLength = 0
	}
	areaLength := uint64(len(v.area))
	canonicalVarlen := !trusted && v.requiresCanonicalVarlenMarshal(dataLength)
	if dataLength > uint64(len(v.data)) {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector data is shorter than its marshal length",
		)
	}
	var canonicalOffset []uint32
	var canonicalFirst []bool
	if canonicalVarlen {
		if dataLength%types.VarlenaSize != 0 {
			return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
				"varlen vector data is not descriptor aligned",
			)
		}
		areaLength = 0
		descriptors := MustFixedColNoTypeCheck[types.Varlena](v)
		if uint64(len(descriptors))*types.VarlenaSize != dataLength {
			return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
				"varlen vector descriptor count does not match marshal length",
			)
		}
		canonicalOffset = make([]uint32, len(descriptors))
		canonicalFirst = make([]bool, len(descriptors))
		type areaSpan struct{ offset, length uint32 }
		seen := make(map[areaSpan]uint32, len(descriptors))
		for index := range descriptors {
			if v.IsNull(uint64(index)) || descriptors[index].IsSmall() {
				continue
			}
			offset, length := descriptors[index].OffsetLen()
			end := uint64(offset) + uint64(length)
			if end > uint64(len(v.area)) {
				return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
					"varlen vector descriptor is outside its area",
				)
			}
			span := areaSpan{offset: offset, length: length}
			if compactOffset, ok := seen[span]; ok {
				canonicalOffset[index] = compactOffset
				continue
			}
			if uint64(length) > maxWireBuffer-areaLength {
				return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
					"canonical varlen area exceeds marshal format",
				)
			}
			canonicalOffset[index] = uint32(areaLength)
			canonicalFirst[index] = true
			seen[span] = uint32(areaLength)
			areaLength += uint64(length)
		}
	}
	normalizeNullVarlen := false
	zeroNullVarlen := false
	if trusted {
		// Clearing every null row's descriptor while writing gives the same
		// bytes as normalizing only when a stale one exists, without the scan.
		zeroNullVarlen = isVarlenaMarshalType(v.typ.Oid) && v.HasNull() &&
			dataLength%types.VarlenaSize == 0
	} else if !canonicalVarlen && isVarlenaMarshalType(v.typ.Oid) &&
		v.HasNull() && dataLength%types.VarlenaSize == 0 {
		descriptors := MustFixedColNoTypeCheck[types.Varlena](v)
		if uint64(len(descriptors))*types.VarlenaSize == dataLength {
			iterator := v.nsp.GetBitmap().IteratorValue()
			for iterator.HasNext() {
				row := iterator.Next()
				if row >= uint64(len(descriptors)) {
					break
				}
				if !descriptors[row].IsSmall() {
					normalizeNullVarlen = true
					break
				}
			}
		}
	}
	nullLength := uint64(v.nsp.MarshalSize())
	if dataLength > maxWireBuffer ||
		areaLength > maxWireBuffer ||
		nullLength > maxWireBuffer {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector buffer exceeds marshal format",
		)
	}
	total := uint64(1+types.TSize+4+4+4+4+1) +
		dataLength + areaLength + nullLength
	if total > uint64(^uint(0)>>1) {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector marshal size exceeds platform limit",
		)
	}
	var normalizedVarlenData []byte
	if normalizeNullVarlen {
		descriptors := MustFixedColNoTypeCheck[types.Varlena](v)
		normalizedVarlenData = make([]byte, int(dataLength))
		copy(normalizedVarlenData, v.data[:dataLength])
		normalizedDescriptors := unsafe.Slice(
			(*types.Varlena)(unsafe.Pointer(&normalizedVarlenData[0])),
			len(descriptors),
		)
		{
			iterator := v.nsp.GetBitmap().IteratorValue()
			for iterator.HasNext() {
				row := iterator.Next()
				if row >= uint64(len(normalizedDescriptors)) {
					break
				}
				normalizedDescriptors[row] = types.Varlena{}
			}
		}
	}
	return MarshalBinaryPlan{
		vector:               v,
		size:                 int(total),
		dataLength:           uint32(dataLength),
		areaLength:           uint32(areaLength),
		nullLength:           uint32(nullLength),
		canonicalVarlen:      canonicalVarlen,
		normalizedVarlenData: normalizedVarlenData,
		canonicalOffset:      canonicalOffset,
		canonicalFirst:       canonicalFirst,
		zeroNullVarlen:       zeroNullVarlen,
	}, nil
}

func isVarlenaMarshalType(oid types.T) bool {
	return vectorTypeOps[oid].varlen
}

// requiresCanonicalVarlenMarshal keeps the original bulk wire image for an
// ordinary owned vector whose area layout has already been proven safe. A
// borrowed/aliased area or a window retaining a larger source area still takes
// the canonical path so offsets and payload reachability are validated before
// they are written. Ordinary nullable vectors stay on the bulk path; stale
// non-small NULL descriptors are normalized by PrepareMarshalBinary.
func (v *Vector) requiresCanonicalVarlenMarshal(dataLength uint64) bool {
	if !isVarlenaMarshalType(v.typ.Oid) || dataLength == 0 {
		return false
	}
	return !v.VarlenaAreaIsDisjoint()
}

func (v *Vector) MarshalBinarySize() (int, error) {
	plan, err := v.PrepareMarshalBinary()
	return plan.Size(), err
}

func (v *Vector) MarshalBinaryTo(w io.Writer) error {
	plan, err := v.PrepareMarshalBinary()
	if err != nil {
		return err
	}
	return plan.MarshalTo(w)
}

func (p MarshalBinaryPlan) MarshalTo(w io.Writer) error {
	v := p.vector
	if v == nil || w == nil {
		return io.ErrClosedPipe
	}
	if err := writeVectorMarshalByte(w, uint8(v.class)); err != nil {
		return err
	}
	if err := types.WriteFull(w, types.EncodeType(&v.typ)); err != nil {
		return err
	}

	if err := writeVectorMarshalUint32(w, uint32(v.length)); err != nil {
		return err
	}

	if err := writeVectorMarshalUint32(w, p.dataLength); err != nil {
		return err
	}
	if p.dataLength > 0 {
		if p.canonicalVarlen {
			for index, descriptor := range MustFixedColNoTypeCheck[types.Varlena](v) {
				canonical := descriptor
				if v.IsNull(uint64(index)) {
					canonical = types.Varlena{}
				} else if !descriptor.IsSmall() {
					_, length := descriptor.OffsetLen()
					canonical.SetOffsetLen(p.canonicalOffset[index], length)
				}
				bytes := unsafe.Slice((*byte)(unsafe.Pointer(&canonical)), types.VarlenaSize)
				if err := types.WriteFull(w, bytes); err != nil {
					return err
				}
			}
		} else {
			data := v.data[:p.dataLength]
			if p.normalizedVarlenData != nil {
				data = p.normalizedVarlenData
			}
			if err := types.WriteFull(w, data); err != nil {
				return err
			}
		}
	}

	if err := writeVectorMarshalUint32(w, p.areaLength); err != nil {
		return err
	}
	if p.areaLength > 0 {
		if p.canonicalVarlen {
			for index, descriptor := range MustFixedColNoTypeCheck[types.Varlena](v) {
				if v.IsNull(uint64(index)) || descriptor.IsSmall() || !p.canonicalFirst[index] {
					continue
				}
				offset, length := descriptor.OffsetLen()
				if err := types.WriteFull(w, v.area[offset:offset+length]); err != nil {
					return err
				}
			}
		} else {
			if err := types.WriteFull(w, v.area); err != nil {
				return err
			}
		}
	}

	if err := writeVectorMarshalUint32(w, p.nullLength); err != nil {
		return err
	}
	if p.nullLength > 0 {
		if err := v.nsp.MarshalTo(w); err != nil {
			return err
		}
	}

	if v.sorted {
		return writeVectorMarshalByte(w, 1)
	}
	return writeVectorMarshalByte(w, 0)
}

// MarshalToBytes writes the plan's encoding into dst, which must hold at
// least Size bytes, and returns the bytes written. It allocates nothing. The
// plan must come from PrepareMarshalBinaryTrusted: the canonical varlen
// layout is written only by MarshalTo.
func (p MarshalBinaryPlan) MarshalToBytes(dst []byte) (int, error) {
	v := p.vector
	if v == nil {
		return 0, io.ErrClosedPipe
	}
	if p.canonicalVarlen {
		return 0, moerr.NewInvalidInputNoCtx("canonical varlen plan cannot marshal to bytes")
	}
	if len(dst) < p.size {
		return 0, io.ErrShortBuffer
	}
	// The plan is a snapshot: a vector whose layout changed since must not
	// write past Size, so the destination is capped and checked first.
	if v.nsp.MarshalSize() != int(p.nullLength) ||
		uint64(len(v.data)) < uint64(p.dataLength) ||
		uint64(len(v.area)) < uint64(p.areaLength) {
		return 0, moerr.NewInvalidInputNoCtx("vector changed while marshaling")
	}
	dst = dst[:p.size:p.size]
	dst[0] = uint8(v.class)
	n := 1
	n += copy(dst[n:], types.EncodeType(&v.typ))
	binary.NativeEndian.PutUint32(dst[n:], uint32(v.length))
	binary.NativeEndian.PutUint32(dst[n+4:], p.dataLength)
	n += 8
	if p.dataLength > 0 {
		data := dst[n : n+int(p.dataLength)]
		if p.normalizedVarlenData != nil {
			copy(data, p.normalizedVarlenData)
		} else {
			copy(data, v.data[:p.dataLength])
		}
		if p.zeroNullVarlen {
			descriptors := unsafe.Slice(
				(*types.Varlena)(unsafe.Pointer(&data[0])),
				len(data)/types.VarlenaSize,
			)
			iterator := v.nsp.GetBitmap().IteratorValue()
			for iterator.HasNext() {
				row := iterator.Next()
				if row >= uint64(len(descriptors)) {
					break
				}
				descriptors[row] = types.Varlena{}
			}
		}
		n += int(p.dataLength)
	}
	binary.NativeEndian.PutUint32(dst[n:], p.areaLength)
	n += 4
	n += copy(dst[n:], v.area[:p.areaLength])
	binary.NativeEndian.PutUint32(dst[n:], p.nullLength)
	n += 4
	if p.nullLength > 0 {
		n += v.nsp.MarshalToBytes(dst[n:])
	}
	if n != p.size-1 {
		return 0, moerr.NewInvalidInputNoCtx("vector changed while marshaling")
	}
	dst[n] = 0
	if v.sorted {
		dst[n] = 1
	}
	return p.size, nil
}

type vectorPrimitiveWriter interface {
	WriteByte(byte) error
	WriteUint32(uint32) error
}

func writeVectorMarshalByte(w io.Writer, value byte) error {
	if typed, ok := w.(vectorPrimitiveWriter); ok {
		return typed.WriteByte(value)
	}
	var data [1]byte
	data[0] = value
	return types.WriteFull(w, data[:])
}

func writeVectorMarshalUint32(w io.Writer, value uint32) error {
	if typed, ok := w.(vectorPrimitiveWriter); ok {
		return typed.WriteUint32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], value)
	return types.WriteFull(w, data[:])
}

// UnmarshalBinary binds a vector to its binary encoding after fully validating
// the representation. In addition to constant-time framing, size, and overflow
// checks, it verifies null-bitmap contents and every varlena or array payload.
// Callers must use this checked API for wire, disk, RPC, or otherwise
// unvalidated bytes.
func (v *Vector) UnmarshalBinary(data []byte) error {
	return v.unmarshalBinary(data, true)
}

// UnmarshalBinaryTrusted binds a vector to an encoding that has already passed
// UnmarshalBinary and has remained immutable since that validation. It keeps
// all constant-time framing and representation checks, but skips the linear
// null-bitmap and varlen payload scans.
//
// Callers must not use this method for wire, disk, RPC, or otherwise
// unvalidated bytes. Prefer UnmarshalBinary unless the caller owns an explicit
// validation boundary.
func (v *Vector) UnmarshalBinaryTrusted(data []byte) error {
	return v.unmarshalBinary(data, false)
}

type vectorBinaryLayout struct {
	class  byte
	typ    types.Type
	length int
	data   []byte
	area   []byte
	nulls  []byte
	sorted bool
}

type vectorBinaryCursor struct {
	data   []byte
	offset int
}

func (c *vectorBinaryCursor) read(size int) ([]byte, error) {
	if size < 0 || c.offset > len(c.data)-size {
		return nil, io.ErrUnexpectedEOF
	}
	value := c.data[c.offset : c.offset+size]
	c.offset += size
	return value, nil
}

func (c *vectorBinaryCursor) readUint32() (uint32, error) {
	value, err := c.read(4)
	if err != nil {
		return 0, err
	}
	return types.DecodeUint32(value), nil
}

func decodeVectorBinaryLayout(
	data []byte,
	validateValues bool,
) (vectorBinaryLayout, error) {
	cursor := vectorBinaryCursor{data: data}
	class, err := cursor.read(1)
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	typData, err := cursor.read(types.TSize)
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	length, err := cursor.readUint32()
	if err != nil || uint64(length) > uint64(math.MaxInt) {
		if err != nil {
			return vectorBinaryLayout{}, err
		}
		return vectorBinaryLayout{}, moerr.NewInvalidInputNoCtx("vector length exceeds platform limit")
	}
	readSized := func() ([]byte, error) {
		size, err := cursor.readUint32()
		if err != nil {
			return nil, err
		}
		if uint64(size) > uint64(math.MaxInt) {
			return nil, moerr.NewInvalidInputNoCtx("vector buffer exceeds platform limit")
		}
		return cursor.read(int(size))
	}
	vectorData, err := readSized()
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	area, err := readSized()
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	nullData, err := readSized()
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	sorted, err := cursor.read(1)
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	if cursor.offset != len(cursor.data) {
		return vectorBinaryLayout{}, moerr.NewInvalidInputNoCtx("trailing vector wire data")
	}
	if sorted[0] > 1 {
		return vectorBinaryLayout{}, moerr.NewInvalidInputNoCtx("invalid vector sorted flag")
	}
	if err = validateVectorNullBitmap(nullData, validateValues); err != nil {
		return vectorBinaryLayout{}, err
	}
	var decodedNulls nulls.Nulls
	if len(nullData) > 0 {
		if err = decodedNulls.ReadNoCopy(nullData); err != nil {
			return vectorBinaryLayout{}, err
		}
	}
	typ, err := types.DecodeTypeChecked(typData)
	if err != nil {
		return vectorBinaryLayout{}, err
	}
	if err = validateVectorBinary(
		class[0],
		typ,
		length,
		vectorData,
		area,
		&decodedNulls,
		validateValues,
	); err != nil {
		return vectorBinaryLayout{}, err
	}
	return vectorBinaryLayout{
		class:  class[0],
		typ:    typ,
		length: int(length),
		data:   vectorData,
		area:   area,
		nulls:  nullData,
		sorted: sorted[0] != 0,
	}, nil
}

// unmarshalBinary copies data into v's pool, or, for a read-only vector,
// makes v reference data without copying; the caller then keeps data alive
// and unmodified for as long as v is read.
func (v *Vector) unmarshalBinary(data []byte, validateValues bool) error {
	if v == nil {
		return io.ErrClosedPipe
	}
	if v.mp != nil {
		return v.unmarshalBinaryCopy(data, validateValues, v.mp)
	}
	if v.hasBackingStorage() {
		return moerr.NewInternalErrorNoCtx("cannot replace vector storage without Free")
	}
	layout, err := decodeVectorBinaryLayout(data, validateValues)
	if err != nil {
		return err
	}
	// The data, area and null bitmap are all read-only references to data.
	// The null bitmap has no pool, so a write to it panics instead of
	// reaching memory the vector does not own (for example the FileService
	// cache).
	var decodedNulls nulls.Nulls
	if len(layout.nulls) > 0 {
		if err = decodedNulls.ReadNoCopy(layout.nulls); err != nil {
			return err
		}
	}
	*v = Vector{
		class:  int(layout.class),
		typ:    layout.typ,
		length: layout.length,
		data:   layout.data,
		area:   layout.area,
		nsp:    decodedNulls,
		sorted: layout.sorted,
	}
	return nil
}

func validateVectorBinary(
	class byte,
	typ types.Type,
	length uint32,
	data, area []byte,
	nsp *nulls.Nulls,
	validateValues bool,
) error {
	if class > CONSTANT {
		return moerr.NewInvalidInputNoCtx("invalid vector class")
	}
	typeSize, err := canonicalVectorTypeSize(typ)
	if err != nil {
		return err
	}
	if class == CONSTANT {
		if len(data) != 0 && len(data) != typeSize {
			return moerr.NewInvalidInputNoCtx("invalid constant vector data size")
		}
	} else if uint64(len(data)) != uint64(length)*uint64(typeSize) {
		return moerr.NewInvalidInputNoCtx("invalid vector data size")
	}
	if validateValues && typ.IsVarlen() {
		values := types.DecodeSlice[types.Varlena](data)
		arrayElementSize := 0
		switch typ.Oid {
		case types.T_array_float32, types.T_array_float64, types.T_array_bf16,
			types.T_array_float16, types.T_array_int8, types.T_array_uint8:
			arrayElementSize = typ.GetArrayElementSize()
		}
		for i := range values {
			// Null varlen slots may retain stale offset/length metadata. The
			// payload is never dereferenced, so only validate live values.
			if nsp.Contains(uint64(i)) {
				continue
			}
			var payloadLen uint32
			if values[i].IsSmall() {
				payloadLen = uint32(values[i][0])
			} else {
				offset, size := values[i].OffsetLen()
				if uint64(offset) > uint64(len(area)) || uint64(size) > uint64(len(area))-uint64(offset) {
					return moerr.NewInvalidInputNoCtx("invalid vector varlen offset")
				}
				payloadLen = size
			}
			if arrayElementSize > 0 && payloadLen%uint32(arrayElementSize) != 0 {
				return moerr.NewInvalidInputNoCtx("invalid vector array payload size")
			}
			if typ.Oid.IsBlockScaledArray() {
				if _, err := types.ParseBlockScaledCell(values[i].GetByteSlice(area)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// The bitmap length tracks allocated coverage and may exceed the vector's
// logical length after range operations or reuse. Validate only the bitmap's
// own representation invariants here.
func validateVectorNullBitmap(data []byte, validateValues bool) error {
	if len(data) == 0 {
		return nil
	}
	if len(data) < bitmap.MarshalHeaderSize {
		return io.ErrUnexpectedEOF
	}
	count := types.DecodeInt64(data[:8])
	bitmapLen := types.DecodeUint64(data[8:16])
	bitmapDataLen := types.DecodeUint64(data[16:24])
	if count < 0 || bitmapLen > uint64(1<<63-1) || uint64(count) > bitmapLen ||
		bitmapDataLen%8 != 0 ||
		bitmapDataLen != uint64(len(data)-bitmap.MarshalHeaderSize) {
		return moerr.NewInvalidInputNoCtx("invalid vector null bitmap")
	}
	if bitmapDataLen != ((bitmapLen+63)/64)*8 {
		return moerr.NewInvalidInputNoCtx("invalid vector null bitmap size")
	}
	if !validateValues {
		return nil
	}
	words := types.DecodeSlice[uint64](data[bitmap.MarshalHeaderSize:])
	actualCount := int64(0)
	for i, word := range words {
		if i == len(words)-1 && bitmapLen%64 != 0 && word>>uint(bitmapLen%64) != 0 {
			return moerr.NewInvalidInputNoCtx("invalid vector null bitmap bits")
		}
		actualCount += int64(bits.OnesCount64(word))
	}
	if actualCount != count {
		return moerr.NewInvalidInputNoCtx("invalid vector null bitmap count")
	}
	return nil
}

func canonicalVectorTypeSize(typ types.Type) (int, error) {
	if typ.Oid != types.T_any && !vectorTypeOps[typ.Oid].supported {
		return 0, moerr.NewInvalidInputNoCtx("unknown vector type")
	}

	canonicalSize := typ.Oid.TypeLen()
	if typ.TypeSize() != canonicalSize {
		return 0, moerr.NewInvalidInputNoCtx("invalid vector type size")
	}
	return canonicalSize, nil
}

// UnmarshalBinaryWithCopy is UnmarshalBinary with the caller's pool, which
// must be v's pool.
func (v *Vector) UnmarshalBinaryWithCopy(data []byte, mp *mpool.MPool) error {
	if v == nil {
		return io.ErrClosedPipe
	}
	return v.unmarshalBinaryCopy(data, true, mp)
}

// unmarshalBinaryCopy decodes data into memory allocated from v's pool; the
// decoded vector never aliases data. v must not hold memory.
func (v *Vector) unmarshalBinaryCopy(data []byte, validateValues bool, mp *mpool.MPool) error {
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.hasBackingStorage() {
		return moerr.NewInternalErrorNoCtx(
			"cannot replace vector storage without Free",
		)
	}
	layout, err := decodeVectorBinaryLayout(data, validateValues)
	if err != nil {
		return err
	}
	decoded := NewVec(layout.typ, v.mp)
	committed := false
	defer func() {
		if !committed {
			decoded.Free(v.mp)
		}
	}()
	decoded.class = int(layout.class)
	decoded.length = layout.length
	if len(layout.data) > 0 {
		decoded.data, err = decoded.alloc(v.mp, len(layout.data))
		if err != nil {
			return err
		}
		copy(decoded.data, layout.data)
	}
	if len(layout.area) > 0 {
		decoded.area, err = decoded.alloc(v.mp, len(layout.area))
		if err != nil {
			return err
		}
		copy(decoded.area, layout.area)
	}
	if len(layout.nulls) > 0 {
		// decoded's bitmap has v's pool, so Read copies into it.
		if err = decoded.nsp.Read(layout.nulls); err != nil {
			return err
		}
	}
	decoded.sorted = layout.sorted
	*v = *decoded
	committed = true
	return nil
}

func (v *Vector) UnmarshalWithReader(r io.Reader, mp *mpool.MPool) (retErr error) {
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v == nil || r == nil {
		return io.ErrClosedPipe
	}
	v.ResetWithSameType()
	defer func() {
		if retErr != nil && v.typ.Oid == types.T_json {
			v.CleanOnlyData()
		}
	}()
	var err error

	encodedClass, err := types.ReadByteAsInt(r)
	if err != nil {
		return err
	}
	v.class = encodedClass

	if v.typ, err = types.ReadType(r); err != nil {
		return err
	}

	if v.length, err = types.ReadInt32AsInt(r); err != nil {
		return err
	}
	if v.length < 0 {
		return moerr.NewInvalidInputNoCtx("negative vector length")
	}
	if v.length > math.MaxUint32 {
		return moerr.NewInvalidInputNoCtx("vector length exceeds marshal format")
	}

	// read data
	dataLen, dataBuf, err := v.readSizeBytes(r, mp, true)
	if err != nil {
		return err
	}
	if dataLen > 0 {
		v.data = dataBuf
	}

	// read area
	areaLen, areaBuf, err := v.readSizeBytes(r, mp, false)
	if err != nil {
		return err
	}
	if areaLen > 0 {
		v.area = areaBuf
	}

	if err = v.readNullsWithReader(r, mp); err != nil {
		return err
	}

	v.sorted, err = types.ReadBool(r)
	if err != nil {
		return err
	}
	return validateVectorBinary(
		byte(v.class),
		v.typ,
		uint32(v.length),
		v.data[:int(dataLen)],
		v.area[:int(areaLen)],
		&v.nsp,
		true,
	)
}

// readNullsWithReader reads the null bitmap straight into words allocated
// from the vector's pool: one copy, from the reader into the bitmap.
func (v *Vector) readNullsWithReader(r io.Reader, _ *mpool.MPool) error {
	size, err := types.ReadInt32(r)
	if err != nil {
		return err
	}
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative vector null bitmap size")
	}
	if err = validateStreamingReadSize(r, int64(size)); err != nil {
		return err
	}
	if size == 0 {
		v.nsp.Reset()
		return nil
	}
	if size < bitmap.MarshalHeaderSize {
		return moerr.NewInvalidInputNoCtx("invalid bitmap wire size")
	}
	var header [bitmap.MarshalHeaderSize]byte
	if _, err = io.ReadFull(r, header[:]); err != nil {
		return err
	}
	_, bitLength, _, err := bitmap.DecodeMarshalHeader(header[:])
	if err != nil {
		return moerr.NewInvalidInputNoCtx("invalid vector null bitmap")
	}
	if bitLength > int64(math.MaxInt) {
		return moerr.NewInvalidInputNoCtx("vector null bitmap exceeds platform limit")
	}
	payload, err := v.nsp.GetBitmap().PrepareUnmarshal(header[:], int(size))
	if err != nil {
		return err
	}
	if _, err = io.ReadFull(r, payload); err != nil {
		v.nsp.Reset()
		return err
	}
	if err = v.nsp.GetBitmap().Validate(); err != nil {
		v.nsp.Reset()
		return err
	}
	return nil
}

func (v *Vector) ToConst() {
	v.class = CONSTANT
}

// PreExtend use to expand the capacity of the vector.
// PreExtend does not change the length of the vector.
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.class == CONSTANT {
		return nil
	}
	return extend(v, rows, mp)
}

// PreExtendNulls ensures allocation-accounted null storage can represent rows.
// Unaccounted vectors are unchanged.
func (v *Vector) PreExtendNulls(rows int, mp *mpool.MPool) error {
	if err := v.checkMP(mp); err != nil {
		return err
	}
	return v.ensureNullCapacity(rows)
}

// PreExtendArea use to expand the mpool and area of vector
// extraAreaSize: the size of area to be extended
// mp: mpool
func (v *Vector) PreExtendWithArea(rows int, extraAreaSize int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}

	// pre-extend vector, the fixed len part
	if err := v.PreExtend(rows, mp); err != nil {
		return err
	}

	// check if required size is already satisfied
	area1 := v.GetArea()
	voff := len(area1)
	if voff+extraAreaSize <= cap(area1) {
		return nil
	}

	// grow area
	var err error
	oldSz := len(area1)
	area1, err = v.growArea(mp, voff+extraAreaSize)
	if err != nil {
		return err
	}
	area1 = area1[:oldSz] // This is important.

	// set area
	v.area = area1

	return nil
}

// SelectedBatchPreflight is what PreExtendSelectedBatch admitted for one
// destination vector: the row count after publication and the exact
// non-inline payload bytes. The caller keeps it and hands it to the matching
// UnionBatchPreflighted call, which consumes it. The zero value admits
// nothing.
type SelectedBatchPreflight struct {
	areaBytes int
	rowCount  int
	ready     bool
}

// Ready reports whether p holds an unconsumed preflight.
func (p *SelectedBatchPreflight) Ready() bool {
	return p != nil && p.ready
}

// take consumes p.
func (p *SelectedBatchPreflight) take() (areaBytes, rowCount int, ok bool) {
	if !p.Ready() {
		return 0, 0, false
	}
	areaBytes, rowCount = p.areaBytes, p.rowCount
	*p = SelectedBatchPreflight{}
	return areaBytes, rowCount, true
}

// PreExtendSelectedBatch reserves every physical allocation needed by the
// matching UnionBatchPreflighted call. It performs the selected varlena payload
// scan once and returns its result, so publication can skip its otherwise
// duplicate pregrow scan. On error the result is the zero value.
func (v *Vector) PreExtendSelectedBatch(
	w *Vector,
	offset int,
	cnt int,
	flags []uint8,
	targetRows int,
	mp *mpool.MPool,
) (SelectedBatchPreflight, error) {
	return v.preExtendSelectedBatch(
		w, offset, cnt, flags, targetRows, false, mp,
	)
}

// PreExtendSelectedBatchValidated is the Group-internal variant for a
// selection whose 0/1 shape and selected-row count were already proved by the
// hash insertion plan. It retains the generic preflight's bounds and capacity
// checks while avoiding repeated validation for every group-key column.
func (v *Vector) PreExtendSelectedBatchValidated(
	w *Vector,
	offset int,
	cnt int,
	flags []uint8,
	targetRows int,
	mp *mpool.MPool,
) (SelectedBatchPreflight, error) {
	return v.preExtendSelectedBatch(
		w, offset, cnt, flags, targetRows, true, mp,
	)
}

func (v *Vector) preExtendSelectedBatch(
	w *Vector,
	offset int,
	cnt int,
	flags []uint8,
	targetRows int,
	selectionValidated bool,
	mp *mpool.MPool,
) (SelectedBatchPreflight, error) {
	if v == nil || w == nil || offset < 0 || cnt < 0 || targetRows < 0 ||
		len(flags) != cnt || !w.CoversLogicalRows(offset, cnt) {
		return SelectedBatchPreflight{}, mpool.ErrAllocationAccountInvalid
	}
	// The preflight proof cannot retain an aliased source for publication.
	// Reject it here instead of letting UnionBatchPreflighted allocate a clone
	// after the caller has committed its surrounding state transition.
	if v.dataAliases(w) || v.typ.IsVarlen() && byteSlicesOverlap(v.area, w.area) {
		return SelectedBatchPreflight{}, mpool.ErrAllocationAccountInvalid
	}

	selectedRows := 0
	if selectionValidated {
		if targetRows < v.length {
			return SelectedBatchPreflight{}, mpool.ErrAllocationAccountInvalid
		}
		selectedRows = targetRows - v.length
	}
	selectedAreaBytes := 0
	var values []types.Varlena
	varlen := w.GetType().IsVarlen() && !w.IsConstNull()
	if varlen {
		ToSliceNoTypeCheck(w, &values)
	}
	hasNull := varlen && !w.IsConst() && !w.GetNulls().IsEmpty()
	if !selectionValidated || varlen && !w.IsConst() {
		for row, flag := range flags {
			if !selectionValidated && flag > 1 {
				return SelectedBatchPreflight{}, mpool.ErrAllocationAccountInvalid
			}
			if flag == 0 {
				continue
			}
			if !selectionValidated {
				selectedRows++
			}
			if !varlen || w.IsConst() ||
				hasNull && w.GetNulls().Contains(uint64(offset+row)) ||
				values[offset+row].IsSmall() {
				continue
			}
			_, length := values[offset+row].OffsetLen()
			if int(length) > math.MaxInt-selectedAreaBytes {
				return SelectedBatchPreflight{}, mpool.ErrAllocationAllocatorLimit
			}
			selectedAreaBytes += int(length)
		}
	}
	if targetRows < v.length ||
		!selectionValidated && selectedRows != targetRows-v.length {
		return SelectedBatchPreflight{}, mpool.ErrAllocationAccountInvalid
	}
	if varlen && w.IsConst() && selectedRows != 0 && !values[0].IsSmall() {
		_, length := values[0].OffsetLen()
		selectedAreaBytes = int(length)
	}

	// Even an append that fits existing capacity must finish COW before its
	// publication proof is admitted; publication cannot allocate or shrink it.
	if selectedRows > 0 {
		if err := v.checkMP(mp); err != nil {
			return SelectedBatchPreflight{}, err
		}
	}
	if w.HasNull() {
		if err := v.PreExtendNulls(targetRows, mp); err != nil {
			return SelectedBatchPreflight{}, err
		}
	}
	// Publication extends fixed descriptors before it copies payload. Admit the
	// descriptor backing here as part of the same physical work unit.
	if targetRows > v.length && targetRows > v.Capacity() {
		if err := v.PreExtend(targetRows-v.length, mp); err != nil {
			return SelectedBatchPreflight{}, err
		}
	}
	if selectedAreaBytes > cap(v.area)-len(v.area) {
		if err := v.PreExtendWithArea(0, selectedAreaBytes, mp); err != nil {
			return SelectedBatchPreflight{}, err
		}
	}
	return SelectedBatchPreflight{
		areaBytes: selectedAreaBytes,
		rowCount:  targetRows,
		ready:     true,
	}, nil
}

// UnionBatchPreflighted publishes a selection after PreExtendSelectedBatch.
// It consumes preflight, which must be that call's result for v.
func (v *Vector) UnionBatchPreflighted(
	w *Vector,
	offset int64,
	cnt int,
	flags []uint8,
	preflight *SelectedBatchPreflight,
	mp *mpool.MPool,
) error {
	selectedAreaBytes, preflightRowCount, ok := preflight.take()
	if v == nil || !ok {
		return mpool.ErrAllocationAccountInvariant
	}
	return v.unionBatchPreflighted(
		w, offset, cnt, flags, mp, selectedAreaBytes, preflightRowCount)
}

// Dup copies v into a new vector allocated from mp, which becomes the copy's
// pool.
func (v *Vector) Dup(mp *mpool.MPool) (*Vector, error) {
	return v.dup(mp)
}

func (v *Vector) dup(mp *mpool.MPool) (*Vector, error) {
	w := NewVec(v.typ, mp)
	w.class = v.class
	w.typ = v.typ
	w.sorted = v.sorted

	if v.IsConstNull() {
		w.length = v.length
		return w, nil
	}

	var err error
	dataLen := v.typ.TypeSize()
	if v.IsConst() {
		if err := extend(w, 1, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
	} else {
		if err := extend(w, v.length, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
		dataLen *= v.length
	}
	// A bitmap may be shorter than a sparse vector or longer than a reused vector
	// that was shortened with SetLength. Preserve both the complete row domain
	// and the source bitmap extent. Len does not materialize borrowed Arrow
	// validity, which lets the destination admit its owned bitmap first.
	nullLength := v.GetNulls().Len()
	if nullLength > 0 {
		if err := w.ensureNullCapacity(max(v.length, int(nullLength))); err != nil {
			w.Free(mp)
			return nil, err
		}
	}
	w.length = v.length
	if err = w.nsp.InitWith(&v.nsp, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	copy(w.data, v.data[:dataLen])

	if len(v.area) > 0 {
		if w.area, err = w.alloc(mp, len(v.area)); err != nil {
			w.Free(mp)
			return nil, err
		}
		copy(w.area, v.area)
	}
	return w, nil
}

// CloneToFlatCompact returns a deep, flat copy of v. Unlike Dup, the clone only
// retains varlen payload referenced by the vector's logical rows, so stale or
// unreferenced bytes in area are not propagated into batch memory accounting.
// An accounted MPool view makes the clone off-heap and charges it to the
// view's account.
func (v *Vector) CloneToFlatCompact(mp *mpool.MPool) (*Vector, error) {
	w := NewVec(v.typ, mp)
	if v.class != FLAT || (!v.typ.IsFixedLen() && !v.typ.IsVarlen()) {
		if err := GetUnionAllFunction(v.typ, mp)(w, v); err != nil {
			w.Free(mp)
			return nil, err
		}
		return w, nil
	}
	if v.length == 0 {
		return w, nil
	}
	if err := extendWithBitmaps(
		w,
		v.length,
		mp,
		!v.nsp.IsEmpty(),
	); err != nil {
		w.Free(mp)
		return nil, err
	}
	w.length = v.length
	copyBitmapWithinLength(&w.nsp, &v.nsp, v.length)

	if v.typ.IsFixedLen() {
		dataLen := v.length * v.typ.TypeSize()
		copy(w.data[:dataLen], v.data[:dataLen])
		return w, nil
	}

	var src, dst []types.Varlena
	ToSliceNoTypeCheck(v, &src)
	ToSliceNoTypeCheck(w, &dst)
	totalArea := 0
	for i := range src {
		if v.nsp.Contains(uint64(i)) || src[i].IsSmall() {
			continue
		}
		_, n := src[i].OffsetLen()
		totalArea += int(n)
	}
	if totalArea > 0 {
		var err error
		w.area, err = w.alloc(mp, totalArea)
		if err != nil {
			w.Free(mp)
			return nil, err
		}
	}

	offset := 0
	for i := range src {
		if v.nsp.Contains(uint64(i)) {
			dst[i] = types.Varlena{}
			continue
		}
		if src[i].IsSmall() {
			dst[i] = src[i]
			continue
		}
		value := src[i].GetByteSlice(v.area)
		copy(w.area[offset:], value)
		dst[i].SetOffsetLen(uint32(offset), uint32(len(value)))
		offset += len(value)
	}
	return w, nil
}

func copyBitmapWithinLength(dst, src *nulls.Nulls, length int) {
	if src.IsEmpty() {
		return
	}
	limit := uint64(length)
	src.Foreach(func(row uint64) bool {
		if row >= limit {
			return false
		}
		dst.Add(row)
		return true
	})
}

// Shrink compacts a vector. For nonconstant vectors, sels must be ordered.
func (v *Vector) Shrink(sels []int64, negate bool, mp *mpool.MPool) error {

	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
		}
		if negate {
			v.length -= len(sels)
		} else {
			v.length = len(sels)
		}
		return nil
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		newLength := len(sels)
		if negate {
			newLength = v.length - len(sels)
		}
		if !negate || len(sels) > 0 {
			nulls.FilterInPlaceOrdered(&v.nsp, sels, negate)
		}
		v.length = newLength
		trimVectorBitmap(&v.nsp, newLength)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	ops, ok := opsOf(v)
	if !ok {
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
	// A varlena vector shrinks its descriptors, not its area. For our
	// vector, this may well be the right thing. To shrink the area as well,
	// each varlena value would have to be copied and its pointer swizzled.
	ops.shrink(v, sels, negate)

	return nil
}

func (v *Vector) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64, mp *mpool.MPool) error {
	if negate && sels.Count() == 0 {
		return nil
	}
	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
		}
		if negate {
			v.length -= sels.Count()
		} else {
			v.length = sels.Count()
		}
		return nil
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		newLength := sels.Count()
		if negate {
			newLength = v.length - newLength
		}
		nulls.FilterByMaskInPlaceWithOffset(&v.nsp, sels, negate, offset)
		v.length = newLength
		trimVectorBitmap(&v.nsp, newLength)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	ops, ok := opsOf(v)
	if !ok {
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
	// A varlena vector shrinks its descriptors, not its area. For our
	// vector, this may well be the right thing. To shrink the area as well,
	// each varlena value would have to be copied and its pointer swizzled.
	ops.shrinkByMask(v, sels, negate, offset)
	return nil
}

// Shuffle use to shrink vectors, sels can be disordered
func (v *Vector) Shuffle(sels []int64, mp *mpool.MPool) (err error) {
	if v.IsConst() {
		if v.typ.Oid == types.T_any {
			if err := v.checkMP(mp); err != nil {
				return err
			}
			v.length = len(sels)
		}
		return nil
	}
	// ANY has no value payload; its row metadata still follows the selection.
	if v.typ.Oid == types.T_any && (v.length == 0 || v.AllNull()) {
		if err := v.checkMP(mp); err != nil {
			return err
		}
		if err := v.remapShuffleBitmaps(sels, mp); err != nil {
			return err
		}
		v.length = len(sels)
		return nil
	}
	if v.typ.Oid == types.T_any {
		return moerr.NewNotSupportedNoCtx("non-NULL ANY selection")
	}
	ops, ok := opsOf(v)
	if !ok {
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}
	err = ops.shuffle(v, sels, mp)

	if err != nil {
		return err
	}
	return err
}

// ShuffleWithBuf is like Shuffle but reuses a scratch buffer to avoid
// alloc/free churn when the permutation preserves the element count.
// buf is grown as needed and retained across calls.
func (v *Vector) ShuffleWithBuf(sels []int64, mp *mpool.MPool, buf *[]byte) (err error) {
	if v.IsConst() || v.typ.Oid == types.T_any {
		return v.Shuffle(sels, mp)
	}
	// The reusable buffer is Go-heap storage and therefore has no physical
	// allocation provenance. Allocation-accounted vectors must use Shuffle,
	// whose replacement data and bitmap scratch are admitted to their owner.
	if mp.IsView() {
		return v.Shuffle(sels, mp)
	}
	// Fall back to allocating Shuffle if the selection changes the element
	// count.
	if err := v.checkMP(mp); err != nil {
		return err
	}
	if len(sels) != v.length {
		return v.Shuffle(sels, mp)
	}
	return v.shuffleWithBufData(sels, buf)
}

// trimVectorBitmap drops stale rows left by a previous shortened view.
func trimVectorBitmap(bitmap *nulls.Nulls, length int) {
	if uint64(length) < uint64(bitmap.Len()) {
		nulls.RemoveRange(bitmap, uint64(length), uint64(bitmap.Len()))
	}
}

// shuffleWithBufData performs the common payload/bitmap permutation after all
// fallible metadata preparation, then updates ordering and area properties.
func (v *Vector) shuffleWithBufData(sels []int64, buf *[]byte) (err error) {
	ops, ok := opsOf(v)
	if !ok {
		panic(fmt.Sprintf("unexpect type %s for function vector.ShuffleWithBuf", v.typ))
	}
	err = ops.shuffleWithBuf(v, sels, buf)

	return err
}

// Copy simply does v[vi] = w[wi]
func (v *Vector) Copy(w *Vector, vi, wi int64, mp *mpool.MPool) error {
	// Preserve an aliased source row before COW or growth can release it.
	if v.dataAliases(w) && v.typ.IsVarlen() {
		return v.withClonedSource(w, int(wi), int(wi)+1, mp, func(source *Vector) error {
			return v.Copy(source, vi, 0, mp)
		})
	}
	// Area growth must not replace data after we capture a destination header.
	if err := v.checkMP(mp); err != nil {
		return err
	}
	sourceNull := w.IsConstNull() ||
		(!w.IsConst() && w.GetNulls().Contains(uint64(wi)))
	if sourceNull && mp.IsView() {
		if err := v.ensureNullCapacity(int(vi) + 1); err != nil {
			return err
		}
	}
	if w.class == CONSTANT {
		if w.IsConstNull() {
			if !v.typ.IsFixedLen() {
				vva := MustFixedColNoTypeCheck[types.Varlena](v)
				// Null varlen slots may retain stale offset/len metadata, so clear
				// the destination header before marking the row null.
				vva[vi] = types.Varlena{}
			}
			v.nsp.Set(uint64(vi))
			return nil
		}
		// Non-null constant vectors still share the regular null/data path below.
		wi = 0
	}
	if sourceNull {
		if !v.typ.IsFixedLen() {
			vva := MustFixedColNoTypeCheck[types.Varlena](v)
			vva[vi] = types.Varlena{}
		}
		v.GetNulls().Set(uint64(vi))
		return nil
	}
	if v.typ.IsFixedLen() {
		sz := v.typ.TypeSize()
		copy(v.data[vi*int64(sz):(vi+1)*int64(sz)], w.data[wi*int64(sz):(wi+1)*int64(sz)])
	} else {
		vva := MustFixedColNoTypeCheck[types.Varlena](v)
		wva := MustFixedColNoTypeCheck[types.Varlena](w)
		if wva[wi].IsSmall() {
			vva[vi] = wva[wi]
		} else {
			bs := wva[wi].GetByteSlice(w.area)
			if err := BuildVarlenaFromByteSlice(v, &vva[vi], &bs, mp); err != nil {
				return err
			}
		}
	}

	v.GetNulls().Unset(uint64(vi))
	return nil
}

// GetUnionAllFunction: A more sensible function for copying vector,
// which avoids having to do type conversions and type judgements every time you append.
func GetUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	union := getUnionAllFunction(typ, mp)
	return func(v, w *Vector) error {
		if typ.Oid == types.T_any {
			if v.length < 0 || w.length < 0 || w.length > math.MaxInt-v.length {
				return moerr.NewInternalErrorNoCtx("invalid ANY vector length")
			}
			if err := validBitmapRows(v.length + w.length); err != nil {
				return err
			}
			if (v.length != 0 && !v.AllNull()) || (w.length != 0 && !w.AllNull()) {
				return moerr.NewInternalErrorNoCtx("cannot copy a non-NULL value of type ANY")
			}
		}

		// A Window retains the area lease but may alias our owned descriptors.
		if v.dataAliases(w) {
			return v.withClonedSource(w, 0, w.length, mp, func(source *Vector) error {
				return GetUnionAllFunction(typ, mp)(v, source)
			})
		}
		if err := v.checkMP(mp); err != nil {
			return err
		}
		if w.IsConst() && typ.Oid != types.T_any {
			// The raw const append helpers classify their input as ordinary data.
			// UnionMulti copies a source value instead and therefore preserves its
			// provenance without a second, post-publication sidecar transition.
			return v.UnionMulti(w, 0, w.length, mp)
		}
		return union(v, w)
	}
}

func getUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	if typ.Oid == types.T_any {
		return func(v, w *Vector) error {
			if w.length == 0 {
				return nil
			}
			oldLength := v.length
			if err := extendWithBitmaps(v, w.length, mp, true); err != nil {
				return err
			}
			// SetLength can leave metadata past the visible prefix on a reused
			// destination. New rows must not inherit those stale NULL bits.
			if end := v.nsp.Len(); end > int64(oldLength) {
				nulls.RemoveRange(&v.nsp, uint64(oldLength), uint64(end))
			}
			v.nsp.AddRange(uint64(oldLength), uint64(oldLength+w.length))
			v.setLengthAfterExtend(oldLength + w.length)
			return nil
		}
	}
	ops := &vectorTypeOps[typ.Oid]
	if !ops.supported {
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionFunction", typ))
	}
	return ops.unionAll(mp)
}

// unionNsp is a simple and quick union of nulls, but not a good one.
func unionNsp(dst *nulls.Nulls, more *nulls.Nulls, oldLength int, moreLength int) {
	u64offset := uint64(oldLength)
	u64Length := uint64(moreLength)

	moreNp := more.GetBitmap()
	if moreNp == nil || moreNp.IsEmpty() || moreLength == 0 {
		return
	}

	for i := u64Length - 1; i != 0; i-- {
		if moreNp.Contains(i) {
			dst.Set(i + u64offset)
		}
	}
	if moreNp.Contains(0) {
		dst.Set(u64offset)
	}
}

// unionAllFixed is getUnionAllFunction for a type stored as T.
func unionAllFixed[T types.FixedSizeT](mp *mpool.MPool) func(v, w *Vector) error {
	var zero T
	return func(v, w *Vector) error {
		if w.IsConstNull() {
			if err := appendMultiFixed(v, zero, true, w.length, mp); err != nil {
				return err
			}
			return nil
		}
		if w.IsConst() {
			ws := MustFixedColNoTypeCheck[T](w)
			if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
				return err
			}
			return nil
		}
		if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any()); err != nil {
			return err
		}
		if w.nsp.Any() {
			unionNsp(&v.nsp, &w.nsp, v.length, w.length)
		}
		sz := v.typ.TypeSize()
		copy(v.data[v.length*sz:], w.data[:w.length*sz])
		v.setLengthAfterExtend(v.length + w.length)
		return nil
	}
}

// unionAllVarlen is getUnionAllFunction for a varlena type.
func unionAllVarlen(mp *mpool.MPool) func(v, w *Vector) error {
	return func(v, w *Vector) error {
		if w.IsConstNull() {
			if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
				return err
			}
			return nil
		}
		ws := MustFixedColNoTypeCheck[types.Varlena](w)
		if w.IsConst() {
			if err := appendMultiBytes(v, ws[0].GetByteSlice(w.area), false, w.length, mp); err != nil {
				return err
			}
			return nil
		}
		if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any()); err != nil {
			return err
		}
		if sz := len(v.area) + len(w.area); sz > cap(v.area) {
			area, err := v.growArea(mp, sz)
			if err != nil {
				return err
			}
			v.area = area[:len(v.area)]
		}

		var err error
		vs := toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+w.length)

		bm := w.nsp.GetBitmap()
		if bm != nil && !bm.IsEmpty() {
			for i := range ws {
				if bm.Contains(uint64(i)) {
					vs[v.length] = types.Varlena{}
					v.nsp.Add(uint64(v.length))
				} else {
					err = BuildVarlenaFromVarlena(v, &vs[v.length], &ws[i], &w.area, mp)
					if err != nil {
						return err
					}
				}
				v.setLengthAfterExtend(v.length + 1)
			}
		} else {
			for i := range ws {
				err = BuildVarlenaFromVarlena(v, &vs[v.length], &ws[i], &w.area, mp)
				if err != nil {
					return err
				}
				v.setLengthAfterExtend(v.length + 1)
			}
		}
		return nil
	}
}

// GetConstSetFunction: A more sensible function for const vector set,
// which avoids having to do type conversions and type judgements every time you append.
func getConstSetFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64, length int) error {
	switch typ.Oid {
	case types.T_any:
		return func(v, w *Vector, sel int64, length int) error {
			if !w.IsNull(uint64(sel)) {
				return moerr.NewInternalErrorNoCtx("cannot copy a non-NULL value of type ANY")
			}
			return SetConstNull(v, length, mp)
		}
	case types.T_bool:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[bool](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_bit:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int8:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int8](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_int64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[int64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint8:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint8](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float8:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Float8](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float4:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Float4](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_bf16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.BF16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float16:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Float16](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uint64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[uint64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[float32](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_float64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[float64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_date:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Date](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_year:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.MoYear](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_datetime:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Datetime](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_time:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Time](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_timestamp:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Timestamp](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_enum:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Enum](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_decimal64:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Decimal64](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_decimal128:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Decimal128](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_decimal256:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Decimal256](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_uuid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Uuid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_TS:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.TS](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_Rowid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Rowid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text, types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_array_float8, types.T_array_float4, types.T_datalink, types.T_geometry, types.T_geometry32:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Varlena](w)
			v.area = v.area[:0]
			if w.IsConst() {
				return SetConstBytes(v, ws[0].GetByteSlice(w.area), length, mp)
			}
			return SetConstBytes(v, ws[sel].GetByteSlice(w.area), length, mp)
		}
	case types.T_Blockid:
		return func(v, w *Vector, sel int64, length int) error {
			if w.IsConstNull() || w.nsp.Contains(uint64(sel)) {
				return SetConstNull(v, length, mp)
			}
			ws := MustFixedColNoTypeCheck[types.Blockid](w)
			if w.IsConst() {
				return SetConstFixed(v, ws[0], length, mp)
			}
			return SetConstFixed(v, ws[sel], length, mp)
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetConstSetFunction", typ))
	}
}

func GetConstSetFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64, length int) error {
	set := getConstSetFunction(typ, mp)
	return func(v, w *Vector, sel int64, length int) error {
		if v == nil || w == nil || sel < 0 || sel >= int64(w.Length()) || length < 0 {
			return moerr.NewInvalidInputNoCtx("invalid const vector selection")
		}
		if v.dataAliases(w) {
			return v.withClonedSource(w, int(sel), int(sel)+1, mp, func(source *Vector) error { return GetConstSetFunction(typ, mp)(v, source, 0, length) })
		}
		if err := v.checkMP(mp); err != nil {
			return err
		}
		return set(v, w, sel, length)
	}
}

// fillSlice broadcasts val across s[start:end] using exponential copy doubling:
// write one element, then double the filled region with copy() — O(log n) memmoves
// instead of n scalar element stores. Used on the hot const-broadcast path.
func fillSlice[T any](s []T, start, end int, val T) {
	if start >= end {
		return
	}
	s[start] = val
	for n := 1; start+n < end; n *= 2 {
		copy(s[start+n:end], s[start:start+n])
	}
}

// broadcastFixed fills dst (whose length is a multiple of unit and whose leading
// `unit` bytes already hold the value) by repeating that unit across the rest via
// copy doubling — one growing memmove region instead of a per-slot copy loop.
func broadcastFixed(dst []byte, unit int) {
	for n := unit; n < len(dst); {
		n += copy(dst[n:], dst[:n])
	}
}

// pregrowVarlenaArea grows vec.area's capacity once (a single mpool realloc) to fit
// an additional totalBytes of non-inline varlena content, so the subsequent per-row
// BuildVarlenaNoInline appends never re-grow — eliminating incremental realloc churn.
// Length is preserved; only capacity grows. No-op without an mpool or when capacity
// already suffices. totalBytes may be an over-estimate (e.g. counting null rows that
// are later skipped) — over-reserving is harmless.
func pregrowVarlenaArea(vec *Vector, totalBytes int, mp *mpool.MPool) error {
	if mp == nil || totalBytes <= 0 {
		return nil
	}
	need := len(vec.area) + totalBytes
	if need <= cap(vec.area) {
		return nil
	}
	origLen := len(vec.area)
	grown, err := vec.growArea(mp, need)
	if err != nil {
		return err
	}
	vec.area = grown[:origLen]
	return nil
}

func (v *Vector) UnionNull(mp *mpool.MPool) error {
	if err := v.checkMP(mp); err != nil {
		return err
	}
	return appendOneFixed(v, 0, true, mp)
}

// FixedSourceRow identifies an immutable source for a single append call.
type FixedSourceRow struct {
	Vec *Vector
	Row int64
}

// TryUnionFlatFixedRows copies fixed-size payloads across source vectors without
// changing their order, including inline CHAR/VARCHAR descriptors. A
// false result leaves v untouched, allowing the caller to use UnionOne.
// A true result transfers no source ownership; v owns every copied payload.
func (v *Vector) TryUnionFlatFixedRows(rows []FixedSourceRow, mp *mpool.MPool) (bool, error) {
	inlineStrings := v.typ.Oid == types.T_char || v.typ.Oid == types.T_varchar
	if len(rows) < 2 || mp == nil || v.class != FLAT || v.typ.IsVarlen() && !inlineStrings {
		return false, nil
	}
	size := v.typ.TypeSize()
	if size <= 0 || rows[0].Vec == nil {
		return false, nil
	}
	hasNulls, hasValues := false, false
	for _, row := range rows {
		w := row.Vec
		if w == nil || w == v || w.class != FLAT || w.typ != v.typ ||
			row.Row < 0 || row.Row >= int64(w.length) {
			return false, nil
		}
		isNull := !w.nsp.IsEmpty() && w.nsp.Contains(uint64(row.Row))
		if inlineStrings && !isNull && w.data[int(row.Row)*size] > types.VarlenaInlineSize {
			return false, nil
		}
		hasNulls = hasNulls || isNull
		hasValues = hasValues || !isNull
	}
	oldLen := v.length
	if err := extendWithBitmaps(v, len(rows), mp, hasNulls && mp.IsView()); err != nil {
		return true, err
	}
	// Keep the nullable path separate: ordinary fixed columns need neither
	// row-by-row NULL checks nor scalar append/admission work in the gather loop.
	if hasNulls {
		for i, row := range rows {
			if !row.Vec.nsp.IsEmpty() && row.Vec.nsp.Contains(uint64(row.Row)) {
				v.nsp.Add(uint64(oldLen + i))
				if inlineStrings {
					clear(v.data[(oldLen+i)*size : (oldLen+i+1)*size])
				}
				continue
			}
			dst, src := (oldLen+i)*size, int(row.Row)*size
			copy(v.data[dst:dst+size], row.Vec.data[src:src+size])
		}
	} else {
		switch size {
		case 8:
			for i, row := range rows {
				*(*uint64)(unsafe.Pointer(&v.data[(oldLen+i)*8])) =
					*(*uint64)(unsafe.Pointer(&row.Vec.data[row.Row*8]))
			}
		case 4:
			for i, row := range rows {
				*(*uint32)(unsafe.Pointer(&v.data[(oldLen+i)*4])) =
					*(*uint32)(unsafe.Pointer(&row.Vec.data[row.Row*4]))
			}
		default:
			for i, row := range rows {
				dst, src := (oldLen+i)*size, int(row.Row)*size
				copy(v.data[dst:dst+size], row.Vec.data[src:src+size])
			}
		}
	}
	v.length = oldLen + len(rows)
	return true, nil
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionOne(w *Vector, sel int64, mp *mpool.MPool) error {
	if v.dataAliases(w) {
		return v.withClonedSource(w, int(sel), int(sel)+1, mp, func(source *Vector) error {
			return v.UnionOne(source, 0, mp)
		})
	}
	// Reused owned capacity can hold a stale out-of-line descriptor under a
	// new NULL row; clear it so the row never points into the area.
	clearNullDescriptor := v.typ.IsVarlen() && !v.IsConst()
	// Scalar NULL inspection is only needed for constant source vectors.
	sourceNull := (w.IsConst() && w.IsConstNull()) ||
		(!w.IsConst() && !w.nsp.IsEmpty() && w.nsp.Contains(uint64(sel)))
	if err := extendWithBitmaps(
		v,
		1,
		mp,
		sourceNull && mp.IsView(),
	); err != nil {
		return err
	}
	varlen := v.typ.IsVarlen()

	oldLen := v.length
	v.length = oldLen + 1
	if sourceNull {
		if clearNullDescriptor {
			var values []types.Varlena
			ToSliceNoTypeCheck(v, &values)
			values[oldLen] = types.Varlena{}
		}
		v.nsp.Add(uint64(oldLen))
		return nil
	}
	if w.IsConst() {
		sel = 0
	}
	if varlen {
		var vs, ws []types.Varlena
		ToSliceNoTypeCheck(v, &vs)
		ToSliceNoTypeCheck(w, &ws)
		err := BuildVarlenaFromVarlena(v, &vs[oldLen], &ws[sel], &w.area, mp)
		if err != nil {
			return err
		}
		// Each append copies its own payload, including repeated selections
		// and constant sources. Preserve an existing proof, never create one
		// for a borrowed area or an already aliased destination.
	} else {
		tlen := v.GetType().TypeSize()
		switch tlen {
		case 8:
			p1 := unsafe.Pointer(&v.data[oldLen*8])
			p2 := unsafe.Pointer(&w.data[sel*8])
			*(*int64)(p1) = *(*int64)(p2)
		case 4:
			p1 := unsafe.Pointer(&v.data[oldLen*4])
			p2 := unsafe.Pointer(&w.data[sel*4])
			*(*int32)(p1) = *(*int32)(p2)
		case 2:
			p1 := unsafe.Pointer(&v.data[oldLen*2])
			p2 := unsafe.Pointer(&w.data[sel*2])
			*(*int16)(p1) = *(*int16)(p2)
		case 1:
			v.data[oldLen] = w.data[sel]
		default:
			copy(v.data[oldLen*tlen:(oldLen+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
		}
	}
	return nil
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionMulti(w *Vector, sel int64, cnt int, mp *mpool.MPool) error {
	return v.unionMultiFlat(w, sel, cnt, mp)
}

func (v *Vector) unionMultiFlat(w *Vector, sel int64, cnt int, mp *mpool.MPool) error {
	if cnt == 0 {
		return nil
	}
	if v.dataAliases(w) {
		return v.withClonedSource(w, int(sel), int(sel)+1, mp, func(source *Vector) error {
			return v.UnionMulti(source, 0, cnt, mp)
		})
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}

	sourceNull := w.IsConstNull() ||
		(!w.IsConst() && w.nsp.Contains(uint64(sel)))
	if err := extendWithBitmaps(
		v,
		cnt,
		mp,
		sourceNull && mp.IsView(),
	); err != nil {
		return err
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + cnt)
	if w.IsConst() {
		if sourceNull {
			v.nsp.AddRange(uint64(oldLen), uint64(oldLen+cnt))
			return nil
		}
		sel = 0
	} else if sourceNull {
		v.nsp.AddRange(uint64(oldLen), uint64(oldLen+cnt))
		return nil
	}
	if v.GetType().IsVarlen() {
		var err error
		var va types.Varlena
		var ws []types.Varlena
		ToSliceNoTypeCheck(w, &ws)
		err = BuildVarlenaFromVarlena(v, &va, &ws[sel], &w.area, mp)
		if err != nil {
			return err
		}
		var col []types.Varlena
		ToSliceNoTypeCheck(v, &col)
		fillSlice(col, oldLen, v.length, va)
	} else {
		tlen := v.GetType().TypeSize()
		copy(v.data[oldLen*tlen:(oldLen+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
		broadcastFixed(v.data[oldLen*tlen:v.length*tlen], tlen)
	}
	return nil
}

func (v *Vector) Union(w *Vector, sels []int64, mp *mpool.MPool) error {
	return unionT[int64](v, w, sels, mp)
}
func (v *Vector) UnionInt32(w *Vector, sels []int32, mp *mpool.MPool) error {
	return unionT[int32](v, w, sels, mp)
}

func unionT[T int32 | int64](v, w *Vector, sels []T, mp *mpool.MPool) error {

	if len(sels) == 0 {
		return nil
	}
	if v.dataAliases(w) {
		// Keep source row indices unchanged, including reordered selections.
		return v.withClonedSource(w, 0, w.length, mp, func(source *Vector) error {
			return unionT(v, source, sels, mp)
		})
	}
	// Classify the source cheaply before considering a reused destination.
	// Metadata-bearing, nullable and constant sources keep the ordinary path.
	if cap(v.data) > 0 && w.class == FLAT && w.nsp.IsEmpty() {
		width := v.typ.TypeSize()
		if (width == 4 || width == 8) && v.class == FLAT && v.typ == w.typ &&
			len(sels) <= cap(v.data)/width-v.length && v.nsp.IsEmpty() {
			oldLen := v.length
			v.data = v.data[:cap(v.data)]
			if width == 8 {
				unionFixedSelected[uint64](v.data[oldLen*8:(oldLen+len(sels))*8], w.data, sels)
			} else {
				unionFixedSelected[uint32](v.data[oldLen*4:(oldLen+len(sels))*4], w.data, sels)
			}
			v.length = oldLen + len(sels)
			return nil
		}
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}

	if err := extendWithBitmaps(
		v,
		len(sels),
		mp,
		w.IsConstNull() || !w.nsp.IsEmpty(),
	); err != nil {
		return err
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + len(sels))
	if w.IsConst() {
		if w.IsConstNull() {
			v.nsp.AddRange(uint64(oldLen), uint64(oldLen+len(sels)))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			var ws []types.Varlena
			ToSliceNoTypeCheck(w, &ws)
			err = BuildVarlenaFromVarlena(v, &va, &ws[0], &w.area, mp)
			if err != nil {
				return err
			}
			var col []types.Varlena
			ToSliceNoTypeCheck(v, &col)
			fillSlice(col, oldLen, v.length, va)
		} else {
			tlen := v.GetType().TypeSize()
			dst := v.data[oldLen*tlen : v.length*tlen]
			copy(dst[:tlen], w.data[:tlen])
			broadcastFixed(dst, tlen)
		}

		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena
		ToSliceNoTypeCheck(v, &vCol)
		ToSliceNoTypeCheck(w, &wCol)
		// pre-grow the area once for the selected non-inline, non-null rows so the
		// per-row BuildVarlenaNoInline appends below never realloc. Null rows are NOT
		// copied (the loop below skips them via w.nsp), and a reused vector can retain
		// a stale non-inline header in a null slot — counting those would reserve area
		// for dead payload (large needless mp.Grow / alloc failure), so exclude them.
		total := 0
		hasNull := !w.GetNulls().IsEmpty()
		// With both areas empty, valid non-NULL descriptors are either inline
		// or empty non-inline values at offset zero. Neither needs area growth
		// or rebasing, so copy them directly without a separate sizing pass.
		emptyAreas := !hasNull && len(w.area) == 0 && len(v.area) == 0
		if !emptyAreas {
			for _, sel := range sels {
				if hasNull && w.nsp.Contains(uint64(sel)) {
					continue
				}
				if !wCol[sel].IsSmall() {
					_, l := wCol[sel].OffsetLen()
					total += int(l)
				}
			}
			if err = pregrowVarlenaArea(v, total, mp); err != nil {
				return err
			}
		}
		// Reuse the area sizing pass to select an inline-copy batch. The small
		// check still handles non-inline zero-length descriptors correctly.
		// Mixed/area payloads keep the original loops.
		if emptyAreas {
			target := vCol[oldLen : oldLen+len(sels)]
			for i, sel := range sels {
				target[i] = wCol[sel]
			}
		} else if total == 0 {
			if hasNull {
				for i, sel := range sels {
					if w.nsp.Contains(uint64(sel)) {
						v.nsp.Add(uint64(oldLen + i))
						continue
					}
					if wCol[sel].IsSmall() {
						vCol[oldLen+i] = wCol[sel]
					} else if err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp); err != nil {
						return err
					}
				}
			} else {
				for i, sel := range sels {
					if wCol[sel].IsSmall() {
						vCol[oldLen+i] = wCol[sel]
					} else if err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp); err != nil {
						return err
					}
				}
			}
		} else if hasNull {
			for i, sel := range sels {
				if w.nsp.Contains(uint64(sel)) {
					v.nsp.Add(uint64(oldLen + i))
					continue
				}
				err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp)
				if err != nil {
					return err
				}
			}
		} else {
			for i, sel := range sels {
				err = BuildVarlenaFromVarlena(v, &vCol[oldLen+i], &wCol[sel], &w.area, mp)
				if err != nil {
					return err
				}
			}
		}
		// Selection materialization copies every non-inline value into a fresh
		// destination range. It cannot introduce aliases for a non-const source,
		// even when source rows repeat or arrive out of order.
	} else {
		tlen := v.GetType().TypeSize()
		if !w.nsp.IsEmpty() {
			for i, sel := range sels {
				if w.nsp.Contains(uint64(sel)) {
					v.nsp.Add(uint64(oldLen + i))
					continue
				}
				dstOffset, srcOffset := (oldLen+i)*tlen, int(sel)*tlen
				copy(v.data[dstOffset:dstOffset+tlen], w.data[srcOffset:srcOffset+tlen])
			}
		} else {
			// Cases are payload widths in bytes; len(sels) is the output row count.
			// Typed carriers copy physical bits without per-row byte offsets.
			// Logical type and metadata remain owned by this append path.
			switch tlen {
			case 8:
				unionFixedSelected[uint64](v.data[oldLen*8:(oldLen+len(sels))*8], w.data, sels)
			case 4:
				unionFixedSelected[uint32](v.data[oldLen*4:(oldLen+len(sels))*4], w.data, sels)
			case 2:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*2])
					p2 := unsafe.Pointer(&w.data[int(sel)*2])
					*(*int16)(p1) = *(*int16)(p2)
				}
			case 1:
				for i, sel := range sels {
					v.data[(oldLen + i)] = w.data[int(sel)]
				}
			default:
				unionWideFixedSelected(v, w, oldLen, sels, tlen)
			}
		}
	}

	return nil
}

// unionWideFixedSelected handles widths that otherwise require a dynamic copy
// per row.
func unionWideFixedSelected[S int32 | int64](v, w *Vector, oldLen int, sels []S, width int) {
	// Bound the byte ranges before constructing typed views. The backing
	// allocation can contain padding, especially for 12/20/24-byte values.
	source := w.data[:w.length*width]
	target := v.data[oldLen*width : (oldLen+len(sels))*width]
	switch width {
	case 12:
		unionFixedSelected[types.TS](target, source, sels)
	case 16:
		unionFixedSelected[types.Decimal128](target, source, sels)
	case 20:
		unionFixedSelected[types.Blockid](target, source, sels)
	case 24:
		unionFixedSelected[types.Rowid](target, source, sels)
	case 32:
		unionFixedSelected[types.Decimal256](target, source, sels)
	default:
		for i, sel := range sels {
			copy(target[i*width:(i+1)*width], source[int(sel)*width:(int(sel)+1)*width])
		}
	}
}

// unionFixedSelected copies physical values only. unionT owns capacity,
// logical length and metadata; the representative type has the selected width,
// but need not be the vector's logical SQL type (e.g. UUID and Decimal128).
func unionFixedSelected[T types.FixedSizeT, S int32 | int64](target, source []byte, sels []S) {
	var value T
	width := int(unsafe.Sizeof(value))
	src := util.UnsafeSlice[T](unsafe.Pointer(unsafe.SliceData(source)), len(source)/width)
	dst := util.UnsafeSlice[T](unsafe.Pointer(unsafe.SliceData(target)), len(sels))
	shuffle.FixedLengthShuffle(src, dst, sels)
}

// UnionBatch may partially modify the destination on failure. To preserve it
// for reuse, take an append checkpoint and roll back, or reset the vector.
func (v *Vector) UnionBatch(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	return v.unionBatch(w, offset, cnt, flags, mp, 0, 0, false)
}

func (v *Vector) unionBatch(
	w *Vector,
	offset int64,
	cnt int,
	flags []uint8,
	mp *mpool.MPool,
	selectedAreaBytes int,
	preflightRowCount int,
	preflighted bool,
) error {
	return v.unionBatchFlat(w, offset, cnt, flags, mp, selectedAreaBytes, preflightRowCount, preflighted)
}

func (v *Vector) unionBatchFlat(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool, selectedAreaBytes, preflightRowCount int, preflighted bool) error {
	if selectedAreaBytes < 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	var addCnt int
	if preflighted {
		if preflightRowCount < v.length {
			return moerr.NewInternalErrorNoCtx(
				"preflighted vector row count is invalid")
		}
		addCnt = preflightRowCount - v.length
	} else if flags != nil {
		addCnt = 0
		for i := range flags {
			addCnt += int(flags[i])
		}
	} else {
		addCnt = cnt
	}

	if addCnt == 0 {
		return nil
	}
	// UnionBatch permits the source to be the destination itself or a borrowed
	// Window into it. Preserve that source before any destination growth: mpool
	// growth may replace and release both the varlena headers and area backing,
	// leaving an aliased source slice pointing at returned storage.
	if w != nil && (v.dataAliases(w) ||
		v.typ.IsVarlen() && byteSlicesOverlap(v.area, w.area)) {
		sourceRows := cnt
		if flags != nil {
			sourceRows = len(flags)
		}
		return v.withClonedSource(w, int(offset), int(offset)+sourceRows, mp, func(source *Vector) error {
			return v.UnionBatch(source, 0, cnt, flags, mp)
		})
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	oldLen := v.length
	if preflighted {
		if preflightRowCount != v.length+addCnt ||
			preflightRowCount*v.typ.TypeSize() > cap(v.data) {
			return moerr.NewInternalErrorNoCtx(
				"preflighted vector row capacity is insufficient")
		}
	} else {
		if err := extendWithBitmaps(
			v,
			addCnt,
			mp,
			w.IsConstNull() || !w.nsp.IsEmpty(),
		); err != nil {
			return err
		}
	}

	if w.IsConst() {
		oldLen := v.length
		v.setLengthAfterExtend(v.length + addCnt)
		if w.IsConstNull() {
			v.nsp.AddRange(uint64(oldLen), uint64(v.length))
		} else if v.GetType().IsVarlen() {
			var err error
			var va types.Varlena
			var ws []types.Varlena
			ToSliceNoTypeCheck(w, &ws)
			err = BuildVarlenaFromVarlena(v, &va, &ws[0], &w.area, mp)
			if err != nil {
				return err
			}
			var col []types.Varlena
			ToSliceNoTypeCheck(v, &col)
			fillSlice(col, oldLen, v.length, va)
		} else {
			tlen := v.GetType().TypeSize()
			copy(v.data[oldLen*tlen:(oldLen+1)*tlen], w.data[:tlen])
			broadcastFixed(v.data[oldLen*tlen:v.length*tlen], tlen)
		}

		return nil
	}

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena

		vCol = toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+addCnt)
		ToSliceNoTypeCheck(w, &wCol)

		// Fast path for a contiguous logical source range. A borrowed Window keeps
		// the parent's complete area, so copying w.area merely because the window's
		// offset is zero can amplify one logical range into a copy of every sibling
		// range. Derive the live payload span from the selected descriptors first.
		if flags == nil || preflighted && addCnt == cnt {
			areaBytes := -1
			if preflighted {
				areaBytes = selectedAreaBytes
			}
			var fast bool
			fast, err = v.unionBatchContiguousVarlenRange(
				w, vCol, wCol, int(offset), cnt, areaBytes, mp)
			if err != nil {
				return err
			}
			if fast {
				return nil
			}
		}

		// pre-grow the area once for the non-inline, non-null source rows in this
		// append so the per-row BuildVarlenaNoInline calls below never realloc. Null
		// rows are NOT copied (the loops below skip them via w.nsp), and a reused
		// vector can retain a stale non-inline header in a null slot — counting those
		// would reserve area for dead payload (large needless mp.Grow / alloc
		// failure), so exclude them to match what's actually appended.
		if !preflighted {
			total := 0
			hasNull := !w.nsp.IsEmpty()
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if hasNull && w.nsp.Contains(uint64(offset)+uint64(i)) {
						continue
					}
					if s := &wCol[int(offset)+i]; !s.IsSmall() {
						_, l := s.OffsetLen()
						total += int(l)
					}
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if hasNull && w.nsp.Contains(uint64(offset)+uint64(i)) {
						continue
					}
					if s := &wCol[int(offset)+i]; !s.IsSmall() {
						_, l := s.OffsetLen()
						total += int(l)
					}
				}
			}
			if err = pregrowVarlenaArea(v, total, mp); err != nil {
				return err
			}
		} else if selectedAreaBytes > cap(v.area)-len(v.area) {
			return moerr.NewInternalErrorNoCtx(
				"preflighted vector area capacity is insufficient",
			)
		}

		if !w.nsp.IsEmpty() {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						v.nsp.Add(uint64(v.length))
					} else {
						err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
						if err != nil {
							return err
						}
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						v.nsp.Add(uint64(v.length))
					} else {
						err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
						if err != nil {
							return err
						}
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			}
		} else {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
					if err != nil {
						return err
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					err = BuildVarlenaFromVarlena(v, &vCol[v.length], &wCol[int(offset)+i], &w.area, mp)
					if err != nil {
						return err
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			}
		}
	} else {
		tlen := v.GetType().TypeSize()
		if !w.nsp.IsEmpty() {
			if flags == nil {
				// Fixed-width NULL slots contain no payload references. Copy the
				// range once; only their bitmap, not those opaque bytes, is visible.
				copy(v.data[oldLen*tlen:(oldLen+cnt)*tlen], w.data[int(offset)*tlen:(int(offset)+cnt)*tlen])
				for i := 0; i < cnt; i++ {
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						v.nsp.Add(uint64(oldLen + i))
					}
				}
				v.setLengthAfterExtend(oldLen + cnt)
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						v.nsp.Add(uint64(v.length))
					} else {
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			}
		} else {
			if flags == nil {
				if w.nsp.Any() {
					for i := 0; i < cnt; i++ {
						if w.nsp.Contains(uint64(offset) + uint64(i)) {
							v.nsp.Add(uint64(v.length))
						}
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
						v.setLengthAfterExtend(v.length + 1)
					}
				} else {
					copy(v.data[v.length*tlen:(v.length+cnt)*tlen], w.data[(int(offset))*tlen:(int(offset)+cnt)*tlen])
					v.setLengthAfterExtend(v.length + cnt)
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					v.setLengthAfterExtend(v.length + 1)
				}
			}
		}
	}

	return nil
}

// String function is used to visually display the vector,
// which is used to implement the Printf interface
func (v *Vector) String() string {
	switch v.typ.Oid {
	case types.T_bool:
		return vecToString[bool](v)
	case types.T_bit:
		return vecToString[uint64](v)
	case types.T_int8:
		return vecToString[int8](v)
	case types.T_int16:
		return vecToString[int16](v)
	case types.T_int32:
		return vecToString[int32](v)
	case types.T_int64:
		return vecToString[int64](v)
	case types.T_uint8:
		return vecToString[uint8](v)
	case types.T_uint16:
		return vecToString[uint16](v)
	case types.T_uint32:
		return vecToString[uint32](v)
	case types.T_uint64:
		return vecToString[uint64](v)
	case types.T_float32:
		return vecToString[float32](v)
	case types.T_float64:
		return vecToString[float64](v)
	case types.T_bf16:
		return vecToString[types.BF16](v)
	case types.T_float16:
		return vecToString[types.Float16](v)
	case types.T_float8:
		return vecToString[types.Float8](v)
	case types.T_float4:
		return vecToString[types.Float4](v)
	case types.T_date:
		return vecToString[types.Date](v)
	case types.T_datetime:
		return vecToString[types.Datetime](v)
	case types.T_time:
		return vecToString[types.Time](v)
	case types.T_timestamp:
		return vecToString[types.Timestamp](v)
	case types.T_enum:
		return vecToString[types.Enum](v)
	case types.T_year:
		return vecToString[types.MoYear](v)
	case types.T_decimal64:
		return vecToString[types.Decimal64](v)
	case types.T_decimal128:
		return vecToString[types.Decimal128](v)
	case types.T_decimal256:
		return vecToString[types.Decimal256](v)
	case types.T_uuid:
		return vecToString[types.Uuid](v)
	case types.T_TS:
		return vecToString[types.TS](v)
	case types.T_Rowid:
		return vecToString[types.Rowid](v)
	case types.T_Blockid:
		return vecToString[types.Blockid](v)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		col := InefficientMustStrCol(v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			} else {
				return col[0]
			}
		}
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
		} else {
			return fmt.Sprintf("%v", col)
		}
		//return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	case types.T_array_float32:
		//NOTE: Don't merge this with T_Varchar. We need to retrieve the Array and print the values.
		col := MustArrayCol[float32](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			} else {
				return types.ArrayToString[float32](col[0])
			}
		}

		str := types.ArraysToString[float32](col, types.DefaultArraysToStringSep)
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
		}
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_float64:
		//NOTE: Don't merge this with T_Varchar. We need to retrieve the Array and print the values.
		col := MustArrayCol[float64](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			} else {
				return types.ArrayToString[float64](col[0])
			}
		}
		str := types.ArraysToString[float64](col, types.DefaultArraysToStringSep)
		if v.nsp.Any() {
			return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
		}
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_bf16:
		col := MustArrayCol[types.BF16](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			}
			return types.ArrayToString[types.BF16](col[0])
		}
		str := types.ArraysToString[types.BF16](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_float16:
		col := MustArrayCol[types.Float16](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			}
			return types.ArrayToString[types.Float16](col[0])
		}
		str := types.ArraysToString[types.Float16](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_float8, types.T_array_float4:
		return blockScaledVectorString(v)
	case types.T_array_uint8:
		col := MustArrayCol[uint8](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			}
			return types.ArrayToString[uint8](col[0])
		}
		str := types.ArraysToString[uint8](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_int8:
		col := MustArrayCol[int8](v)
		if len(col) == 1 {
			if v.nsp.Contains(0) {
				return "null"
			}
			return types.ArrayToString[int8](col[0])
		}
		str := types.ArraysToString[int8](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	default:
		panic("vec to string unknown types.")
	}
}

func blockScaledVectorString(v *Vector) string {
	if v.IsConstNull() {
		return "null"
	}
	n := v.Length()
	if v.IsConst() {
		n = 1
	}
	strs := make([]string, n)
	for i := range strs {
		strs[i] = v.RowToString(i)
	}
	if n == 1 {
		return strs[0]
	}
	return fmt.Sprintf("%v-%s", strings.Join(strs, types.DefaultArraysToStringSep), v.nsp.GetBitmap().String())
}

// RowToString returns row idx as text: "null" for a NULL row, otherwise
// types.ValueText of the value.
func (v *Vector) RowToString(idx int) string {
	if v.IsNull(uint64(idx)) {
		return "null"
	}
	return types.ValueText(v.typ, GetAny(v, idx, false))
}

func SetConstNull(vec *Vector, length int, mp *mpool.MPool) error {
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	if len(vec.data) > 0 {
		vec.data = vec.data[:0]
	}
	vec.class = CONSTANT
	vec.length = length
	return nil
}

func SetConstFixed[T any](vec *Vector, val T, length int, mp *mpool.MPool) error {
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	vec.length = length

	col := toSliceOfLengthNoTypeCheck[T](vec, 1)
	col[0] = val
	return nil
}

func SetConstBytes(vec *Vector, val []byte, length int, mp *mpool.MPool) error {
	if vec.bytesAlias(val) {
		return vec.withClonedBytes(val, mp, func(source []byte) error { return SetConstBytes(vec, source, length, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	oldAreaLen := len(vec.area)
	var value types.Varlena
	if err := BuildVarlenaFromByteSlice(vec, &value, &val, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	if err := extend(vec, 1, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	col[0] = value
	vec.length = length
	return nil
}

func SetConstByteJson(vec *Vector, bj bytejson.ByteJson, length int, mp *mpool.MPool) error {
	if vec.bytesAlias(bj.Data) {
		return vec.withClonedBytes(bj.Data, mp, func(source []byte) error { bj.Data = source; return SetConstByteJson(vec, bj, length, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	oldAreaLen := len(vec.area)
	var value types.Varlena
	if err := BuildVarlenaFromByteJson(vec, &value, bj, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	if err := extend(vec, 1, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	col[0] = value
	vec.length = length
	return nil
}

func SetConstByteJsonEncoded(
	vec *Vector,
	enc bytejson.ByteJsonDataEncoder,
	length int,
	mp *mpool.MPool,
) error {
	size, err := encodedVarlenaSize(enc)
	if err != nil {
		return err
	}
	if vec.needsValueStaging(size) {
		return vec.withOwnedMutation(1, size, mp, func(owned *Vector) error { return SetConstByteJsonEncoded(owned, enc, length, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	oldAreaLen := len(vec.area)
	var value types.Varlena
	if err := BuildVarlenaFromByteJsonEncoded(vec, &value, enc, mp); err != nil {
		return err
	}
	if err := extend(vec, 1, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	col[0] = value
	vec.length = length
	return nil
}

// SetConstArray set current vector as Constant_Array vector of given length.
func SetConstArray[T types.ArrayElement](vec *Vector, val []T, length int, mp *mpool.MPool) error {
	if raw := types.ArrayToBytes(val); vec.bytesAlias(raw) {
		return vec.withClonedBytes(raw, mp, func(source []byte) error { return SetConstArray(vec, types.BytesToArray[T](source), length, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	var err error

	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	err = BuildVarlenaFromArray(vec, &col[0], &val, mp)
	if err != nil {
		return err
	}
	vec.length = length
	return nil
}

// WARNING: AppendAny() append value with any type will cause memory escape to heap which will result in slow GC.
// If you know the actual type, better use the AppendFixed() to append the values.
// Only use when you have no choice, e.g. you are dealing with column with any type that don't know in advanced.
func AppendAny(vec *Vector, val any, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}

	if isNull {
		return appendOneFixed(vec, 0, true, mp)
	}

	if ops, ok := opsOf(vec); ok {
		return ops.appendAny(vec, val, mp)
	}
	return nil
}

// Equal reports whether v and o have the same type and length, NULL in the
// same rows, and equal values in the others.
func (v *Vector) Equal(o *Vector) bool {
	if v.Length() != o.Length() || v.typ != o.typ {
		return false
	}
	for i := 0; i < v.Length(); i++ {
		isNull := v.IsNull(uint64(i))
		if isNull != o.IsNull(uint64(i)) {
			return false
		}
		if isNull {
			continue
		}
		if v.typ.IsVarlen() {
			if !bytes.Equal(v.GetBytesAt(i), o.GetBytesAt(i)) {
				return false
			}
		} else if GetAny(v, i, false) != GetAny(o, i, false) {
			return false
		}
	}
	return true
}

// MustAppendValue appends val, or a NULL, through the vector's own pool and
// panics on an error. It is for tests and for callers whose vector cannot
// fail to grow; other code calls AppendAny and handles the error.
func (v *Vector) MustAppendValue(val any, isNull bool) {
	if err := AppendAny(v, val, isNull, v.mp); err != nil {
		panic(err)
	}
}

// withClonedSource charges a temporary snapshot to the mutation's destination.
// The callback consumes it synchronously. Keeping cleanup here avoids adding a
// defer to the common non-aliasing mutation paths.
func (v *Vector) withClonedSource(w *Vector, start, end int, mp *mpool.MPool, consume func(*Vector) error) error {
	source, err := w.CloneWindow(start, end, mp)
	if err != nil {
		return err
	}
	defer source.Free(mp)
	return consume(source)
}

// bytesAlias checks physical allocations, including unused capacity that COW
// or growth can release while a raw input still references it.
func (v *Vector) bytesAlias(source []byte) bool {
	return byteSlicesOverlap(v.data[:cap(v.data)], source) || byteSlicesOverlap(v.area[:cap(v.area)], source)
}

func (v *Vector) withClonedBytes(source []byte, mp *mpool.MPool, consume func([]byte) error) error {
	if mp == nil {
		return moerr.NewInternalErrorNoCtx("source preservation requires a mpool")
	}
	owned, err := v.alloc(mp, len(source))
	if err != nil {
		return err
	}
	defer v.mp.Free(owned)
	copy(owned, source)
	return consume(owned)
}

// Stage aliased batch values before mutating their source backing. Temporary
// descriptors, metadata and payload all use the destination's account.
func (v *Vector) withStagedValue(rows int, mp *mpool.MPool, build, consume func(*Vector) error) error {
	if mp == nil {
		return moerr.NewInternalErrorNoCtx("source preservation requires a mpool")
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	staged := NewVec(v.typ, mp)
	defer staged.Free(mp)
	if rows > 0 {
		if err := staged.PreExtend(rows, mp); err != nil {
			return err
		}
	}
	if err := build(staged); err != nil {
		return err
	}
	return consume(staged)
}

func (v *Vector) needsValueStaging(size int) bool {
	width := v.typ.TypeSize()
	return width <= 0 || v.length >= cap(v.data)/width ||
		size > types.VarlenaInlineSize && size > cap(v.area)-len(v.area)
}

// Preserve the original backing while an opaque writer/encoder consumes its
// inputs. Admission happens on the private working vector before user code;
// commit and old-backing cleanup happen only after successful publication.
func (v *Vector) withOwnedMutation(rows, area int, mp *mpool.MPool, mutate func(*Vector) error) error {
	if mp == nil {
		return moerr.NewInternalErrorNoCtx("opaque mutation requires a mpool")
	}
	if err := v.checkMP(mp); err != nil {
		return err
	}
	owned, err := v.dup(mp)
	if err != nil {
		return err
	}
	defer owned.Free(mp)
	class := owned.class
	owned.class = FLAT
	reserveRows := max(rows, v.Capacity()-owned.length)
	if area <= types.VarlenaInlineSize {
		area = 0
	}
	reserveArea := max(area, cap(v.area)-len(owned.area))
	if err := owned.PreExtendWithArea(reserveRows, reserveArea, mp); err != nil {
		return err
	}
	owned.class = class
	// Reserved slots can hold unpublished descriptors that rollback must keep.
	copy(owned.data, v.data)
	if err := mutate(owned); err != nil {
		return err
	}
	requiredData := owned.length * v.typ.TypeSize()
	if owned.IsConst() {
		requiredData = min(len(owned.data), v.typ.TypeSize())
	}
	if cap(v.data) >= requiredData {
		// Keep pre-admitted owned descriptors stable for FunctionResult.cols
		// and other synchronous consumers; only the payload needed replacing.
		copy(v.data[:cap(v.data)], owned.data)
		mp.Free(owned.data)
		owned.data = v.data[:cap(v.data)]
		v.data = nil
	}
	*v, *owned = *owned, *v
	return nil
}

func encodedVarlenaSize(enc bytejson.ByteJsonDataEncoder) (int, error) {
	size := uint64(enc.DataSize()) + 1
	if size > math.MaxUint32 || size > uint64(math.MaxInt) {
		return 0, moerr.NewInvalidInputNoCtx("json value is too large")
	}
	return int(size), nil
}

// Every vector owns its memory, so only a vector can alias itself.
func (v *Vector) dataAliases(w *Vector) bool {
	return v == w && byteSlicesOverlap(v.data, w.data)
}

func byteSlicesOverlap(left, right []byte) bool {
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	leftStart := uintptr(unsafe.Pointer(unsafe.SliceData(left)))
	rightStart := uintptr(unsafe.Pointer(unsafe.SliceData(right)))
	return leftStart < rightStart+uintptr(len(right)) &&
		rightStart < leftStart+uintptr(len(left))
}

// unionBatchContiguousVarlenRange appends [offset, offset+cnt) when all
// non-inline, non-null payloads in that row range form one contiguous area
// span. It returns false without modifying vector data when the source area is
// reordered or contains gaps, allowing UnionBatch to use its general path.
func (v *Vector) unionBatchContiguousVarlenRange(
	w *Vector,
	vCol, wCol []types.Varlena,
	offset, cnt, admittedAreaBytes int,
	mp *mpool.MPool,
) (bool, error) {
	hasNull := !w.nsp.IsEmpty()
	areaStart, areaEnd := 0, 0
	hasArea := false

	for i := offset; i < offset+cnt; i++ {
		if hasNull && w.nsp.Contains(uint64(i)) {
			continue
		}
		s := &wCol[i]
		if s.IsSmall() {
			continue
		}
		off, length := s.OffsetLen()
		end := uint64(off) + uint64(length)
		if end > uint64(len(w.area)) {
			return false, nil
		}
		if !hasArea {
			areaStart, areaEnd = int(off), int(end)
			hasArea = true
			continue
		}
		if int(off) != areaEnd {
			return false, nil
		}
		areaEnd = int(end)
	}

	// A preflighted publication may only copy the exact live payload admitted
	// before commit. NULL mutations can leave dead bytes even in disjoint area.
	payloadBytes := areaEnd - areaStart
	if admittedAreaBytes >= 0 && (payloadBytes != admittedAreaBytes ||
		payloadBytes > cap(v.area)-len(v.area)) {
		return false, nil
	}
	oldLen := v.length
	baseOff := len(v.area)
	if hasArea {
		payload := w.area[areaStart:areaEnd]
		if baseOff+len(payload) <= cap(v.area) {
			v.area = append(v.area, payload...)
		} else if mp == nil {
			v.area = append(v.area, payload...)
		} else {
			area, err := v.growArea2(mp, payload, baseOff+len(payload))
			if err != nil {
				return false, err
			}
			v.area = area
		}
	}

	copy(vCol[oldLen:oldLen+cnt], wCol[offset:offset+cnt])
	if !hasNull && areaStart == 0 && baseOff == 0 {
		v.setLengthAfterExtend(v.length + cnt)
		return true, nil
	}
	for i := 0; i < cnt; i++ {
		sourceRow := offset + i
		targetRow := oldLen + i
		if hasNull && w.nsp.Contains(uint64(sourceRow)) {
			v.nsp.Add(uint64(targetRow))
			vCol[targetRow] = types.Varlena{}
			continue
		}
		if !vCol[targetRow].IsSmall() {
			off, length := vCol[targetRow].OffsetLen()
			vCol[targetRow].SetOffsetLen(
				uint32(baseOff)+off-uint32(areaStart), length)
		}
	}
	v.setLengthAfterExtend(v.length + cnt)
	return true, nil
}

func AppendNull(vec *Vector, mp *mpool.MPool) error {
	return appendOneFixed(vec, 0, true, mp)
}

func AppendFixed[T any](vec *Vector, val T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneFixed(vec, val, isNull, mp)
}

func AppendBytes(vec *Vector, val []byte, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneBytes(vec, val, isNull, mp)
}

func (v *Vector) prepareSingleAppendMetadata(isNull bool, mp *mpool.MPool) error {
	// A NULL or an ordinary append to a vector without binary-string metadata
	// needs no admission.
	return nil
}

func (v *Vector) publishSingleAppend() {
	v.length++
}

func AppendBytesWithWriter(vec *Vector, size int, mp *mpool.MPool, writer func([]byte) error) (err error) {
	if vec.IsConst() || size < 0 || mp == nil {
		return moerr.NewInternalErrorNoCtx("invalid direct varlena append")
	}
	if vec.needsValueStaging(size) {
		return vec.withOwnedMutation(1, size, mp, func(owned *Vector) error { return AppendBytesWithWriter(owned, size, mp, writer) })
	}
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, 1))
		}
	}()
	if err = vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
		return err
	}
	// Admit descriptor capacity before invoking writer. Once writer starts, the
	// append must not discover another mpool failure after user code has run.
	if err = extend(vec, 1, mp); err != nil {
		return err
	}

	var value types.Varlena
	if size <= types.VarlenaInlineSize {
		value[0] = byte(size)
		if err = writer(value.ByteSlice()); err != nil {
			return err
		}
	} else {
		if err = vec.PreExtendWithArea(0, size, mp); err != nil {
			return err
		}
		offset := len(vec.area)
		vec.area = vec.area[:offset+size]
		if err = writer(vec.area[offset:]); err != nil {
			return err
		}
		value.SetOffsetLen(uint32(offset), uint32(size))
	}
	return appendOneOwnedVarlena(vec, value, mp)
}

func AppendByteJson(vec *Vector, bj bytejson.ByteJson, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneByteJson(vec, bj, isNull, mp)
}

func AppendByteJsonEncoded(
	vec *Vector,
	enc bytejson.ByteJsonDataEncoder,
	mp *mpool.MPool,
) (retErr error) {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}

	size, err := encodedVarlenaSize(enc)
	if err != nil {
		return err
	}
	if vec.needsValueStaging(size) {
		return vec.withOwnedMutation(1, size, mp, func(owned *Vector) error { return AppendByteJsonEncoded(owned, enc, mp) })
	}
	checkpoint := vec.MakeAppendCheckpoint()
	wasNull := vec.nsp.Contains(uint64(vec.length))
	defer func() {
		if retErr != nil {
			rollbackErr := vec.RollbackAppend(checkpoint, 1)
			retErr = JoinAppendRollbackError(retErr, rollbackErr)
			if wasNull && rollbackErr == nil {
				vec.nsp.Add(uint64(checkpoint.length))
			}
		}
	}()

	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	if err := vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
		return err
	}
	index := vec.length
	values := toSliceOfLengthNoTypeCheck[types.Varlena](vec, index+1)
	oldValue := values[index]
	oldAreaLen := len(vec.area)
	if err := BuildVarlenaFromByteJsonEncoded(vec, &values[index], enc, mp); err != nil {
		vec.area = vec.area[:oldAreaLen]
		values[index] = oldValue
		if wasNull {
			vec.nsp.Add(uint64(index))
		} else {
			vec.nsp.Del(uint64(index))
		}
		return err
	}
	vec.nsp.Del(uint64(index))
	vec.setLengthAfterExtend(vec.length + 1)
	return nil
}

// AppendArray mainly used in tests
func AppendArray[T types.ArrayElement](vec *Vector, val []T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendOneArray[T](vec, val, isNull, mp)
}

func AppendMultiFixed[T any](vec *Vector, vals T, isNull bool, cnt int, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMultiFixed(vec, vals, isNull, cnt, mp)
}

func AppendMultiBytes(vec *Vector, vals []byte, isNull bool, cnt int, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	return appendMultiBytes(vec, vals, isNull, cnt, mp)
}

func AppendFixedList[T any](vec *Vector, ws []T, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendList(vec, ws, isNulls, mp)
}

func AppendBytesList(vec *Vector, ws [][]byte, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendBytesList(vec, ws, isNulls, mp)
}

func AppendStringList(vec *Vector, ws []string, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendStringList(vec, ws, isNulls, mp)
}

// AppendArrayList mainly used in unit tests
func AppendArrayList[T types.ArrayElement](vec *Vector, ws [][]T, isNulls []bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}
	if len(ws) == 0 {
		return nil
	}
	return appendArrayList(vec, ws, isNulls, mp)
}

func appendOneFixed[T any](vec *Vector, val T, isNull bool, mp *mpool.MPool) error {
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}

	if err := extendWithBitmaps(vec, 1, mp, isNull); err != nil {
		return err
	}
	if err := vec.prepareSingleAppendMetadata(isNull, mp); err != nil {
		return err
	}
	length := vec.length
	vec.publishSingleAppend()
	if isNull {
		if vec.typ.IsVarlen() {
			// Reused data capacity can contain a stale descriptor. Keep null rows
			// inside the area-disjoint invariant by making the new slot inline.
			toSliceOfLengthNoTypeCheck[types.Varlena](vec, vec.length)[length] =
				types.Varlena{}
		}
		vec.nsp.Add(uint64(length))
	} else {
		var col []T
		ToSliceNoTypeCheck(vec, &col)
		col[length] = val
	}
	return nil
}

func appendOneBytes(vec *Vector, val []byte, isNull bool, mp *mpool.MPool) (err error) {
	var va types.Varlena
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}
	if !isNull && vec.bytesAlias(val) {
		return vec.withClonedBytes(val, mp, func(sourceBytes []byte) error { return appendOneBytes(vec, sourceBytes, false, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}

	if isNull {
		// AppendBytes is also the generic null append used by expression
		// evaluation. Let appendOneFixed size the slot from vec.typ instead of
		// treating every null as a varlena descriptor.
		return appendOneFixed(vec, va, true, mp)
	} else {
		checkpoint := vec.MakeAppendCheckpoint()
		defer func() {
			if err != nil {
				err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, 1))
			}
		}()
		if err = vec.prepareSingleAppendMetadata(false, mp); err != nil {
			return err
		}
		err = BuildVarlenaFromByteSlice(vec, &va, &val, mp)
		if err != nil {
			return err
		}
		return appendOneOwnedVarlena(vec, va, mp)
	}
}

func appendOneByteJson(vec *Vector, bj bytejson.ByteJson, isNull bool, mp *mpool.MPool) (err error) {
	if !isNull && vec.bytesAlias(bj.Data) {
		return vec.withClonedBytes(bj.Data, mp, func(source []byte) error { bj.Data = source; return appendOneByteJson(vec, bj, false, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	var va types.Varlena
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}

	if isNull {
		return appendOneFixed(vec, va, true, mp)
	} else {
		checkpoint := vec.MakeAppendCheckpoint()
		defer func() {
			if err != nil {
				err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, 1))
			}
		}()
		if err = vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
			return err
		}
		err = BuildVarlenaFromByteJson(vec, &va, bj, mp)
		if err != nil {
			return err
		}
		return appendOneOwnedVarlena(vec, va, mp)
	}
}

// appendOneArray mainly used for unit tests
func appendOneArray[T types.ArrayElement](vec *Vector, val []T, isNull bool, mp *mpool.MPool) (err error) {
	if raw := types.ArrayToBytes(val); !isNull && vec.bytesAlias(raw) {
		return vec.withClonedBytes(raw, mp, func(source []byte) error { return appendOneArray(vec, types.BytesToArray[T](source), false, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	var va types.Varlena
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}

	if isNull {
		return appendOneFixed(vec, va, true, mp)
	} else {
		checkpoint := vec.MakeAppendCheckpoint()
		defer func() {
			if err != nil {
				err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, 1))
			}
		}()
		if err = vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
			return err
		}
		err = BuildVarlenaFromArray[T](vec, &va, &val, mp)
		if err != nil {
			return err
		}
		return appendOneOwnedVarlena(vec, va, mp)
	}
}

// appendOneOwnedVarlena installs a descriptor built against vec.area. The
// value is either inline or references a freshly appended area range, so this
// preserves an existing disjoint-area proof.
func appendOneOwnedVarlena(vec *Vector, value types.Varlena, mp *mpool.MPool) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	index := vec.length
	vec.publishSingleAppend()
	toSliceOfLengthNoTypeCheck[types.Varlena](vec, vec.length)[index] = value
	return nil
}

func appendMultiFixed[T any](vec *Vector, val T, isNull bool, cnt int, mp *mpool.MPool) error {
	if err := extendWithBitmaps(vec, cnt, mp, isNull); err != nil {
		return err
	}
	if !isNull {
		if err := vec.prepareOrdinaryAppendMetadata(cnt, mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + cnt)
	if isNull {
		if vec.typ.IsVarlen() && cnt > 0 {
			clear(toSliceOfLengthNoTypeCheck[types.Varlena](vec, vec.length)[length:])
		}
		vec.nsp.AddRange(uint64(length), uint64(length+cnt))
	} else if cnt > 0 {
		// XXX check cnt > 0 to avoid issue #23295
		var col []T
		ToSlice(vec, &col)
		fillSlice(col, length, length+cnt, val)
	}
	return nil
}

func appendMultiBytes(vec *Vector, val []byte, isNull bool, cnt int, mp *mpool.MPool) (err error) {
	if !isNull && vec.bytesAlias(val) {
		return vec.withClonedBytes(val, mp, func(source []byte) error { return appendMultiBytes(vec, source, false, cnt, mp) })
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	// A non-inline value is materialized once and its descriptor is broadcast.
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, cnt))
		}
	}()
	var va types.Varlena
	if err = extendWithBitmaps(vec, cnt, mp, isNull); err != nil {
		return err
	}
	if !isNull {
		if err = vec.prepareOrdinaryAppendMetadata(cnt, mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + cnt)
	if isNull {
		vec.nsp.AddRange(uint64(length), uint64(length+cnt))
	} else {
		var col []types.Varlena
		ToSliceNoTypeCheck(vec, &col)
		err = BuildVarlenaFromByteSlice(vec, &va, &val, mp)
		if err != nil {
			return err
		}
		for i := 0; i < cnt; i++ {
			col[length+i] = va
		}
	}
	return nil
}

func appendList[T any](vec *Vector, vals []T, isNulls []bool, mp *mpool.MPool) error {
	if raw := types.EncodeSlice(vals); (len(vals) > vec.Capacity()-vec.length) && vec.bytesAlias(raw) {
		return vec.withClonedBytes(raw, mp, func(source []byte) error { return appendList(vec, types.DecodeSlice[T](source), isNulls, mp) })
	}
	hasNull := slices.Contains(isNulls, true)
	if err := extendWithBitmaps(
		vec,
		len(vals),
		mp,
		hasNull,
	); err != nil {
		return err
	}
	if !(len(isNulls) > 0 && !slices.Contains(isNulls, false)) {
		if err := vec.prepareOrdinaryAppendMetadata(len(vals), mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + len(vals))
	col := MustFixedColWithTypeCheck[T](vec)
	if !hasNull && (len(isNulls) == 0 || len(isNulls) >= len(vals)) {
		// No null rows: copy the whole input at once. copy is memmove, so an
		// input overlapping the destination (e.g. the vector's own unused
		// capacity) is appended as it was before the call.
		copy(col[length:], vals)
		return nil
	}
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			vec.nsp.Add(uint64(length + i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(vec *Vector, vals [][]byte, isNulls []bool, mp *mpool.MPool) (err error) {
	mayMove := len(vals) > vec.Capacity()-vec.length
	if !mayMove {
		remaining := cap(vec.area) - len(vec.area)
		for i, value := range vals {
			if (len(isNulls) == 0 || !isNulls[i]) && len(value) > types.VarlenaInlineSize {
				if len(value) > remaining {
					mayMove = true
					break
				}
				remaining -= len(value)
			}
		}
	}
	for i, value := range vals {
		if !mayMove {
			break
		}
		if (len(isNulls) == 0 || !isNulls[i]) && vec.bytesAlias(value) {
			return vec.withStagedValue(len(vals), mp,
				func(staged *Vector) error { return appendBytesList(staged, vals, isNulls, mp) },
				func(staged *Vector) error { return vec.UnionBatch(staged, 0, staged.length, nil, mp) })
		}
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, len(vals)))
		}
	}()
	pregrow := len(vals) >= 16 && canPregrowRawByteList(vec, isNulls)
	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
	); err != nil {
		return err
	}
	if !(len(isNulls) > 0 && !slices.Contains(isNulls, false)) {
		if err = vec.prepareOrdinaryAppendMetadata(len(vals), mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + len(vals))
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	if pregrow {
		var total uint64
		maxArea := min(uint64(^uint32(0)), uint64(^uint(0)>>1))
		for _, w := range vals {
			if len(w) > types.VarlenaInlineSize {
				if uint64(len(w)) > maxArea-total {
					total = 0 // keep the original per-row growth on overflow
					break
				}
				total += uint64(len(w))
			}
		}
		if err = pregrowVarlenaArea(vec, int(total), mp); err != nil {
			return err
		}
	}
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			col[length+i] = types.Varlena{}
			vec.nsp.Add(uint64(length + i))
		} else {
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &w, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func appendStringList(vec *Vector, vals []string, isNulls []bool, mp *mpool.MPool) (err error) {
	mayMove := len(vals) > vec.Capacity()-vec.length
	if !mayMove {
		remaining := cap(vec.area) - len(vec.area)
		for i, value := range vals {
			if (len(isNulls) == 0 || !isNulls[i]) && len(value) > types.VarlenaInlineSize {
				if len(value) > remaining {
					mayMove = true
					break
				}
				remaining -= len(value)
			}
		}
	}
	for i, value := range vals {
		if !mayMove {
			break
		}
		if (len(isNulls) == 0 || !isNulls[i]) && vec.bytesAlias(util.UnsafeStringToBytes(value)) {
			return vec.withStagedValue(len(vals), mp,
				func(staged *Vector) error { return appendStringList(staged, vals, isNulls, mp) },
				func(staged *Vector) error { return vec.UnionBatch(staged, 0, staged.length, nil, mp) })
		}
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, len(vals)))
		}
	}()
	pregrow := len(vals) >= 16 && canPregrowRawByteList(vec, isNulls)

	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
	); err != nil {
		return err
	}
	if !(len(isNulls) > 0 && !slices.Contains(isNulls, false)) {
		if err = vec.prepareOrdinaryAppendMetadata(len(vals), mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + len(vals))
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	if pregrow {
		var total uint64
		maxArea := min(uint64(^uint32(0)), uint64(^uint(0)>>1))
		for _, w := range vals {
			if len(w) > types.VarlenaInlineSize {
				if uint64(len(w)) > maxArea-total {
					total = 0 // keep the original per-row growth on overflow
					break
				}
				total += uint64(len(w))
			}
		}
		if err = pregrowVarlenaArea(vec, int(total), mp); err != nil {
			return err
		}
	}
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			col[length+i] = types.Varlena{}
			vec.nsp.Add(uint64(length + i))
		} else {
			bs := []byte(w)
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// A fresh, empty destination cannot own any input bytes, so reserving its
// area cannot invalidate a caller's source slices. These types also have no
// per-value admission that could change error precedence.
func canPregrowRawByteList(vec *Vector, isNulls []bool) bool {
	if cap(vec.data) != 0 || cap(vec.area) != 0 || len(isNulls) != 0 ||
		vec.length != 0 {
		return false
	}
	switch vec.typ.Oid {
	case types.T_varbinary:
		return true
	case types.T_varchar:
		return true
	default:
		return false
	}
}

// appendArrayList mainly used for unit tests
func appendArrayList[T types.ArrayElement](vec *Vector, vals [][]T, isNulls []bool, mp *mpool.MPool) (err error) {
	mayMove := len(vals) > vec.Capacity()-vec.length
	if !mayMove {
		remaining := cap(vec.area) - len(vec.area)
		for i, value := range vals {
			size := len(types.ArrayToBytes(value))
			if (len(isNulls) == 0 || !isNulls[i]) && size > types.VarlenaInlineSize {
				if size > remaining {
					mayMove = true
					break
				}
				remaining -= size
			}
		}
	}
	for i, value := range vals {
		if !mayMove {
			break
		}
		if (len(isNulls) == 0 || !isNulls[i]) && vec.bytesAlias(types.ArrayToBytes(value)) {
			return vec.withStagedValue(len(vals), mp,
				func(staged *Vector) error { return appendArrayList(staged, vals, isNulls, mp) },
				func(staged *Vector) error { return vec.UnionBatch(staged, 0, staged.length, nil, mp) })
		}
	}
	if err := vec.checkMP(mp); err != nil {
		return err
	}
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			err = JoinAppendRollbackError(err, vec.RollbackAppend(checkpoint, len(vals)))
		}
	}()

	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
	); err != nil {
		return err
	}
	if !(len(isNulls) > 0 && !slices.Contains(isNulls, false)) {
		if err = vec.prepareOrdinaryAppendMetadata(len(vals), mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + len(vals))
	col := MustFixedColNoTypeCheck[types.Varlena](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			col[length+i] = types.Varlena{}
			vec.nsp.Add(uint64(length + i))
		} else {
			bs := w
			err = BuildVarlenaFromArray[T](vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func shrinkFixed[T types.FixedSizeT](v *Vector, sels []int64, negate bool) {
	vs := MustFixedColNoTypeCheck[T](v)
	if !negate {
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		nulls.FilterInPlaceOrdered(&v.nsp, sels, false)
		v.length = len(sels)
	} else if len(sels) > 0 {
		for oldIdx, newIdx, selIdx, sel, length := 0, 0, 0, sels[0], v.length; oldIdx < length; oldIdx++ {
			if oldIdx != int(sel) {
				vs[newIdx] = vs[oldIdx]
				newIdx++
			} else {
				selIdx++
				if selIdx >= len(sels) {
					for idx := oldIdx + 1; idx < v.length; idx++ {
						vs[newIdx] = vs[idx]
						newIdx++
					}
					break
				}
				sel = sels[selIdx]
			}
		}
		nulls.FilterInPlaceOrdered(&v.nsp, sels, true)
		v.length -= len(sels)
	}
}

func shrinkFixedByMask[T types.FixedSizeT](v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64) {
	vs := MustFixedColNoTypeCheck[T](v)
	length := sels.Count()
	if !negate {
		itr := sels.Iterator()
		idx := 0
		for itr.HasNext() {
			vs[idx] = vs[itr.Next()+offset]
			idx++
		}
		nulls.FilterByMaskInPlaceWithOffset(&v.nsp, sels, false, offset)
		v.length = length
	} else if length > 0 {
		itr := sels.Iterator()
		sel := itr.Next() + offset
		for oldIdx, newIdx := 0, 0; oldIdx < v.length; oldIdx++ {
			if oldIdx != int(sel) {
				vs[newIdx] = vs[oldIdx]
				newIdx++
			} else {
				if !itr.HasNext() {
					for idx := oldIdx + 1; idx < v.length; idx++ {
						vs[newIdx] = vs[idx]
						newIdx++
					}
					break
				}
				sel = itr.Next() + offset
			}
		}
		nulls.FilterByMaskInPlaceWithOffset(&v.nsp, sels, true, offset)
		v.length -= length
	}
}

// shuffleWithSortedProof gathers into independent storage while preserving an
// existing ordering proof only when the selection is nondecreasing.
func shuffleWithSortedProof[T types.FixedSizeT](src, dst []T, sels []int64, sorted bool) (bool, bool) {
	if !sorted {
		shuffle.FixedLengthShuffle(src, dst, sels)
		return false, false
	}
	if len(sels) == 0 {
		return true, true
	}
	start := sels[0]
	k := 1
	for k < len(sels) && sels[k] == sels[k-1]+1 {
		k++
	}
	copy(dst[:k], src[start:start+int64(k)])
	for i := k; i < len(sels); i++ {
		dst[i] = src[sels[i]]
		if sels[i] < sels[i-1] {
			shuffle.FixedLengthShuffle(src, dst[i+1:], sels[i+1:])
			return false, false
		}
	}
	return true, k == len(sels) && start == 0
}

// shuffleFixedNoTypeCheck is always used after type check. and we can use ToSliceNoTypeCheck here.
func shuffleFixedNoTypeCheck[T types.FixedSizeT](v *Vector, sels []int64, mp *mpool.MPool) error {
	sz := v.typ.TypeSize()
	olddata := v.data[:v.length*sz]
	ns := len(sels)
	var vs []T
	ToFixedColNoTypeCheck(v, &vs)
	data, err := v.alloc(mp, ns*v.GetType().TypeSize())
	if err != nil {
		return err
	}
	ws := util.UnsafeSliceCastToLength[T](data, ns)

	preserved, _ := shuffleWithSortedProof(vs, ws, sels, v.sorted)
	if err := v.remapShuffleBitmaps(sels, mp); err != nil {
		mp.Free(data)
		return err
	}
	mp.Free(olddata)
	v.data = data
	v.length = ns
	v.sorted = preserved
	return nil
}

// remapShuffleBitmaps preserves Shuffle's arbitrary-selection semantics.
func (v *Vector) remapShuffleBitmaps(sels []int64, _ *mpool.MPool) error {
	return nulls.Filter(&v.nsp, sels, false)
}

// shuffleFixedNoTypeCheckWithBuf permutes elements using a reusable scratch
// buffer instead of allocating a new data buffer. Only valid when
// len(sels) == v.length (caller checks).
func shuffleFixedNoTypeCheckWithBuf[T types.FixedSizeT](v *Vector, sels []int64, buf *[]byte) error {
	sz := v.typ.TypeSize()
	ns := len(sels)
	needed := ns * sz

	if cap(*buf) < needed {
		*buf = make([]byte, needed)
	} else {
		*buf = (*buf)[:needed]
	}

	var vs []T
	ToFixedColNoTypeCheck(v, &vs)
	ws := util.UnsafeSliceCastToLength[T](*buf, ns)

	preserved, identityProven := shuffleWithSortedProof(vs, ws, sels, v.sorted)
	// Remap the nulls first: it can fail allocating, and v is still intact.
	if err := nulls.Filter(&v.nsp, sels, false); err != nil {
		return err
	}
	if !identityProven || byteSlicesOverlap(v.data[:needed], *buf) {
		copy(v.data[:needed], *buf)
	}
	v.length = ns
	v.sorted = preserved
	return nil
}

func vecToString[T types.FixedSizeT](v *Vector) string {
	col := MustFixedColWithTypeCheck[T](v)
	if len(col) == 1 {
		if v.nsp.Contains(0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", col[0])
		}
	}
	if v.nsp.Any() {
		return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	} else {
		return fmt.Sprintf("%v", col)
	}
}

// Window returns a read-only reference vector (nil pool) to the half-open row
// range [start, end) of v. It allocates nothing from a pool and shares v's
// memory, so it is valid only as long as v is unchanged (for an operator's
// input, until the next Next). Free only drops its references. A caller that
// needs to modify the rows or keep them longer copies them into a vector it
// owns with CloneWindow or CloneWindowTo.
func (v *Vector) Window(start, end int) (*Vector, error) {
	return v.window(start, end, false)
}

// WindowByLogicalRows is Window in a caller-owned logical row domain. A
// non-empty const vector without row-specific metadata may broadcast its
// single physical value beyond Length; ordinary vectors keep the strict
// physical range check used by Window.
func (v *Vector) WindowByLogicalRows(start, end int) (*Vector, error) {
	return v.window(start, end, true)
}

func (v *Vector) window(start, end int, logicalRows bool) (*Vector, error) {
	if !v.validWindowRange(start, end, logicalRows) {
		return nil, moerr.NewInvalidInputNoCtx("invalid vector window")
	}
	return v.readOnlyWindow(start, end), nil
}

// WindowInto is Window into a descriptor the caller already holds: w becomes a
// read-only reference to [start, end) of v's memory without allocating a new
// Vector. w must be read-only (a reference or a zero Vector) and must not own
// its nulls bitmap (a read-only vector can, e.g. Arrow validity); whatever it
// referenced before is dropped.
func (v *Vector) WindowInto(w *Vector, start, end int) error {
	if !w.IsReadOnly() || !w.nsp.IsReadOnly() {
		return moerr.NewInternalErrorNoCtx("vector window target owns memory")
	}
	if !v.validWindowRange(start, end, false) {
		return moerr.NewInvalidInputNoCtx("invalid vector window")
	}
	v.readOnlyWindowInto(w, start, end)
	return nil
}

// readOnlyWindow is a read-only reference to [start, end) of v's memory. Row
// metadata is copied.
func (v *Vector) readOnlyWindow(start, end int) *Vector {
	w := &Vector{}
	v.readOnlyWindowInto(w, start, end)
	return w
}

func (v *Vector) readOnlyWindowInto(w *Vector, start, end int) {
	*w = Vector{typ: v.typ, class: v.class, length: end - start, sorted: v.sorted}
	w.nsp.InitView(&v.nsp, start, end)
	if v.IsConst() {
		if !v.IsConstNull() {
			w.data = v.data
		}
		w.area = v.area
		return
	}
	if start != end {
		w.data = v.data[start*v.typ.TypeSize() : end*v.typ.TypeSize()]
	}
	if v.typ.IsVarlen() {
		w.area = v.area
	}
}

func (v *Vector) validWindowRange(start, end int, logicalRows bool) bool {
	if start < 0 || end < start {
		return false
	}
	if !logicalRows {
		return end <= v.Length()
	}
	return v.CoversLogicalRows(start, end-start)
}

// CoversLogicalRows reports whether [start, start+rows) is backed by physical
// rows or by scalar const broadcast semantics.
func (v *Vector) CoversLogicalRows(start, rows int) bool {
	if v == nil || start < 0 || rows < 0 {
		return false
	}
	if start <= v.Length() && rows <= v.Length()-start {
		return true
	}
	if !v.IsConst() {
		return false
	}
	if rows == 0 {
		return true
	}
	// Nullness is a scalar const property.
	return v.Length() > 0
}

// copyWindowBitmaps copies the null bits of [start, end) into w.
func (v *Vector) copyWindowBitmaps(w *Vector, start, end int, mp *mpool.MPool) error {
	length := end - start
	if v.nsp.CountRange(uint64(start), uint64(end)) > 0 {
		if err := w.PreExtendNulls(length, mp); err != nil {
			return err
		}
		return nulls.Range(&v.nsp, uint64(start), uint64(end), uint64(start), &w.nsp, mp)
	}
	return nil
}

// CloneWindow copies [start, end) into a new vector allocated from mp, which
// becomes the copy's pool. It is then safe to destroy the original.
func (v *Vector) CloneWindow(start, end int, mp *mpool.MPool) (*Vector, error) {
	if start == end {
		return NewVec(v.typ, mp), nil
	}
	if end > v.Length() {
		panic(fmt.Sprintf("CloneWindow end %d >= length %d", end, v.Length()))
	}
	w := NewVec(v.typ, mp)
	if err := v.CloneWindowTo(w, start, end, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	return w, nil
}

// CloneWindowTo copies [start, end) into w; mp must be w's pool.
func (v *Vector) CloneWindowTo(w *Vector, start, end int, mp *mpool.MPool) error {
	if err := w.checkMP(mp); err != nil {
		return err
	}
	if start == end {
		return nil
	}
	if err := v.copyWindowBitmaps(w, start, end, mp); err != nil {
		return err
	}
	if v.IsConstNull() {
		w.class = CONSTANT
		w.length = end - start
		// Keep owned capacity available for reuse and eventual Free.
		w.data = w.data[:0]
		return nil
	} else if v.IsConst() {
		if v.typ.IsVarlen() {
			w.class = CONSTANT
			return SetConstBytes(w, v.GetBytesAt(0), end-start, mp)
		}
		if err := w.PreExtend(1, mp); err != nil {
			return err
		}
		copy(w.data, v.data)
		w.class = v.class
		w.length = end - start
		w.sorted = v.sorted
		return nil
	}
	length := (end - start) * v.typ.TypeSize()
	err := w.PreExtend(end-start, mp)
	if err != nil {
		return err
	}
	w.length = end - start
	if v.GetType().IsVarlen() {
		// Expose the proof only after every destination descriptor has been
		// independently materialized. An allocation failure can leave a
		// partially initialized logical range.
		var vCol, wCol []types.Varlena
		ToSliceNoTypeCheck(v, &vCol)
		ToSliceNoTypeCheck(w, &wCol)
		for i := start; i < end; i++ {
			if v.nsp.Contains(uint64(i)) {
				wCol[i-start] = types.Varlena{}
				continue
			}
			bs := vCol[i].GetByteSlice(v.area)
			err = BuildVarlenaFromByteSlice(w, &wCol[i-start], &bs, mp)
			if err != nil {
				return err
			}
		}
	} else {
		tlen := v.typ.TypeSize()
		copy(w.data[:length], v.data[start*tlen:end*tlen])
	}
	return nil
}

// GetSumValue returns the sum value of the vector.
// if the length is 0 or all null or the vector is not numeric, return false
func (v *Vector) GetSumValue() (ok bool, sumv []byte) {
	if v.Length() == 0 || v.AllNull() || !v.typ.IsNumeric() {
		return
	}
	if v.typ.IsDecimal() && v.typ.Oid != types.T_decimal64 {
		return
	}
	ok = true
	switch v.typ.Oid {
	case types.T_bit:
		sumVal := IntegerGetSum[uint64, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_int8:
		sumVal := IntegerGetSum[int8, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int16:
		sumVal := IntegerGetSum[int16, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int32:
		sumVal := IntegerGetSum[int32, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_int64:
		sumVal := IntegerGetSum[int64, int64](v)
		sumv = types.EncodeInt64(&sumVal)
	case types.T_uint8:
		sumVal := IntegerGetSum[uint8, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint16:
		sumVal := IntegerGetSum[uint16, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint32:
		sumVal := IntegerGetSum[uint32, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_uint64:
		sumVal := IntegerGetSum[uint64, uint64](v)
		sumv = types.EncodeUint64(&sumVal)
	case types.T_float32:
		sumVal := FloatGetSum[float32](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_float64:
		sumVal := FloatGetSum[float64](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_bf16:
		sumVal := LowPrecFloatGetSum[types.BF16](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_float16:
		sumVal := LowPrecFloatGetSum[types.Float16](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_float8:
		sumVal := LowPrecFloatGetSum[types.Float8](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_float4:
		sumVal := LowPrecFloatGetSum[types.Float4](v)
		sumv = types.EncodeFloat64(&sumVal)
	case types.T_decimal64:
		sumVal := Decimal64GetSum(v)
		sumv = types.EncodeDecimal64(&sumVal)
	default:
		panic(fmt.Sprintf("unsupported type %s", v.GetType().String()))
	}
	return
}

// GetMinMaxValue returns the min and max value of the vector.
// if the length is 0 or all null, return false
func (v *Vector) GetMinMaxValue() (ok bool, minv, maxv []byte) {
	if v.Length() == 0 || v.AllNull() {
		return
	}
	ok = true
	switch v.typ.Oid {
	case types.T_bool:
		var minVal, maxVal bool
		col := MustFixedColNoTypeCheck[bool](v)
		if v.HasNull() {
			first := true
			for i := range col {
				if v.IsNull(uint64(i)) {
					continue
				}
				value := col[i]
				if first {
					minVal, maxVal = value, value
					first = false
				} else if value != minVal {
					minVal, maxVal = false, true
					break
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for _, value := range col[1:] {
				if value != minVal {
					minVal, maxVal = false, true
					break
				}
			}
		}
		minv = types.EncodeBool(&minVal)
		maxv = types.EncodeBool(&maxVal)

	case types.T_bit:
		minVal, maxVal := IntegerGetMinAndMax[uint64](v)
		minv = types.EncodeUint64(&minVal)
		maxv = types.EncodeUint64(&maxVal)

	case types.T_int8:
		minVal, maxVal := IntegerGetMinAndMax[int8](v)
		minv = types.EncodeInt8(&minVal)
		maxv = types.EncodeInt8(&maxVal)

	case types.T_int16:
		minVal, maxVal := IntegerGetMinAndMax[int16](v)
		minv = types.EncodeInt16(&minVal)
		maxv = types.EncodeInt16(&maxVal)

	case types.T_int32:
		minVal, maxVal := IntegerGetMinAndMax[int32](v)
		minv = types.EncodeInt32(&minVal)
		maxv = types.EncodeInt32(&maxVal)

	case types.T_int64:
		minVal, maxVal := IntegerGetMinAndMax[int64](v)
		minv = types.EncodeInt64(&minVal)
		maxv = types.EncodeInt64(&maxVal)

	case types.T_uint8:
		minVal, maxVal := IntegerGetMinAndMax[uint8](v)
		minv = types.EncodeUint8(&minVal)
		maxv = types.EncodeUint8(&maxVal)

	case types.T_uint16:
		minVal, maxVal := IntegerGetMinAndMax[uint16](v)
		minv = types.EncodeUint16(&minVal)
		maxv = types.EncodeUint16(&maxVal)

	case types.T_uint32:
		minVal, maxVal := IntegerGetMinAndMax[uint32](v)
		minv = types.EncodeUint32(&minVal)
		maxv = types.EncodeUint32(&maxVal)

	case types.T_uint64:
		minVal, maxVal := IntegerGetMinAndMax[uint64](v)
		minv = types.EncodeUint64(&minVal)
		maxv = types.EncodeUint64(&maxVal)

	case types.T_float32:
		minVal, maxVal, hasComparableValue := FloatGetMinAndMax[float32](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFloat32(&minVal)
		maxv = types.EncodeFloat32(&maxVal)

	case types.T_float64:
		minVal, maxVal, hasComparableValue := FloatGetMinAndMax[float64](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFloat64(&minVal)
		maxv = types.EncodeFloat64(&maxVal)

	case types.T_bf16:
		minVal, maxVal, hasComparableValue := LowPrecFloatGetMinAndMax[types.BF16](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_float16:
		minVal, maxVal, hasComparableValue := LowPrecFloatGetMinAndMax[types.Float16](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_float8:
		minVal, maxVal, hasComparableValue := LowPrecFloatGetMinAndMax[types.Float8](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_float4:
		minVal, maxVal, hasComparableValue := LowPrecFloatGetMinAndMax[types.Float4](v)
		if !hasComparableValue {
			ok = false
			return
		}
		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_date:
		minVal, maxVal := IntegerGetMinAndMax[types.Date](v)
		minv = types.EncodeDate(&minVal)
		maxv = types.EncodeDate(&maxVal)

	case types.T_year:
		minVal, maxVal := IntegerGetMinAndMax[types.MoYear](v)
		minv = types.EncodeMoYear(&minVal)
		maxv = types.EncodeMoYear(&maxVal)

	case types.T_datetime:
		minVal, maxVal := IntegerGetMinAndMax[types.Datetime](v)
		minv = types.EncodeDatetime(&minVal)
		maxv = types.EncodeDatetime(&maxVal)

	case types.T_time:
		minVal, maxVal := IntegerGetMinAndMax[types.Time](v)
		minv = types.EncodeTime(&minVal)
		maxv = types.EncodeTime(&maxVal)

	case types.T_timestamp:
		minVal, maxVal := IntegerGetMinAndMax[types.Timestamp](v)
		minv = types.EncodeTimestamp(&minVal)
		maxv = types.EncodeTimestamp(&maxVal)

	case types.T_enum:
		minVal, maxVal := IntegerGetMinAndMax[types.Enum](v)
		minv = types.EncodeEnum(&minVal)
		maxv = types.EncodeEnum(&maxVal)

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		var minVal, maxVal types.Decimal64
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Less(minVal) {
						minVal = col[i]
					}
					if maxVal.Less(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Less(minVal) {
					minVal = col[i]
				}
				if maxVal.Less(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeDecimal64(&minVal)
		maxv = types.EncodeDecimal64(&maxVal)

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		var minVal, maxVal types.Decimal128
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Less(minVal) {
						minVal = col[i]
					}
					if maxVal.Less(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Less(minVal) {
					minVal = col[i]
				}
				if maxVal.Less(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeDecimal128(&minVal)
		maxv = types.EncodeDecimal128(&maxVal)

	case types.T_decimal256:
		col := MustFixedColNoTypeCheck[types.Decimal256](v)
		var minVal, maxVal types.Decimal256
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Less(minVal) {
						minVal = col[i]
					}
					if maxVal.Less(col[i]) {
						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Less(minVal) {
					minVal = col[i]
				}
				if maxVal.Less(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeDecimal256(&minVal)
		maxv = types.EncodeDecimal256(&maxVal)

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		var minVal, maxVal types.TS
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].LT(&minVal) {
						minVal = col[i]
					}
					if maxVal.LT(&col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].LT(&minVal) {
					minVal = col[i]
				}
				if maxVal.LT(&col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		var minVal, maxVal types.Uuid
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].Lt(minVal) {
						minVal = col[i]
					}
					if maxVal.Lt(col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].Lt(minVal) {
					minVal = col[i]
				}
				if maxVal.Lt(col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeUuid(&minVal)
		maxv = types.EncodeUuid(&maxVal)

	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		var minVal, maxVal types.Rowid
		if v.HasNull() {
			first := true
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					if col[i].LT(&minVal) {
						minVal = col[i]
					}
					if maxVal.LT(&col[i]) {

						maxVal = col[i]
					}
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				if col[i].LT(&minVal) {
					minVal = col[i]
				}
				if maxVal.LT(&col[i]) {
					maxVal = col[i]
				}
			}
		}

		minv = types.EncodeFixed(minVal)
		maxv = types.EncodeFixed(maxVal)

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		minv, maxv = VarlenGetMinMax(v)
	case types.T_array_float32:
		// Zone map Comparator should be consistent with the SQL Comparator for Array.
		// Hence, we are not using bytesComparator for Array.
		// [Update]: We won't be using the Min and Max inside the ZM. Vector index is going to be handled
		// outside the zonemap via indexing techniques like HNSW etc.
		// For Array ZM, we will mostly make it uninitialized or set theoretical min and max.
		_minv, _maxv := ArrayGetMinMax[float32](v)
		minv = types.ArrayToBytes[float32](_minv)
		maxv = types.ArrayToBytes[float32](_maxv)
	case types.T_array_float64:
		_minv, _maxv := ArrayGetMinMax[float64](v)
		minv = types.ArrayToBytes[float64](_minv)
		maxv = types.ArrayToBytes[float64](_maxv)
	case types.T_array_bf16:
		_minv, _maxv := ArrayElementGetMinMax[types.BF16](v)
		minv = types.ArrayToBytes[types.BF16](_minv)
		maxv = types.ArrayToBytes[types.BF16](_maxv)
	case types.T_array_float16:
		_minv, _maxv := ArrayElementGetMinMax[types.Float16](v)
		minv = types.ArrayToBytes[types.Float16](_minv)
		maxv = types.ArrayToBytes[types.Float16](_maxv)
	case types.T_array_int8:
		_minv, _maxv := ArrayElementGetMinMax[int8](v)
		minv = types.ArrayToBytes[int8](_minv)
		maxv = types.ArrayToBytes[int8](_maxv)
	case types.T_array_uint8:
		_minv, _maxv := ArrayElementGetMinMax[uint8](v)
		minv = types.ArrayToBytes[uint8](_minv)
		maxv = types.ArrayToBytes[uint8](_maxv)
	case types.T_array_float8, types.T_array_float4:
		// Block-scaled cells have no value order and vector zonemaps are not used for
		// pruning (ZM.IsArray); flush still records byte-wise bounds.
		minv, maxv = VarlenGetMinMax(v)
	default:
		panic(fmt.Sprintf("unsupported type %s", v.GetType().String()))
	}
	return
}

func compareOrderedRows[T cmp.Ordered](v *Vector, left, right int) int {
	return cmp.Compare(GetFixedAtNoTypeCheck[T](v, left), GetFixedAtNoTypeCheck[T](v, right))
}

// compareFloatRows orders two low-precision float rows (bf16/float16/float8/float4)
// by widened float VALUE. Raw bits do not order floats (the sign bit inverts), so
// the values are compared through ToFloat32 with cmp.Compare (NaN-aware ordering).
func compareFloatRows[T types.LowPrecFloat](v *Vector, left, right int) int {
	return cmp.Compare(
		GetFixedAtNoTypeCheck[T](v, left).ToFloat32(),
		GetFixedAtNoTypeCheck[T](v, right).ToFloat32(),
	)
}

func supportsInplaceSort(oid types.T) bool {
	switch oid {
	case types.T_bool, types.T_bit,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_bf16, types.T_float16, types.T_float8, types.T_float4,
		types.T_date, types.T_year, types.T_datetime, types.T_time, types.T_timestamp, types.T_enum,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_TS, types.T_uuid, types.T_Rowid,
		types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16,
		types.T_array_float16, types.T_array_int8, types.T_array_uint8:
		return true
	default:
		return false
	}
}

func compareVectorRows(v *Vector, left, right int) int {
	switch v.typ.Oid {
	case types.T_bool:
		leftValue, rightValue := GetFixedAtNoTypeCheck[bool](v, left), GetFixedAtNoTypeCheck[bool](v, right)
		if leftValue == rightValue {
			return 0
		}
		if !leftValue {
			return -1
		}
		return 1
	case types.T_bit:
		return compareOrderedRows[uint64](v, left, right)
	case types.T_int8:
		return compareOrderedRows[int8](v, left, right)
	case types.T_int16:
		return compareOrderedRows[int16](v, left, right)
	case types.T_int32:
		return compareOrderedRows[int32](v, left, right)
	case types.T_int64:
		return compareOrderedRows[int64](v, left, right)
	case types.T_uint8:
		return compareOrderedRows[uint8](v, left, right)
	case types.T_uint16:
		return compareOrderedRows[uint16](v, left, right)
	case types.T_uint32:
		return compareOrderedRows[uint32](v, left, right)
	case types.T_uint64:
		return compareOrderedRows[uint64](v, left, right)
	case types.T_float32:
		return compareOrderedRows[float32](v, left, right)
	case types.T_float64:
		return compareOrderedRows[float64](v, left, right)
	case types.T_bf16:
		return compareFloatRows[types.BF16](v, left, right)
	case types.T_float16:
		return compareFloatRows[types.Float16](v, left, right)
	case types.T_float8:
		return compareFloatRows[types.Float8](v, left, right)
	case types.T_float4:
		return compareFloatRows[types.Float4](v, left, right)
	case types.T_date:
		return compareOrderedRows[types.Date](v, left, right)
	case types.T_year:
		return compareOrderedRows[types.MoYear](v, left, right)
	case types.T_datetime:
		return compareOrderedRows[types.Datetime](v, left, right)
	case types.T_time:
		return compareOrderedRows[types.Time](v, left, right)
	case types.T_timestamp:
		return compareOrderedRows[types.Timestamp](v, left, right)
	case types.T_enum:
		return compareOrderedRows[types.Enum](v, left, right)
	case types.T_decimal64:
		return GetFixedAtNoTypeCheck[types.Decimal64](v, left).Compare(
			GetFixedAtNoTypeCheck[types.Decimal64](v, right))
	case types.T_decimal128:
		return GetFixedAtNoTypeCheck[types.Decimal128](v, left).Compare(
			GetFixedAtNoTypeCheck[types.Decimal128](v, right))
	case types.T_decimal256:
		return GetFixedAtNoTypeCheck[types.Decimal256](v, left).Compare(
			GetFixedAtNoTypeCheck[types.Decimal256](v, right))
	case types.T_TS:
		leftValue, rightValue := GetFixedAtNoTypeCheck[types.TS](v, left), GetFixedAtNoTypeCheck[types.TS](v, right)
		if leftValue.LT(&rightValue) {
			return -1
		}
		if rightValue.LT(&leftValue) {
			return 1
		}
		return 0
	case types.T_uuid:
		return GetFixedAtNoTypeCheck[types.Uuid](v, left).Compare(
			GetFixedAtNoTypeCheck[types.Uuid](v, right))
	case types.T_Rowid:
		leftValue, rightValue := GetFixedAtNoTypeCheck[types.Rowid](v, left), GetFixedAtNoTypeCheck[types.Rowid](v, right)
		if leftValue.LT(&rightValue) {
			return -1
		}
		if rightValue.LT(&leftValue) {
			return 1
		}
		return 0
	case types.T_array_float32:
		return types.ArrayCompare[float32](
			types.BytesToArray[float32](v.GetBytesAt(left)), types.BytesToArray[float32](v.GetBytesAt(right)))
	case types.T_array_float64:
		return types.ArrayCompare[float64](
			types.BytesToArray[float64](v.GetBytesAt(left)), types.BytesToArray[float64](v.GetBytesAt(right)))
	case types.T_array_bf16:
		return types.ArrayElementCompare[types.BF16](
			types.BytesToArray[types.BF16](v.GetBytesAt(left)), types.BytesToArray[types.BF16](v.GetBytesAt(right)))
	case types.T_array_float16:
		return types.ArrayElementCompare[types.Float16](
			types.BytesToArray[types.Float16](v.GetBytesAt(left)), types.BytesToArray[types.Float16](v.GetBytesAt(right)))
	case types.T_array_int8:
		return types.ArrayElementCompare[int8](
			types.BytesToArray[int8](v.GetBytesAt(left)), types.BytesToArray[int8](v.GetBytesAt(right)))
	case types.T_array_uint8:
		return types.ArrayElementCompare[uint8](
			types.BytesToArray[uint8](v.GetBytesAt(left)), types.BytesToArray[uint8](v.GetBytesAt(right)))
	case types.T_array_float8, types.T_array_float4:
		return types.CompareBlockScaledFromBytes(v.GetBytesAt(left), v.GetBytesAt(right), false)
	default:
		return bytes.Compare(v.GetBytesAt(left), v.GetBytesAt(right))
	}
}

type vectorMetadataSorter struct {
	vector  *Vector
	varlena []types.Varlena
}

func (s vectorMetadataSorter) Len() int { return s.vector.length }

func (s vectorMetadataSorter) Less(left, right int) bool {
	leftNull, rightNull := s.vector.IsNull(uint64(left)), s.vector.IsNull(uint64(right))
	if leftNull != rightNull {
		return leftNull
	}
	if !leftNull {
		if order := compareVectorRows(s.vector, left, right); order != 0 {
			return order < 0
		}
	}
	return false
}

func setBitmapRow(value *bitmap.Bitmap, row int, enabled bool) {
	value.Remove(uint64(row))
	if enabled {
		value.Add(uint64(row))
	}
}

func (s vectorMetadataSorter) Swap(left, right int) {
	if left == right {
		return
	}
	if s.varlena != nil {
		s.varlena[left], s.varlena[right] = s.varlena[right], s.varlena[left]
	} else {
		leftBytes := s.vector.GetRawBytesAt(left)
		rightBytes := s.vector.GetRawBytesAt(right)
		for i := range leftBytes {
			leftBytes[i], rightBytes[i] = rightBytes[i], leftBytes[i]
		}
	}
	leftNull, rightNull := s.vector.IsNull(uint64(left)), s.vector.IsNull(uint64(right))
	setBitmapRow(s.vector.nsp.GetBitmap(), left, rightNull)
	setBitmapRow(s.vector.nsp.GetBitmap(), right, leftNull)
}

func (v *Vector) sortRowsEquivalent(left, right int) bool {
	if v.IsNull(uint64(left)) != v.IsNull(uint64(right)) {
		return false
	}
	return v.IsNull(uint64(left)) || compareVectorRows(v, left, right) == 0
}

func (v *Vector) copySortedRow(destination, source int, varlena []types.Varlena) {
	if destination == source {
		return
	}
	if varlena != nil {
		varlena[destination] = varlena[source]
	} else {
		copy(v.GetRawBytesAt(destination), v.GetRawBytesAt(source))
	}
	setBitmapRow(v.nsp.GetBitmap(), destination, v.IsNull(uint64(source)))
}

func (v *Vector) inplaceSortRowMetadata(compact bool) bool {
	if !v.nsp.Any() ||
		v.IsConst() || !supportsInplaceSort(v.typ.Oid) {
		return false
	}

	var varlena []types.Varlena
	if v.typ.IsVarlen() {
		varlena = MustFixedColNoTypeCheck[types.Varlena](v)
	}
	// Empty external-storage bitmaps need no capacity: swaps only remove false
	// bits from them. Expanding those bitmaps would panic even though there is no
	// row metadata to move (runtime-filter vectors commonly use this layout).
	if !v.nsp.GetBitmap().IsEmpty() {
		if err := v.nsp.GetBitmap().TryExpandWithSize(v.length); err != nil {
			panic(err)
		}
	}
	sort.Stable(vectorMetadataSorter{vector: v, varlena: varlena})
	newLength := v.length
	if compact && v.length > 1 {
		write := 1
		for read := 1; read < v.length; read++ {
			if v.sortRowsEquivalent(write-1, read) {
				continue
			}
			v.copySortedRow(write, read, varlena)
			write++
		}
		newLength = write
	}
	if newLength < v.length {
		v.nsp.GetBitmap().RemoveRange(uint64(newLength), uint64(v.length))
		v.length = newLength
	}
	v.sorted = true
	return true
}

// sortLowPrecFloatCol sorts a low-precision float column (bf16/float16/float8/float4)
// by widened float value; raw uint bits do not order floats (the sign bit inverts).
func sortLowPrecFloatCol[T types.LowPrecFloat](v *Vector) {
	col := MustFixedColNoTypeCheck[T](v)
	slices.SortFunc(col, func(a, b T) int {
		return cmp.Compare(a.ToFloat32(), b.ToFloat32())
	})
}

// sortAndCompactLowPrecFloatCol sorts by float value then removes value-duplicates
// in place, returning the compacted length. Equality is by ToFloat32 (so +0/-0
// collapse and NaN never compacts), matching the native float32
// slices.Sort+Compact path.
func sortAndCompactLowPrecFloatCol[T types.LowPrecFloat](v *Vector) int {
	col := MustFixedColNoTypeCheck[T](v)
	slices.SortFunc(col, func(a, b T) int {
		return cmp.Compare(a.ToFloat32(), b.ToFloat32())
	})
	return len(slices.CompactFunc(col, func(a, b T) bool {
		return a.ToFloat32() == b.ToFloat32()
	}))
}

// InplaceSortAndCompact prepares writable data and row metadata before sorting.
// A leased varlen payload can remain readonly: only descriptors are reordered.
func (v *Vector) InplaceSortAndCompact(mp *mpool.MPool) error {
	if !supportsInplaceSort(v.typ.Oid) {
		return nil
	}
	if v.IsConst() || v.length <= 1 {
		v.SetSorted(true)
		return nil
	}
	{
		if err := v.checkMP(mp); err != nil {
			return err
		}
		if !v.nsp.IsEmpty() {
			if err := v.ensureNullCapacity(v.length); err != nil {
				return err
			}
		}
	}
	if v.inplaceSortRowMetadata(true) {
		return nil
	}
	publishCompactedLength := func(length int) {
		// Compact already wrote the result into the descriptor prefix. Do not
		// re-append it: that can require COW/admission, but this API has no pool.
		v.SetLength(length)
		v.nsp.Reset()
		v.sorted = true
	}

	switch v.GetType().Oid {
	case types.T_bool:
		col := MustFixedColNoTypeCheck[bool](v)
		slices.SortFunc(col, func(a, b bool) int {
			if a == b {
				return 0
			}
			if !a { // false sorts before true
				return -1
			}
			return 1
		})
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_bf16:
		if n := sortAndCompactLowPrecFloatCol[types.BF16](v); n != v.length {
			publishCompactedLength(n)
		}
	case types.T_float16:
		if n := sortAndCompactLowPrecFloatCol[types.Float16](v); n != v.length {
			publishCompactedLength(n)
		}
	case types.T_float8:
		if n := sortAndCompactLowPrecFloatCol[types.Float8](v); n != v.length {
			publishCompactedLength(n)
		}
	case types.T_float4:
		if n := sortAndCompactLowPrecFloatCol[types.Float4](v); n != v.length {
			publishCompactedLength(n)
		}

	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		slices.SortFunc(col, func(a, b types.Decimal64) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Decimal64) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		slices.SortFunc(col, func(a, b types.Decimal128) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Decimal128) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_decimal256:
		col := MustFixedColNoTypeCheck[types.Decimal256](v)
		slices.SortFunc(col, func(a, b types.Decimal256) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Decimal256) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		slices.SortFunc(col, func(a, b types.TS) int {
			if a.LT(&b) {
				return -1
			}
			if b.LT(&a) {
				return 1
			}
			return 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.TS) bool {
			return a.Equal(&b)
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		slices.SortFunc(col, func(a, b types.Uuid) int {
			return a.Compare(b)
		})
		newCol := slices.CompactFunc(col, func(a, b types.Uuid) bool {
			return a.Compare(b) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}
	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		slices.SortFunc(col, func(a, b types.Rowid) int {
			if a.LT(&b) {
				return -1
			}
			if b.LT(&a) {
				return 1
			}
			return 0
		})
		newCol := slices.CompactFunc(col, func(a, b types.Rowid) bool {
			return a.EQ(&b)
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return bytes.Compare(a.GetByteSlice(area), b.GetByteSlice(area))
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return bytes.Equal(a.GetByteSlice(area), b.GetByteSlice(area))
		})

		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_array_float32:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&a, area),
				types.GetArray[float32](&b, area),
			)
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&a, area),
				types.GetArray[float32](&b, area),
			) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}

	case types.T_array_float64:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&a, area),
				types.GetArray[float64](&b, area),
			)
		})
		newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&a, area),
				types.GetArray[float64](&b, area),
			) == 0
		})
		if len(newCol) != len(col) {
			publishCompactedLength(len(newCol))
		}
	case types.T_array_bf16:
		inplaceSortAndCompactArrayElement[types.BF16](v, publishCompactedLength)
	case types.T_array_float16:
		inplaceSortAndCompactArrayElement[types.Float16](v, publishCompactedLength)
	case types.T_array_int8:
		inplaceSortAndCompactArrayElement[int8](v, publishCompactedLength)
	case types.T_array_uint8:
		inplaceSortAndCompactArrayElement[uint8](v, publishCompactedLength)
	default:
		return nil
	}
	// Sorting happened even when compaction did not change the vector length.
	// Keep the metadata invariant aligned with the physical ordering.
	v.SetSorted(true)
	return nil
}

// inplaceSortAndCompactArrayElement sorts+dedups a narrow-typed vector using the
// float32-bridged comparator (so bf16/f16 order by value, not by raw bits).
func inplaceSortAndCompactArrayElement[T types.ArrayElement](v *Vector, publishCompactedLength func(int)) {
	col, area := MustVarlenaRawData(v)
	sort.Slice(col, func(i, j int) bool {
		return types.ArrayElementCompare[T](
			types.GetArray[T](&col[i], area),
			types.GetArray[T](&col[j], area),
		) < 0
	})
	newCol := slices.CompactFunc(col, func(a, b types.Varlena) bool {
		return types.ArrayElementCompare[T](
			types.GetArray[T](&a, area),
			types.GetArray[T](&b, area),
		) == 0
	})
	if len(newCol) != len(col) {
		publishCompactedLength(len(newCol))
	}
}

// InplaceSort orders every supported vector type without compacting duplicate
// values and marks that order in the vector metadata. Unsupported types remain
// untouched and unflagged.
func (v *Vector) InplaceSort(mp *mpool.MPool) error {
	if !supportsInplaceSort(v.typ.Oid) {
		return nil
	}
	if v.IsConst() || v.length <= 1 {
		v.SetSorted(true)
		return nil
	}
	{
		if err := v.checkMP(mp); err != nil {
			return err
		}
		if !v.nsp.IsEmpty() {
			if err := v.ensureNullCapacity(v.length); err != nil {
				return err
			}
		}
	}
	if v.inplaceSortRowMetadata(false) {
		return nil
	}
	switch v.GetType().Oid {
	case types.T_bool:
		col := MustFixedColNoTypeCheck[bool](v)
		slices.SortFunc(col, func(a, b bool) int {
			if a == b {
				return 0
			}
			if !a { // false sorts before true
				return -1
			}
			return 1
		})

	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)

	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](v)
		slices.Sort(col)

	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](v)
		slices.Sort(col)

	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](v)
		slices.Sort(col)

	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](v)
		slices.Sort(col)

	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](v)
		slices.Sort(col)

	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](v)
		slices.Sort(col)

	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](v)
		slices.Sort(col)

	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)

	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](v)
		slices.Sort(col)

	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](v)
		slices.Sort(col)

	case types.T_bf16:
		sortLowPrecFloatCol[types.BF16](v)
	case types.T_float16:
		sortLowPrecFloatCol[types.Float16](v)
	case types.T_float8:
		sortLowPrecFloatCol[types.Float8](v)
	case types.T_float4:
		sortLowPrecFloatCol[types.Float4](v)

	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](v)
		slices.Sort(col)

	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](v)
		slices.Sort(col)

	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](v)
		slices.Sort(col)

	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](v)
		slices.Sort(col)

	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](v)
		slices.Sort(col)

	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](v)
		slices.Sort(col)

	case types.T_decimal64:
		col := MustFixedColNoTypeCheck[types.Decimal64](v)
		slices.SortFunc(col, func(a, b types.Decimal64) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})

	case types.T_decimal128:
		col := MustFixedColNoTypeCheck[types.Decimal128](v)
		slices.SortFunc(col, func(a, b types.Decimal128) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})

	case types.T_decimal256:
		col := MustFixedColNoTypeCheck[types.Decimal256](v)
		slices.SortFunc(col, func(a, b types.Decimal256) int {
			if a.Less(b) {
				return -1
			}
			if b.Less(a) {
				return 1
			}
			return 0
		})

	case types.T_TS:
		col := MustFixedColNoTypeCheck[types.TS](v)
		slices.SortFunc(col, func(a, b types.TS) int {
			if a.LT(&b) {
				return -1
			}
			if b.LT(&a) {
				return 1
			}
			return 0
		})

	case types.T_uuid:
		col := MustFixedColNoTypeCheck[types.Uuid](v)
		slices.SortFunc(col, func(a, b types.Uuid) int {
			return a.Compare(b)
		})

	case types.T_Rowid:
		col := MustFixedColNoTypeCheck[types.Rowid](v)
		slices.SortFunc(col, func(a, b types.Rowid) int {
			if a.LT(&b) {
				return -1
			}
			if b.LT(&a) {
				return 1
			}
			return 0
		})

	case types.T_char, types.T_varchar, types.T_json, types.T_binary, types.T_varbinary, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return bytes.Compare(a.GetByteSlice(area), b.GetByteSlice(area))
		})

	case types.T_array_float32:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return types.ArrayCompare[float32](
				types.GetArray[float32](&a, area),
				types.GetArray[float32](&b, area),
			)
		})
	case types.T_array_float64:
		col, area := MustVarlenaRawData(v)
		slices.SortFunc(col, func(a, b types.Varlena) int {
			return types.ArrayCompare[float64](
				types.GetArray[float64](&a, area),
				types.GetArray[float64](&b, area),
			)
		})
	case types.T_array_bf16:
		sortArrayElement[types.BF16](v)
	case types.T_array_float16:
		sortArrayElement[types.Float16](v)
	case types.T_array_int8:
		sortArrayElement[int8](v)
	case types.T_array_uint8:
		sortArrayElement[uint8](v)
	default:
		return nil
	}
	v.SetSorted(true)
	return nil
}

// sortArrayElement sorts a narrow-typed vector in place using the
// float32-bridged comparator.
func sortArrayElement[T types.ArrayElement](v *Vector) {
	col, area := MustVarlenaRawData(v)
	sort.Slice(col, func(i, j int) bool {
		return types.ArrayElementCompare[T](
			types.GetArray[T](&col[i], area),
			types.GetArray[T](&col[j], area),
		) < 0
	})
}

func BuildVarlenaInline(v1, v2 *types.Varlena) {
	// use three dword operation to improve performance
	p1 := v1.UnsafePtr()
	p2 := v2.UnsafePtr()
	*(*int64)(p1) = *(*int64)(p2)
	*(*int64)(unsafe.Add(p1, 8)) = *(*int64)(unsafe.Add(p2, 8))
	*(*int64)(unsafe.Add(p1, 16)) = *(*int64)(unsafe.Add(p2, 16))
}

func BuildVarlenaNoInline(vec *Vector, v1 *types.Varlena, bs *[]byte, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	return buildVarlenaNoInline(vec, v1, bs, m)
}

func buildVarlenaNoInline(vec *Vector, v1 *types.Varlena, bs *[]byte, m *mpool.MPool) error {
	vlen := len(*bs)
	area1 := vec.GetArea()
	voff := len(area1)
	if voff+vlen <= cap(area1) {
		area1 = append(area1, *bs...)
		v1.SetOffsetLen(uint32(voff), uint32(vlen))
		vec.area = area1
		return nil
	}
	if m == nil {
		area1 = append(area1, *bs...)
		v1.SetOffsetLen(uint32(voff), uint32(vlen))
		vec.area = area1
		return nil
	}
	var err error
	area1, err = vec.growArea2(m, *bs, voff+vlen)
	if err != nil {
		return err
	}
	v1.SetOffsetLen(uint32(voff), uint32(vlen))
	vec.area = area1
	return nil
}

func BuildVarlenaNoInlineFromByteJson(vec *Vector, v1 *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	return buildVarlenaNoInlineFromByteJson(vec, v1, bj, m)
}

func buildVarlenaNoInlineFromByteJson(vec *Vector, v1 *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
	vlen := len(bj.Data) + 1
	area1 := vec.GetArea()
	voff := len(area1)

	var err error
	if voff+vlen > cap(area1) && m != nil {
		// Pass nil to Grow2, we can grow area1 to voff+vlen without
		// copy bytejson data.
		area1, err = vec.growArea2(m, nil, voff+vlen)
		if err != nil {
			return err
		}
		area1[voff] = byte(bj.Type)
		copy(area1[voff+1:voff+vlen], bj.Data)
	} else {
		area1 = append(area1, byte(bj.Type))
		area1 = append(area1, bj.Data...)
	}

	v1.SetOffsetLen(uint32(voff), uint32(vlen))
	vec.area = area1
	return nil
}

// BuildVarlenaFromVarlena requires v2/area to belong to an admitted source
// vector of the same type. The source bytes must stay immutable during copy.
func BuildVarlenaFromVarlena(vec *Vector, v1, v2 *types.Varlena, area *[]byte, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	if (*v2)[0] <= types.VarlenaInlineSize {
		BuildVarlenaInline(v1, v2)
		return nil
	}
	voff, vlen := v2.OffsetLen()
	bs := (*area)[voff : voff+vlen]
	return buildVarlenaNoInline(vec, v1, &bs, m)
}

func BuildVarlenaFromByteSlice(vec *Vector, v *types.Varlena, bs *[]byte, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	vlen := len(*bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], *bs)
		return nil
	}
	return buildVarlenaNoInline(vec, v, bs, m)
}

func BuildVarlenaFromByteJson(vec *Vector, v *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	stored, err := bj.StorageCompatible()
	if err != nil {
		return err
	}
	bj = stored
	vlen := len(bj.Data) + 1
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		v[1] = byte(bj.Type)
		copy(v[2:vlen+1], bj.Data)
		return nil
	}
	return buildVarlenaNoInlineFromByteJson(vec, v, bj, m)
}

// BuildVarlenaFromByteJsonEncoded encodes enc directly into the vector. An
// encoder whose source can be invalid validates it first.
func BuildVarlenaFromByteJsonEncoded(
	vec *Vector, v *types.Varlena, enc bytejson.ByteJsonDataEncoder, m *mpool.MPool,
) error {
	if validator, ok := enc.(bytejson.ByteJsonDataValidator); ok {
		if err := validator.ValidateData(); err != nil {
			return err
		}
	}
	dataSize := uint64(enc.DataSize())
	storageSize := dataSize + 1
	maxInt := uint64(^uint(0) >> 1)
	if storageSize > uint64(^uint32(0)) || storageSize > maxInt {
		return moerr.NewInvalidInputNoCtx("json value is too large")
	}

	if storageSize <= types.VarlenaInlineSize {
		oldValue := *v
		clear(v[:])
		v[0] = byte(storageSize)
		v[1] = enc.TypeCode()
		dst := v[2 : 2+int(dataSize)]
		n, err := enc.EncodeDataInto(dst)
		if err != nil {
			*v = oldValue
			return err
		}
		if n != len(dst) {
			*v = oldValue
			return moerr.NewInternalErrorNoCtxf(
				"bytejson encoder size mismatch: expected %d, got %d", len(dst), n,
			)
		}
		return nil
	}

	oldAreaLen := len(vec.area)
	newAreaLen := uint64(oldAreaLen) + storageSize
	if newAreaLen > uint64(^uint32(0)) || newAreaLen > maxInt {
		return moerr.NewInvalidInputNoCtx("json vector area is too large")
	}

	if int(newAreaLen) > cap(vec.area) {
		newArea, err := vec.growArea2(m, nil, int(newAreaLen))
		if err != nil {
			return err
		}
		// Grow2 may have freed the old area. Install the replacement before
		// invoking an encoder that can fail.
		vec.area = newArea
	} else {
		vec.area = vec.area[:int(newAreaLen)]
	}

	vec.area[oldAreaLen] = enc.TypeCode()
	dst := vec.area[oldAreaLen+1 : int(newAreaLen)]
	n, err := enc.EncodeDataInto(dst)
	if err != nil {
		vec.area = vec.area[:oldAreaLen]
		return err
	}
	if n != len(dst) {
		vec.area = vec.area[:oldAreaLen]
		return moerr.NewInternalErrorNoCtxf(
			"bytejson encoder size mismatch: expected %d, got %d", len(dst), n,
		)
	}
	v.SetOffsetLen(uint32(oldAreaLen), uint32(storageSize))
	return nil
}

// BuildVarlenaFromArray convert array to Varlena so that it can be stored in the vector
func BuildVarlenaFromArray[T types.ArrayElement](vec *Vector, v *types.Varlena, array *[]T, m *mpool.MPool) error {
	if err := vec.checkMP(m); err != nil {
		return err
	}
	_bs := types.ArrayToBytes[T](*array)
	bs := &_bs
	vlen := len(*bs)
	if vlen <= types.VarlenaInlineSize {
		// first clear varlena to 0
		p1 := v.UnsafePtr()
		*(*int64)(p1) = 0
		*(*int64)(unsafe.Add(p1, 8)) = 0
		*(*int64)(unsafe.Add(p1, 16)) = 0
		v[0] = byte(vlen)
		copy(v[1:1+vlen], *bs)
		return nil
	}
	return BuildVarlenaNoInline(vec, v, bs, m)
}

// Intersection2VectorOrdered does a ∩ b ==> ret, keeps all item unique and sorted
// it assumes that a and b all sorted already
func Intersection2VectorOrdered[T types.OrderedT | types.Decimal128 | types.Decimal256](
	a, b []T,
	ret *Vector,
	mp *mpool.MPool,
	cmp func(x, y T) int) (err error) {

	var preVal T
	var idxA, idxB int

	minAB := min(len(a), len(b))

	if err = ret.PreExtend(minAB, mp); err != nil {
		return err
	}

	for idxA < len(a) && idxB < len(b) {
		var cmpRet int

		if cmpRet = cmp(a[idxA], b[idxB]); cmpRet == 0 {
			if ret.Length() == 0 || cmp(preVal, a[idxA]) != 0 {
				if err = AppendFixed(ret, a[idxA], false, mp); err != nil {
					return err
				}

				preVal = a[idxA]
			}

			idxA++
			idxB++

		} else if cmpRet < 0 {
			idxA++

		} else {
			idxB++
		}
	}

	return nil
}

// Union2VectorOrdered does a ∪ b ==> ret, keeps all item unique and sorted
// it assumes that a and b all sorted already
func Union2VectorOrdered[T types.OrderedT | types.Decimal128 | types.Decimal256](
	a, b []T,
	ret *Vector,
	mp *mpool.MPool,
	cmp func(x, y T) int) (err error) {

	var i, j int
	var prevVal T
	var lenA, lenB = len(a), len(b)

	if err = ret.PreExtend(lenA+lenB, mp); err != nil {
		return err
	}

	for i < lenA && j < lenB {
		if cmp(a[i], b[j]) <= 0 {
			if (i == 0 && j == 0) || cmp(prevVal, a[i]) != 0 {
				prevVal = a[i]
				if err = AppendFixed(ret, a[i], false, mp); err != nil {
					return err
				}
			}
			i++
		} else {
			if (i == 0 && j == 0) || cmp(prevVal, b[j]) != 0 {
				prevVal = b[j]
				if err = AppendFixed(ret, b[j], false, mp); err != nil {
					return err
				}
			}
			j++
		}
	}

	for ; i < lenA; i++ {
		if (i == 0 && j == 0) || cmp(prevVal, a[i]) != 0 {
			prevVal = a[i]
			if err = AppendFixed(ret, a[i], false, mp); err != nil {
				return err
			}
		}
	}

	for ; j < lenB; j++ {
		if (i == 0 && j == 0) || cmp(prevVal, b[j]) != 0 {
			prevVal = b[j]
			if err = AppendFixed(ret, b[j], false, mp); err != nil {
				return err
			}
		}
	}
	return nil
}

// Intersection2VectorVarlen does a ∩ b ==> ret, keeps all item unique and sorted
// it assumes that va and vb all sorted already
func Intersection2VectorVarlen(
	va, vb *Vector,
	ret *Vector,
	mp *mpool.MPool) (err error) {

	var preVal []byte
	var idxA, idxB int

	cola, areaa := MustVarlenaRawData(va)
	colb, areab := MustVarlenaRawData(vb)

	minAB := min(len(cola), len(colb))
	if err = ret.PreExtend(minAB, mp); err != nil {
		return err
	}

	for idxA < len(cola) && idxB < len(colb) {
		var cmpRet int

		bytesA := cola[idxA].GetByteSlice(areaa)
		bytesB := colb[idxB].GetByteSlice(areab)

		if cmpRet = bytes.Compare(bytesA, bytesB); cmpRet == 0 {
			if ret.Length() == 0 || !bytes.Equal(preVal, bytesA) {
				if err = AppendBytes(ret, bytesA, false, mp); err != nil {
					return err
				}

				preVal = bytesA
			}

			idxA++
			idxB++

		} else if cmpRet < 0 {
			idxA++

		} else {
			idxB++
		}
	}

	return nil
}

// Union2VectorValen does a ∪ b ==> ret, keeps all item unique and sorted
// it assumes that va and vb all sorted already
func Union2VectorValen(
	va, vb *Vector,
	ret *Vector,
	mp *mpool.MPool) (err error) {

	var i, j int
	var prevVal []byte

	cola, areaa := MustVarlenaRawData(va)
	colb, areab := MustVarlenaRawData(vb)

	var lenA, lenB = len(cola), len(colb)

	if err = ret.PreExtend(lenA+lenB, mp); err != nil {
		return err
	}

	for i < lenA && j < lenB {
		ba := cola[i].GetByteSlice(areaa)
		bb := colb[j].GetByteSlice(areab)

		if bytes.Compare(ba, bb) <= 0 {
			if (i == 0 && j == 0) || !bytes.Equal(prevVal, ba) {
				prevVal = ba
				if err = AppendBytes(ret, ba, false, mp); err != nil {
					return err
				}
			}
			i++
		} else {
			if (i == 0 && j == 0) || !bytes.Equal(prevVal, bb) {
				prevVal = bb
				if err = AppendBytes(ret, bb, false, mp); err != nil {
					return err
				}
			}
			j++
		}
	}

	for ; i < lenA; i++ {
		ba := cola[i].GetByteSlice(areaa)
		if (i == 0 && j == 0) || !bytes.Equal(prevVal, ba) {
			prevVal = ba
			if err = AppendBytes(ret, ba, false, mp); err != nil {
				return err
			}
		}
	}

	for ; j < lenB; j++ {
		bb := colb[j].GetByteSlice(areab)
		if (i == 0 && j == 0) || !bytes.Equal(prevVal, bb) {
			prevVal = bb
			if err = AppendBytes(ret, bb, false, mp); err != nil {
				return err
			}
		}
	}

	return nil
}

// FillRawPtrLen writes v's raw (pointer, length) pairs for native code. A
// read-only null bitmap (a window view) may be shifted, which C cannot read,
// so it is handed over as a clean copy. The returned slice holds that copy:
// the caller must keep it alive until the native call returns.
func (v *Vector) FillRawPtrLen(dest []uintptr) (keepAlive []uint64) {
	// The null map's first word and its length in bits; (0, 0) when it has
	// no storage.
	dest[0], dest[1] = 0, 0
	if v.nsp.IsReadOnly() {
		bm := v.nsp.GetBitmap()
		if keepAlive = bm.WordsCovering(int(bm.Len())); len(keepAlive) > 0 {
			dest[0] = uintptr(unsafe.Pointer(&keepAlive[0]))
			dest[1] = uintptr(bm.Len())
		}
	} else if ptr := nulls.Ptr(&v.nsp); ptr != nil {
		dest[0] = uintptr(unsafe.Pointer(ptr))
		dest[1] = uintptr(v.nsp.GetBitmap().Len())
	}
	ds := v.UnsafeGetRawData()
	dest[2] = uintptr(unsafe.Pointer(&ds[0]))
	dest[3] = uintptr(len(ds))
	if len(v.area) == 0 {
		dest[4], dest[5] = 0, 0
	} else {
		dest[4] = uintptr(unsafe.Pointer(&v.area[0]))
		dest[5] = uintptr(len(v.area))
	}
	return keepAlive
}

// hasBackingStorage reports whether v holds any data, area or bitmap storage.
func (v *Vector) hasBackingStorage() bool {
	return cap(v.data) != 0 ||
		cap(v.area) != 0 ||
		v.nsp.GetBitmap().CapWords() != 0
}

// The null bitmap allocates from the vector's pool and is freed with the
// vector. When a vector grows, it reserves room in the bitmap for all of its
// rows; a bitmap also grows itself on write.

func validBitmapRows(rows int) error {
	if rows < 0 || rows > math.MaxInt-63 {
		return moerr.NewInvalidInputNoCtxf("invalid bitmap row count %d", rows)
	}
	return nil
}

func (v *Vector) ensureNullCapacity(rows int) error {
	if err := validBitmapRows(rows); err != nil {
		return err
	}
	return v.nsp.GetBitmap().Reserve(rows)
}

func errVectorReadOnly() error {
	return moerr.NewInternalErrorNoCtx("vector is read-only: it has no mpool")
}

// checkMP verifies that mp, passed by a caller operating on v, is exactly the
// pool v was created with; a different pool, even a view of the same pool, is
// an error. An accounted view is therefore supplied when the vector is
// created. A read-only vector (nil pool) accepts no allocation or mutation.
func (v *Vector) checkMP(mp *mpool.MPool) error {
	if v.mp == nil {
		return errVectorReadOnly()
	}
	if mp != v.mp {
		return moerr.NewInternalErrorNoCtxf(
			"vector mpool mismatch: vector belongs to %p, call passed %p", v.mp, mp)
	}
	return nil
}

// mustCheckMP is checkMP for calls that cannot return an error.
func (v *Vector) mustCheckMP(mp *mpool.MPool) {
	if err := v.checkMP(mp); err != nil {
		panic(err)
	}
}

// alloc allocates size bytes from v's pool; mp must be that pool.
func (v *Vector) alloc(mp *mpool.MPool, size int) ([]byte, error) {
	if err := v.checkMP(mp); err != nil {
		return nil, err
	}
	return v.mp.Alloc(size, true)
}

func (v *Vector) growData(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.data, size)
}

func (v *Vector) growArea(mp *mpool.MPool, size int) ([]byte, error) {
	return v.growOwned(mp, v.area, size)
}

// growOwned grows old to size bytes through v's pool; mp must be that pool.
// A size past the allocator limit returns ErrAllocationAllocatorLimit, which
// callers classify as memory exhaustion; mpool's own limit error stays a plain
// moerr for its other users.
func (v *Vector) growOwned(mp *mpool.MPool, old []byte, size int) ([]byte, error) {
	if err := v.checkMP(mp); err != nil {
		return nil, err
	}
	if size > cap(old) {
		if _, ok := mpool.GrowCapacity(int64(cap(old)), int64(size)); !ok {
			return nil, mpool.ErrAllocationAllocatorLimit
		}
	}
	return v.mp.Grow(old, size, true)
}

func (v *Vector) growArea2(
	mp *mpool.MPool,
	src []byte,
	size int,
) ([]byte, error) {
	oldLen := len(v.area)
	if size < oldLen+len(src) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"mpool grow2 actually shrinks, %d+%d, %d",
			oldLen,
			len(src),
			size,
		)
	}
	grown, err := v.growArea(mp, size)
	if err != nil {
		return nil, err
	}
	copy(grown[oldLen:oldLen+len(src)], src)
	return grown, nil
}

func (v *Vector) readSizeBytes(
	r io.Reader,
	mp *mpool.MPool,
	data bool,
) (int32, []byte, error) {
	size, err := types.ReadInt32(r)
	if err != nil {
		return 0, nil, err
	}
	var old []byte
	if data {
		old = v.data
	} else {
		old = v.area
	}
	if size == 0 {
		if old != nil {
			old = old[:0]
		}
		if data {
			v.data = old
		} else {
			v.area = old
		}
		return 0, old, nil
	}
	if size < 0 {
		return size, nil, moerr.NewInvalidInputNoCtx(
			"negative vector buffer size",
		)
	}
	if err := validateStreamingReadSize(r, int64(size)); err != nil {
		return size, nil, err
	}
	var buf []byte
	if data {
		buf, err = v.growData(mp, int(size))
	} else {
		buf, err = v.growArea(mp, int(size))
	}
	if err != nil {
		return 0, nil, err
	}
	// Grow may already have freed the old allocation. Publish its replacement
	// before reading so a short reader still leaves one reachable cleanup owner.
	if data {
		v.data = buf
	} else {
		v.area = buf
	}
	if _, err = io.ReadFull(r, buf); err != nil {
		return size, buf, err
	}
	return size, buf, nil
}

func validateStreamingReadSize(r io.Reader, size int64) error {
	if size < 0 {
		return moerr.NewInvalidInputNoCtx("negative vector buffer size")
	}
	var remaining int64 = -1
	switch reader := r.(type) {
	case *io.LimitedReader:
		remaining = reader.N
	case interface{ Len() int }:
		remaining = int64(reader.Len())
	}
	if remaining >= 0 && size > remaining {
		return io.ErrUnexpectedEOF
	}
	return nil
}
