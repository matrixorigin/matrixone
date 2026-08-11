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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/bits"
	"slices"
	"sort"
	"time"
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
	DIST            // dictionary vector
)

// PrepareParamKind preserves the source conversion category of a transient
// text vector produced by MySQL prepared-statement execution.
type PrepareParamKind uint8

// Values are bit-packed into bool sections for remote Process transport. Keep
// the numeric assignments stable and within three bits.
const (
	PrepareParamNone PrepareParamKind = iota
	PrepareParamInteger
	PrepareParamFloat
	PrepareParamDecimal
	PrepareParamBoolean
)

// MergePrepareParamKinds folds two observed source categories.  Equal
// categories are idempotent; a conflict conservatively becomes ordinary
// string conversion.  This is intentionally commutative and associative so
// parallel reductions cannot make the result depend on arrival order.
func MergePrepareParamKinds(left, right PrepareParamKind) PrepareParamKind {
	if left == right {
		return left
	}
	return PrepareParamNone
}

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
	gsp nulls.Nulls // grouping list

	cantFreeData bool
	cantFreeArea bool

	sorted bool // for some optimization

	// FIXME: Bad design! Will be deleted soon.
	isBin            bool
	prepareParamKind PrepareParamKind
	// prepareParamKindSeen distinguishes an observed string/byte source
	// (kind None) from an empty vector that has not contributed a value yet.
	// It is local lineage state and is not part of the vector wire format.
	prepareParamKindSeen bool
	// prepareParamKinds is allocated only when one logical vector contains
	// different source categories. A nil slice keeps the existing scalar fast
	// path for ordinary and uniform vectors.
	prepareParamKinds []PrepareParamKind
	// prepareParamKindsMP owns the optional sidecar allocation and is cleared
	// whenever that sidecar is released, so reused vectors cannot retain a stale
	// MPool pointer across query generations.
	prepareParamKindsMP *mpool.MPool
	// binaryString records byte-string semantics for dynamically typed values.
	// Unlike isBin, it does not change numeric conversion into big-endian literal
	// conversion and is therefore safe to preserve across local materialization.
	binaryString bool
	// binaryStringRows is allocated only when one vector mixes character and
	// byte-string rows. Set bits identify byte-string rows; nil keeps the uniform
	// scalar fast path. A bitmap limits the exceptional representation to one bit
	// per row instead of adding a byte to every Vector row.
	binaryStringRows       *bitmap.Bitmap
	binaryStringRowsActive bool

	offHeap bool

	// allocationAccount selects the account for this vector's first owned
	// off-heap data and area allocations. Physical MPool metadata owns release.
	allocationAccount *AllocationAccountSelection

	// areaDisjoint proves that every live non-inline varlena descriptor owns a
	// distinct range in area. Spill projections use it to avoid scanning normal
	// append-built vectors; operations that can introduce aliases clear it.
	areaDisjoint bool
}

func toSliceOfLengthNoTypeCheck[T any](vec *Vector, length int) []T {
	if length == 0 {
		return nil
	}
	checkTypeIfRaceDetectorEnabled[T](vec)
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
	v.resetPrepareParamKind()

	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}

	v.length = 0
	v.nsp.Clear()
	v.gsp.Clear()
	v.sorted = false
	v.isBin = false
	v.resetBinaryString()
	v.areaDisjoint = true
}

func (v *Vector) ResetWithSameType() {
	v.resetPrepareParamKind()
	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.length = 0
	v.nsp.Reset()
	v.gsp.Reset()
	v.sorted = false
	v.isBin = false
	v.resetBinaryString()
	v.areaDisjoint = true
}

func (v *Vector) ResetArea() {
	v.area = v.area[:0]
	v.areaDisjoint = v.length == 0
}

// TODO: It is semantically same as Reset, need to merge them later.
func (v *Vector) ResetWithNewType(t *types.Type) {
	v.typ = *t
	v.resetPrepareParamKind()
	v.class = FLAT
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.nsp.Clear()
	v.gsp.Clear()
	v.length = 0
	v.sorted = false
	v.isBin = false
	v.resetBinaryString()
	v.areaDisjoint = true
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
	binaryStringBytes := 0
	if v.binaryStringRows != nil {
		binaryStringBytes = v.binaryStringRows.Size()
		if capacity := v.binaryStringRows.ExternalStorageCapacity(); capacity > 0 {
			binaryStringBytes = 8 * capacity
		}
	}
	return cap(v.data) +
		cap(v.area) +
		cap(v.prepareParamKinds)*int(unsafe.Sizeof(PrepareParamKind(0))) +
		binaryStringBytes +
		8*v.nsp.GetBitmap().ExternalStorageCapacity() +
		8*v.gsp.GetBitmap().ExternalStorageCapacity()
}

func (v *Vector) SetLength(n int) {
	if n < 0 {
		panic("negative vector length")
	}
	if v.typ.IsVarlen() && n != v.length {
		v.areaDisjoint = false
	}
	if err := v.preExtendPrepareParamKinds(n, nil); err != nil {
		panic(err)
	}
	oldLength := v.length
	v.setLengthAfterExtend(n)
	if v.binaryStringRowsActive {
		if n > oldLength {
			v.binaryStringRows.TryExpandWithSize(n)
		} else if n < oldLength {
			v.binaryStringRows.RemoveRange(uint64(n), uint64(oldLength))
		}
		v.normalizeBinaryStringRows()
	}
}

// AppendCheckpoint captures the logical state changed by append operations.
// Capacity growth is deliberately not rolled back: it remains owned by the
// vector and can be reused by a later append.
type AppendCheckpoint struct {
	length               int
	areaLength           int
	areaDisjoint         bool
	sorted               bool
	prepareParamKind     PrepareParamKind
	prepareParamKindSeen bool
	hadPrepareParamKinds bool
	binaryString         bool
	hadBinaryStringRows  bool
}

func (v *Vector) MakeAppendCheckpoint() AppendCheckpoint {
	return AppendCheckpoint{
		length:               v.length,
		areaLength:           len(v.area),
		areaDisjoint:         v.areaDisjoint,
		sorted:               v.sorted,
		prepareParamKind:     v.prepareParamKind,
		prepareParamKindSeen: v.prepareParamKindSeen,
		hadPrepareParamKinds: v.prepareParamKinds != nil,
		binaryString:         v.binaryString,
		hadBinaryStringRows:  v.binaryStringRowsActive,
	}
}

// RollbackAppend restores the logical state captured before an attempted
// append. attemptedRows is needed because grouping bits can be published
// before a varlen copy fails and advances length.
func (v *Vector) RollbackAppend(checkpoint AppendCheckpoint, attemptedRows int) {
	if checkpoint.length < 0 || checkpoint.length > v.length ||
		checkpoint.areaLength < 0 || checkpoint.areaLength > len(v.area) ||
		attemptedRows < 0 {
		panic("invalid vector append checkpoint")
	}
	end := max(v.length, checkpoint.length+attemptedRows)
	nulls.RemoveRange(&v.nsp, uint64(checkpoint.length), uint64(end))
	nulls.RemoveRange(&v.gsp, uint64(checkpoint.length), uint64(end))
	v.length = checkpoint.length
	v.area = v.area[:checkpoint.areaLength]
	// A failed varlen append may already have exposed shared or partial area
	// layout. Roll back logical lengths, but keep this proof fail-closed.
	v.areaDisjoint = checkpoint.areaDisjoint && v.areaDisjoint
	v.sorted = checkpoint.sorted
	if checkpoint.hadPrepareParamKinds {
		if len(v.prepareParamKinds) < checkpoint.length {
			panic("prepared parameter sidecar lost during append rollback")
		}
		clear(v.prepareParamKinds[checkpoint.length:])
		v.prepareParamKinds = v.prepareParamKinds[:checkpoint.length]
	} else {
		v.releasePrepareParamKinds()
	}
	v.prepareParamKind = checkpoint.prepareParamKind
	v.prepareParamKindSeen = checkpoint.prepareParamKindSeen
	if checkpoint.hadBinaryStringRows {
		if v.binaryStringRows == nil || !v.binaryStringRowsActive {
			panic("binary-string sidecar lost during append rollback")
		}
		v.binaryStringRows.RemoveRange(uint64(checkpoint.length), uint64(end))
		v.binaryString = checkpoint.binaryString
	} else {
		v.setBinaryStringScalar(checkpoint.binaryString)
	}
}

// Size of data, I think this function is inherently broken.  This
// Size is not meaningful other than used in (approximate) memory accounting.
func (v *Vector) Size() int {
	binaryStringBytes := 0
	if v.binaryStringRows != nil {
		binaryStringBytes = v.binaryStringRows.Size()
	}
	return v.length*v.typ.TypeSize() + len(v.area) + binaryStringBytes +
		len(v.prepareParamKinds)*int(unsafe.Sizeof(PrepareParamKind(0)))
}

func (v *Vector) GetType() *types.Type {
	return &v.typ
}

// Bug #23240
// This is very dangerous.   We changed vector type
// but did not change the underlying data.   So the length
// and capacity are all messed up.
func (v *Vector) SetType(typ types.Type) {
	if v.typ.IsVarlen() || typ.IsVarlen() {
		// An empty logical range has no descriptors, even if reusable backing
		// storage still contains stale bytes.
		v.areaDisjoint = v.length == 0
	}
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

func (v *Vector) SetOffHeap(offHeap bool) {
	if !offHeap && v.allocationAccount != nil {
		panic("allocation-accounted vector must remain off-heap")
	}
	v.offHeap = offHeap
}

func (v *Vector) SetTypeScale(scale int32) {
	v.typ.Scale = scale
}

func (v *Vector) GetNulls() *nulls.Nulls {
	return &v.nsp
}

func (v *Vector) GetGrouping() *nulls.Nulls {
	return &v.gsp
}

func (v *Vector) SetNulls(nsp *nulls.Nulls) {
	v.nsp.Clear()
	if nsp == nil {
		return
	}
	v.nsp.Or(nsp)
	if v.prepareParamKinds != nil {
		v.nsp.Foreach(func(row uint64) bool {
			v.clearPrepareParamKindAt(int(row))
			return true
		})
	}
	if v.binaryStringRowsActive {
		v.nsp.Foreach(func(row uint64) bool {
			if row < uint64(v.length) {
				v.binaryStringRows.Remove(row)
			}
			return true
		})
		v.normalizeBinaryStringRows()
	}
	if v.AllNull() {
		v.resetPrepareParamKind()
		v.resetBinaryString()
	}
}

func (v *Vector) SetAllNulls(length int) {
	v.nsp.InitWithSize(int(length))
	v.nsp.AddRange(0, uint64(length))
	v.resetPrepareParamKind()
	v.resetBinaryString()
}

func (v *Vector) SetGrouping(gsp *nulls.Nulls) {
	v.gsp.Clear()
	if gsp == nil {
		return
	}
	v.gsp.Or(gsp)
}

func (v *Vector) HasNull() bool {
	return v.IsConstNull() || !v.nsp.IsEmpty()
}

func (v *Vector) HasGrouping() bool {
	return !v.gsp.IsEmpty()
}

func (v *Vector) AllNull() bool {
	return v.IsConstNull() || (v.length != 0 && v.nsp.Count() == v.length)
}

func (v *Vector) GetIsBin() bool {
	return v.isBin
}

func (v *Vector) SetIsBin(isBin bool) {
	v.isBin = isBin
}

func (v *Vector) GetPrepareParamKind() PrepareParamKind {
	return v.prepareParamKind
}

// HasPrepareParamKind reports whether at least one non-NULL logical row has
// an observed source category. It distinguishes an ordinary/unobserved
// vector from an observed ordinary-string category (PrepareParamNone).
func (v *Vector) HasPrepareParamKind() bool {
	return v != nil && v.prepareParamKindSeen
}

func (v *Vector) SetPrepareParamKind(kind PrepareParamKind) {
	v.prepareParamKind = kind
	v.prepareParamKindSeen = true
	v.releasePrepareParamKinds()
}

// GetPrepareParamKindAt returns the source category for one logical row.
// Constants use their single physical value for every logical row. The scalar
// field remains the common fast path; heterogeneous vectors consult the
// optional row sidecar.
func (v *Vector) GetPrepareParamKindAt(row int) PrepareParamKind {
	if v == nil {
		return PrepareParamNone
	}
	if v.IsConst() {
		row = 0
	}
	if row < 0 || row >= v.length || v.IsNull(uint64(row)) {
		return PrepareParamNone
	}
	if row >= 0 && row < len(v.prepareParamKinds) {
		return v.prepareParamKinds[row]
	}
	return v.prepareParamKind
}

// GetPrepareParamKinds returns the exact row sidecar, or nil for the uniform
// scalar representation. Callers must treat the returned slice as read-only.
func (v *Vector) GetPrepareParamKinds() []PrepareParamKind {
	return v.prepareParamKinds
}

// SetPrepareParamKinds installs exact row categories. NULL rows are ignored
// when deciding whether the vector is observed or uniform. The sidecar is
// retained only when non-NULL rows disagree, preserving the scalar fast path.
func (v *Vector) SetPrepareParamKinds(kinds []PrepareParamKind) {
	if err := v.SetPrepareParamKindsWithMP(kinds, nil); err != nil {
		panic(err)
	}
}

// SetPrepareParamKindsWithMP is the ownership-aware form used by execution
// paths. Heterogeneous row metadata is charged to the vector's MPool (and its
// allocation account, when present) just like physical vector storage.
func (v *Vector) SetPrepareParamKindsWithMP(kinds []PrepareParamKind, mp *mpool.MPool) error {
	if len(kinds) == 0 || v.length == 0 {
		v.resetPrepareParamKind()
		return nil
	}
	if len(kinds) != v.length {
		return moerr.NewInvalidInputNoCtxf(
			"prepared parameter row count %d does not match vector length %d",
			len(kinds), v.length)
	}

	var (
		first PrepareParamKind
		seen  bool
		mixed bool
	)
	for row := 0; row < v.length && row < len(kinds); row++ {
		if v.IsNull(uint64(row)) {
			continue
		}
		kind := kinds[row]
		if !seen {
			first = kind
			seen = true
		} else if first != kind {
			mixed = true
		}
	}
	if !seen {
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = false
		v.releasePrepareParamKinds()
		return nil
	}
	if !mixed {
		v.prepareParamKind = first
		v.prepareParamKindSeen = true
		v.releasePrepareParamKinds()
		return nil
	}
	kindsCopy, owner, err := v.allocatePrepareParamKinds(v.length, mp)
	if err != nil {
		return err
	}
	clear(kindsCopy)
	copy(kindsCopy, kinds)
	v.releasePrepareParamKinds()
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = seen
	v.prepareParamKinds = kindsCopy
	v.prepareParamKindsMP = owner
	return nil
}

// SetPrepareParamKindsFromReader restores exact row provenance without first
// materializing a data-scaled Go-heap slice.  Spill/transport decoders use it
// while the stable batch payload is still owned by the reader; heterogeneous
// metadata is allocated by the vector's MPool owner and uniform metadata is
// immediately collapsed back to the scalar representation.
func (v *Vector) SetPrepareParamKindsFromReader(r io.Reader, n int, mp *mpool.MPool) error {
	if v == nil || r == nil {
		return io.ErrClosedPipe
	}
	if n < 0 || n != v.length {
		return moerr.NewInvalidInputNoCtxf(
			"prepared parameter row count %d does not match vector length %d", n, v.length)
	}
	if n == 0 {
		v.resetPrepareParamKind()
		return nil
	}
	kinds, owner, err := v.allocatePrepareParamKinds(n, mp)
	if err != nil {
		return err
	}
	var one [1]byte
	for row := range kinds {
		if _, err = io.ReadFull(r, one[:]); err != nil {
			if owner != nil {
				mpool.FreeSlice(owner, kinds)
			}
			return err
		}
		kind := PrepareParamKind(one[0])
		if kind > PrepareParamBoolean {
			if owner != nil {
				mpool.FreeSlice(owner, kinds)
			}
			return moerr.NewInvalidInputNoCtxf(
				"invalid prepared parameter row kind %d", kind)
		}
		kinds[row] = kind
	}

	var first PrepareParamKind
	seen := false
	mixed := false
	for row, kind := range kinds {
		if v.IsNull(uint64(row)) {
			continue
		}
		if !seen {
			first, seen = kind, true
		} else if first != kind {
			mixed = true
		}
	}
	v.releasePrepareParamKinds()
	if !seen {
		if owner != nil {
			mpool.FreeSlice(owner, kinds)
		}
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = false
		return nil
	}
	if !mixed {
		if owner != nil {
			mpool.FreeSlice(owner, kinds)
		}
		v.prepareParamKind = first
		v.prepareParamKindSeen = true
		return nil
	}
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	v.prepareParamKinds = kinds
	v.prepareParamKindsMP = owner
	return nil
}

// SetPrepareParamKindsAndBinaryStringFromReader restores the two row-exact
// provenance sidecars from one encoded byte per row. binaryMask is removed
// before validating the prepared-parameter kind. The temporary kind storage
// is MPool-owned and is collapsed immediately when all non-NULL rows agree.
func (v *Vector) SetPrepareParamKindsAndBinaryStringFromReader(
	r io.Reader,
	n int,
	mp *mpool.MPool,
	binaryMask byte,
) error {
	if v == nil || r == nil {
		return io.ErrClosedPipe
	}
	if n < 0 || n != v.length {
		return moerr.NewInvalidInputNoCtxf(
			"prepared parameter row count %d does not match vector length %d", n, v.length)
	}
	if binaryMask == 0 || binaryMask&(binaryMask-1) != 0 ||
		binaryMask <= byte(PrepareParamBoolean) {
		return moerr.NewInvalidInputNoCtxf("invalid binary-string row mask %d", binaryMask)
	}
	if n == 0 {
		v.resetPrepareParamKind()
		v.SetIsBinaryString(false)
		return nil
	}
	kinds, owner, err := v.allocatePrepareParamKinds(n, mp)
	if err != nil {
		return err
	}
	releaseKinds := func() {
		if owner != nil {
			mpool.FreeSlice(owner, kinds)
		}
	}
	var one [1]byte
	for row := range kinds {
		if _, err = io.ReadFull(r, one[:]); err != nil {
			releaseKinds()
			return err
		}
		kind := PrepareParamKind(one[0] &^ binaryMask)
		if kind > PrepareParamBoolean {
			releaseKinds()
			return moerr.NewInvalidInputNoCtxf(
				"invalid prepared parameter row kind %d", kind)
		}
		// Keep the binary bit in the temporary kind byte. This single accounted
		// slice stages both sidecars until every allocation has succeeded.
		kinds[row] = PrepareParamKind(one[0])
	}

	var first PrepareParamKind
	seen := false
	mixed := false
	nonNull := 0
	binaryCount := 0
	physicalRows := n
	if v.IsConst() {
		physicalRows = 1
	}
	for row := 0; row < physicalRows; row++ {
		if v.IsNull(uint64(row)) {
			continue
		}
		nonNull++
		encoded := byte(kinds[row])
		kind := PrepareParamKind(encoded &^ binaryMask)
		if encoded&binaryMask != 0 {
			binaryCount++
		}
		if !seen {
			first, seen = kind, true
		} else if first != kind {
			mixed = true
		}
	}
	mixedBinary := binaryCount > 0 && binaryCount < nonNull
	if mixedBinary {
		if err := v.ensureBinaryStringCapacity(v.length, mp); err != nil {
			releaseKinds()
			return err
		}
	}
	// All fallible work is complete. Publish the new generation from here.
	switch {
	case binaryCount == 0:
		v.setBinaryStringScalar(false)
	case binaryCount == nonNull:
		v.setBinaryStringScalar(true)
	default:
		v.binaryStringRows.InitWithSize(int64(v.length))
		for row := 0; row < physicalRows; row++ {
			if byte(kinds[row])&binaryMask != 0 && !v.IsNull(uint64(row)) {
				v.binaryStringRows.Add(uint64(row))
			}
		}
		v.binaryString = true
		v.binaryStringRowsActive = true
	}
	if mixed {
		for row := range kinds {
			kinds[row] = PrepareParamKind(byte(kinds[row]) &^ binaryMask)
		}
	}
	v.releasePrepareParamKinds()
	switch {
	case !seen:
		releaseKinds()
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = false
	case !mixed:
		releaseKinds()
		v.prepareParamKind = first
		v.prepareParamKindSeen = true
	default:
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = true
		v.prepareParamKinds = kinds
		v.prepareParamKindsMP = owner
	}
	return nil
}

// SetPrepareParamKindAt updates one logical row and promotes a scalar vector
// to the sidecar representation only when that row conflicts with the scalar
// category. It is intended for data movers that already copied the row.
func (v *Vector) SetPrepareParamKindAt(row int, kind PrepareParamKind) {
	if err := v.SetPrepareParamKindAtWithMP(row, kind, nil); err != nil {
		panic(err)
	}
}

// SetPrepareParamKindAtWithMP is the ownership-aware row setter.
func (v *Vector) SetPrepareParamKindAtWithMP(row int, kind PrepareParamKind, mp *mpool.MPool) error {
	if v == nil || row < 0 {
		return nil
	}
	if v.IsConst() {
		row = 0
	}
	if row >= v.length {
		return nil
	}
	if v.IsNull(uint64(row)) {
		v.clearPrepareParamKindAt(row)
		if v.AllNull() {
			v.resetPrepareParamKind()
		}
		return nil
	}
	if v.prepareParamKinds == nil && (!v.prepareParamKindSeen || v.prepareParamKind == kind) {
		v.prepareParamKind = kind
		v.prepareParamKindSeen = true
		return nil
	}
	if v.prepareParamKinds == nil {
		kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
		if err != nil {
			return err
		}
		for i := range kinds {
			kinds[i] = v.prepareParamKind
		}
		v.prepareParamKinds = kinds
		v.prepareParamKindsMP = owner
	}
	v.prepareParamKinds[row] = kind
	v.prepareParamKindSeen = true
	v.prepareParamKind = PrepareParamNone
	return nil
}

func (v *Vector) resetPrepareParamKind() {
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = false
	v.releasePrepareParamKinds()
}

func (v *Vector) releasePrepareParamKinds() {
	if cap(v.prepareParamKinds) != 0 && v.prepareParamKindsMP != nil {
		mpool.FreeSlice(v.prepareParamKindsMP, v.prepareParamKinds)
	}
	v.prepareParamKinds = nil
	// Do not retain an MPool pointer after the sidecar it owns has been
	// released.  Vectors are routinely Reset and reused by a later query;
	// retaining the old owner would make a subsequent non-MP setter allocate
	// against a stale/freed pool.
	v.prepareParamKindsMP = nil
}

func (v *Vector) allocatePrepareParamKinds(n int, mp *mpool.MPool) ([]PrepareParamKind, *mpool.MPool, error) {
	if n <= 0 {
		return nil, nil, nil
	}
	owner := mp
	if owner == nil {
		owner = v.prepareParamKindsMP
	}
	if v.allocationAccount != nil {
		if owner == nil {
			return nil, nil, mpool.ErrAllocationAccountInvalid
		}
		kinds, err := mpool.MakeSliceAccounted[PrepareParamKind](
			n,
			owner,
			v.allocationAccount.account,
			v.allocationAccount.owner,
			v.allocationAccount.dataSite,
		)
		return kinds, owner, err
	}
	if owner == nil {
		return make([]PrepareParamKind, n), nil, nil
	}
	kinds, err := mpool.MakeSlice[PrepareParamKind](n, owner, true)
	return kinds, owner, err
}

// preExtendPrepareParamKinds reserves row-parallel provenance capacity without
// publishing a new logical vector length. Append and aggregate growth call it
// before mutating length so allocation failure remains an ordinary error.
func (v *Vector) preExtendPrepareParamKinds(n int, mp *mpool.MPool) error {
	if v.prepareParamKinds == nil || n <= cap(v.prepareParamKinds) {
		return nil
	}
	newCapacity, ok := mpool.GrowCapacity(int64(cap(v.prepareParamKinds)), int64(n))
	if !ok {
		return moerr.NewInternalErrorNoCtxf(
			"invalid prepared parameter sidecar capacity, old %d, required %d",
			cap(v.prepareParamKinds), n)
	}
	oldLength := len(v.prepareParamKinds)
	kinds, owner, err := v.allocatePrepareParamKinds(int(newCapacity), mp)
	if err != nil {
		return err
	}
	copy(kinds, v.prepareParamKinds)
	v.releasePrepareParamKinds()
	v.prepareParamKinds = kinds[:oldLength]
	v.prepareParamKindsMP = owner
	return nil
}

// needsPrepareOrdinaryAppend is the inlineable fast-path guard for raw appends.
// Check the default None value first so both unobserved and observed ordinary
// vectors return after one comparison; inspect the sidecar only for a prepared
// scalar candidate.
func (v *Vector) needsPrepareOrdinaryAppend() bool {
	return v.prepareParamKind != PrepareParamNone && v.prepareParamKindSeen &&
		v.prepareParamKinds == nil
}

// prepareOrdinaryAppend promotes a prepared scalar provenance to the exact
// row representation when a raw append introduces ordinary non-NULL rows.
// Callers invoke it after their physical capacity preflight when possible and
// before publishing the new logical length. The common unobserved/ordinary,
// NULL-only, and existing-sidecar paths remain allocation-free.
func (v *Vector) prepareOrdinaryAppend(rows int, mp *mpool.MPool) error {
	if rows <= 0 || !v.needsPrepareOrdinaryAppend() {
		return nil
	}
	if v.length == 0 || v.AllNull() {
		// Metadata on an empty/all-NULL prefix has no value owner. The appended
		// ordinary rows establish the first real provenance without a sidecar.
		v.resetPrepareParamKind()
		return nil
	}
	kinds, owner, err := v.allocatePrepareParamKinds(v.length+rows, mp)
	if err != nil {
		return err
	}
	clear(kinds)
	for row := 0; row < v.length; row++ {
		kinds[row] = v.prepareParamKind
	}
	v.releasePrepareParamKinds()
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	v.prepareParamKinds = kinds[:v.length]
	v.prepareParamKindsMP = owner
	return nil
}

func (v *Vector) prepareOrdinaryBinaryStringAppend(rows int, mp *mpool.MPool) error {
	if rows <= 0 || v.binaryStringRowsActive || !v.binaryString {
		return nil
	}
	switch v.typ.Oid {
	case types.T_binary, types.T_varbinary, types.T_blob:
		// Static binary types remain byte strings regardless of append origin.
		return nil
	}
	if v.length == 0 || v.AllNull() {
		v.resetBinaryString()
		return nil
	}
	if err := v.ensureBinaryStringCapacity(v.length+rows, mp); err != nil {
		return err
	}
	v.binaryStringRows.InitWithSize(int64(v.length + rows))
	v.binaryStringRows.AddRange(0, uint64(v.length))
	if !v.nsp.EmptyByFlag() {
		iterator := v.nsp.GetBitmap().Iterator()
		for iterator.HasNext() {
			row := iterator.Next()
			if row < uint64(v.length) {
				v.binaryStringRows.Remove(row)
			}
		}
	}
	v.binaryStringRowsActive = true
	return nil
}

func (v *Vector) prepareOrdinaryAppendMetadata(rows int, mp *mpool.MPool) error {
	if err := v.prepareOrdinaryAppend(rows, mp); err != nil {
		return err
	}
	return v.prepareOrdinaryBinaryStringAppend(rows, mp)
}

// setLengthAfterExtend publishes a length whose row-parallel capacities were
// already reserved. It cannot allocate and initializes newly visible ordinary
// rows with PrepareParamNone.
func (v *Vector) setLengthAfterExtend(n int) {
	if v.prepareParamKinds != nil {
		if n > cap(v.prepareParamKinds) {
			panic("prepared parameter sidecar capacity was not extended")
		}
		oldLength := len(v.prepareParamKinds)
		v.prepareParamKinds = v.prepareParamKinds[:n]
		if n > oldLength {
			clear(v.prepareParamKinds[oldLength:])
		}
	}
	v.length = n
}

func (v *Vector) copyPrepareParamKindToWithMP(dst *Vector, mp *mpool.MPool) error {
	oldKind := dst.prepareParamKind
	oldSeen := dst.prepareParamKindSeen
	if v.prepareParamKinds != nil {
		if mp == nil {
			mp = v.prepareParamKindsMP
		}
		kinds, owner, err := dst.allocatePrepareParamKinds(len(v.prepareParamKinds), mp)
		if err != nil {
			dst.prepareParamKind = oldKind
			dst.prepareParamKindSeen = oldSeen
			return err
		}
		copy(kinds, v.prepareParamKinds)
		dst.releasePrepareParamKinds()
		dst.prepareParamKinds = kinds
		dst.prepareParamKindsMP = owner
		dst.prepareParamKind = v.prepareParamKind
		dst.prepareParamKindSeen = v.prepareParamKindSeen
	} else {
		dst.releasePrepareParamKinds()
		dst.prepareParamKind = v.prepareParamKind
		dst.prepareParamKindSeen = v.prepareParamKindSeen
	}
	if !dst.prepareParamKindSeen && dst.prepareParamKind == PrepareParamNone &&
		v.length > 0 && !v.AllNull() {
		// A non-empty ordinary string vector is an observed None source even
		// when it was created without prepared-parameter metadata.
		dst.prepareParamKindSeen = true
	}
	return nil
}

// CopyPrepareParamMetadataTo copies the exact source-category metadata to a
// destination vector after its logical rows have been materialized.
func (v *Vector) CopyPrepareParamMetadataTo(dst *Vector) {
	if err := v.CopyPrepareParamMetadataToWithMP(dst, nil); err != nil {
		panic(err)
	}
}

// CopyPrepareParamMetadataToWithMP is the ownership-aware metadata copy used
// by batch/pSpool/clone data movers.
func (v *Vector) CopyPrepareParamMetadataToWithMP(dst *Vector, mp *mpool.MPool) error {
	if v == nil || dst == nil {
		return nil
	}
	return v.copyPrepareParamKindToWithMP(dst, mp)
}

func (v *Vector) copyPrepareParamKindWindowToWithMP(dst *Vector, start, end int, mp *mpool.MPool) error {
	if v.prepareParamKinds == nil {
		return v.copyPrepareParamKindToWithMP(dst, mp)
	}
	if start < 0 || end < start || end > len(v.prepareParamKinds) {
		return nil
	}
	var (
		first PrepareParamKind
		seen  bool
		mixed bool
	)
	for row := start; row < end; row++ {
		if v.IsNull(uint64(row)) {
			continue
		}
		kind := v.prepareParamKinds[row]
		if !seen {
			first, seen = kind, true
		} else if first != kind {
			mixed = true
			break
		}
	}
	if !seen {
		dst.resetPrepareParamKind()
		return nil
	}
	if !mixed {
		dst.releasePrepareParamKinds()
		dst.prepareParamKind = first
		dst.prepareParamKindSeen = true
		return nil
	}
	if mp == nil {
		mp = v.prepareParamKindsMP
	}
	kinds, owner, err := dst.allocatePrepareParamKinds(end-start, mp)
	if err != nil {
		return err
	}
	copy(kinds, v.prepareParamKinds[start:end])
	dst.releasePrepareParamKinds()
	dst.prepareParamKinds = kinds
	dst.prepareParamKindsMP = owner
	dst.prepareParamKind = PrepareParamNone
	dst.prepareParamKindSeen = true
	return nil
}

func (v *Vector) mergePrepareParamKindAt(row int, kind PrepareParamKind, sourceHasValue bool, destinationHasValue bool, mp *mpool.MPool) error {
	if !sourceHasValue {
		return nil
	}
	if v.prepareParamKinds != nil {
		if row >= 0 && row < len(v.prepareParamKinds) {
			v.prepareParamKinds[row] = kind
		}
		v.prepareParamKindSeen = true
		v.prepareParamKind = PrepareParamNone
		return nil
	}
	if !v.prepareParamKindSeen && kind == PrepareParamNone {
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = true
		return nil
	}
	if !destinationHasValue && !v.hasPrepareParamValueExcept(row) {
		v.prepareParamKind = kind
		v.prepareParamKindSeen = true
		v.prepareParamKinds = nil
		return nil
	}
	if !v.prepareParamKindSeen {
		if v.length > 0 && !v.AllNull() {
			kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
			if err != nil {
				return err
			}
			for i := range kinds {
				kinds[i] = PrepareParamNone
			}
			if row >= 0 && row < len(kinds) {
				kinds[row] = kind
			}
			v.prepareParamKind = PrepareParamNone
			v.prepareParamKindSeen = true
			v.prepareParamKinds = kinds
			v.prepareParamKindsMP = owner
		} else {
			v.prepareParamKind = kind
			v.prepareParamKindSeen = true
		}
		return nil
	}
	if v.prepareParamKind != kind {
		kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
		if err != nil {
			return err
		}
		v.prepareParamKinds = kinds
		v.prepareParamKindsMP = owner
		for i := range v.prepareParamKinds {
			v.prepareParamKinds[i] = v.prepareParamKind
		}
		if row >= 0 && row < len(v.prepareParamKinds) {
			v.prepareParamKinds[row] = kind
		}
		v.prepareParamKind = PrepareParamNone
	}
	return nil
}

// prepareParamKindAppendStart records an already-materialized ordinary prefix
// before an append propagation pass. Without this one-time check, a fresh
// pre-grown destination would mistake its later rows for conflicting
// provenance and allocate a row sidecar for an otherwise uniform source.
func (v *Vector) prepareParamKindAppendStart(oldLength int) {
	if oldLength <= 0 {
		return
	}
	// CountRange is bitmap-backed and avoids a row scan on the ordinary
	// uniform append path. Reserved null slots are not evidence of a prior
	// value, so only a non-NULL prefix establishes the conservative None
	// summary.
	limit := min(oldLength, v.length)
	if limit <= 0 {
		return
	}
	if !v.nsp.EmptyByFlag() && v.nsp.Count() >= limit &&
		v.nsp.GetBitmap().CountRange(0, uint64(limit)) == limit {
		// Scalar metadata attached to an all-NULL prefix has no value owner.
		// Drop it before the first appended value establishes the lineage. A
		// preflighted sidecar is already sized for the pending append, so retain
		// its storage and let row propagation populate the new range without a
		// post-publication allocation.
		if v.prepareParamKinds == nil {
			v.resetPrepareParamKind()
		} else {
			v.prepareParamKind = PrepareParamNone
			v.prepareParamKindSeen = false
		}
		return
	}
	if !v.prepareParamKindSeen {
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = true
	}
}

// mergeUniformPrepareParamKind handles the common scalar-source append in
// constant time. A heterogeneous destination still needs row writes, so its
// callers use appendPrepareParamKindAt instead.
func (v *Vector) mergeUniformPrepareParamKind(oldLength int, kind PrepareParamKind, sourceHasValue bool, mp *mpool.MPool) error {
	if !sourceHasValue {
		return nil
	}
	v.prepareParamKindAppendStart(oldLength)
	if v.prepareParamKinds != nil {
		return nil
	}
	if !v.prepareParamKindSeen {
		v.prepareParamKind = kind
		v.prepareParamKindSeen = true
		return nil
	}
	if v.prepareParamKind == kind {
		return nil
	}
	kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
	if err != nil {
		return err
	}
	for row := range kinds {
		kinds[row] = v.prepareParamKind
	}
	for row := oldLength; row < len(kinds); row++ {
		kinds[row] = kind
	}
	v.releasePrepareParamKinds()
	v.prepareParamKinds = kinds
	v.prepareParamKindsMP = owner
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	return nil
}

func (v *Vector) appendPrepareParamKindAt(row int, kind PrepareParamKind, mp *mpool.MPool) error {
	if v.prepareParamKinds != nil {
		if row >= 0 && row < len(v.prepareParamKinds) {
			v.prepareParamKinds[row] = kind
		}
		v.prepareParamKindSeen = true
		v.prepareParamKind = PrepareParamNone
		return nil
	}
	if !v.prepareParamKindSeen {
		v.prepareParamKind = kind
		v.prepareParamKindSeen = true
		return nil
	}
	if v.prepareParamKind == kind {
		return nil
	}
	kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
	if err != nil {
		return err
	}
	for i := range kinds {
		kinds[i] = v.prepareParamKind
	}
	if row >= 0 && row < len(kinds) {
		kinds[row] = kind
	}
	v.releasePrepareParamKinds()
	v.prepareParamKinds = kinds
	v.prepareParamKindsMP = owner
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	return nil
}

func (v *Vector) hasPrepareParamValueExcept(row int) bool {
	for i := 0; i < v.length; i++ {
		if i != row && !v.IsNull(uint64(i)) {
			return true
		}
	}
	return false
}

func (v *Vector) normalizePrepareParamKinds() {
	if v.prepareParamKinds == nil {
		return
	}
	var (
		first PrepareParamKind
		seen  bool
	)
	for row := 0; row < v.length && row < len(v.prepareParamKinds); row++ {
		if v.IsNull(uint64(row)) {
			continue
		}
		kind := v.prepareParamKinds[row]
		if !seen {
			first = kind
			seen = true
		} else if first != kind {
			v.prepareParamKind = PrepareParamNone
			v.prepareParamKindSeen = true
			return
		}
	}
	if !seen {
		v.resetPrepareParamKind()
		return
	}
	v.prepareParamKind = first
	v.prepareParamKindSeen = true
	v.releasePrepareParamKinds()
}

type prepareParamKindAppendSummary struct {
	kind  PrepareParamKind
	seen  bool
	mixed bool
}

func (s *prepareParamKindAppendSummary) observe(kind PrepareParamKind) {
	if !s.seen {
		s.kind = kind
		s.seen = true
		return
	}
	if s.kind != kind {
		s.mixed = true
	}
}

func summarizePrepareParamKindAll(w *Vector) prepareParamKindAppendSummary {
	var summary prepareParamKindAppendSummary
	if w == nil || w.length == 0 || w.IsConstNull() {
		return summary
	}
	if w.prepareParamKinds == nil {
		if prepareParamRangeHasValue(w, 0, w.length, nil) {
			summary.observe(w.prepareParamKind)
		}
		return summary
	}
	for row := 0; row < w.length; row++ {
		if w.IsNull(uint64(row)) {
			continue
		}
		summary.observe(w.GetPrepareParamKindAt(row))
		if summary.mixed {
			break
		}
	}
	return summary
}

func summarizePrepareParamKindOne(w *Vector, sel int64) prepareParamKindAppendSummary {
	var summary prepareParamKindAppendSummary
	if w == nil || w.IsConstNull() || (!w.IsConst() && w.IsNull(uint64(sel))) {
		return summary
	}
	if w.IsConst() {
		sel = 0
	}
	summary.observe(w.GetPrepareParamKindAt(int(sel)))
	return summary
}

func summarizePrepareParamKindSelection[T int32 | int64](
	w *Vector,
	sels []T,
) prepareParamKindAppendSummary {
	var summary prepareParamKindAppendSummary
	if w == nil || len(sels) == 0 || w.IsConstNull() {
		return summary
	}
	if w.prepareParamKinds == nil {
		if w.IsConst() || w.GetNulls().EmptyByFlag() {
			summary.observe(w.prepareParamKind)
			return summary
		}
		for _, sel := range sels {
			if !w.IsNull(uint64(sel)) {
				summary.observe(w.prepareParamKind)
				break
			}
		}
		return summary
	}
	for _, sel := range sels {
		row := int64(sel)
		if w.IsNull(uint64(row)) {
			continue
		}
		summary.observe(w.GetPrepareParamKindAt(int(row)))
		if summary.mixed {
			break
		}
	}
	return summary
}

func summarizePrepareParamKindBatch(
	w *Vector,
	offset int64,
	cnt int,
	flags []uint8,
) prepareParamKindAppendSummary {
	var summary prepareParamKindAppendSummary
	if w == nil || cnt <= 0 || w.IsConstNull() {
		return summary
	}
	if w.prepareParamKinds == nil {
		if prepareParamRangeHasValue(w, int(offset), cnt, flags) {
			summary.observe(w.prepareParamKind)
		}
		return summary
	}
	if flags == nil {
		for i := 0; i < cnt; i++ {
			row := int(offset) + i
			if w.IsNull(uint64(row)) {
				continue
			}
			summary.observe(w.GetPrepareParamKindAt(row))
			if summary.mixed {
				break
			}
		}
		return summary
	}
	for i, selected := range flags {
		if selected == 0 {
			continue
		}
		row := int(offset) + i
		if w.IsNull(uint64(row)) {
			continue
		}
		summary.observe(w.GetPrepareParamKindAt(row))
		if summary.mixed {
			break
		}
	}
	return summary
}

// preflightPrepareParamKindAppend ensures that provenance propagation cannot
// allocate after an append publishes payload or logical length. Representation
// changes made here preserve every existing row's category; capacity growth is
// intentionally retained after a later payload allocation failure, like data
// and bitmap pre-extension.
func (v *Vector) preflightPrepareParamKindAppend(
	finalLength int,
	summary prepareParamKindAppendSummary,
	mp *mpool.MPool,
) error {
	if finalLength < v.length {
		return moerr.NewInternalErrorNoCtxf(
			"invalid prepared parameter append length %d below %d", finalLength, v.length)
	}
	if v.prepareParamKinds != nil {
		return v.preExtendPrepareParamKinds(finalLength, mp)
	}
	if !summary.seen {
		return nil
	}

	destinationHasValue := v.length > 0 && !v.AllNull()
	destinationKind := v.prepareParamKind
	if !v.prepareParamKindSeen {
		destinationKind = PrepareParamNone
	}
	if !destinationHasValue {
		// Empty/all-NULL rows do not own source semantics. Discard a stale
		// scalar before the first appended value establishes the new lineage.
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = false
		if !summary.mixed {
			return nil
		}
	} else if !summary.mixed && destinationKind == summary.kind {
		return nil
	}

	kinds, owner, err := v.allocatePrepareParamKinds(finalLength, mp)
	if err != nil {
		return err
	}
	clear(kinds)
	if destinationHasValue {
		for row := 0; row < v.length; row++ {
			kinds[row] = destinationKind
		}
	}
	v.releasePrepareParamKinds()
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = destinationHasValue
	v.prepareParamKinds = kinds[:v.length]
	v.prepareParamKindsMP = owner
	return nil
}

// preflightPrepareParamKindCopy materializes an exact representation before
// Copy overwrites a visible row. The later metadata merge is then a bounded
// row assignment and cannot report an allocation failure after payload data
// has already changed.
func (v *Vector) preflightPrepareParamKindCopy(
	row int,
	kind PrepareParamKind,
	destinationHasValue bool,
	mp *mpool.MPool,
) error {
	if v.prepareParamKinds != nil {
		return nil
	}
	if !v.prepareParamKindSeen && kind == PrepareParamNone {
		return nil
	}
	if !destinationHasValue && !v.hasPrepareParamValueExcept(row) {
		return nil
	}
	if v.prepareParamKindSeen && v.prepareParamKind == kind {
		return nil
	}

	destinationKind := v.prepareParamKind
	if !v.prepareParamKindSeen {
		destinationKind = PrepareParamNone
	}
	kinds, owner, err := v.allocatePrepareParamKinds(v.length, mp)
	if err != nil {
		return err
	}
	if destinationKind == PrepareParamNone {
		clear(kinds)
	} else {
		for i := range kinds {
			kinds[i] = destinationKind
		}
	}
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	v.prepareParamKinds = kinds
	v.prepareParamKindsMP = owner
	return nil
}

// PreflightUnionOnePrepareParamKinds reserves all provenance state that a
// subsequent UnionOne can require. Batch uses it for every column before any
// column publishes a row.
func (v *Vector) PreflightUnionOnePrepareParamKinds(
	w *Vector,
	sel int64,
	mp *mpool.MPool,
) error {
	return v.preflightPrepareParamKindAppend(
		v.length+1,
		summarizePrepareParamKindOne(w, sel),
		mp,
	)
}

// PreflightUnionPrepareParamKinds is the batch-level preflight for Union.
func (v *Vector) PreflightUnionPrepareParamKinds(
	w *Vector,
	sels []int64,
	mp *mpool.MPool,
) error {
	return v.preflightPrepareParamKindAppend(
		v.length+len(sels),
		summarizePrepareParamKindSelection(w, sels),
		mp,
	)
}

// PreflightUnionBatchPrepareParamKinds is the batch-level preflight for
// UnionBatch and whole-vector append paths.
func (v *Vector) PreflightUnionBatchPrepareParamKinds(
	w *Vector,
	offset int64,
	cnt int,
	flags []uint8,
	mp *mpool.MPool,
) error {
	addCnt := cnt
	if flags != nil {
		addCnt = 0
		for _, selected := range flags {
			addCnt += int(selected)
		}
	}
	finalLength := v.length + addCnt
	if finalLength < v.length {
		return moerr.NewInternalErrorNoCtxf(
			"invalid prepared parameter append length %d below %d", finalLength, v.length)
	}
	if v.prepareParamKinds != nil {
		return v.preExtendPrepareParamKinds(finalLength, mp)
	}
	// A fresh destination can adopt a uniform source directly during the
	// propagation pass. Keep this common reset-and-UnionBatch path O(1) without
	// constructing and re-checking a summary; heterogeneous sources still need
	// the exact preflight below.
	if v.length == 0 && w != nil && w.prepareParamKinds == nil {
		if prepareParamRangeHasValue(w, int(offset), cnt, flags) {
			v.prepareParamKind = PrepareParamNone
			v.prepareParamKindSeen = false
		}
		return nil
	}
	return v.preflightPrepareParamKindAppend(
		finalLength,
		summarizePrepareParamKindBatch(w, offset, cnt, flags),
		mp,
	)
}

func (v *Vector) propagatePrepareParamKindsAll(w *Vector, oldLength int, mp *mpool.MPool) error {
	if w == nil || w.length == 0 {
		return nil
	}
	if w.prepareParamKinds == nil {
		if !prepareParamRangeHasValue(w, 0, w.length, nil) {
			return nil
		}
		if v.prepareParamKinds == nil {
			return v.mergeUniformPrepareParamKind(oldLength, w.prepareParamKind, true, mp)
		}
	}
	v.prepareParamKindAppendStart(oldLength)
	for row := 0; row < w.length; row++ {
		if w.IsNull(uint64(row)) {
			continue
		}
		if err := v.appendPrepareParamKindAt(oldLength+row, w.GetPrepareParamKindAt(row), mp); err != nil {
			return err
		}
	}
	return nil
}

func (v *Vector) propagatePrepareParamKindsBatch(
	w *Vector,
	oldLength int,
	offset int64,
	cnt int,
	flags []uint8,
	mp *mpool.MPool,
) error {
	if w == nil || cnt <= 0 {
		return nil
	}
	if w.prepareParamKinds == nil && v.prepareParamKinds == nil &&
		prepareParamRangeHasValue(w, int(offset), cnt, flags) {
		return v.mergeUniformPrepareParamKind(oldLength, w.prepareParamKind, true, mp)
	}
	v.prepareParamKindAppendStart(oldLength)
	output := oldLength
	if flags == nil {
		for i := 0; i < cnt; i++ {
			row := int(offset) + i
			if !w.IsNull(uint64(row)) {
				if err := v.appendPrepareParamKindAt(output, w.GetPrepareParamKindAt(row), mp); err != nil {
					return err
				}
			}
			output++
		}
		return nil
	}
	for i, selected := range flags {
		if selected == 0 {
			continue
		}
		row := int(offset) + i
		if !w.IsNull(uint64(row)) {
			if err := v.appendPrepareParamKindAt(output, w.GetPrepareParamKindAt(row), mp); err != nil {
				return err
			}
		}
		output++
	}
	return nil
}

func propagatePrepareParamKindsSelection[T int32 | int64](
	v *Vector,
	w *Vector,
	oldLength int,
	sels []T,
	mp *mpool.MPool,
) error {
	if w.prepareParamKinds == nil && v.prepareParamKinds == nil {
		hasValue := false
		if w.IsConstNull() {
			return nil
		}
		if w.GetNulls().EmptyByFlag() {
			hasValue = len(sels) > 0
		} else {
			for _, sel := range sels {
				if !w.IsNull(uint64(sel)) {
					hasValue = true
					break
				}
			}
		}
		if hasValue {
			return v.mergeUniformPrepareParamKind(oldLength, w.prepareParamKind, true, mp)
		}
		return nil
	}
	v.prepareParamKindAppendStart(oldLength)
	for i, sel := range sels {
		row := int64(sel)
		if w.IsNull(uint64(row)) {
			continue
		}
		if err := v.appendPrepareParamKindAt(oldLength+i, w.GetPrepareParamKindAt(int(row)), mp); err != nil {
			return err
		}
	}
	return nil
}

// prepareParamRangeHasValue is deliberately cheap for the common no-null
// source. Only sparse null ranges require a scan to distinguish an all-NULL
// selection from one that contributes the uniform scalar kind.
func prepareParamRangeHasValue(w *Vector, offset, cnt int, flags []uint8) bool {
	if w == nil || cnt <= 0 || w.IsConstNull() {
		return false
	}
	if w.IsConst() || w.GetNulls().EmptyByFlag() {
		if flags == nil {
			return true
		}
		for _, selected := range flags {
			if selected != 0 {
				return true
			}
		}
		return false
	}
	if flags == nil {
		for i := 0; i < cnt; i++ {
			if !w.IsNull(uint64(offset + i)) {
				return true
			}
		}
		return false
	}
	for i, selected := range flags {
		if selected != 0 && !w.IsNull(uint64(offset+i)) {
			return true
		}
	}
	return false
}

func (v *Vector) clearPrepareParamKindAt(row int) {
	if v.prepareParamKinds != nil && row >= 0 && row < len(v.prepareParamKinds) {
		v.prepareParamKinds[row] = PrepareParamNone
	}
}

func (v *Vector) GetIsBinaryString() bool {
	return v.binaryString
}

func firstMPool(pools []*mpool.MPool) *mpool.MPool {
	if len(pools) == 0 {
		return nil
	}
	return pools[0]
}

func (v *Vector) HasBinaryStringRows() bool {
	return v != nil && v.binaryStringRowsActive
}

func (v *Vector) SetIsBinaryString(binaryString bool) {
	v.setBinaryStringScalar(binaryString)
}

func (v *Vector) setBinaryStringScalar(binaryString bool) {
	v.binaryString = binaryString
	v.binaryStringRowsActive = false
	if v.binaryStringRows != nil {
		v.binaryStringRows.Reset()
	}
}

func (v *Vector) resetBinaryString() {
	v.setBinaryStringScalar(false)
}

// GetIsBinaryStringAt returns the selected value's string semantics. Constants
// have one physical value, while mixed flat vectors consult the optional bitmap.
func (v *Vector) GetIsBinaryStringAt(row int) bool {
	if v == nil {
		return false
	}
	if v.IsConst() {
		row = 0
	}
	if row < 0 || row >= v.length || v.IsNull(uint64(row)) {
		return false
	}
	switch v.typ.Oid {
	case types.T_binary, types.T_varbinary, types.T_blob:
		return true
	}
	if v.binaryStringRowsActive {
		return v.binaryStringRows.Contains(uint64(row))
	}
	return v.binaryString
}

// GetBinaryStringMetadataAt reports only dynamic byte-string provenance.
// Static BINARY/VARBINARY/BLOB semantics are already carried by the type and
// must not force a newer transient wire format.
func (v *Vector) GetBinaryStringMetadataAt(row int) bool {
	if v == nil {
		return false
	}
	if v.IsConst() {
		row = 0
	}
	if row < 0 || row >= v.length || v.IsNull(uint64(row)) {
		return false
	}
	if v.binaryStringRowsActive {
		return v.binaryStringRows.Contains(uint64(row))
	}
	return v.binaryString
}

// SetIsBinaryStringAt records row-exact provenance and allocates the bitmap
// only when the new row disagrees with the uniform representation.
func (v *Vector) SetIsBinaryStringAt(row int, binaryString bool, pools ...*mpool.MPool) error {
	return v.setIsBinaryStringAt(row, binaryString, true, pools...)
}

func (v *Vector) setIsBinaryStringAt(row int, binaryString, normalize bool, pools ...*mpool.MPool) error {
	if v == nil || row < 0 || v.length == 0 {
		return nil
	}
	if v.IsConst() {
		if v.IsNull(0) {
			return nil
		}
		v.setBinaryStringScalar(binaryString)
		return nil
	}
	if row >= v.length || v.IsNull(uint64(row)) {
		return nil
	}
	if !v.binaryStringRowsActive {
		if v.binaryString == binaryString {
			return nil
		}
		mp := firstMPool(pools)
		if err := v.ensureBinaryStringCapacity(v.length, mp); err != nil {
			return err
		}
		v.binaryStringRows.InitWithSize(int64(v.length))
		v.binaryStringRowsActive = true
		if v.binaryString {
			v.binaryStringRows.AddRange(0, uint64(v.length))
			nullsIterator := v.nsp.GetBitmap().Iterator()
			for nullsIterator.HasNext() {
				nullRow := nullsIterator.Next()
				if nullRow < uint64(v.length) {
					v.binaryStringRows.Remove(nullRow)
				}
			}
		}
	} else if v.binaryStringRows.Len() < int64(v.length) {
		if err := v.ensureBinaryStringCapacity(v.length, firstMPool(pools)); err != nil {
			return err
		}
		v.binaryStringRows.TryExpandWithSize(v.length)
	}
	if binaryString {
		v.binaryStringRows.Add(uint64(row))
	} else {
		v.binaryStringRows.Remove(uint64(row))
	}
	if normalize {
		v.normalizeBinaryStringRows()
	}
	return nil
}

// SetBinaryStringRows installs row-exact provenance, collapsing uniform input
// back to the scalar representation.
func (v *Vector) SetBinaryStringRows(rows []bool) error {
	return v.SetBinaryStringRowsWithMP(rows, nil)
}

func (v *Vector) SetBinaryStringRowsWithMP(rows []bool, mp *mpool.MPool) error {
	if len(rows) != v.length {
		return moerr.NewInvalidInputNoCtxf(
			"binary-string row count %d does not match vector length %d", len(rows), v.length)
	}
	if len(rows) == 0 {
		v.resetBinaryString()
		return nil
	}
	if v.IsConst() {
		if v.IsNull(0) {
			v.setBinaryStringScalar(false)
		} else {
			v.setBinaryStringScalar(rows[0])
		}
		return nil
	}
	nonNull, binaryCount := 0, 0
	for row, binaryString := range rows {
		if v.IsNull(uint64(row)) {
			continue
		}
		nonNull++
		if binaryString {
			binaryCount++
		}
	}
	if binaryCount == 0 {
		v.setBinaryStringScalar(false)
		return nil
	}
	if binaryCount == nonNull {
		v.setBinaryStringScalar(true)
		return nil
	}
	if err := v.ensureBinaryStringCapacity(v.length, mp); err != nil {
		return err
	}
	v.binaryStringRows.InitWithSize(int64(v.length))
	v.binaryStringRowsActive = true
	for row, binaryString := range rows {
		if binaryString && !v.IsNull(uint64(row)) {
			v.binaryStringRows.Add(uint64(row))
		}
	}
	v.normalizeBinaryStringRows()
	return nil
}

func (v *Vector) normalizeBinaryStringRows() {
	if !v.binaryStringRowsActive {
		return
	}
	// Both bitmaps maintain their population count incrementally, so row-wise
	// result construction remains O(n) instead of rescanning the vector after
	// every SetIsBinaryStringAt call.
	nonNull := v.length - v.nsp.GetBitmap().CountRange(0, uint64(v.length))
	count := v.binaryStringRows.CountRange(0, uint64(v.length))
	switch {
	case count == 0:
		v.setBinaryStringScalar(false)
	case count == nonNull:
		v.setBinaryStringScalar(true)
	default:
		// GetIsBinaryString remains a conservative summary for legacy callers.
		v.binaryString = true
	}
}

func (v *Vector) clearBinaryStringAt(row int) {
	if v.binaryStringRowsActive {
		v.binaryStringRows.Remove(uint64(row))
		v.normalizeBinaryStringRows()
	} else if v.AllNull() {
		v.binaryString = false
	}
}

type binaryStringAppendSummary struct {
	seen   bool
	binary bool
	text   bool
}

func (summary *binaryStringAppendSummary) observe(binaryString bool) {
	summary.seen = true
	if binaryString {
		summary.binary = true
	} else {
		summary.text = true
	}
}

func summarizeBinaryStringOne(w *Vector, row int) (summary binaryStringAppendSummary) {
	if w == nil || w.length == 0 || w.IsNull(uint64(row)) {
		return summary
	}
	summary.observe(w.GetBinaryStringMetadataAt(row))
	return summary
}

func (v *Vector) uniformBinaryString() (bool, bool) {
	if v == nil || v.binaryStringRowsActive {
		return false, false
	}
	return v.binaryString, true
}

func summarizeBinaryStringAll(w *Vector) (summary binaryStringAppendSummary) {
	if binaryString, uniform := w.uniformBinaryString(); uniform {
		if w.IsConst() {
			if !w.IsConstNull() && w.length > 0 {
				summary.observe(binaryString)
			}
		} else if w.length > w.nsp.GetBitmap().CountRange(0, uint64(w.length)) {
			summary.observe(binaryString)
		}
		return summary
	}
	for row := 0; row < w.length; row++ {
		if !w.IsNull(uint64(row)) {
			summary.observe(w.GetBinaryStringMetadataAt(row))
			if summary.binary && summary.text {
				break
			}
		}
	}
	return summary
}

func summarizeBinaryStringSelection[T int32 | int64](w *Vector, sels []T) (summary binaryStringAppendSummary) {
	binaryString, uniform := w.uniformBinaryString()
	for _, selected := range sels {
		row := int(selected)
		if !w.IsNull(uint64(row)) {
			if uniform {
				summary.observe(binaryString)
				break
			}
			summary.observe(w.GetBinaryStringMetadataAt(row))
			if summary.binary && summary.text {
				break
			}
		}
	}
	return summary
}

func summarizeBinaryStringBatch(w *Vector, offset int64, cnt int, flags []uint8) (summary binaryStringAppendSummary) {
	binaryString, uniform := w.uniformBinaryString()
	for i := 0; i < cnt; i++ {
		if flags != nil && flags[i] == 0 {
			continue
		}
		row := int(offset) + i
		if !w.IsNull(uint64(row)) {
			if uniform {
				summary.observe(binaryString)
				break
			}
			summary.observe(w.GetBinaryStringMetadataAt(row))
			if summary.binary && summary.text {
				break
			}
		}
	}
	return summary
}

// preflightBinaryStringAppend admits any row bitmap before payload or length
// publication. Capacity growth may remain after a later failure, but visible
// values and provenance stay unchanged.
func (v *Vector) preflightBinaryStringAppend(
	finalLength int,
	summary binaryStringAppendSummary,
	mp *mpool.MPool,
) error {
	if !summary.seen {
		return nil
	}
	if v.binaryStringRowsActive {
		return v.ensureBinaryStringCapacity(finalLength, mp)
	}
	needsRows := summary.binary && summary.text
	if !needsRows {
		hasDestinationValue := v.length > v.nsp.GetBitmap().CountRange(0, uint64(v.length))
		needsRows = hasDestinationValue &&
			(v.binaryString && summary.text || !v.binaryString && summary.binary)
	}
	if needsRows {
		return v.ensureBinaryStringCapacity(finalLength, mp)
	}
	return nil
}

func (v *Vector) preflightBinaryStringCopy(row int, binaryString bool, mp *mpool.MPool) error {
	if v.binaryStringRowsActive {
		return v.ensureBinaryStringCapacity(v.length, mp)
	}
	if v.binaryString == binaryString || !v.hasPrepareParamValueExcept(row) {
		return nil
	}
	return v.ensureBinaryStringCapacity(v.length, mp)
}

func (v *Vector) PreflightUnionOneBinaryString(w *Vector, sel int64, mp *mpool.MPool) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	return v.preflightBinaryStringAppend(v.length+1, summarizeBinaryStringOne(w, int(sel)), mp)
}

func (v *Vector) PreflightUnionBinaryString(w *Vector, sels []int64, mp *mpool.MPool) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	return v.preflightBinaryStringAppend(v.length+len(sels), summarizeBinaryStringSelection(w, sels), mp)
}

func (v *Vector) PreflightUnionBatchBinaryString(
	w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool,
) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	addCount := cnt
	if flags != nil {
		addCount = 0
		for _, selected := range flags {
			addCount += int(selected)
		}
	}
	return v.preflightBinaryStringAppend(
		v.length+addCount,
		summarizeBinaryStringBatch(w, offset, cnt, flags),
		mp,
	)
}

func (v *Vector) prepareRemappedBinaryStringRows(sels []int64, mp *mpool.MPool) (
	bitmap.Bitmap, []uint64, error,
) {
	var remapped bitmap.Bitmap
	if !v.binaryStringRowsActive {
		return remapped, nil, nil
	}
	words := (len(sels) + 63) / 64
	var storage []uint64
	var err error
	if v.allocationAccount == nil {
		storage, err = mpool.MakeSlice[uint64](words, mp, v.offHeap)
	} else {
		storage, err = mpool.MakeSliceAccounted[uint64](
			words, mp, v.allocationAccount.account, v.allocationAccount.owner,
			v.allocationAccount.nullsSite)
	}
	if err != nil {
		return remapped, nil, err
	}
	remapped.InstallExternalStorage(storage)
	remapped.InitWithSize(int64(len(sels)))
	for destination, source := range sels {
		if source >= 0 && source < v.binaryStringRows.Len() &&
			v.binaryStringRows.Contains(uint64(source)) {
			remapped.Add(uint64(destination))
		}
	}
	return remapped, storage, nil
}

func (v *Vector) releaseRemappedBinaryStringRows(remapped *bitmap.Bitmap, storage []uint64, mp *mpool.MPool) {
	remapped.ReleaseExternalStorage()
	mpool.FreeSlice(mp, storage)
}

func (v *Vector) publishRemappedBinaryStringRows(remapped *bitmap.Bitmap) {
	if !v.binaryStringRowsActive {
		return
	}
	v.binaryStringRows.InitWith(remapped)
	v.binaryStringRowsActive = true
	v.normalizeBinaryStringRows()
}

func (v *Vector) copyBinaryStringTo(dst *Vector, mp *mpool.MPool) error {
	if !v.binaryStringRowsActive {
		dst.setBinaryStringScalar(v.binaryString)
		return nil
	}
	if err := dst.ensureBinaryStringCapacity(v.length, mp); err != nil {
		return err
	}
	dst.binaryStringRows.InitWith(v.binaryStringRows)
	dst.binaryString = true
	dst.binaryStringRowsActive = true
	dst.normalizeBinaryStringRows()
	return nil
}

func (v *Vector) copyBinaryStringWindowTo(dst *Vector, start, end int, mp *mpool.MPool) error {
	if !v.binaryStringRowsActive {
		dst.setBinaryStringScalar(v.binaryString)
		return nil
	}
	if start == end {
		dst.setBinaryStringScalar(v.binaryString)
		return nil
	}
	if err := dst.ensureBinaryStringCapacity(end-start, mp); err != nil {
		return err
	}
	dst.binaryStringRows.InitWithSize(int64(end - start))
	for row := start; row < end; row++ {
		if v.IsNull(uint64(row)) {
			continue
		}
		if v.binaryStringRows.Contains(uint64(row)) {
			dst.binaryStringRows.Add(uint64(row - start))
		}
	}
	dst.binaryString = true
	dst.binaryStringRowsActive = true
	dst.normalizeBinaryStringRows()
	return nil
}

func (v *Vector) propagateBinaryStringAll(w *Vector, oldLength int, mp *mpool.MPool) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	if !v.binaryString && !v.binaryStringRowsActive &&
		!w.binaryString && !w.binaryStringRowsActive {
		return nil
	}
	for row := 0; row < w.length; row++ {
		if !w.IsNull(uint64(row)) {
			if err := v.setIsBinaryStringAt(oldLength+row, w.GetBinaryStringMetadataAt(row), false, mp); err != nil {
				return err
			}
		}
	}
	v.normalizeBinaryStringRows()
	return nil
}

func propagateBinaryStringSelection[T int32 | int64](v, w *Vector, oldLength int, sels []T, mp *mpool.MPool) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	if !v.binaryString && !v.binaryStringRowsActive &&
		!w.binaryString && !w.binaryStringRowsActive {
		return nil
	}
	for output, selected := range sels {
		row := int(selected)
		if !w.IsNull(uint64(row)) {
			if err := v.setIsBinaryStringAt(oldLength+output, w.GetBinaryStringMetadataAt(row), false, mp); err != nil {
				return err
			}
		}
	}
	v.normalizeBinaryStringRows()
	return nil
}

func (v *Vector) propagateBinaryStringBatch(w *Vector, oldLength int, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	if !v.typ.Oid.IsMySQLString() && !w.typ.Oid.IsMySQLString() {
		return nil
	}
	if !v.binaryString && !v.binaryStringRowsActive &&
		!w.binaryString && !w.binaryStringRowsActive {
		return nil
	}
	output := oldLength
	for i := 0; i < cnt; i++ {
		if flags != nil && flags[i] == 0 {
			continue
		}
		row := int(offset) + i
		if !w.IsNull(uint64(row)) {
			if err := v.setIsBinaryStringAt(output, w.GetBinaryStringMetadataAt(row), false, mp); err != nil {
				return err
			}
		}
		output++
	}
	v.normalizeBinaryStringRows()
	return nil
}

func (v *Vector) NeedDup() bool {
	return v.cantFreeArea || v.cantFreeData
}

// make sure the type check is done before calling this function
func GetFixedAtNoTypeCheck[T any](v *Vector, idx int) T {
	if v.IsConst() {
		idx = 0
	}
	var slice []T
	ToSliceNoTypeCheck(v, &slice)
	return slice[idx]
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
	if v.IsConst() {
		i = 0
	}
	var bs []types.Varlena
	ToSliceNoTypeCheck(v, &bs)
	return bs[i].GetByteSlice(v.area)
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
	if v.data != nil {
		v.length = 0
	}
	if v.area != nil {
		v.area = v.area[:0]
	}
	v.nsp.Clear()
	v.gsp.Clear()
	v.sorted = false
	v.resetPrepareParamKind()
	v.resetBinaryString()
	v.areaDisjoint = v.length == 0
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
	switch vec.typ.Oid {
	case types.T_bool:
		return GetFixedAtNoTypeCheck[bool](vec, i)
	case types.T_bit:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_int8:
		return GetFixedAtNoTypeCheck[int8](vec, i)
	case types.T_int16:
		return GetFixedAtNoTypeCheck[int16](vec, i)
	case types.T_int32:
		return GetFixedAtNoTypeCheck[int32](vec, i)
	case types.T_int64:
		return GetFixedAtNoTypeCheck[int64](vec, i)
	case types.T_uint8:
		return GetFixedAtNoTypeCheck[uint8](vec, i)
	case types.T_uint16:
		return GetFixedAtNoTypeCheck[uint16](vec, i)
	case types.T_uint32:
		return GetFixedAtNoTypeCheck[uint32](vec, i)
	case types.T_uint64:
		return GetFixedAtNoTypeCheck[uint64](vec, i)
	case types.T_float32:
		return GetFixedAtNoTypeCheck[float32](vec, i)
	case types.T_float64:
		return GetFixedAtNoTypeCheck[float64](vec, i)
	case types.T_date:
		return GetFixedAtNoTypeCheck[types.Date](vec, i)
	case types.T_datetime:
		return GetFixedAtNoTypeCheck[types.Datetime](vec, i)
	case types.T_time:
		return GetFixedAtNoTypeCheck[types.Time](vec, i)
	case types.T_timestamp:
		return GetFixedAtNoTypeCheck[types.Timestamp](vec, i)
	case types.T_year:
		return GetFixedAtNoTypeCheck[types.MoYear](vec, i)
	case types.T_enum:
		return GetFixedAtNoTypeCheck[types.Enum](vec, i)
	case types.T_decimal64:
		return GetFixedAtNoTypeCheck[types.Decimal64](vec, i)
	case types.T_decimal128:
		return GetFixedAtNoTypeCheck[types.Decimal128](vec, i)
	case types.T_decimal256:
		return GetFixedAtNoTypeCheck[types.Decimal256](vec, i)
	case types.T_uuid:
		return GetFixedAtNoTypeCheck[types.Uuid](vec, i)
	case types.T_TS:
		return GetFixedAtNoTypeCheck[types.TS](vec, i)
	case types.T_Rowid:
		return GetFixedAtNoTypeCheck[types.Rowid](vec, i)
	case types.T_Blockid:
		return GetFixedAtNoTypeCheck[types.Blockid](vec, i)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		ret := vec.GetBytesAt(i)
		if deepCopy {
			copied := make([]byte, len(ret))
			copy(copied, ret)
			ret = copied
		}
		return ret
	}
	return nil
}

func NewVec(typ types.Type) *Vector {
	vec := NewVecFromReuse()
	vec.typ = typ
	vec.class = FLAT
	vec.areaDisjoint = true
	return vec
}

func NewOffHeapVecWithType(typ types.Type) *Vector {
	vec := NewVec(typ)
	vec.offHeap = true
	return vec
}

func NewOffHeapVecWithTypeAndData(typ types.Type, data []byte, length, cap int) *Vector {
	vec := NewVec(typ)
	vec.offHeap = true
	vec.data = data
	vec.length = length
	vec.areaDisjoint = !typ.IsVarlen() || length == 0
	return vec
}

func NewOffHeapVec() *Vector {
	vec := NewVecFromReuse()
	vec.offHeap = true
	vec.areaDisjoint = true
	return vec
}

func NewVecWithData(
	typ types.Type,
	length int,
	data []byte,
	area []byte,
) *Vector {
	vec := NewVec(typ)
	vec.length = length
	vec.data = data
	vec.area = area
	vec.areaDisjoint = !typ.IsVarlen() || length == 0
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
	vec := NewVec(typ)
	vec.length = length
	vec.areaDisjoint = !typ.IsVarlen() || length == 0
	var err error
	if len(data) > 0 {
		vec.data, err = vec.allocData(mp, len(data))
		if err != nil {
			vec.Free(mp)
			return nil, err
		}
		copy(vec.data, data)
	}
	if len(area) > 0 {
		vec.area, err = vec.allocArea(mp, len(area))
		if err != nil {
			vec.Free(mp)
			return nil, err
		}
		copy(vec.area, area)
	}
	return vec, nil
}

func NewConstNull(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT
	vec.length = length
	return vec
}

func NewRollupConst(typ types.Type, length int, mp *mpool.MPool) *Vector {
	vec := NewVecFromReuse()
	vec.gsp.AddRange(0, uint64(length))
	vec.typ = typ
	vec.class = CONSTANT
	vec.length = length
	return vec
}

func NewConstFixed[T any](typ types.Type, val T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstFixed(vec, val, length, mp)
	}

	return vec, err
}

func NewConstBytes(typ types.Type, val []byte, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstBytes(vec, val, length, mp)
	}

	return vec, err
}

// NewConstArray Creates a Const_Array Vector
func NewConstArray[T types.ArrayElement](typ types.Type, val []T, length int, mp *mpool.MPool) (vec *Vector, err error) {
	vec = NewVecFromReuse()
	vec.typ = typ
	vec.class = CONSTANT

	if length > 0 {
		err = SetConstArray[T](vec, val, length, mp)
	}

	return vec, err
}

func (v *Vector) IsConst() bool {
	return v.class == CONSTANT
}

func (v *Vector) IsGrouping() bool {
	return v.length > 0 &&
		v.length == v.gsp.Count() &&
		v.length == v.gsp.GetBitmap().CountRange(0, uint64(v.length))
}

func (v *Vector) SetClass(class int) {
	if v.typ.IsVarlen() && class != v.class {
		v.areaDisjoint = v.length == 0
	}
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
	v.nsp.Add(i)
	v.clearPrepareParamKindAt(int(i))
	v.clearBinaryStringAt(int(i))
	if v.AllNull() {
		v.resetPrepareParamKind()
	}
}

func (v *Vector) UnsetNull(i uint64) {
	v.nsp.Del(i)
}

// call this function if type already checked
func SetFixedAtNoTypeCheck[T types.FixedSizeT](v *Vector, idx int, t T) error {
	if v.typ.IsVarlen() {
		// A caller-provided varlena descriptor can alias an existing area range.
		v.areaDisjoint = false
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
	if v.typ.IsVarlen() {
		// A caller-provided varlena descriptor can alias an existing area range.
		v.areaDisjoint = false
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
	disjoint := v.areaDisjoint
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
	if disjoint {
		v.areaDisjoint = true
	}
	return nil
}

// SetBytesAtFrom replaces a varlen payload and its dynamic binary-string
// provenance as one logical row state. Capacity is admitted before the payload
// changes, so a metadata allocation failure cannot leave a partial overwrite.
func SetBytesAtFrom(v *Vector, idx int, source *Vector, sourceRow int, mp *mpool.MPool) error {
	return SetBytesAtWithBinaryString(
		v, idx, source.GetBytesAt(sourceRow), source.GetBinaryStringMetadataAt(sourceRow), mp)
}

func SetBytesAtWithBinaryString(
	v *Vector, idx int, value []byte, binaryString bool, mp *mpool.MPool,
) error {
	if err := v.preflightBinaryStringCopy(idx, binaryString, mp); err != nil {
		return err
	}
	if err := SetBytesAt(v, idx, value, mp); err != nil {
		return err
	}
	if !v.binaryStringRowsActive && !v.hasPrepareParamValueExcept(idx) {
		v.setBinaryStringScalar(binaryString)
		return nil
	}
	return v.SetIsBinaryStringAt(idx, binaryString, mp)
}

func (v *Vector) SetRawBytesAtFrom(idx int, source *Vector, sourceRow int, mp *mpool.MPool) error {
	binaryString := source.GetBinaryStringMetadataAt(sourceRow)
	if err := v.preflightBinaryStringCopy(idx, binaryString, mp); err != nil {
		return err
	}
	if err := v.SetRawBytesAt(idx, source.GetRawBytesAt(sourceRow), mp); err != nil {
		return err
	}
	if !v.binaryStringRowsActive && !v.hasPrepareParamValueExcept(idx) {
		v.setBinaryStringScalar(binaryString)
		return nil
	}
	return v.SetIsBinaryStringAt(idx, binaryString, mp)
}

func SetStringAt(v *Vector, idx int, bs string, mp *mpool.MPool) error {
	return SetBytesAt(v, idx, []byte(bs), mp)
}

func (v *Vector) SetRawBytesAt(i int, bs []byte, mp *mpool.MPool) error {
	if v.typ.IsVarlen() {
		return SetBytesAt(v, i, bs, mp)
	} else {
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

// VarlenaAreaIsDisjoint reports whether logical non-inline payload is bounded
// by the vector's physical area without inspecting its descriptors. Const
// vectors intentionally do not expose this proof because broadcasting one
// descriptor changes logical materialization multiplicity.
func (v *Vector) VarlenaAreaIsDisjoint() bool {
	return v != nil && v.typ.IsVarlen() && !v.IsConst() && v.areaDisjoint
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

	if !v.cantFreeData {
		mp.Free(v.data)
	}
	if !v.cantFreeArea {
		mp.Free(v.area)
	}
	v.freeBitmapStorage(mp)
	v.class = FLAT
	v.data = nil
	v.area = nil
	v.length = 0
	v.cantFreeData = false
	v.cantFreeArea = false

	v.nsp.Reset()
	v.gsp.Reset()
	v.sorted = false
	v.isBin = false
	v.binaryString = false
	v.binaryStringRowsActive = false
	v.binaryStringRows = nil
	v.resetPrepareParamKind()
	v.prepareParamKindsMP = nil
	v.allocationAccount = nil
	v.areaDisjoint = true

	// if !v.OnUsed || v.OnPut {
	// 	panic("free vector which unalloc or in put list")
	// }
	// v.OnUsed = false
	// v.OnPut = false
	// if len(v.FreeMsg) > 20 {
	// 	v.FreeMsg = v.FreeMsg[1:]
	// }
	// v.FreeMsg = append(v.FreeMsg, time.Now().String()+" : typ="+v.typ.DescString()+" "+string(debug.Stack()))

	//reuse.Free[Vector](v, nil)
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

// MarshalBinaryPlan is a validated, allocation-free snapshot of one Vector's
// wire lengths. It lets batch writers size once and encode once.
type MarshalBinaryPlan struct {
	vector     *Vector
	size       int
	dataLength uint32
	areaLength uint32
	nullLength uint32
}

func (p MarshalBinaryPlan) Size() int {
	return p.size
}

func (v *Vector) PrepareMarshalBinary() (MarshalBinaryPlan, error) {
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
	nullLength := uint64(v.nsp.MarshalSize())
	if dataLength > maxWireBuffer ||
		areaLength > maxWireBuffer ||
		nullLength > maxWireBuffer {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector buffer exceeds marshal format",
		)
	}
	if dataLength > uint64(len(v.data)) {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector data is shorter than its marshal length",
		)
	}
	total := uint64(1+types.TSize+4+4+4+4+1) +
		dataLength + areaLength + nullLength
	if total > uint64(^uint(0)>>1) {
		return MarshalBinaryPlan{}, moerr.NewInvalidInputNoCtx(
			"vector marshal size exceeds platform limit",
		)
	}
	return MarshalBinaryPlan{
		vector:     v,
		size:       int(total),
		dataLength: uint32(dataLength),
		areaLength: uint32(areaLength),
		nullLength: uint32(nullLength),
	}, nil
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
	if err := writeVectorMarshalBytes(w, types.EncodeType(&v.typ)); err != nil {
		return err
	}

	if err := writeVectorMarshalUint32(w, uint32(v.length)); err != nil {
		return err
	}

	if err := writeVectorMarshalUint32(w, p.dataLength); err != nil {
		return err
	}
	if p.dataLength > 0 {
		if err := writeVectorMarshalBytes(w, v.data[:p.dataLength]); err != nil {
			return err
		}
	}

	if err := writeVectorMarshalUint32(w, p.areaLength); err != nil {
		return err
	}
	if p.areaLength > 0 {
		if err := writeVectorMarshalBytes(w, v.area); err != nil {
			return err
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
	return writeVectorMarshalBytes(w, data[:])
}

func writeVectorMarshalUint32(w io.Writer, value uint32) error {
	if typed, ok := w.(vectorPrimitiveWriter); ok {
		return typed.WriteUint32(value)
	}
	var data [4]byte
	binary.NativeEndian.PutUint32(data[:], value)
	return writeVectorMarshalBytes(w, data[:])
}

func writeVectorMarshalBytes(w io.Writer, value []byte) error {
	written, err := w.Write(value)
	if err != nil {
		return err
	}
	if written != len(value) {
		return io.ErrShortWrite
	}
	return nil
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
	typ := types.DecodeType(typData)
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

func (v *Vector) unmarshalBinary(data []byte, validateValues bool) error {
	if v == nil {
		return io.ErrClosedPipe
	}
	v.areaDisjoint = false
	if v.allocationAccount != nil {
		return allocationAccountInvalid(
			"cannot install aliases in an accounted vector",
		)
	}
	layout, err := decodeVectorBinaryLayout(data, validateValues)
	if err != nil {
		return err
	}
	if v.hasOwnedBackingStorage() {
		return allocationAccountInvalid(
			"cannot replace owned vector storage with aliases",
		)
	}
	var decodedNulls nulls.Nulls
	if len(layout.nulls) > 0 {
		if err = decodedNulls.ReadNoCopy(layout.nulls); err != nil {
			return err
		}
	}
	v.class = int(layout.class)
	v.typ = layout.typ
	v.length = layout.length
	v.data = layout.data
	v.area = layout.area
	v.nsp = decodedNulls
	v.gsp.Reset()
	v.sorted = layout.sorted
	v.resetPrepareParamKind()
	v.resetBinaryString()
	v.cantFreeData = true
	v.cantFreeArea = true
	v.prepareParamKindsMP = nil
	v.allocationAccount = nil
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
	if class > DIST {
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
	switch typ.Oid {
	case types.T_any,
		types.T_bit,
		types.T_bool,
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
		types.T_date, types.T_time, types.T_datetime, types.T_timestamp, types.T_year,
		types.T_char, types.T_varchar, types.T_json, types.T_uuid,
		types.T_binary, types.T_varbinary, types.T_enum, types.T_geometry, types.T_geometry32,
		types.T_blob, types.T_text, types.T_datalink,
		types.T_TS, types.T_Rowid, types.T_Blockid,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16,
		types.T_array_int8, types.T_array_uint8:
	default:
		return 0, moerr.NewInvalidInputNoCtx("unknown vector type")
	}

	canonicalSize := typ.Oid.TypeLen()
	if typ.TypeSize() != canonicalSize {
		return 0, moerr.NewInvalidInputNoCtx("invalid vector type size")
	}
	return canonicalSize, nil
}

func (v *Vector) UnmarshalBinaryWithCopy(data []byte, mp *mpool.MPool) error {
	if v == nil || mp == nil {
		return io.ErrClosedPipe
	}
	v.areaDisjoint = false
	if v.hasBackingStorage() {
		return allocationAccountInvalid(
			"cannot replace vector storage without Free",
		)
	}
	layout, err := decodeVectorBinaryLayout(data, true)
	if err != nil {
		return err
	}
	decoded := NewVec(layout.typ)
	decoded.offHeap = v.offHeap
	if v.allocationAccount != nil {
		if err = decoded.SetAllocationAccount(v.allocationAccount); err != nil {
			return err
		}
	}
	committed := false
	defer func() {
		if !committed {
			decoded.Free(mp)
		}
	}()
	decoded.class = int(layout.class)
	decoded.length = layout.length
	if len(layout.data) > 0 {
		decoded.data, err = decoded.allocData(mp, len(layout.data))
		if err != nil {
			return err
		}
		copy(decoded.data, layout.data)
	}
	if len(layout.area) > 0 {
		decoded.area, err = decoded.allocArea(mp, len(layout.area))
		if err != nil {
			return err
		}
		copy(decoded.area, layout.area)
	}
	if len(layout.nulls) > 0 {
		if decoded.allocationAccount != nil {
			_, bitLength, _, decodeErr := bitmap.DecodeMarshalHeader(layout.nulls)
			if decodeErr != nil || bitLength > int64(math.MaxInt) {
				return moerr.NewInvalidInputNoCtx("invalid vector null bitmap")
			}
			if err = decoded.ensureNullCapacity(int(bitLength), mp); err != nil {
				return err
			}
		}
		if err = decoded.nsp.Read(layout.nulls); err != nil {
			return err
		}
	}
	decoded.sorted = layout.sorted
	*v = *decoded
	committed = true
	return nil
}

func (v *Vector) UnmarshalWithReader(r io.Reader, mp *mpool.MPool) error {
	if v == nil || r == nil {
		return io.ErrClosedPipe
	}
	v.areaDisjoint = false
	v.ResetWithSameType()
	var err error

	if v.class, err = types.ReadByteAsInt(r); err != nil {
		return err
	}

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

func (v *Vector) readNullsWithReader(r io.Reader, mp *mpool.MPool) error {
	if v.allocationAccount == nil {
		nspLen, err := types.ReadInt32(r)
		if err != nil {
			return err
		}
		if nspLen < 0 {
			return moerr.NewInvalidInputNoCtx("negative vector null bitmap size")
		}
		if err = validateStreamingReadSize(r, int64(nspLen)); err != nil {
			return err
		}
		if nspLen > 0 {
			nspBuf := make([]byte, nspLen)
			if _, err = io.ReadFull(r, nspBuf); err != nil {
				return err
			}
			if err := validateVectorNullBitmap(nspBuf, true); err != nil {
				return err
			}
			return v.nsp.Read(nspBuf)
		}
		v.nsp.Reset()
		return nil
	}

	size, err := types.ReadInt32(r)
	if err != nil {
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
	if err = v.ensureNullCapacity(int(bitLength), mp); err != nil {
		return err
	}
	payload, err := v.nsp.GetBitmap().PrepareExternalUnmarshal(
		header[:],
		int(size),
	)
	if err != nil {
		return err
	}
	if _, err = io.ReadFull(r, payload); err != nil {
		v.nsp.Reset()
		return err
	}
	return v.nsp.GetBitmap().Validate()
}

// GroupingMarshalBinarySize returns the optional grouping bitmap wire size.
func (v *Vector) GroupingMarshalBinarySize() int {
	if v == nil {
		return 0
	}
	return v.gsp.MarshalSize()
}

// MarshalGroupingTo writes the optional grouping bitmap without changing the
// stable Vector wire format.
func (v *Vector) MarshalGroupingTo(w io.Writer) error {
	if v == nil || w == nil {
		return io.ErrClosedPipe
	}
	return v.gsp.MarshalTo(w)
}

// UnmarshalGroupingFromReader restores a grouping bitmap whose size is framed
// by the caller.
func (v *Vector) UnmarshalGroupingFromReader(
	r io.Reader,
	size int,
	mp *mpool.MPool,
) error {
	if v == nil || r == nil || size < 0 {
		return moerr.NewInvalidInputNoCtx("invalid vector grouping bitmap")
	}
	if size == 0 {
		v.gsp.Reset()
		return nil
	}
	if size < bitmap.MarshalHeaderSize {
		return moerr.NewInvalidInputNoCtx("invalid vector grouping bitmap")
	}
	var header [bitmap.MarshalHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	_, bitLength, _, err := bitmap.DecodeMarshalHeader(header[:])
	if err != nil || bitLength > int64(math.MaxInt) {
		return moerr.NewInvalidInputNoCtx("invalid vector grouping bitmap")
	}
	if v.allocationAccount == nil {
		data := make([]byte, size)
		copy(data, header[:])
		if _, err = io.ReadFull(r, data[len(header):]); err != nil {
			return err
		}
		if err = validateVectorNullBitmap(data, true); err != nil {
			return err
		}
		v.gsp.Reset()
		return v.gsp.Read(data)
	}
	if err = v.ensureGroupingCapacity(int(bitLength), mp); err != nil {
		return err
	}
	payload, err := v.gsp.GetBitmap().PrepareExternalUnmarshal(header[:], size)
	if err != nil {
		return err
	}
	if _, err = io.ReadFull(r, payload); err != nil {
		v.gsp.Reset()
		return err
	}
	return v.gsp.GetBitmap().Validate()
}

func (v *Vector) ToConst() {
	if v.typ.IsVarlen() {
		// A constant's single physical descriptor is logically broadcast.
		v.areaDisjoint = false
	}
	v.class = CONSTANT
}

// PreExtend use to expand the capacity of the vector.
// PreExtend does not change the length of the vector.
func (v *Vector) PreExtend(rows int, mp *mpool.MPool) error {
	if v.class == CONSTANT {
		return nil
	}
	return extend(v, rows, mp)
}

// PreExtendBitmap ensures allocation-accounted null and grouping storage can
// represent rows without allocating vector data. Unaccounted vectors are
// unchanged.
func (v *Vector) PreExtendBitmap(rows int, mp *mpool.MPool) error {
	return v.ensureBitmapCapacity(rows, mp)
}

// PreExtendNulls ensures allocation-accounted null storage can represent rows.
// Unaccounted vectors are unchanged.
func (v *Vector) PreExtendNulls(rows int, mp *mpool.MPool) error {
	return v.ensureNullCapacity(rows, mp)
}

// PreExtendGrouping ensures allocation-accounted grouping storage can
// represent rows. Unaccounted vectors are unchanged.
func (v *Vector) PreExtendGrouping(rows int, mp *mpool.MPool) error {
	return v.ensureGroupingCapacity(rows, mp)
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

// Dup use to copy an identical vector
func (v *Vector) Dup(mp *mpool.MPool) (*Vector, error) {
	if v.allocationAccount != nil {
		return v.dup(mp, true, true, v.allocationAccount)
	}
	return v.dup(mp, false, v.offHeap, nil)
}

// DupOffHeap copies a vector with all owned backing data allocated off-heap.
func (v *Vector) DupOffHeap(mp *mpool.MPool) (*Vector, error) {
	return v.dup(mp, true, true, v.allocationAccount)
}

// DupOffHeapWithAllocation copies a vector into an explicitly selected
// destination account. Passing nil creates an unaccounted destination.
func (v *Vector) DupOffHeapWithAllocation(
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	return v.dup(mp, true, true, selection)
}

func (v *Vector) dup(
	mp *mpool.MPool,
	offHeap bool,
	areaOffHeap bool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	w := NewVecFromReuse()
	w.offHeap = offHeap
	if selection != nil {
		if err := w.SetAllocationAccount(selection); err != nil {
			return nil, err
		}
	}
	w.class = v.class
	w.typ = v.typ
	w.sorted = v.sorted
	if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
		w.Free(mp)
		return nil, err
	}

	if v.IsConstNull() {
		w.length = v.length
		if err := v.copyBinaryStringTo(w, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
		if v.HasGrouping() {
			if err := w.ensureGroupingCapacity(
				max(v.length, int(v.GetGrouping().GetBitmap().Len())),
				mp,
			); err != nil {
				w.Free(mp)
				return nil, err
			}
			w.GetGrouping().InitWith(v.GetGrouping())
		}
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
	// and the source bitmap extent before InitWith copies its storage.
	if v.GetNulls().GetBitmap().Len() > 0 {
		if err := w.ensureNullCapacity(
			max(v.length, int(v.GetNulls().GetBitmap().Len())),
			mp,
		); err != nil {
			w.Free(mp)
			return nil, err
		}
	}
	if v.GetGrouping().GetBitmap().Len() > 0 {
		if err := w.ensureGroupingCapacity(
			max(v.length, int(v.GetGrouping().GetBitmap().Len())),
			mp,
		); err != nil {
			w.Free(mp)
			return nil, err
		}
	}
	w.length = v.length
	w.GetNulls().InitWith(v.GetNulls())
	w.GetGrouping().InitWith(v.GetGrouping())
	if err := v.copyBinaryStringTo(w, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	copy(w.data, v.data[:dataLen])

	if len(v.area) > 0 {
		if w.area, err = w.allocOwned(mp, len(v.area), areaOffHeap, false); err != nil {
			w.Free(mp)
			return nil, err
		}
		copy(w.area, v.area)
	}
	w.areaDisjoint = v.areaDisjoint
	return w, nil
}

// CloneToFlatCompact returns a deep, flat copy of v. Unlike Dup, the clone only
// retains varlen payload referenced by the vector's logical rows, so stale or
// unreferenced bytes in area are not propagated into batch memory accounting.
func (v *Vector) CloneToFlatCompact(mp *mpool.MPool) (*Vector, error) {
	if v.allocationAccount != nil {
		return nil, allocationAccountInvalid(
			"accounted compact clone requires a destination selection",
		)
	}
	return v.cloneToFlatCompact(mp, nil)
}

// CloneToFlatCompactWithAllocation creates an off-heap compact copy under the
// explicit destination selection. Passing nil creates an unaccounted
// destination and is reserved for a deliberate ownership boundary.
func (v *Vector) CloneToFlatCompactWithAllocation(
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	return v.cloneToFlatCompact(mp, selection)
}

func (v *Vector) cloneToFlatCompact(
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	var w *Vector
	if selection == nil {
		w = NewVec(v.typ)
	} else {
		w = NewOffHeapVecWithType(v.typ)
		if err := w.SetAllocationAccount(selection); err != nil {
			return nil, err
		}
	}
	if v.class != FLAT || (!v.typ.IsFixedLen() && !v.typ.IsVarlen()) {
		if err := GetUnionAllFunction(v.typ, mp)(w, v); err != nil {
			w.Free(mp)
			return nil, err
		}
		copyBitmapWithinLength(&w.gsp, &v.gsp, v.length)
		if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
		return w, nil
	}
	if v.length == 0 {
		w.setBinaryStringScalar(v.binaryString)
		if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
		return w, nil
	}
	if err := extendWithBitmaps(
		w,
		v.length,
		mp,
		!v.nsp.EmptyByFlag(),
		!v.gsp.EmptyByFlag(),
	); err != nil {
		w.Free(mp)
		return nil, err
	}
	w.length = v.length
	copyBitmapWithinLength(&w.nsp, &v.nsp, v.length)
	copyBitmapWithinLength(&w.gsp, &v.gsp, v.length)
	if err := v.copyBinaryStringTo(w, mp); err != nil {
		w.Free(mp)
		return nil, err
	}

	if v.typ.IsFixedLen() {
		dataLen := v.length * v.typ.TypeSize()
		copy(w.data[:dataLen], v.data[:dataLen])
		if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
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
		w.area, err = w.allocArea(mp, totalArea)
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
	w.areaDisjoint = true
	if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	return w, nil
}

func copyBitmapWithinLength(dst, src *nulls.Nulls, length int) {
	if src.EmptyByFlag() {
		return
	}
	limit := uint64(length)
	src.Foreach(func(row uint64) bool {
		if row >= limit {
			return false
		}
		nulls.Add(dst, row)
		return true
	})
}

// Shrink use to shrink vectors, sels must be guaranteed to be ordered
func (v *Vector) Shrink(sels []int64, negate bool) {
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}

	shrinkSortedCheckIfRaceDetectorEnabled(sels)
	oldKinds := v.prepareParamKinds
	oldLength := v.length

	if v.IsConst() {
		if negate {
			v.length -= len(sels)
		} else {
			v.length = len(sels)
		}
		return
	}

	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixed[bool](v, sels, negate)
	case types.T_bit:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_int8:
		shrinkFixed[int8](v, sels, negate)
	case types.T_int16:
		shrinkFixed[int16](v, sels, negate)
	case types.T_int32:
		shrinkFixed[int32](v, sels, negate)
	case types.T_int64:
		shrinkFixed[int64](v, sels, negate)
	case types.T_uint8:
		shrinkFixed[uint8](v, sels, negate)
	case types.T_uint16:
		shrinkFixed[uint16](v, sels, negate)
	case types.T_uint32:
		shrinkFixed[uint32](v, sels, negate)
	case types.T_uint64:
		shrinkFixed[uint64](v, sels, negate)
	case types.T_float32:
		shrinkFixed[float32](v, sels, negate)
	case types.T_float64:
		shrinkFixed[float64](v, sels, negate)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixed[types.Varlena](v, sels, negate)
	case types.T_date:
		shrinkFixed[types.Date](v, sels, negate)
	case types.T_datetime:
		shrinkFixed[types.Datetime](v, sels, negate)
	case types.T_time:
		shrinkFixed[types.Time](v, sels, negate)
	case types.T_timestamp:
		shrinkFixed[types.Timestamp](v, sels, negate)
	case types.T_year:
		shrinkFixed[types.MoYear](v, sels, negate)
	case types.T_enum:
		shrinkFixed[types.Enum](v, sels, negate)
	case types.T_decimal64:
		shrinkFixed[types.Decimal64](v, sels, negate)
	case types.T_decimal128:
		shrinkFixed[types.Decimal128](v, sels, negate)
	case types.T_decimal256:
		shrinkFixed[types.Decimal256](v, sels, negate)
	case types.T_uuid:
		shrinkFixed[types.Uuid](v, sels, negate)
	case types.T_TS:
		shrinkFixed[types.TS](v, sels, negate)
	case types.T_Rowid:
		shrinkFixed[types.Rowid](v, sels, negate)
	case types.T_Blockid:
		shrinkFixed[types.Blockid](v, sels, negate)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
	v.remapPrepareParamKindsAfterShrink(oldKinds, oldLength, sels, negate)
	if v.binaryStringRowsActive {
		v.binaryStringRows.RemapOrdered(sels, negate)
		v.normalizeBinaryStringRows()
	}
}

func (v *Vector) ShrinkByMask(sels *bitmap.Bitmap, negate bool, offset uint64) {
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}
	if v.IsConst() {
		if negate {
			v.length -= sels.Count()
		} else {
			v.length = sels.Count()
		}
		return
	}
	oldKinds := v.prepareParamKinds
	oldLength := v.length

	switch v.typ.Oid {
	case types.T_bool:
		shrinkFixedByMask[bool](v, sels, negate, offset)
	case types.T_bit:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_int8:
		shrinkFixedByMask[int8](v, sels, negate, offset)
	case types.T_int16:
		shrinkFixedByMask[int16](v, sels, negate, offset)
	case types.T_int32:
		shrinkFixedByMask[int32](v, sels, negate, offset)
	case types.T_int64:
		shrinkFixedByMask[int64](v, sels, negate, offset)
	case types.T_uint8:
		shrinkFixedByMask[uint8](v, sels, negate, offset)
	case types.T_uint16:
		shrinkFixedByMask[uint16](v, sels, negate, offset)
	case types.T_uint32:
		shrinkFixedByMask[uint32](v, sels, negate, offset)
	case types.T_uint64:
		shrinkFixedByMask[uint64](v, sels, negate, offset)
	case types.T_float32:
		shrinkFixedByMask[float32](v, sels, negate, offset)
	case types.T_float64:
		shrinkFixedByMask[float64](v, sels, negate, offset)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		// XXX shrink varlena, but did not shrink area.  For our vector, this
		// may well be the right thing.  If want to shrink area as well, we
		// have to copy each varlena value and swizzle pointer.
		shrinkFixedByMask[types.Varlena](v, sels, negate, offset)
	case types.T_date:
		shrinkFixedByMask[types.Date](v, sels, negate, offset)
	case types.T_datetime:
		shrinkFixedByMask[types.Datetime](v, sels, negate, offset)
	case types.T_time:
		shrinkFixedByMask[types.Time](v, sels, negate, offset)
	case types.T_timestamp:
		shrinkFixedByMask[types.Timestamp](v, sels, negate, offset)
	case types.T_year:
		shrinkFixedByMask[types.MoYear](v, sels, negate, offset)
	case types.T_enum:
		shrinkFixedByMask[types.Enum](v, sels, negate, offset)
	case types.T_decimal64:
		shrinkFixedByMask[types.Decimal64](v, sels, negate, offset)
	case types.T_decimal128:
		shrinkFixedByMask[types.Decimal128](v, sels, negate, offset)
	case types.T_decimal256:
		shrinkFixedByMask[types.Decimal256](v, sels, negate, offset)
	case types.T_uuid:
		shrinkFixedByMask[types.Uuid](v, sels, negate, offset)
	case types.T_TS:
		shrinkFixedByMask[types.TS](v, sels, negate, offset)
	case types.T_Rowid:
		shrinkFixedByMask[types.Rowid](v, sels, negate, offset)
	case types.T_Blockid:
		shrinkFixedByMask[types.Blockid](v, sels, negate, offset)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shrink", v.typ))
	}
	v.remapPrepareParamKindsAfterShrinkMask(oldKinds, oldLength, sels, negate, offset)
	if v.binaryStringRowsActive {
		v.binaryStringRows.RemapMaskOrderedWithOffset(sels, negate, offset)
		v.normalizeBinaryStringRows()
	}
}

func (v *Vector) remapPrepareParamKindsAfterShrink(
	oldKinds []PrepareParamKind,
	oldLength int,
	sels []int64,
	negate bool,
) {
	if oldKinds == nil {
		return
	}
	newLength := v.length
	if !negate {
		for i, sel := range sels {
			if i >= newLength {
				break
			}
			if sel >= 0 && int(sel) < oldLength && int(sel) < len(oldKinds) {
				// sels is ordered, so the destination never overtakes the
				// source.  The sidecar can therefore be compacted in place.
				oldKinds[i] = oldKinds[sel]
			} else {
				oldKinds[i] = PrepareParamNone
			}
		}
	} else {
		write := 0
		selIdx := 0
		for row := 0; row < oldLength && row < len(oldKinds); row++ {
			if selIdx < len(sels) && int64(row) == sels[selIdx] {
				selIdx++
				continue
			}
			if write < newLength {
				oldKinds[write] = oldKinds[row]
				write++
			}
		}
	}
	v.finishInPlacePrepareParamKindRemap(oldKinds, oldLength, newLength)
}

func (v *Vector) remapPrepareParamKindsAfterShrinkMask(
	oldKinds []PrepareParamKind,
	oldLength int,
	sels *bitmap.Bitmap,
	negate bool,
	offset uint64,
) {
	if oldKinds == nil {
		return
	}
	newLength := v.length
	if !negate {
		itr := sels.Iterator()
		for row := 0; row < newLength && itr.HasNext(); row++ {
			sel := itr.Next() + offset
			if sel < uint64(oldLength) {
				oldKinds[row] = oldKinds[sel]
			} else {
				oldKinds[row] = PrepareParamNone
			}
		}
	} else if sels.Count() > 0 {
		itr := sels.Iterator()
		next := itr.Next() + offset
		write := 0
		for row := 0; row < oldLength && row < len(oldKinds); row++ {
			if uint64(row) == next {
				if itr.HasNext() {
					next = itr.Next() + offset
				}
				continue
			}
			if write < newLength {
				oldKinds[write] = oldKinds[row]
				write++
			}
		}
	}
	v.finishInPlacePrepareParamKindRemap(oldKinds, oldLength, newLength)
}

func (v *Vector) finishInPlacePrepareParamKindRemap(
	kinds []PrepareParamKind,
	oldLength int,
	newLength int,
) {
	if newLength == 0 {
		v.resetPrepareParamKind()
		return
	}
	if newLength < oldLength && newLength < len(kinds) {
		clear(kinds[newLength:min(oldLength, len(kinds))])
	}
	v.prepareParamKinds = kinds[:newLength]
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	v.normalizePrepareParamKinds()
}

// remapPrepareParamKindsAfterShuffle uses the existing sidecar whenever the
// selection fits in it.  A larger selection is uncommon, but its replacement
// sidecar is prepared before the payload shuffle so an MPool error cannot
// leave a half-committed vector or panic after the payload changed.
func (v *Vector) remapPrepareParamKindsAfterShuffle(
	oldKinds []PrepareParamKind,
	sels []int64,
	prepared []PrepareParamKind,
	preparedOwner *mpool.MPool,
) {
	if oldKinds == nil {
		return
	}
	if prepared != nil {
		for i, sel := range sels {
			if sel >= 0 && int(sel) < len(oldKinds) {
				prepared[i] = oldKinds[sel]
			} else {
				prepared[i] = PrepareParamNone
			}
		}
		v.releasePrepareParamKinds()
		v.prepareParamKinds = prepared
		v.prepareParamKindsMP = preparedOwner
		v.prepareParamKind = PrepareParamNone
		v.prepareParamKindSeen = true
		v.normalizePrepareParamKinds()
		return
	}
	newLength := len(sels)
	if newLength == 0 {
		v.resetPrepareParamKind()
		return
	}
	// Keep the original source category in the low three bits and stage the
	// output category in the next three bits.  The sidecar categories are a
	// five-value enum, so this lets arbitrary permutations and duplicate
	// selections be remapped in place without a second allocation.
	for i := range oldKinds {
		oldKinds[i] &= 0x07
	}
	for i, sel := range sels {
		if sel >= 0 && int(sel) < len(oldKinds) {
			oldKinds[i] |= (oldKinds[sel] & 0x07) << 3
		} else {
			oldKinds[i] |= PrepareParamNone << 3
		}
		oldKinds[i] |= 0x40
	}
	for i := 0; i < newLength; i++ {
		oldKinds[i] = PrepareParamKind((oldKinds[i] >> 3) & 0x07)
	}
	clear(oldKinds[newLength:])
	v.prepareParamKinds = oldKinds[:newLength]
	v.prepareParamKind = PrepareParamNone
	v.prepareParamKindSeen = true
	v.normalizePrepareParamKinds()
}

// Shuffle use to shrink vectors, sels can be disordered
func (v *Vector) Shuffle(sels []int64, mp *mpool.MPool) (err error) {
	if v.IsConst() {
		return nil
	}
	if v.binaryStringRowsActive {
		if err = v.ensureBinaryStringCapacity(len(sels), mp); err != nil {
			return err
		}
	}
	remappedBinary, remappedStorage, err := v.prepareRemappedBinaryStringRows(sels, mp)
	if err != nil {
		return err
	}
	defer v.releaseRemappedBinaryStringRows(&remappedBinary, remappedStorage, mp)
	oldKinds := v.prepareParamKinds
	var preparedKinds []PrepareParamKind
	var preparedOwner *mpool.MPool
	if oldKinds != nil && len(sels) > len(oldKinds) {
		preparedKinds, preparedOwner, err = v.allocatePrepareParamKinds(len(sels), mp)
		if err != nil {
			return err
		}
	}

	switch v.typ.Oid {
	case types.T_bool:
		err = shuffleFixedNoTypeCheck[bool](v, sels, mp)
	case types.T_bit:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_int8:
		err = shuffleFixedNoTypeCheck[int8](v, sels, mp)
	case types.T_int16:
		err = shuffleFixedNoTypeCheck[int16](v, sels, mp)
	case types.T_int32:
		err = shuffleFixedNoTypeCheck[int32](v, sels, mp)
	case types.T_int64:
		err = shuffleFixedNoTypeCheck[int64](v, sels, mp)
	case types.T_uint8:
		err = shuffleFixedNoTypeCheck[uint8](v, sels, mp)
	case types.T_uint16:
		err = shuffleFixedNoTypeCheck[uint16](v, sels, mp)
	case types.T_uint32:
		err = shuffleFixedNoTypeCheck[uint32](v, sels, mp)
	case types.T_uint64:
		err = shuffleFixedNoTypeCheck[uint64](v, sels, mp)
	case types.T_float32:
		err = shuffleFixedNoTypeCheck[float32](v, sels, mp)
	case types.T_float64:
		err = shuffleFixedNoTypeCheck[float64](v, sels, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		err = shuffleFixedNoTypeCheck[types.Varlena](v, sels, mp)
	case types.T_date:
		err = shuffleFixedNoTypeCheck[types.Date](v, sels, mp)
	case types.T_datetime:
		err = shuffleFixedNoTypeCheck[types.Datetime](v, sels, mp)
	case types.T_time:
		err = shuffleFixedNoTypeCheck[types.Time](v, sels, mp)
	case types.T_timestamp:
		err = shuffleFixedNoTypeCheck[types.Timestamp](v, sels, mp)
	case types.T_year:
		err = shuffleFixedNoTypeCheck[types.MoYear](v, sels, mp)
	case types.T_enum:
		err = shuffleFixedNoTypeCheck[types.Enum](v, sels, mp)
	case types.T_decimal64:
		err = shuffleFixedNoTypeCheck[types.Decimal64](v, sels, mp)
	case types.T_decimal128:
		err = shuffleFixedNoTypeCheck[types.Decimal128](v, sels, mp)
	case types.T_decimal256:
		err = shuffleFixedNoTypeCheck[types.Decimal256](v, sels, mp)
	case types.T_uuid:
		err = shuffleFixedNoTypeCheck[types.Uuid](v, sels, mp)
	case types.T_TS:
		err = shuffleFixedNoTypeCheck[types.TS](v, sels, mp)
	case types.T_Rowid:
		err = shuffleFixedNoTypeCheck[types.Rowid](v, sels, mp)
	case types.T_Blockid:
		err = shuffleFixedNoTypeCheck[types.Blockid](v, sels, mp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.typ))
	}

	if err != nil {
		if preparedOwner != nil {
			mpool.FreeSlice(preparedOwner, preparedKinds)
		}
		return err
	}
	v.remapPrepareParamKindsAfterShuffle(oldKinds, sels, preparedKinds, preparedOwner)
	v.publishRemappedBinaryStringRows(&remappedBinary)
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}
	return err
}

// ShuffleWithBuf is like Shuffle but reuses a scratch buffer to avoid
// alloc/free churn when the permutation preserves the element count.
// buf is grown as needed and retained across calls.
func (v *Vector) ShuffleWithBuf(sels []int64, mp *mpool.MPool, buf *[]byte) (err error) {
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}
	if v.IsConst() {
		return nil
	}
	oldKinds := v.prepareParamKinds
	// The reusable buffer is Go-heap storage and therefore has no physical
	// allocation provenance. Allocation-accounted vectors must use Shuffle,
	// whose replacement data and bitmap scratch are admitted to their owner.
	if v.allocationAccount != nil {
		return v.Shuffle(sels, mp)
	}
	// Fall back to allocating Shuffle if the vector doesn't own its data
	// or the selection changes the element count.
	if v.cantFreeData || len(sels) != v.length {
		return v.Shuffle(sels, mp)
	}
	if v.binaryStringRowsActive {
		if err = v.ensureBinaryStringCapacity(len(sels), mp); err != nil {
			return err
		}
	}
	remappedBinary, remappedStorage, err := v.prepareRemappedBinaryStringRows(sels, mp)
	if err != nil {
		return err
	}
	defer v.releaseRemappedBinaryStringRows(&remappedBinary, remappedStorage, mp)

	switch v.typ.Oid {
	case types.T_bool:
		err = shuffleFixedNoTypeCheckWithBuf[bool](v, sels, buf)
	case types.T_bit:
		err = shuffleFixedNoTypeCheckWithBuf[uint64](v, sels, buf)
	case types.T_int8:
		err = shuffleFixedNoTypeCheckWithBuf[int8](v, sels, buf)
	case types.T_int16:
		err = shuffleFixedNoTypeCheckWithBuf[int16](v, sels, buf)
	case types.T_int32:
		err = shuffleFixedNoTypeCheckWithBuf[int32](v, sels, buf)
	case types.T_int64:
		err = shuffleFixedNoTypeCheckWithBuf[int64](v, sels, buf)
	case types.T_uint8:
		err = shuffleFixedNoTypeCheckWithBuf[uint8](v, sels, buf)
	case types.T_uint16:
		err = shuffleFixedNoTypeCheckWithBuf[uint16](v, sels, buf)
	case types.T_uint32:
		err = shuffleFixedNoTypeCheckWithBuf[uint32](v, sels, buf)
	case types.T_uint64:
		err = shuffleFixedNoTypeCheckWithBuf[uint64](v, sels, buf)
	case types.T_float32:
		err = shuffleFixedNoTypeCheckWithBuf[float32](v, sels, buf)
	case types.T_float64:
		err = shuffleFixedNoTypeCheckWithBuf[float64](v, sels, buf)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		err = shuffleFixedNoTypeCheckWithBuf[types.Varlena](v, sels, buf)
	case types.T_date:
		err = shuffleFixedNoTypeCheckWithBuf[types.Date](v, sels, buf)
	case types.T_datetime:
		err = shuffleFixedNoTypeCheckWithBuf[types.Datetime](v, sels, buf)
	case types.T_time:
		err = shuffleFixedNoTypeCheckWithBuf[types.Time](v, sels, buf)
	case types.T_timestamp:
		err = shuffleFixedNoTypeCheckWithBuf[types.Timestamp](v, sels, buf)
	case types.T_year:
		err = shuffleFixedNoTypeCheckWithBuf[types.MoYear](v, sels, buf)
	case types.T_enum:
		err = shuffleFixedNoTypeCheckWithBuf[types.Enum](v, sels, buf)
	case types.T_decimal64:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal64](v, sels, buf)
	case types.T_decimal128:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal128](v, sels, buf)
	case types.T_decimal256:
		err = shuffleFixedNoTypeCheckWithBuf[types.Decimal256](v, sels, buf)
	case types.T_uuid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Uuid](v, sels, buf)
	case types.T_TS:
		err = shuffleFixedNoTypeCheckWithBuf[types.TS](v, sels, buf)
	case types.T_Rowid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Rowid](v, sels, buf)
	case types.T_Blockid:
		err = shuffleFixedNoTypeCheckWithBuf[types.Blockid](v, sels, buf)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.ShuffleWithBuf", v.typ))
	}

	if err == nil {
		v.remapPrepareParamKindsAfterShuffle(oldKinds, sels, nil, nil)
		v.publishRemappedBinaryStringRows(&remappedBinary)
	}
	return err
}

// Copy simply does v[vi] = w[wi]
func (v *Vector) Copy(w *Vector, vi, wi int64, mp *mpool.MPool) error {
	disjoint := v.areaDisjoint
	destinationHasValue := vi >= 0 && vi < int64(v.length) && !v.IsNull(uint64(vi))
	sourceGrouping := w.GetGrouping().Contains(uint64(wi))
	sourceNull := w.IsConstNull() ||
		(!w.IsConst() && w.GetNulls().Contains(uint64(wi)))
	sourceHasValue := w.length > 0 && !sourceNull
	if sourceHasValue {
		kind := w.GetPrepareParamKindAt(int(wi))
		if err := v.preflightPrepareParamKindCopy(
			int(vi), kind, destinationHasValue, mp); err != nil {
			return err
		}
		if err := v.preflightBinaryStringCopy(
			int(vi), w.GetBinaryStringMetadataAt(int(wi)), mp); err != nil {
			return err
		}
	}
	if sourceGrouping && v.allocationAccount != nil {
		if err := v.ensureGroupingCapacity(int(vi)+1, mp); err != nil {
			return err
		}
	}
	if sourceNull && v.allocationAccount != nil {
		if err := v.ensureNullCapacity(int(vi)+1, mp); err != nil {
			return err
		}
	}
	if sourceGrouping {
		v.GetGrouping().Set(uint64(vi))
	} else {
		v.GetGrouping().Unset(uint64(vi))
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
			v.clearPrepareParamKindAt(int(vi))
			if v.AllNull() {
				v.resetPrepareParamKind()
			}
			v.clearBinaryStringAt(int(vi))
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
		v.clearPrepareParamKindAt(int(vi))
		if v.AllNull() {
			v.resetPrepareParamKind()
		}
		v.clearBinaryStringAt(int(vi))
		return nil
	}
	if v.typ.IsFixedLen() {
		sz := v.typ.TypeSize()
		copy(v.data[vi*int64(sz):(vi+1)*int64(sz)], w.data[wi*int64(sz):(wi+1)*int64(sz)])
	} else {
		var err error
		vva := MustFixedColNoTypeCheck[types.Varlena](v)
		wva := MustFixedColNoTypeCheck[types.Varlena](w)
		if wva[wi].IsSmall() {
			vva[vi] = wva[wi]
		} else {
			bs := wva[wi].GetByteSlice(w.area)
			err = BuildVarlenaFromByteSlice(v, &vva[vi], &bs, mp)
			if err != nil {
				return err
			}
		}
	}

	v.GetNulls().Unset(uint64(vi))
	// Copy either installs an inline value or appends a fresh area range. The
	// overwritten descriptor becomes dead, so a valid disjoint proof survives.
	if v.typ.IsVarlen() && disjoint {
		v.areaDisjoint = true
	}
	if sourceHasValue {
		kind := w.GetPrepareParamKindAt(int(wi))
		if err := v.mergePrepareParamKindAt(int(vi), kind, true, destinationHasValue, mp); err != nil {
			return err
		}
		binaryString := w.GetBinaryStringMetadataAt(int(wi))
		if !v.binaryStringRowsActive && !v.hasPrepareParamValueExcept(int(vi)) {
			v.setBinaryStringScalar(binaryString)
		} else if err := v.SetIsBinaryStringAt(int(vi), binaryString, mp); err != nil {
			return err
		}
	}
	return nil
}

// GetUnionAllFunction: A more sensible function for copying vector,
// which avoids having to do type conversions and type judgements every time you append.
func GetUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	union := getUnionAllFunction(typ, mp)
	return func(v, w *Vector) error {
		if w.IsConst() {
			// The raw const append helpers classify their input as ordinary data.
			// UnionMulti copies a source value instead and therefore preserves its
			// provenance without a second, post-publication sidecar transition.
			// Preserve the const vector's logical grouping bitmap separately:
			// UnionMulti broadcasts one physical row, while grouping can differ
			// across the const vector's logical rows.
			oldLength := v.length
			if w.gsp.Any() {
				if err := v.ensureGroupingCapacity(oldLength+w.length, mp); err != nil {
					return err
				}
			}
			if err := v.UnionMulti(w, 0, w.length, mp); err != nil {
				return err
			}
			if w.gsp.Any() {
				nulls.RemoveRange(&v.gsp, uint64(oldLength), uint64(oldLength+w.length))
				unionVectorBitmap(&v.gsp, &w.gsp, oldLength, w.length)
			}
			return nil
		}
		oldLength := v.length
		if err := v.preflightPrepareParamKindAppend(
			oldLength+w.length,
			summarizePrepareParamKindAll(w),
			mp,
		); err != nil {
			return err
		}
		if err := v.preflightBinaryStringAppend(
			oldLength+w.length,
			summarizeBinaryStringAll(w),
			mp,
		); err != nil {
			return err
		}
		if w.gsp.Any() {
			if err := v.ensureGroupingCapacity(oldLength+w.length, mp); err != nil {
				return err
			}
		}
		if err := union(v, w); err != nil {
			return err
		}
		if w.gsp.Any() {
			unionVectorBitmap(&v.gsp, &w.gsp, oldLength, w.length)
		}
		if err := v.propagatePrepareParamKindsAll(w, oldLength, mp); err != nil {
			return err
		}
		if err := v.propagateBinaryStringAll(w, oldLength, mp); err != nil {
			return err
		}
		return nil
	}
}

func unionVectorBitmap(
	destination *nulls.Nulls,
	source *nulls.Nulls,
	offset int,
	length int,
) {
	for row := 0; row < length; row++ {
		if source.Contains(uint64(row)) {
			destination.Set(uint64(offset + row))
		}
	}
}

func getUnionAllFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector) error {
	// a more simple and quickly union nsp but not good.
	unionNsp := func(dst *nulls.Nulls, more *nulls.Nulls, oldLength int, moreLength int) {
		u64offset := uint64(oldLength)
		u64Length := uint64(moreLength)

		moreNp := more.GetBitmap()
		if moreNp == nil || moreNp.EmptyByFlag() || moreLength == 0 {
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

	switch typ.Oid {
	case types.T_bool:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[bool](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_bit:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_int8:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int8](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_int16:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int16](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_int32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_int64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[int64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_uint8:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint8](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_uint16:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint16](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_uint32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_uint64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[uint64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_float32:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[float32](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_float64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[float64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_date:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Date](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_year:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.MoYear](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_datetime:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Datetime](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_time:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Time](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_timestamp:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Timestamp](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_enum:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Enum](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_decimal64:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Decimal64](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_decimal128:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Decimal128](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_decimal256:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, types.Decimal256{}, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Decimal256](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_uuid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Uuid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_TS:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.TS](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_Rowid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Rowid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
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
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
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
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}

			bm := w.nsp.GetBitmap()
			if bm != nil && !bm.EmptyByFlag() {
				for i := range ws {
					if bm.Contains(uint64(i)) {
						vs[v.length] = types.Varlena{}
						nulls.Add(&v.nsp, uint64(v.length))
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
	case types.T_Blockid:
		return func(v, w *Vector) error {
			if w.IsConstNull() {
				if err := appendMultiFixed(v, 0, true, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if w.IsConst() {
				ws := MustFixedColNoTypeCheck[types.Blockid](w)
				if err := appendMultiFixed(v, ws[0], false, w.length, mp); err != nil {
					return err
				}
				return nil
			}
			if err := extendWithBitmaps(v, w.length, mp, w.nsp.Any(), w.gsp.Any()); err != nil {
				return err
			}
			if w.nsp.Any() {
				unionNsp(&v.nsp, &w.nsp, v.length, w.length)
			}
			if w.gsp.Any() {
				unionNsp(&v.gsp, &w.gsp, v.length, w.length)
			}
			sz := v.typ.TypeSize()
			copy(v.data[v.length*sz:], w.data[:w.length*sz])
			v.setLengthAfterExtend(v.length + w.length)
			return nil
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.GetUnionFunction", typ))
	}
}

// GetConstSetFunction: A more sensible function for const vector set,
// which avoids having to do type conversions and type judgements every time you append.
func getConstSetFunction(typ types.Type, mp *mpool.MPool) func(v, w *Vector, sel int64, length int) error {
	switch typ.Oid {
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
		types.T_json, types.T_blob, types.T_text, types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
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
		grouping := w.gsp.Contains(uint64(sel))
		if grouping {
			if err := v.ensureGroupingCapacity(length, mp); err != nil {
				return err
			}
		}
		if err := set(v, w, sel, length); err != nil {
			return err
		}
		// SetConst* materializes a selected source row into one logical
		// constant value.  The value and its prepared-parameter provenance are
		// one state: copy only the selected row's category, never the source
		// sidecar's whole row layout.  NULL has no observed source category.
		if length == 0 || w.IsConstNull() || w.IsNull(uint64(sel)) {
			v.resetPrepareParamKind()
			v.resetBinaryString()
		} else {
			v.SetPrepareParamKind(w.GetPrepareParamKindAt(int(sel)))
			v.SetIsBinaryString(w.GetBinaryStringMetadataAt(int(sel)))
		}
		v.gsp.Reset()
		if grouping && length > 0 {
			v.gsp.AddRange(0, uint64(length))
		}
		return nil
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
	return appendOneFixed(v, 0, true, mp)
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionOne(w *Vector, sel int64, mp *mpool.MPool) error {
	sourceGrouping := nulls.Contains(&w.gsp, uint64(sel))
	sourceNull := w.IsConstNull() ||
		(!w.IsConst() && nulls.Contains(&w.nsp, uint64(sel)))
	if err := v.PreflightUnionOnePrepareParamKinds(w, sel, mp); err != nil {
		return err
	}
	if err := v.PreflightUnionOneBinaryString(w, sel, mp); err != nil {
		return err
	}
	if err := extendWithBitmaps(
		v,
		1,
		mp,
		sourceNull && v.allocationAccount != nil,
		sourceGrouping && v.allocationAccount != nil,
	); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + 1)
	sourceHasValue := !sourceNull
	if sourceGrouping {
		nulls.Add(&v.gsp, uint64(oldLen))
	}
	if w.IsConst() {
		if sourceNull {
			nulls.Add(&v.nsp, uint64(oldLen))
			return nil
		}
		sel = 0
	} else if sourceNull {
		nulls.Add(&v.nsp, uint64(oldLen))
		return nil
	}
	if v.GetType().IsVarlen() {
		var vs, ws []types.Varlena
		ToSliceNoTypeCheck(v, &vs)
		ToSliceNoTypeCheck(w, &ws)
		err := BuildVarlenaFromVarlena(v, &vs[oldLen], &ws[sel], &w.area, mp)
		if err != nil {
			return err
		}
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

	if sourceHasValue {
		v.prepareParamKindAppendStart(oldLen)
		if err := v.appendPrepareParamKindAt(oldLen, w.GetPrepareParamKindAt(int(sel)), mp); err != nil {
			return err
		}
		if err := v.SetIsBinaryStringAt(oldLen, w.GetBinaryStringMetadataAt(int(sel)), mp); err != nil {
			return err
		}
	}
	return nil
}

func appendSelectedGrouping[T int32 | int64](
	dst *Vector,
	src *Vector,
	oldLength int,
	sels []T,
) {
	if src.gsp.EmptyByFlag() {
		return
	}
	for i, sel := range sels {
		if src.gsp.Contains(uint64(sel)) {
			nulls.Add(&dst.gsp, uint64(oldLength+i))
		}
	}
}

// It is simply append. the purpose of retention is ease of use
func (v *Vector) UnionMulti(w *Vector, sel int64, cnt int, mp *mpool.MPool) error {
	if cnt == 0 {
		return nil
	}
	if err := v.preflightPrepareParamKindAppend(
		v.length+cnt,
		summarizePrepareParamKindOne(w, sel),
		mp,
	); err != nil {
		return err
	}
	if err := v.preflightBinaryStringAppend(
		v.length+cnt,
		summarizeBinaryStringOne(w, int(sel)),
		mp,
	); err != nil {
		return err
	}

	sourceGrouping := nulls.Contains(&w.gsp, uint64(sel))
	sourceNull := w.IsConstNull() ||
		(!w.IsConst() && nulls.Contains(&w.nsp, uint64(sel)))
	if err := extendWithBitmaps(
		v,
		cnt,
		mp,
		sourceNull && v.allocationAccount != nil,
		sourceGrouping && v.allocationAccount != nil,
	); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + cnt)
	sourceHasValue := !sourceNull
	if sourceGrouping {
		nulls.AddRange(&v.gsp, uint64(oldLen), uint64(oldLen+cnt))
	}
	if w.IsConst() {
		if sourceNull {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+cnt))
			return nil
		}
		sel = 0
	} else if sourceNull {
		nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+cnt))
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

	if sourceHasValue {
		v.prepareParamKindAppendStart(oldLen)
	}
	for i := 0; i < cnt; i++ {
		if sourceHasValue {
			if err := v.appendPrepareParamKindAt(oldLen+i, w.GetPrepareParamKindAt(int(sel)), mp); err != nil {
				return err
			}
			if err := v.setIsBinaryStringAt(oldLen+i, w.GetBinaryStringMetadataAt(int(sel)), false, mp); err != nil {
				return err
			}
		}
	}
	v.normalizeBinaryStringRows()
	return nil
}

func appendBatchGrouping(
	dst *Vector,
	src *Vector,
	oldLength int,
	offset int64,
	cnt int,
	flags []uint8,
) {
	if src.gsp.EmptyByFlag() {
		return
	}
	output := oldLength
	if flags == nil {
		for i := range cnt {
			if src.gsp.Contains(uint64(offset) + uint64(i)) {
				nulls.Add(&dst.gsp, uint64(output+i))
			}
		}
		return
	}
	for i, selected := range flags {
		if selected == 0 {
			continue
		}
		if src.gsp.Contains(uint64(offset) + uint64(i)) {
			nulls.Add(&dst.gsp, uint64(output))
		}
		output++
	}
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
	if err := v.preflightPrepareParamKindAppend(
		v.length+len(sels),
		summarizePrepareParamKindSelection(w, sels),
		mp,
	); err != nil {
		return err
	}
	if err := v.preflightBinaryStringAppend(
		v.length+len(sels),
		summarizeBinaryStringSelection(w, sels),
		mp,
	); err != nil {
		return err
	}

	if err := extendWithBitmaps(
		v,
		len(sels),
		mp,
		w.IsConstNull() || !w.nsp.EmptyByFlag(),
		w.IsGrouping() || !w.gsp.EmptyByFlag(),
	); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}

	oldLen := v.length
	v.setLengthAfterExtend(v.length + len(sels))
	if w.IsConst() {
		if w.IsGrouping() {
			nulls.AddRange(&v.gsp, uint64(oldLen), uint64(oldLen+len(sels)))
		}
		if w.IsConstNull() {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(oldLen+len(sels)))
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

		if err := propagatePrepareParamKindsSelection(v, w, oldLen, sels, mp); err != nil {
			return err
		}
		if err := propagateBinaryStringSelection(v, w, oldLen, sels, mp); err != nil {
			return err
		}
		return nil
	}
	appendSelectedGrouping(v, w, oldLen, sels)

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
		hasNull := !w.GetNulls().EmptyByFlag()
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
		if !w.GetNulls().EmptyByFlag() {
			for i, sel := range sels {
				if w.nsp.Contains(uint64(sel)) {
					nulls.Add(&v.nsp, uint64(oldLen+i))
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
	} else {
		tlen := v.GetType().TypeSize()
		if !w.nsp.EmptyByFlag() {
			for i, sel := range sels {
				if w.nsp.Contains(uint64(sel)) {
					nulls.Add(&v.nsp, uint64(oldLen+i))
					continue
				}
				copy(v.data[(oldLen+i)*tlen:(oldLen+i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
			}
		} else {
			switch tlen {
			case 8:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*8])
					p2 := unsafe.Pointer(&w.data[int(sel)*8])
					*(*int64)(p1) = *(*int64)(p2)
				}
			case 4:
				for i, sel := range sels {
					p1 := unsafe.Pointer(&v.data[(oldLen+i)*4])
					p2 := unsafe.Pointer(&w.data[int(sel)*4])
					*(*int32)(p1) = *(*int32)(p2)
				}
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
				for i, sel := range sels {
					copy(v.data[(oldLen+i)*tlen:(oldLen+i+1)*tlen], w.data[int(sel)*tlen:(int(sel)+1)*tlen])
				}
			}
		}
	}

	if err := propagatePrepareParamKindsSelection(v, w, oldLen, sels, mp); err != nil {
		return err
	}
	if err := propagateBinaryStringSelection(v, w, oldLen, sels, mp); err != nil {
		return err
	}
	return nil
}

func (v *Vector) UnionBatch(w *Vector, offset int64, cnt int, flags []uint8, mp *mpool.MPool) error {
	addCnt := 0
	if flags == nil {
		addCnt = cnt
	} else {
		for i := range flags {
			addCnt += int(flags[i])
		}
	}

	if addCnt == 0 {
		return nil
	}
	oldLen := v.length
	if err := v.PreflightUnionBatchPrepareParamKinds(w, offset, cnt, flags, mp); err != nil {
		return err
	}
	if err := v.PreflightUnionBatchBinaryString(w, offset, cnt, flags, mp); err != nil {
		return err
	}

	if err := extendWithBitmaps(
		v,
		addCnt,
		mp,
		w.IsConstNull() || !w.nsp.EmptyByFlag(),
		w.IsGrouping() || !w.gsp.EmptyByFlag(),
	); err != nil {
		return err
	}
	if v.typ.IsVarlen() {
		v.areaDisjoint = false
	}

	if w.IsConst() {
		oldLen := v.length
		v.setLengthAfterExtend(v.length + addCnt)
		if w.IsGrouping() {
			nulls.AddRange(&v.gsp, uint64(oldLen), uint64(v.length))
		}
		if w.IsConstNull() {
			nulls.AddRange(&v.nsp, uint64(oldLen), uint64(v.length))
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

		if err := v.propagatePrepareParamKindsBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
			return err
		}
		if err := v.propagateBinaryStringBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
			return err
		}
		return nil
	}
	appendBatchGrouping(v, w, v.length, offset, cnt, flags)

	if v.GetType().IsVarlen() {
		var err error
		var vCol, wCol []types.Varlena

		vCol = toSliceOfLengthNoTypeCheck[types.Varlena](v, v.length+addCnt)
		ToSliceNoTypeCheck(w, &wCol)

		// Fast path: appending an entire in-order source varlen vector — the block-scan
		// materialization path. The general loop below calls BuildVarlenaFromVarlena
		// per row, which copies each row's content and writes each header individually:
		// N small memmoves plus incremental area growth, which the scan CPU profile
		// showed is ~50% of a table scan. Here we instead copy the whole source area in
		// ONE memmove and the whole header array in another, then rebase the non-inline
		// offsets with an unsafe walk. Nulls are fine: a null row's content is not in
		// w.area, and its header is never read — we just propagate w's null/grouping
		// bitmaps (shifted by oldLen) and zero the null rows' copied headers so no
		// rebased garbage offset lingers. Semantically identical to the loop.
		if flags == nil && offset == 0 && cnt == w.length {
			oldLen := v.length
			baseOff := len(v.area)
			if len(w.area) > 0 {
				// preserve mpool semantics: append within cap, else mpool Grow2 (so
				// v.area stays mpool-tracked rather than escaping to the Go heap).
				if baseOff+len(w.area) <= cap(v.area) {
					v.area = append(v.area, w.area...)
				} else if mp == nil {
					if v.allocationAccount != nil {
						return moerr.NewInternalErrorNoCtx(
							"accounted vector area growth does not have a mpool",
						)
					}
					v.area = append(v.area, w.area...)
				} else {
					v.area, err = v.growArea2(mp, w.area, baseOff+len(w.area))
					if err != nil {
						return err
					}
				}
			}
			// one memmove of the header array; inline varlenas carry their bytes here.
			copy(vCol[oldLen:oldLen+cnt], wCol[:cnt])
			// non-inline headers hold an offset into w.area; rebase into v.area. An
			// inline varlena has s[0] <= 23 (its length byte), never the 0xffffffff
			// big-header sentinel, so the check is exact.
			if baseOff != 0 && len(w.area) > 0 {
				for i := oldLen; i < oldLen+cnt; i++ {
					if !vCol[i].IsSmall() {
						offset, length := vCol[i].OffsetLen()
						vCol[i].SetOffsetLen(offset+uint32(baseOff), length)
					}
				}
			}
			// propagate null bits and clear those (never-read) headers so a copied
			// big-header offset can't linger as a dangling reference into v.area.
			// Same [0,cnt) bound as gsp above: a stale nsp bit at i >= cnt would
			// index vCol (len oldLen+cnt) out of range and panic.
			if !w.nsp.EmptyByFlag() {
				base, ucnt := uint64(oldLen), uint64(cnt)
				w.nsp.Foreach(func(i uint64) bool {
					if i < ucnt {
						nulls.Add(&v.nsp, base+i)
						vCol[oldLen+int(i)] = types.Varlena{}
					}
					return true
				})
			}
			v.setLengthAfterExtend(v.length + cnt)
			if err := v.propagatePrepareParamKindsBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
				return err
			}
			if err := v.propagateBinaryStringBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
				return err
			}
			return nil
		}

		// pre-grow the area once for the non-inline, non-null source rows in this
		// append so the per-row BuildVarlenaNoInline calls below never realloc. Null
		// rows are NOT copied (the loops below skip them via w.nsp), and a reused
		// vector can retain a stale non-inline header in a null slot — counting those
		// would reserve area for dead payload (large needless mp.Grow / alloc
		// failure), so exclude them to match what's actually appended.
		{
			total := 0
			hasNull := !w.nsp.EmptyByFlag()
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
		}

		if !w.nsp.EmptyByFlag() {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
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
						nulls.Add(&v.nsp, uint64(v.length))
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
		if !w.nsp.EmptyByFlag() {
			if flags == nil {
				for i := 0; i < cnt; i++ {
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
					} else {
						copy(v.data[v.length*tlen:(v.length+1)*tlen], w.data[(int(offset)+i)*tlen:(int(offset)+i+1)*tlen])
					}
					v.setLengthAfterExtend(v.length + 1)
				}
			} else {
				for i := range flags {
					if flags[i] == 0 {
						continue
					}
					if w.nsp.Contains(uint64(offset) + uint64(i)) {
						nulls.Add(&v.nsp, uint64(v.length))
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
							nulls.Add(&v.nsp, uint64(v.length))
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

	if err := v.propagatePrepareParamKindsBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
		return err
	}
	if err := v.propagateBinaryStringBatch(w, oldLen, offset, cnt, flags, mp); err != nil {
		return err
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
			if nulls.Contains(&v.nsp, 0) {
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
			if nulls.Contains(&v.nsp, 0) {
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
			if nulls.Contains(&v.nsp, 0) {
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
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			}
			return types.ArrayToString[types.BF16](col[0])
		}
		str := types.ArraysToString[types.BF16](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_float16:
		col := MustArrayCol[types.Float16](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			}
			return types.ArrayToString[types.Float16](col[0])
		}
		str := types.ArraysToString[types.Float16](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_uint8:
		col := MustArrayCol[uint8](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			}
			return types.ArrayToString[uint8](col[0])
		}
		str := types.ArraysToString[uint8](col, types.DefaultArraysToStringSep)
		return fmt.Sprintf("%v-%s", str, v.nsp.GetBitmap().String())
	case types.T_array_int8:
		col := MustArrayCol[int8](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
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

func implFixedRowToString[T types.FixedSizeT](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return fmt.Sprintf("%v", GetFixedAtNoTypeCheck[T](v, 0))
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return fmt.Sprintf("%v", GetFixedAtNoTypeCheck[T](v, idx))
	}
}

func implTimestampRowToString(v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	loc := time.Local
	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return GetFixedAtNoTypeCheck[types.Timestamp](v, 0).String2(loc, v.typ.Scale)
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return GetFixedAtNoTypeCheck[types.Timestamp](v, idx).String2(loc, v.typ.Scale)
	}
}

func implDatetimeRowToString(v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	var dt types.Datetime
	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			dt = GetFixedAtNoTypeCheck[types.Datetime](v, 0)
		}
	} else {
		if v.nsp.Contains(uint64(idx)) {
			return "null"
		} else {
			dt = GetFixedAtNoTypeCheck[types.Datetime](v, idx)
		}
	}

	return dt.String2(v.typ.Scale)
}

func implDecimalRowToString[T types.DecimalWithFormat](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return GetFixedAtNoTypeCheck[T](v, 0).Format(v.typ.Scale)
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return GetFixedAtNoTypeCheck[T](v, idx).Format(v.typ.Scale)
	}
}

func implArrayRowToString[T types.ArrayElement](v *Vector, idx int) string {
	if v.IsConstNull() {
		return "null"
	}

	if v.IsConst() {
		if nulls.Contains(&v.nsp, 0) {
			return "null"
		} else {
			return types.ArrayToString(GetArrayAt[T](v, 0))
		}
	}
	if v.nsp.Contains(uint64(idx)) {
		return "null"
	} else {
		return types.ArrayToString(GetArrayAt[T](v, idx))
	}
}

func (v *Vector) RowToString(idx int) string {
	switch v.typ.Oid {
	case types.T_bool:
		return implFixedRowToString[bool](v, idx)
	case types.T_bit:
		return implFixedRowToString[uint64](v, idx)
	case types.T_int8:
		return implFixedRowToString[int8](v, idx)
	case types.T_int16:
		return implFixedRowToString[int16](v, idx)
	case types.T_int32:
		return implFixedRowToString[int32](v, idx)
	case types.T_int64:
		return implFixedRowToString[int64](v, idx)
	case types.T_uint8:
		return implFixedRowToString[uint8](v, idx)
	case types.T_uint16:
		return implFixedRowToString[uint16](v, idx)
	case types.T_uint32:
		return implFixedRowToString[uint32](v, idx)
	case types.T_uint64:
		return implFixedRowToString[uint64](v, idx)
	case types.T_float32:
		return implFixedRowToString[float32](v, idx)
	case types.T_float64:
		return implFixedRowToString[float64](v, idx)
	case types.T_date:
		return implFixedRowToString[types.Date](v, idx)
	case types.T_year:
		return implFixedRowToString[types.MoYear](v, idx)
	case types.T_datetime:
		return implDatetimeRowToString(v, idx)
	case types.T_time:
		return implFixedRowToString[types.Time](v, idx)
	case types.T_timestamp:
		return implTimestampRowToString(v, idx)
	case types.T_enum:
		return implFixedRowToString[types.Enum](v, idx)
	case types.T_decimal64:
		return implDecimalRowToString[types.Decimal64](v, idx)
	case types.T_decimal128:
		return implDecimalRowToString[types.Decimal128](v, idx)
	case types.T_decimal256:
		return implDecimalRowToString[types.Decimal256](v, idx)
	case types.T_uuid:
		return implFixedRowToString[types.Uuid](v, idx)
	case types.T_TS:
		return implFixedRowToString[types.TS](v, idx)
	case types.T_Rowid:
		return implFixedRowToString[types.Rowid](v, idx)
	case types.T_Blockid:
		return implFixedRowToString[types.Blockid](v, idx)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
		col := MustFixedColNoTypeCheck[types.Varlena](v)
		if len(col) == 1 {
			if nulls.Contains(&v.nsp, 0) {
				return "null"
			} else {
				return col[0].UnsafeGetString(v.area)
			}
		}
		if v.nsp.Contains(uint64(idx)) {
			return "null"
		} else {
			return col[idx].UnsafeGetString(v.area)
		}
		//return fmt.Sprintf("%v-%s", col, v.nsp.GetBitmap().String())
	case types.T_array_float32:
		return implArrayRowToString[float32](v, idx)
	case types.T_array_float64:
		return implArrayRowToString[float64](v, idx)
	case types.T_array_bf16:
		return implArrayRowToString[types.BF16](v, idx)
	case types.T_array_float16:
		return implArrayRowToString[types.Float16](v, idx)
	case types.T_array_int8:
		return implArrayRowToString[int8](v, idx)
	case types.T_array_uint8:
		return implArrayRowToString[uint8](v, idx)
	default:
		panic("vec to string unknown types.")
	}
}

func SetConstNull(vec *Vector, length int, mp *mpool.MPool) error {
	if vec.typ.IsVarlen() {
		vec.areaDisjoint = false
	}
	if len(vec.data) > 0 {
		vec.data = vec.data[:0]
	}
	vec.class = CONSTANT
	vec.length = length
	return nil
}

func SetConstFixed[T any](vec *Vector, val T, length int, mp *mpool.MPool) error {
	if vec.typ.IsVarlen() {
		vec.areaDisjoint = false
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
	vec.areaDisjoint = false
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	if err := BuildVarlenaFromByteSlice(vec, &col[0], &val, mp); err != nil {
		return err
	}
	vec.length = length
	return nil
}

func SetConstByteJson(vec *Vector, bj bytejson.ByteJson, length int, mp *mpool.MPool) error {
	vec.areaDisjoint = false
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	vec.class = CONSTANT
	col := toSliceOfLengthNoTypeCheck[types.Varlena](vec, 1)
	if err := BuildVarlenaFromByteJson(vec, &col[0], bj, mp); err != nil {
		return err
	}
	vec.length = length
	return nil
}

func SetConstByteJsonEncoded(
	vec *Vector,
	enc bytejson.ByteJsonDataEncoder,
	length int,
	mp *mpool.MPool,
) error {
	vec.areaDisjoint = false
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
	vec.areaDisjoint = false
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

	switch vec.typ.Oid {
	case types.T_bool:
		return appendOneFixed(vec, val.(bool), false, mp)
	case types.T_bit:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_int8:
		return appendOneFixed(vec, val.(int8), false, mp)
	case types.T_int16:
		return appendOneFixed(vec, val.(int16), false, mp)
	case types.T_int32:
		return appendOneFixed(vec, val.(int32), false, mp)
	case types.T_int64:
		return appendOneFixed(vec, val.(int64), false, mp)
	case types.T_uint8:
		return appendOneFixed(vec, val.(uint8), false, mp)
	case types.T_uint16:
		return appendOneFixed(vec, val.(uint16), false, mp)
	case types.T_uint32:
		return appendOneFixed(vec, val.(uint32), false, mp)
	case types.T_uint64:
		return appendOneFixed(vec, val.(uint64), false, mp)
	case types.T_float32:
		return appendOneFixed(vec, val.(float32), false, mp)
	case types.T_float64:
		return appendOneFixed(vec, val.(float64), false, mp)
	case types.T_date:
		return appendOneFixed(vec, val.(types.Date), false, mp)
	case types.T_year:
		return appendOneFixed(vec, val.(types.MoYear), false, mp)
	case types.T_datetime:
		return appendOneFixed(vec, val.(types.Datetime), false, mp)
	case types.T_time:
		return appendOneFixed(vec, val.(types.Time), false, mp)
	case types.T_timestamp:
		return appendOneFixed(vec, val.(types.Timestamp), false, mp)
	case types.T_enum:
		return appendOneFixed(vec, val.(types.Enum), false, mp)
	case types.T_decimal64:
		return appendOneFixed(vec, val.(types.Decimal64), false, mp)
	case types.T_decimal128:
		return appendOneFixed(vec, val.(types.Decimal128), false, mp)
	case types.T_decimal256:
		return appendOneFixed(vec, val.(types.Decimal256), false, mp)
	case types.T_uuid:
		return appendOneFixed(vec, val.(types.Uuid), false, mp)
	case types.T_TS:
		return appendOneFixed(vec, val.(types.TS), false, mp)
	case types.T_Rowid:
		return appendOneFixed(vec, val.(types.Rowid), false, mp)
	case types.T_Blockid:
		return appendOneFixed(vec, val.(types.Blockid), false, mp)
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
		types.T_array_float32, types.T_array_float64, types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8, types.T_datalink, types.T_geometry, types.T_geometry32:
		return appendOneBytes(vec, val.([]byte), false, mp)
	}
	return nil
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
) error {
	if vec.IsConst() {
		panic(moerr.NewInternalErrorNoCtx("append to const vector"))
	}
	if mp == nil {
		panic(moerr.NewInternalErrorNoCtx("vector append does not have a mpool"))
	}

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
	wasNull := vec.nsp.Contains(uint64(index))
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
	if vec.typ.IsVarlen() && !isNull {
		// Generic fixed appends can install an arbitrary varlena descriptor.
		vec.areaDisjoint = false
	}
	if vec.IsConst() {
		return moerr.NewInternalErrorNoCtx("append to const vector")
	}

	if err := extendWithBitmaps(vec, 1, mp, isNull, false); err != nil {
		return err
	}
	if !isNull {
		if err := vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + 1)
	if isNull {
		if vec.typ.IsVarlen() {
			// Reused data capacity can contain a stale descriptor. Keep null rows
			// inside the area-disjoint invariant by making the new slot inline.
			toSliceOfLengthNoTypeCheck[types.Varlena](vec, vec.length)[length] =
				types.Varlena{}
		}
		nulls.Add(&vec.nsp, uint64(length))
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

	if isNull {
		// AppendBytes is also the generic null append used by expression
		// evaluation. Let appendOneFixed size the slot from vec.typ instead of
		// treating every null as a varlena descriptor.
		return appendOneFixed(vec, va, true, mp)
	} else {
		checkpoint := vec.MakeAppendCheckpoint()
		defer func() {
			if err != nil {
				vec.RollbackAppend(checkpoint, 1)
			}
		}()
		if err = vec.prepareOrdinaryAppendMetadata(1, mp); err != nil {
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
				vec.RollbackAppend(checkpoint, 1)
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
				vec.RollbackAppend(checkpoint, 1)
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
func appendOneOwnedVarlena(
	vec *Vector,
	value types.Varlena,
	mp *mpool.MPool,
) error {
	if err := extend(vec, 1, mp); err != nil {
		return err
	}
	index := vec.length
	vec.setLengthAfterExtend(vec.length + 1)
	toSliceOfLengthNoTypeCheck[types.Varlena](vec, vec.length)[index] = value
	return nil
}

func appendMultiFixed[T any](vec *Vector, val T, isNull bool, cnt int, mp *mpool.MPool) error {
	if vec.typ.IsVarlen() && !isNull {
		vec.areaDisjoint = false
	}
	if err := extendWithBitmaps(vec, cnt, mp, isNull, false); err != nil {
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
		nulls.AddRange(&vec.nsp, uint64(length), uint64(length+cnt))
	} else if cnt > 0 {
		// XXX check cnt > 0 to avoid issue #23295
		var col []T
		ToSlice(vec, &col)
		fillSlice(col, length, length+cnt, val)
	}
	return nil
}

func appendMultiBytes(vec *Vector, val []byte, isNull bool, cnt int, mp *mpool.MPool) (err error) {
	// A non-inline value is materialized once and its descriptor is broadcast.
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			vec.RollbackAppend(checkpoint, cnt)
		}
	}()
	vec.areaDisjoint = false
	var va types.Varlena
	if err = extendWithBitmaps(vec, cnt, mp, isNull, false); err != nil {
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
		nulls.AddRange(&vec.nsp, uint64(length), uint64(length+cnt))
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
	if vec.typ.IsVarlen() {
		vec.areaDisjoint = false
	}
	if err := extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
		false,
	); err != nil {
		return err
	}
	if len(isNulls) == 0 || slices.Contains(isNulls, false) {
		if err := vec.prepareOrdinaryAppendMetadata(len(vals), mp); err != nil {
			return err
		}
	}
	length := vec.length
	vec.setLengthAfterExtend(vec.length + len(vals))
	col := MustFixedColWithTypeCheck[T](vec)
	for i, w := range vals {
		if len(isNulls) > 0 && isNulls[i] {
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			col[length+i] = w
		}
	}
	return nil
}

func appendBytesList(vec *Vector, vals [][]byte, isNulls []bool, mp *mpool.MPool) (err error) {
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			vec.RollbackAppend(checkpoint, len(vals))
		}
	}()
	disjoint := vec.areaDisjoint
	vec.areaDisjoint = false
	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
		false,
	); err != nil {
		return err
	}
	if len(isNulls) == 0 || slices.Contains(isNulls, false) {
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
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &w, mp)
			if err != nil {
				return err
			}
		}
	}
	if disjoint {
		vec.areaDisjoint = true
	}
	return nil
}

func appendStringList(vec *Vector, vals []string, isNulls []bool, mp *mpool.MPool) (err error) {
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			vec.RollbackAppend(checkpoint, len(vals))
		}
	}()
	disjoint := vec.areaDisjoint
	vec.areaDisjoint = false

	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
		false,
	); err != nil {
		return err
	}
	if len(isNulls) == 0 || slices.Contains(isNulls, false) {
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
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			bs := []byte(w)
			err = BuildVarlenaFromByteSlice(vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	if disjoint {
		vec.areaDisjoint = true
	}
	return nil
}

// appendArrayList mainly used for unit tests
func appendArrayList[T types.ArrayElement](vec *Vector, vals [][]T, isNulls []bool, mp *mpool.MPool) (err error) {
	checkpoint := vec.MakeAppendCheckpoint()
	defer func() {
		if err != nil {
			vec.RollbackAppend(checkpoint, len(vals))
		}
	}()
	disjoint := vec.areaDisjoint
	vec.areaDisjoint = false

	if err = extendWithBitmaps(
		vec,
		len(vals),
		mp,
		slices.Contains(isNulls, true),
		false,
	); err != nil {
		return err
	}
	if len(isNulls) == 0 || slices.Contains(isNulls, false) {
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
			nulls.Add(&vec.nsp, uint64(length+i))
		} else {
			bs := w
			err = BuildVarlenaFromArray[T](vec, &col[length+i], &bs, mp)
			if err != nil {
				return err
			}
		}
	}
	if disjoint {
		vec.areaDisjoint = true
	}
	return nil
}

func shrinkFixed[T types.FixedSizeT](v *Vector, sels []int64, negate bool) {
	vs := MustFixedColNoTypeCheck[T](v)
	if !negate {
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		nulls.FilterInPlaceOrdered(&v.gsp, sels, false)
		nulls.FilterInPlaceOrdered(&v.nsp, sels, false)
		v.length = len(sels)
	} else if len(sels) > 0 {
		for oldIdx, newIdx, selIdx, sel := 0, 0, 0, sels[0]; oldIdx < v.length; oldIdx++ {
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
		nulls.FilterInPlaceOrdered(&v.gsp, sels, true)
		nulls.FilterInPlaceOrdered(&v.nsp, sels, true)
		v.length -= len(sels)
	}
}

func shrinkFixedByMask[T types.FixedSizeT](v *Vector, sels *bitmap.Bitmap, negate bool, offset uint64) {
	vs := MustFixedColNoTypeCheck[T](v)
	length := sels.Count()
	itr := sels.Iterator()
	if !negate {
		idx := 0
		for itr.HasNext() {
			vs[idx] = vs[itr.Next()+offset]
			idx++
		}
		nulls.FilterByMaskInPlace(&v.gsp, sels, false)
		nulls.FilterByMaskInPlace(&v.nsp, sels, false)
		v.length = length
	} else if length > 0 {
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
		nulls.FilterByMaskInPlace(&v.gsp, sels, true)
		nulls.FilterByMaskInPlace(&v.nsp, sels, true)
		v.length -= length
	}
}

// shuffleFixedNoTypeCheck is always used after type check. and we can use ToSliceNoTypeCheck here.
func shuffleFixedNoTypeCheck[T types.FixedSizeT](v *Vector, sels []int64, mp *mpool.MPool) error {
	sz := v.typ.TypeSize()
	olddata := v.data[:v.length*sz]
	ns := len(sels)
	var vs []T
	ToFixedColNoTypeCheck(v, &vs)
	data, err := v.allocData(mp, ns*v.GetType().TypeSize())
	if err != nil {
		return err
	}
	ws := util.UnsafeSliceCastToLength[T](data, ns)

	shuffle.FixedLengthShuffle(vs, ws, sels)
	if err := v.remapShuffleBitmaps(sels, mp); err != nil {
		mp.Free(data)
		return err
	}
	v.data = data
	// XXX We should never allow "half-owned" vectors later. And unowned vector should be strictly read-only.
	if v.cantFreeData {
		v.cantFreeData = false
	} else {
		mp.Free(olddata)
	}
	v.length = ns
	return nil
}

type bitmapRemapScratch struct {
	destination *bitmap.Bitmap
	value       bitmap.Bitmap
	storage     []uint64
}

func (s *bitmapRemapScratch) release(mp *mpool.MPool) {
	if s == nil || cap(s.storage) == 0 {
		return
	}
	s.value.ReleaseExternalStorage()
	mpool.FreeSlice(mp, s.storage)
	s.storage = nil
}

// remapShuffleBitmaps preserves Shuffle's arbitrary-selection semantics. An
// allocation-accounted vector builds both results in admitted temporary
// storage before publishing either, so rejection cannot leave null and
// grouping ownership half-mutated.
func (v *Vector) remapShuffleBitmaps(sels []int64, mp *mpool.MPool) error {
	if v.allocationAccount == nil {
		nulls.Filter(&v.gsp, sels, false)
		nulls.Filter(&v.nsp, sels, false)
		return nil
	}

	targets := [...]struct {
		destination *bitmap.Bitmap
		site        mpool.AllocationSite
	}{
		{v.gsp.GetBitmap(), v.allocationAccount.groupingSite},
		{v.nsp.GetBitmap(), v.allocationAccount.nullsSite},
	}
	if targets[0].destination.EmptyByFlag() &&
		targets[1].destination.EmptyByFlag() {
		return nil
	}
	if !targets[0].destination.EmptyByFlag() {
		if err := v.ensureGroupingCapacity(len(sels), mp); err != nil {
			return err
		}
	}
	if !targets[1].destination.EmptyByFlag() {
		if err := v.ensureNullCapacity(len(sels), mp); err != nil {
			return err
		}
	}

	var scratch [2]bitmapRemapScratch
	for i, target := range targets {
		if target.destination.EmptyByFlag() {
			continue
		}
		words := (len(sels) + 63) / 64
		storage, err := mpool.MakeSliceAccounted[uint64](
			words,
			mp,
			v.allocationAccount.account,
			v.allocationAccount.owner,
			target.site,
		)
		if err != nil {
			for j := range i {
				scratch[j].release(mp)
			}
			return err
		}
		scratch[i].destination = target.destination
		scratch[i].storage = storage
		scratch[i].value.InstallExternalStorage(storage)
		scratch[i].value.InitWithSize(int64(len(sels)))
		for output, source := range sels {
			if target.destination.Contains(uint64(source)) {
				scratch[i].value.Add(uint64(output))
			}
		}
	}

	for i := range scratch {
		if scratch[i].destination != nil {
			scratch[i].destination.InitWith(&scratch[i].value)
		}
	}
	for i := range scratch {
		scratch[i].release(mp)
	}
	return nil
}

// shuffleFixedNoTypeCheckWithBuf permutes elements using a reusable scratch
// buffer instead of allocating a new data buffer. Only valid when
// len(sels) == v.length and !v.cantFreeData (caller checks).
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

	shuffle.FixedLengthShuffle(vs, ws, sels)
	copy(v.data[:needed], *buf)
	nulls.Filter(&v.gsp, sels, false)
	nulls.Filter(&v.nsp, sels, false)
	v.length = ns
	return nil
}

func vecToString[T types.FixedSizeT](v *Vector) string {
	col := MustFixedColWithTypeCheck[T](v)
	if len(col) == 1 {
		if nulls.Contains(&v.nsp, 0) {
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

// Window returns a "window" into the Vec.
// It selects a half-open range (i.e.[start, end)).
// The returned object is NOT allowed to be modified (
// TODO: Nulls are deep copied.
func (v *Vector) Window(start, end int) (*Vector, error) {
	return v.window(start, end, nil, nil, false)
}

// WindowByLogicalRows returns a borrowed window in a caller-owned logical row
// domain. A non-empty const vector without row-specific metadata may broadcast
// its single physical value beyond Length; ordinary vectors retain the strict
// physical range check used by Window.
func (v *Vector) WindowByLogicalRows(start, end int) (*Vector, error) {
	return v.window(start, end, nil, nil, true)
}

// WindowWithAllocation returns a borrowed data window whose range bitmaps are
// physical allocations in selection. Accounted pressure paths must use this
// form so shrinking an operation cannot create invisible Go-heap owners.
func (v *Vector) WindowWithAllocation(
	start int,
	end int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	if mp == nil || selection == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return v.window(start, end, mp, selection, false)
}

// WindowByLogicalRowsWithAllocation is the allocation-accounted counterpart
// of WindowByLogicalRows.
func (v *Vector) WindowByLogicalRowsWithAllocation(
	start int,
	end int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	if mp == nil || selection == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	return v.window(start, end, mp, selection, true)
}

func (v *Vector) window(
	start int,
	end int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
	logicalRows bool,
) (*Vector, error) {
	if !v.validWindowRange(start, end, logicalRows) {
		return nil, moerr.NewInvalidInputNoCtx("invalid vector window")
	}
	w := NewVec(v.typ)
	if selection != nil {
		w.offHeap = true
		if err := w.SetAllocationAccount(selection); err != nil {
			return nil, err
		}
	}
	w.class = v.class
	w.length = end - start
	w.sorted = v.sorted
	if err := v.copyBinaryStringWindowTo(w, start, end, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	if v.prepareParamKinds != nil {
		if err := v.copyPrepareParamKindWindowToWithMP(w, start, end, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
	} else {
		if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
			w.Free(mp)
			return nil, err
		}
	}
	if err := v.copyWindowBitmaps(w, start, end, mp); err != nil {
		w.Free(mp)
		return nil, err
	}
	if v.IsConst() {
		if v.typ.IsVarlen() {
			w.areaDisjoint = false
		}
		w.data = v.data
		w.area = v.area
		// Const-null is a scalar property. In particular, an offset logical
		// window must not lose it merely because the physical null marker (when
		// present) lives at row zero.
		if v.IsConstNull() {
			w.data = nil
		}
		w.cantFreeArea = true
		w.cantFreeData = true
		return w, nil
	}
	if start != end {
		w.data = v.data[start*v.typ.TypeSize() : end*v.typ.TypeSize()]
	}
	if v.typ.IsVarlen() {
		w.area = v.area
		w.areaDisjoint = v.areaDisjoint
	}
	w.cantFreeData = true
	w.cantFreeArea = true
	return w, nil
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
// rows or by scalar const broadcast semantics. Grouping and heterogeneous
// prepared-parameter provenance are row-specific, so they cannot be extended
// beyond the vector's physical logical domain.
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
	// Nullness and a uniform prepared-parameter category are scalar const
	// properties. Grouping and a provenance sidecar are row-domain metadata;
	// extending either past its physical domain would invent row state.
	return v.Length() > 0 && !v.HasGrouping() && v.prepareParamKinds == nil
}

func (v *Vector) copyWindowBitmaps(w *Vector, start, end int, mp *mpool.MPool) error {
	length := end - start
	hasNull := v.nsp.GetBitmap().CountRange(uint64(start), uint64(end)) > 0
	hasGrouping := v.gsp.GetBitmap().CountRange(uint64(start), uint64(end)) > 0
	if hasNull {
		if err := w.PreExtendNulls(length, mp); err != nil {
			return err
		}
		nulls.Range(&v.nsp, uint64(start), uint64(end), uint64(start), &w.nsp)
	}
	if hasGrouping {
		if err := w.PreExtendGrouping(length, mp); err != nil {
			return err
		}
		nulls.Range(&v.gsp, uint64(start), uint64(end), uint64(start), &w.gsp)
	}
	return nil
}

// CloneWindow Deep copies the content from start to end into another vector. Afterwise it's safe to destroy the original one.
func (v *Vector) CloneWindow(start, end int, mp *mpool.MPool) (*Vector, error) {
	return v.CloneWindowWithAllocation(
		start,
		end,
		mp,
		v.allocationAccount,
	)
}

// CloneWindowWithAllocation deep-copies a window into an explicitly selected
// off-heap destination account.
func (v *Vector) CloneWindowWithAllocation(
	start int,
	end int,
	mp *mpool.MPool,
	selection *AllocationAccountSelection,
) (*Vector, error) {
	if start == end {
		w := NewOffHeapVecWithType(v.typ)
		w.binaryString = v.binaryString
		if selection != nil {
			if err := w.SetAllocationAccount(selection); err != nil {
				return nil, err
			}
		}
		return w, nil
	}
	if end > v.Length() {
		panic(fmt.Sprintf("CloneWindow end %d >= length %d", end, v.Length()))
	}
	w := NewOffHeapVecWithType(v.typ)
	if selection != nil {
		if err := w.SetAllocationAccount(selection); err != nil {
			return nil, err
		}
	}
	if err := v.CloneWindowTo(w, start, end, mp); err != nil {
		if mp != nil {
			w.Free(mp)
		}
		return nil, err
	}
	return w, nil
}

func (v *Vector) CloneWindowTo(w *Vector, start, end int, mp *mpool.MPool) error {
	if start == end {
		w.setBinaryStringScalar(v.binaryString)
		w.resetPrepareParamKind()
		return nil
	}
	if v.prepareParamKinds != nil {
		if err := v.copyPrepareParamKindWindowToWithMP(w, start, end, mp); err != nil {
			return err
		}
	} else {
		if err := v.copyPrepareParamKindToWithMP(w, mp); err != nil {
			return err
		}
	}
	if err := v.copyWindowBitmaps(w, start, end, mp); err != nil {
		return err
	}
	if v.IsConstNull() {
		w.class = CONSTANT
		if v.typ.IsVarlen() {
			w.areaDisjoint = false
		}
		w.length = end - start
		w.data = nil
		return v.copyBinaryStringWindowTo(w, start, end, mp)
	} else if v.IsConst() {
		if v.typ.IsVarlen() {
			w.class = CONSTANT
			if err := SetConstBytes(w, v.GetBytesAt(0), end-start, mp); err != nil {
				return err
			}
			return v.copyBinaryStringWindowTo(w, start, end, mp)
		} else {
			if mp == nil {
				if w.allocationAccount != nil {
					return moerr.NewInternalErrorNoCtx(
						"accounted vector clone does not have a mpool",
					)
				}
				w.data = make([]byte, len(v.data))
				w.cantFreeData = true
			} else {
				if err := w.PreExtend(1, mp); err != nil {
					return err
				}
				copy(w.data, v.data)
			}
			w.class = v.class
			w.length = end - start
			w.sorted = v.sorted
			return v.copyBinaryStringWindowTo(w, start, end, mp)
		}
	}
	length := (end - start) * v.typ.TypeSize()
	if mp == nil {
		if w.allocationAccount != nil {
			return moerr.NewInternalErrorNoCtx(
				"accounted vector clone does not have a mpool",
			)
		}
		w.data = make([]byte, length)
		copy(w.data, v.data[start*v.typ.TypeSize():end*v.typ.TypeSize()])
		w.length = end - start
		if v.typ.IsVarlen() {
			w.area = make([]byte, len(v.area))
			copy(w.area, v.area)
			w.areaDisjoint = v.areaDisjoint
		}
		w.cantFreeData = true
		w.cantFreeArea = true
	} else {
		err := w.PreExtend(end-start, mp)
		if err != nil {
			return err
		}
		w.length = end - start
		if v.GetType().IsVarlen() {
			// Expose the proof only after every destination descriptor has been
			// independently materialized. An allocation failure can leave a
			// partially initialized logical range.
			w.areaDisjoint = false
			var vCol, wCol []types.Varlena
			ToSliceNoTypeCheck(v, &vCol)
			ToSliceNoTypeCheck(w, &wCol)
			for i := start; i < end; i++ {
				if nulls.Contains(&v.nsp, uint64(i)) {
					wCol[i-start] = types.Varlena{}
					continue
				}
				bs := vCol[i].GetByteSlice(v.area)
				err = BuildVarlenaFromByteSlice(w, &wCol[i-start], &bs, mp)
				if err != nil {
					return err
				}
			}
			w.areaDisjoint = true
		} else {
			tlen := v.typ.TypeSize()
			copy(w.data[:length], v.data[start*tlen:end*tlen])
		}
	}

	return v.copyBinaryStringWindowTo(w, start, end, mp)
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
			for i, j := 0, len(col); i < j; i++ {
				if v.IsNull(uint64(i)) {
					continue
				}
				if first {
					minVal, maxVal = col[i], col[i]
					first = false
				} else {
					minVal = minVal && col[i]
					maxVal = maxVal && col[i]
				}
			}
		} else {
			minVal, maxVal = col[0], col[0]
			for i, j := 1, len(col); i < j; i++ {
				minVal = minVal && col[i]
				maxVal = maxVal && col[i]
			}
		}
		minv = types.EncodeBool(&minVal)
		maxv = types.EncodeBool(&maxVal)

	case types.T_bit:
		minVal, maxVal := OrderedGetMinAndMax[uint64](v)
		minv = types.EncodeUint64(&minVal)
		maxv = types.EncodeUint64(&maxVal)

	case types.T_int8:
		minVal, maxVal := OrderedGetMinAndMax[int8](v)
		minv = types.EncodeInt8(&minVal)
		maxv = types.EncodeInt8(&maxVal)

	case types.T_int16:
		minVal, maxVal := OrderedGetMinAndMax[int16](v)
		minv = types.EncodeInt16(&minVal)
		maxv = types.EncodeInt16(&maxVal)

	case types.T_int32:
		minVal, maxVal := OrderedGetMinAndMax[int32](v)
		minv = types.EncodeInt32(&minVal)
		maxv = types.EncodeInt32(&maxVal)

	case types.T_int64:
		minVal, maxVal := OrderedGetMinAndMax[int64](v)
		minv = types.EncodeInt64(&minVal)
		maxv = types.EncodeInt64(&maxVal)

	case types.T_uint8:
		minVal, maxVal := OrderedGetMinAndMax[uint8](v)
		minv = types.EncodeUint8(&minVal)
		maxv = types.EncodeUint8(&maxVal)

	case types.T_uint16:
		minVal, maxVal := OrderedGetMinAndMax[uint16](v)
		minv = types.EncodeUint16(&minVal)
		maxv = types.EncodeUint16(&maxVal)

	case types.T_uint32:
		minVal, maxVal := OrderedGetMinAndMax[uint32](v)
		minv = types.EncodeUint32(&minVal)
		maxv = types.EncodeUint32(&maxVal)

	case types.T_uint64:
		minVal, maxVal := OrderedGetMinAndMax[uint64](v)
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

	case types.T_date:
		minVal, maxVal := OrderedGetMinAndMax[types.Date](v)
		minv = types.EncodeDate(&minVal)
		maxv = types.EncodeDate(&maxVal)

	case types.T_year:
		minVal, maxVal := OrderedGetMinAndMax[types.MoYear](v)
		minv = types.EncodeMoYear(&minVal)
		maxv = types.EncodeMoYear(&maxVal)

	case types.T_datetime:
		minVal, maxVal := OrderedGetMinAndMax[types.Datetime](v)
		minv = types.EncodeDatetime(&minVal)
		maxv = types.EncodeDatetime(&maxVal)

	case types.T_time:
		minVal, maxVal := OrderedGetMinAndMax[types.Time](v)
		minv = types.EncodeTime(&minVal)
		maxv = types.EncodeTime(&maxVal)

	case types.T_timestamp:
		minVal, maxVal := OrderedGetMinAndMax[types.Timestamp](v)
		minv = types.EncodeTimestamp(&minVal)
		maxv = types.EncodeTimestamp(&maxVal)

	case types.T_enum:
		minVal, maxVal := OrderedGetMinAndMax[types.Enum](v)
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
	default:
		panic(fmt.Sprintf("unsupported type %s", v.GetType().String()))
	}
	return
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
		if cmp := bytes.Compare(s.vector.GetBytesAt(left), s.vector.GetBytesAt(right)); cmp != 0 {
			return cmp < 0
		}
	}
	leftGrouping := s.vector.gsp.Contains(uint64(left))
	rightGrouping := s.vector.gsp.Contains(uint64(right))
	if leftGrouping != rightGrouping {
		return !leftGrouping
	}
	leftKind := s.vector.GetPrepareParamKindAt(left)
	rightKind := s.vector.GetPrepareParamKindAt(right)
	if leftKind != rightKind {
		return leftKind < rightKind
	}
	leftBinary := s.vector.GetBinaryStringMetadataAt(left)
	rightBinary := s.vector.GetBinaryStringMetadataAt(right)
	return !leftBinary && rightBinary
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
	s.varlena[left], s.varlena[right] = s.varlena[right], s.varlena[left]
	leftNull, rightNull := s.vector.IsNull(uint64(left)), s.vector.IsNull(uint64(right))
	leftGrouping, rightGrouping := s.vector.gsp.Contains(uint64(left)), s.vector.gsp.Contains(uint64(right))
	setBitmapRow(s.vector.nsp.GetBitmap(), left, rightNull)
	setBitmapRow(s.vector.nsp.GetBitmap(), right, leftNull)
	setBitmapRow(s.vector.gsp.GetBitmap(), left, rightGrouping)
	setBitmapRow(s.vector.gsp.GetBitmap(), right, leftGrouping)
	if s.vector.binaryStringRowsActive {
		leftBinary := s.vector.binaryStringRows.Contains(uint64(left))
		rightBinary := s.vector.binaryStringRows.Contains(uint64(right))
		setBitmapRow(s.vector.binaryStringRows, left, rightBinary)
		setBitmapRow(s.vector.binaryStringRows, right, leftBinary)
	}
	if s.vector.prepareParamKinds != nil {
		s.vector.prepareParamKinds[left], s.vector.prepareParamKinds[right] =
			s.vector.prepareParamKinds[right], s.vector.prepareParamKinds[left]
	}
}

func (v *Vector) sortRowsEquivalent(left, right int) bool {
	if v.IsNull(uint64(left)) != v.IsNull(uint64(right)) ||
		v.gsp.Contains(uint64(left)) != v.gsp.Contains(uint64(right)) ||
		v.GetPrepareParamKindAt(left) != v.GetPrepareParamKindAt(right) ||
		v.GetBinaryStringMetadataAt(left) != v.GetBinaryStringMetadataAt(right) {
		return false
	}
	return v.IsNull(uint64(left)) || bytes.Equal(v.GetBytesAt(left), v.GetBytesAt(right))
}

func (v *Vector) copySortedRow(destination, source int, varlena []types.Varlena) {
	if destination == source {
		return
	}
	varlena[destination] = varlena[source]
	setBitmapRow(v.nsp.GetBitmap(), destination, v.IsNull(uint64(source)))
	setBitmapRow(v.gsp.GetBitmap(), destination, v.gsp.Contains(uint64(source)))
	if v.binaryStringRowsActive {
		setBitmapRow(v.binaryStringRows, destination, v.binaryStringRows.Contains(uint64(source)))
	}
	if v.prepareParamKinds != nil {
		v.prepareParamKinds[destination] = v.prepareParamKinds[source]
	}
}

func (v *Vector) inplaceSortRowMetadata(compact bool) bool {
	if (!v.binaryStringRowsActive && v.prepareParamKinds == nil) || v.IsConst() || !v.typ.IsVarlen() {
		return false
	}
	switch v.typ.Oid {
	case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary,
		types.T_blob, types.T_text, types.T_datalink, types.T_geometry, types.T_geometry32:
	default:
		return false
	}

	varlena := MustFixedColNoTypeCheck[types.Varlena](v)
	v.nsp.GetBitmap().TryExpandWithSize(v.length)
	v.gsp.GetBitmap().TryExpandWithSize(v.length)
	if v.binaryStringRowsActive {
		v.binaryStringRows.TryExpandWithSize(v.length)
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
		v.gsp.GetBitmap().RemoveRange(uint64(newLength), uint64(v.length))
		if v.binaryStringRowsActive {
			v.binaryStringRows.RemoveRange(uint64(newLength), uint64(v.length))
		}
		if v.prepareParamKinds != nil {
			clear(v.prepareParamKinds[newLength:])
			v.prepareParamKinds = v.prepareParamKinds[:newLength]
		}
		v.length = newLength
	}
	if v.prepareParamKinds != nil {
		v.normalizePrepareParamKinds()
	}
	if v.binaryStringRowsActive {
		v.normalizeBinaryStringRows()
	}
	v.sorted = true
	return true
}

// InplaceSortAndCompact @todo optimization in the future
func (v *Vector) InplaceSortAndCompact() {
	if v.inplaceSortRowMetadata(true) {
		return
	}
	cleanDataNotResetArea := func() {
		if v.data != nil {
			v.length = 0
		}
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_bit:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int8:
		col := MustFixedColNoTypeCheck[int8](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int16:
		col := MustFixedColNoTypeCheck[int16](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int32:
		col := MustFixedColNoTypeCheck[int32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_int64:
		col := MustFixedColNoTypeCheck[int64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint8:
		col := MustFixedColNoTypeCheck[uint8](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint16:
		col := MustFixedColNoTypeCheck[uint16](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint32:
		col := MustFixedColNoTypeCheck[uint32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_uint64:
		col := MustFixedColNoTypeCheck[uint64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_float32:
		col := MustFixedColNoTypeCheck[float32](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_float64:
		col := MustFixedColNoTypeCheck[float64](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_date:
		col := MustFixedColNoTypeCheck[types.Date](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_year:
		col := MustFixedColNoTypeCheck[types.MoYear](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_datetime:
		col := MustFixedColNoTypeCheck[types.Datetime](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_time:
		col := MustFixedColNoTypeCheck[types.Time](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_timestamp:
		col := MustFixedColNoTypeCheck[types.Timestamp](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
		}

	case types.T_enum:
		col := MustFixedColNoTypeCheck[types.Enum](v)
		slices.Sort(col)
		newCol := slices.Compact(col)
		if len(newCol) != len(col) {
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			v.CleanOnlyData()
			v.SetSorted(true)
			appendList(v, newCol, nil, nil)
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
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
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
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
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
			cleanDataNotResetArea()
			appendList(v, newCol, nil, nil)
		}
	case types.T_array_bf16:
		inplaceSortAndCompactArrayElement[types.BF16](v, cleanDataNotResetArea)
	case types.T_array_float16:
		inplaceSortAndCompactArrayElement[types.Float16](v, cleanDataNotResetArea)
	case types.T_array_int8:
		inplaceSortAndCompactArrayElement[int8](v, cleanDataNotResetArea)
	case types.T_array_uint8:
		inplaceSortAndCompactArrayElement[uint8](v, cleanDataNotResetArea)
	default:
		return
	}
	// Sorting happened even when compaction did not change the vector length.
	// Keep the metadata invariant aligned with the physical ordering.
	v.SetSorted(true)
}

// inplaceSortAndCompactArrayElement sorts+dedups a narrow-typed vector using the
// float32-bridged comparator (so bf16/f16 order by value, not by raw bits).
func inplaceSortAndCompactArrayElement[T types.ArrayElement](v *Vector, cleanDataNotResetArea func()) {
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
		cleanDataNotResetArea()
		appendList(v, newCol, nil, nil)
	}
}

func (v *Vector) InplaceSort() {
	if v.inplaceSortRowMetadata(false) {
		return
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
	}
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
		if vec.allocationAccount != nil {
			return moerr.NewInternalErrorNoCtx(
				"accounted vector area growth does not have a mpool",
			)
		}
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
		if voff+vlen > cap(area1) && vec.allocationAccount != nil {
			return moerr.NewInternalErrorNoCtx(
				"accounted vector area growth does not have a mpool",
			)
		}
		area1 = append(area1, byte(bj.Type))
		area1 = append(area1, bj.Data...)
	}

	v1.SetOffsetLen(uint32(voff), uint32(vlen))
	vec.area = area1
	return nil
}

func BuildVarlenaFromVarlena(vec *Vector, v1, v2 *types.Varlena, area *[]byte, m *mpool.MPool) error {
	if (*v2)[0] <= types.VarlenaInlineSize {
		BuildVarlenaInline(v1, v2)
		return nil
	}
	voff, vlen := v2.OffsetLen()
	bs := (*area)[voff : voff+vlen]
	return BuildVarlenaNoInline(vec, v1, &bs, m)
}

func BuildVarlenaFromByteSlice(vec *Vector, v *types.Varlena, bs *[]byte, m *mpool.MPool) error {
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

func BuildVarlenaFromByteJson(vec *Vector, v *types.Varlena, bj bytejson.ByteJson, m *mpool.MPool) error {
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
	return BuildVarlenaNoInlineFromByteJson(vec, v, bj, m)
}

func BuildVarlenaFromByteJsonEncoded(
	vec *Vector,
	v *types.Varlena,
	enc bytejson.ByteJsonDataEncoder,
	m *mpool.MPool,
) error {
	dataSize := uint64(enc.DataSize())
	storageSize := dataSize + 1
	maxInt := uint64(^uint(0) >> 1)
	if storageSize > uint64(^uint32(0)) || storageSize > maxInt {
		return moerr.NewInvalidInputNoCtx("json value is too large")
	}

	if storageSize <= types.VarlenaInlineSize {
		clear(v[:])
		v[0] = byte(storageSize)
		v[1] = enc.TypeCode()
		dst := v[2 : 2+int(dataSize)]
		n, err := enc.EncodeDataInto(dst)
		if err != nil {
			return err
		}
		if n != len(dst) {
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

func (v *Vector) FillRawPtrLen(dest []uintptr) {
	dest[0], dest[1] = v.nsp.RawPtrLen()
	ds := v.UnsafeGetRawData()
	dest[2] = uintptr(unsafe.Pointer(&ds[0]))
	dest[3] = uintptr(len(ds))
	if len(v.area) == 0 {
		dest[4], dest[5] = 0, 0
	} else {
		dest[4] = uintptr(unsafe.Pointer(&v.area[0]))
		dest[5] = uintptr(len(v.area))
	}
}
