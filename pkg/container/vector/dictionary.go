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

package vector

import (
	"errors"
	"math"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// dictionaryVector is the immutable composite backing stored in dataLease for
// DICTIONARY only. The ordinary Vector header keeps its original physical layout.
type dictionaryVector struct {
	refs        atomic.Int64
	base        *DictionaryBase
	rows        *RowSelection
	logicalArea int
}

var _ BufferLease = (*dictionaryVector)(nil)

func (d *dictionaryVector) Retain() bool {
	if d == nil {
		return false
	}
	for {
		refs := d.refs.Load()
		if refs <= 0 || refs == math.MaxInt64 {
			return false
		}
		if d.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

func (d *dictionaryVector) Release() {
	if d == nil {
		panic(moerr.NewInvalidStateNoCtx("release nil dictionary backing"))
	}
	refs := d.refs.Add(-1)
	if refs < 0 {
		panic(moerr.NewInvalidStateNoCtx("dictionary backing release underflow"))
	}
	if refs == 0 {
		d.rows.Release()
		d.base.Release()
	}
}

// A dictionary has no contiguous byte payload. Physical callers must flatten
// explicitly; logical callers resolve the base and immutable mapping instead.
func (*dictionaryVector) Bytes() []byte { return nil }

func (d *dictionaryVector) AccountedBytes() int64 {
	if d == nil {
		return 0
	}
	return d.base.RetainedBytes() + d.rows.RetainedBytes()
}

// dictionaryState is used only after the DICTIONARY class has established the
// representation invariant. No descriptor lookup is added to flat row loops.
func (v *Vector) dictionaryState() *dictionaryVector {
	return v.dataLease.(*dictionaryVector)
}

// Must-style physical accessors use one immutable error value so their cold
// rejection branch does not prevent the ordinary slice access from inlining.
var errDictionaryPhysicalAccess = moerr.NewInvalidStateNoCtx("dictionary vector requires a read view or explicit materialization")

func (v *Vector) IsDictionary() bool { return v != nil && v.class == DICTIONARY }

// DictionaryRetainedBytes is a conservative physical pin bound, not logical
// row size and not a second allocation charge. Several dictionaries can refer
// to the same backing; its lease remains the sole physical release owner.
func (v *Vector) DictionaryRetainedBytes() int64 {
	if !v.IsDictionary() {
		return 0
	}
	return v.dictionaryState().AccountedBytes()
}

func (v *Vector) requirePhysical() {
	if v.class == DICTIONARY {
		panic(errDictionaryPhysicalAccess)
	}
}

// NewDictionaryVector creates a private logical header over an immutable base
// and mapping. It retains both incoming handles; callers keep their references.
// No engine producer calls this API until the consumer migration is complete.
func NewDictionaryVector(base *DictionaryBase, rows *RowSelection, mp *mpool.MPool, account *AllocationAccountSelection) (*Vector, error) {
	if mp == nil || !base.Retain() {
		return nil, moerr.NewInvalidInputNoCtx("dictionary base is unavailable")
	}
	defer base.Release()
	if !rows.Retain() {
		return nil, moerr.NewInvalidInputNoCtx("dictionary row selection is unavailable")
	}
	defer rows.Release()
	if !base.vec.CoversLogicalRows(0, rows.domain) {
		return nil, moerr.NewInvalidInputNoCtx("dictionary selection has an incompatible row domain")
	}
	if !dictionaryAccountsMatch(base.vec.allocationAccount, account) ||
		(rows.Length() > 0 && !dictionaryAccountsMatch(rows.storage.allocationAccount, account)) {
		return nil, mpool.ErrAllocationAccountMismatch
	}
	return newDictionaryHeader(base, rows, base.vec, rows.storageFixed(), mp, account)
}

func dictionaryAccountsMatch(left, right *AllocationAccountSelection) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && left.account == right.account)
}

func (s *RowSelection) storageFixed() []int64 {
	return MustFixedColNoTypeCheck[int64](s.storage)
}

func newDictionaryHeader(base *DictionaryBase, mapping *RowSelection, metadata *Vector, logical []int64, mp *mpool.MPool, account *AllocationAccountSelection) (*Vector, error) {
	if metadata.typ.TypeSize() <= 0 || len(logical) > math.MaxInt/metadata.typ.TypeSize() ||
		base.RetainedBytes() > int64(math.MaxInt)-mapping.RetainedBytes() {
		return nil, moerr.NewInvalidInputNoCtx("invalid dictionary representation size")
	}
	result, err := NewOffHeapVecWithTypeAndAllocation(metadata.typ, account)
	if err != nil {
		return nil, err
	}
	if err := copyDictionaryMetadata(result, metadata, logical, mp); err != nil {
		result.Free(mp)
		return nil, err
	}
	if len(logical) == 0 {
		return result, nil
	}
	if int64(result.Allocated()) > int64(math.MaxInt)-base.RetainedBytes()-mapping.RetainedBytes() {
		result.Free(mp)
		return nil, moerr.NewInvalidInputNoCtx("dictionary retained size overflows")
	}
	if !base.Retain() {
		result.Free(mp)
		return nil, moerr.NewInvalidStateNoCtx("dictionary base was released")
	}
	if !mapping.Retain() {
		base.Release()
		result.Free(mp)
		return nil, moerr.NewInvalidStateNoCtx("dictionary selection was released")
	}
	area := 0
	if metadata.typ.IsVarlen() && !base.vec.IsConstNull() {
		for row, index := range mapping.storageFixed()[:len(logical)] {
			if result.IsNull(uint64(row)) {
				continue
			}
			value := base.vec.GetBytesAt(int(index))
			if len(value) > types.VarlenaInlineSize {
				if len(value) > math.MaxInt-area {
					mapping.Release()
					base.Release()
					result.Free(mp)
					return nil, moerr.NewInvalidInputNoCtx("dictionary logical string size overflows")
				}
				area += len(value)
			}
		}
	}
	payload := len(logical) * metadata.typ.TypeSize()
	if area > math.MaxInt-payload || result.Allocated() > math.MaxInt-payload-area {
		mapping.Release()
		base.Release()
		result.Free(mp)
		return nil, moerr.NewInvalidInputNoCtx("dictionary logical size overflows")
	}
	backing := &dictionaryVector{base: base, rows: mapping, logicalArea: area}
	backing.refs.Store(1)
	result.dataLease = backing
	result.class = DICTIONARY
	result.sorted = base.vec.sorted && mapping.monotonic
	return result, nil
}

// SelectDictionary composes a logical selection into one flat/constant base.
// It never retains a chain of dictionary headers. Metadata follows the current
// logical header, independently of the physical row used for its payload.
func (v *Vector) SelectDictionary(rows *RowSelection, mp *mpool.MPool, account *AllocationAccountSelection) (*Vector, error) {
	if v == nil || mp == nil || !rows.Retain() {
		return nil, moerr.NewInvalidInputNoCtx("invalid dictionary composition")
	}
	defer rows.Release()
	if !v.CoversLogicalRows(0, rows.domain) {
		return nil, moerr.NewInvalidInputNoCtx("dictionary composition has an incompatible row domain")
	}
	if rows.Length() > 0 && !dictionaryAccountsMatch(rows.storage.allocationAccount, account) {
		return nil, mpool.ErrAllocationAccountMismatch
	}
	if !v.IsDictionary() {
		base, err := FreezeVector(v, mp, account)
		if errors.Is(err, mpool.ErrAllocationAccountCapacity) {
			// A complete backing/base copy may exceed the budget even when
			// materializing this small logical selection still fits.
			flat, allocErr := NewOffHeapVecWithTypeAndAllocation(v.typ, account)
			if allocErr != nil {
				return nil, allocErr
			}
			if allocErr := flat.Union(v, rows.storageFixed(), mp); allocErr != nil {
				flat.Free(mp)
				return nil, allocErr
			}
			v.CopyExpressionMetadataTo(flat)
			return flat, nil
		}
		if err != nil {
			return nil, err
		}
		defer base.Release()
		return NewDictionaryVector(base, rows, mp, account)
	}
	if !dictionaryAccountsMatch(v.allocationAccount, account) {
		return nil, mpool.ErrAllocationAccountMismatch
	}
	logical := rows.storageFixed()
	if len(logical) == 0 {
		return newDictionaryHeader(v.dictionaryState().base, rows, v, nil, mp, account)
	}
	// The intermediate mapping is an accounted vector, never an unbounded
	// Go-heap copy. Transfer its immutable storage into a selection lease.
	composed, err := NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), account)
	if err != nil {
		return nil, err
	}
	if err := composed.PreExtend(len(logical), mp); err != nil {
		composed.Free(mp)
		return nil, err
	}
	composed.length = len(logical)
	indices := MustFixedColNoTypeCheck[int64](composed)
	old := v.dictionaryState().rows.storageFixed()
	monotonic := true
	for i, row := range logical {
		indices[i] = old[row]
		if i > 0 && indices[i] < indices[i-1] {
			monotonic = false
		}
	}
	lease, err := NewRefCountedBufferLease(composed.data, int64(composed.Allocated()), func() { composed.Free(mp) })
	if err != nil {
		composed.Free(mp)
		return nil, err
	}
	mapping := &RowSelection{lease: lease, storage: composed, domain: v.dictionaryState().rows.domain, monotonic: monotonic}
	defer mapping.Release()
	return newDictionaryHeader(v.dictionaryState().base, mapping, v, logical, mp, account)
}

func copyDictionaryMetadata(dst, source *Vector, rows []int64, mp *mpool.MPool) error {
	if err := dst.preflightStringSourceAppend(len(rows), summarizeStringSourceSelection(source, rows), mp); err != nil {
		return err
	}
	if err := dst.preflightPrepareParamKindAppend(len(rows), summarizePrepareParamKindSelection(source, rows), mp); err != nil {
		return err
	}
	if err := dst.preflightBinaryStringAppend(len(rows), summarizeBinaryStringSelection(source, rows), mp); err != nil {
		return err
	}
	if source.HasNull() {
		if err := dst.ensureNullCapacity(len(rows), mp); err != nil {
			return err
		}
	}
	if source.HasGrouping() {
		if err := dst.ensureGroupingCapacity(len(rows), mp); err != nil {
			return err
		}
	}
	dst.setLengthAfterExtendWithSource(len(rows), source.stringSource, false)
	for i, row := range rows {
		if source.IsNull(uint64(row)) {
			dst.nsp.Add(uint64(i))
		}
		if source.gsp.Contains(uint64(row)) {
			dst.gsp.Add(uint64(i))
		}
	}
	if err := propagatePrepareParamKindsSelection(dst, source, 0, rows, mp); err != nil {
		return err
	}
	if err := propagateBinaryStringSelection(dst, source, 0, rows, mp); err != nil {
		return err
	}
	source.CopyExpressionMetadataTo(dst)
	return nil
}

func (v *Vector) dictionaryPhysicalRow(row int) (*Vector, int) {
	if row < 0 || row >= v.length {
		panic(moerr.NewInvalidInputNoCtx("dictionary logical row out of bounds"))
	}
	backing := v.dictionaryState()
	base := backing.base.vec
	if base.IsConst() {
		return base, 0
	}
	return base, int(backing.rows.storageFixed()[row])
}

func getDictionaryFixedAt[T any](v *Vector, row int) T {
	base, index := v.dictionaryPhysicalRow(row)
	if base.IsConstNull() {
		var zero T
		return zero
	}
	length := base.length
	if base.IsConst() {
		length = 1
	}
	return toSliceOfLengthNoTypeCheck[T](base, length)[index]
}

func getDictionaryBytesAt(v *Vector, row int) []byte {
	base, index := v.dictionaryPhysicalRow(row)
	if base.IsConstNull() {
		return nil
	}
	length := base.length
	if base.IsConst() {
		length = 1
	}
	values := toSliceOfLengthNoTypeCheck[types.Varlena](base, length)
	return values[index].GetByteSlice(base.area)
}

func (v *Vector) releaseDictionary() {
	if !v.IsDictionary() {
		return
	}
	backing := v.dictionaryState()
	// Retire the header first: ordinary backing cleanup must not release this
	// composite owner again, including after a reset or successful COW swap.
	v.dataLease = nil
	v.class = FLAT
	backing.Release()
}

// FixedReadView resolves representation once. Values and Selection are
// immutable borrowed slices, valid while the input header remains live. A nil
// selection means flat indexing; Constant means physical value index zero.
// Nulls always uses logical row coordinates.
type FixedReadView[T any] struct {
	Values    []T
	Selection []int64
	Nulls     *nulls.Nulls
	Rows      int
	Constant  bool
	AllNull   bool
}

func ResolveFixedReadView[T any](v *Vector) FixedReadView[T] {
	checkType[T](v)
	base := v
	var selected []int64
	if v.IsDictionary() {
		backing := v.dictionaryState()
		base = backing.base.vec
		selected = backing.rows.storageFixed()[:v.length]
	}
	length := base.length
	if base.IsConst() {
		length = 1
	}
	if base.IsConstNull() {
		length = 0
	}
	return FixedReadView[T]{Values: toSliceOfLengthNoTypeCheck[T](base, length), Selection: selected,
		Nulls: &v.nsp, Rows: v.length, Constant: base.IsConst(), AllNull: v.AllNull()}
}

type VarlenaReadView struct {
	FixedReadView[types.Varlena]
	Area []byte // immutable; same lifetime as Values
}

func ResolveVarlenaReadView(v *Vector) VarlenaReadView {
	base := v
	if v.IsDictionary() {
		base = v.dictionaryState().base.vec
	}
	return VarlenaReadView{FixedReadView: ResolveFixedReadView[types.Varlena](v), Area: base.area}
}

// EnsureFlat materializes only the selected logical rows. No physical getter
// allocates implicitly. Failure leaves the original representation intact.
func (v *Vector) EnsureFlat(mp *mpool.MPool) error {
	if !v.IsDictionary() {
		return nil
	}
	flat, err := v.materializeDictionary(mp, v.allocationAccount)
	if err != nil {
		return err
	}
	old := *v
	*v = *flat
	old.Free(mp)
	return nil
}

func (v *Vector) EnsureWritable(mp *mpool.MPool) error { return v.MaterializeOwned(mp) }

func (v *Vector) materializeDictionary(mp *mpool.MPool, account *AllocationAccountSelection) (*Vector, error) {
	if mp == nil || v.typ.TypeSize() <= 0 || v.length > math.MaxInt/v.typ.TypeSize() {
		return nil, moerr.NewInvalidInputNoCtx("invalid dictionary materialization size")
	}
	flat, err := NewOffHeapVecWithTypeAndAllocation(v.typ, account)
	if err != nil {
		return nil, err
	}
	flat.length, flat.sorted = v.length, v.sorted
	// Row metadata is already in logical coordinates. Copy it once; payload
	// gathering below must not invoke append helpers that rewrite provenance.
	if err := v.CopyPrepareParamMetadataToWithMP(flat, mp); err != nil {
		flat.Free(mp)
		return nil, err
	}
	if err := v.copyBinaryStringTo(flat, mp); err != nil {
		flat.Free(mp)
		return nil, err
	}
	if err := v.copyWindowBitmapsOwned(flat, 0, v.length, mp); err != nil {
		flat.Free(mp)
		return nil, err
	}
	v.CopyExpressionMetadataTo(flat)
	backing := v.dictionaryState()
	base, mapping := backing.base.vec, backing.rows.storageFixed()
	flat.data, err = flat.allocData(mp, v.length*v.typ.TypeSize())
	if err != nil {
		flat.Free(mp)
		return nil, err
	}
	if backing.logicalArea > 0 {
		flat.area, err = flat.allocArea(mp, backing.logicalArea)
		if err != nil {
			flat.Free(mp)
			return nil, err
		}
		flat.area = flat.area[:0]
	}
	clear(flat.data[:v.length*v.typ.TypeSize()])
	for row, index := range mapping[:v.length] {
		if v.IsNull(uint64(row)) || base.IsConstNull() {
			continue
		}
		if v.typ.IsVarlen() {
			values := toSliceOfLengthNoTypeCheck[types.Varlena](flat, v.length)
			bytes := base.GetBytesAt(int(index))
			if err := BuildVarlenaFromByteSlice(flat, &values[row], &bytes, mp); err != nil {
				flat.Free(mp)
				return nil, err
			}
		} else {
			size := v.typ.TypeSize()
			if base.IsConst() {
				index = 0
			}
			copy(flat.data[row*size:(row+1)*size], base.data[int(index)*size:(int(index)+1)*size])
		}
	}
	return flat, nil
}
