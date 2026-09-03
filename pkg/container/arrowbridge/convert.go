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

package arrowbridge

import (
	"context"
	"math"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bufferlease"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// DefaultMaxPinAmplification is the largest retained-capacity/payload ratio
// for which LOAD keeps a borrowed varlen value area. Above it, materializing
// avoids pinning a large source allocation for a small logical slice.
const DefaultMaxPinAmplification = 4.0

// ConvertOptions contains execution semantics that are intentionally outside
// the Arrow schema. Location is the MatrixOne session timezone.
type ConvertOptions struct {
	// Location defines LOAD's session-timezone conversion for timestamp values.
	// Exact-ABI consumers must not use this option to reinterpret wire types.
	Location *time.Location
	// MaxPinAmplification overrides the LOAD varlen retention threshold.
	MaxPinAmplification float64
	// Allocation charges every materialized MO backing to the caller's
	// statement account. Borrowed Arrow capacity is charged by its source lease.
	Allocation *vector.AllocationAccountSelection
	// ForceMaterialize is a verification/rollback switch used to compare the
	// borrowed path with identical conversion semantics.
	ForceMaterialize bool
}

// ConvertStats separates avoided payload copies from the capacity retained to
// achieve them. Descriptor and mandatory layout conversions are materialized.
type ConvertStats struct {
	// BorrowedPayloadBytes is logical source payload whose copy was avoided.
	BorrowedPayloadBytes int64
	// MaterializedPayloadBytes is payload copied into MO-owned vectors.
	MaterializedPayloadBytes int64
	// RetainedCapacityBytes is physical Arrow capacity pinned by borrowed views.
	RetainedCapacityBytes int64
	// EligiblePayloadBytes is payload that could use the borrowed layout before
	// forced-materialize and pin-amplification policy are applied.
	EligiblePayloadBytes      int64
	BorrowedColumns           int64
	MaterializedColumns       int64
	PinAmplificationFallbacks int64
	UnalignedFallbacks        int64
}

func newOutputVector(
	typ types.Type,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	return vector.NewOffHeapVecWithTypeAndAllocation(typ, selection)
}

// Convert builds a complete batch transactionally. On any error every vector
// and lease already installed in the partial batch is released.
func (p *Plan) Convert(
	ctx context.Context,
	record arrow.RecordBatch,
	mp *mpool.MPool,
	options ConvertOptions,
) (_ *batch.Batch, stats ConvertStats, err error) {
	if p == nil || record == nil || mp == nil {
		return nil, stats, moerr.NewInvalidInput(ctx, "invalid Arrow conversion input")
	}
	if record.NumRows() < 0 || record.NumRows() > int64(math.MaxInt) {
		return nil, stats, moerr.NewInvalidInputf(ctx, "Arrow record row count %d is invalid", record.NumRows())
	}
	if schemaFingerprint(record.Schema()) != p.schemaFingerprint {
		return nil, stats, moerr.NewInvalidInput(ctx, "Arrow record schema does not match the bound schema")
	}
	if record.NumCols() != int64(len(p.columns)) {
		return nil, stats, moerr.NewInvalidInputf(ctx, "Arrow record has %d columns, expected %d", record.NumCols(), len(p.columns))
	}
	if options.Location == nil {
		options.Location = time.UTC
	}
	if options.MaxPinAmplification <= 0 {
		options.MaxPinAmplification = DefaultMaxPinAmplification
	}

	rows := int(record.NumRows())
	bat := batch.NewOffHeap(p.attrs)
	defer func() {
		if err != nil {
			bat.Clean(mp)
		}
	}()
	for outputIndex, binding := range p.columns {
		if err = ctx.Err(); err != nil {
			return nil, stats, err
		}
		column := record.Column(binding.source)
		if column.Len() != rows {
			return nil, stats, moerr.NewInvalidInputf(ctx, "Arrow column %q has %d rows, expected %d", binding.target.Name, column.Len(), rows)
		}
		if binding.target.NotNull && column.NullN() != 0 {
			return nil, stats, moerr.NewConstraintViolationf(ctx, "Arrow column %q contains NULL for a NOT NULL target", binding.target.Name)
		}

		var vec *vector.Vector
		var columnStats ConvertStats
		switch binding.kind {
		case conversionBorrowFixed:
			vec, columnStats, err = convertFixed(
				ctx, column, binding.target.Type, mp, options.Allocation, options.ForceMaterialize,
			)
		case conversionBorrowVarlen:
			vec, columnStats, err = convertVarlen(
				ctx, column, binding.target.Type, mp, options.MaxPinAmplification,
				options.Allocation, options.ForceMaterialize,
			)
		default:
			vec, columnStats, err = materializeConverted(ctx, column, binding, mp, options.Location, options.Allocation)
		}
		if err != nil {
			return nil, stats, err
		}
		bat.Vecs[outputIndex] = vec
		stats.BorrowedPayloadBytes += columnStats.BorrowedPayloadBytes
		stats.MaterializedPayloadBytes += columnStats.MaterializedPayloadBytes
		stats.RetainedCapacityBytes += columnStats.RetainedCapacityBytes
		stats.EligiblePayloadBytes += columnStats.EligiblePayloadBytes
		stats.BorrowedColumns += columnStats.BorrowedColumns
		stats.MaterializedColumns += columnStats.MaterializedColumns
		stats.PinAmplificationFallbacks += columnStats.PinAmplificationFallbacks
		stats.UnalignedFallbacks += columnStats.UnalignedFallbacks
	}
	bat.SetRowCount(rows)
	return bat, stats, nil
}

func convertFixed(
	ctx context.Context,
	column arrow.Array,
	target types.Type,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
	forceMaterialize bool,
) (*vector.Vector, ConvertStats, error) {
	var stats ConvertStats
	if column.DataType().ID() == arrow.TIME64 {
		values, ok := column.(*array.Time64)
		if !ok {
			return nil, stats, moerr.NewInvalidInput(ctx, "invalid Arrow Time64 array")
		}
		const microsPerDay = arrow.Time64(24 * 60 * 60 * 1_000_000)
		for row, value := range values.Time64Values() {
			if err := checkConvertContext(ctx, row); err != nil {
				return nil, stats, err
			}
			if !column.IsNull(row) && (value < 0 || value >= microsPerDay) {
				return nil, stats, moerr.NewConstraintViolationf(ctx, "Arrow Time64 value at row %d is outside [0,24h)", row)
			}
		}
	}

	data := column.Data()
	buffers := data.Buffers()
	if len(buffers) < 2 || buffers[1] == nil {
		if column.Len() == 0 {
			vec, err := newOutputVector(target, selection)
			return vec, stats, err
		}
		return nil, stats, moerr.NewInvalidInput(ctx, "Arrow fixed-width value buffer is missing")
	}
	width := target.TypeSize()
	if width <= 0 || data.Offset() > math.MaxInt/width || column.Len() > math.MaxInt/width {
		return nil, stats, moerr.NewInvalidInput(ctx, "Arrow fixed-width buffer size overflows")
	}
	start := data.Offset() * width
	length := column.Len() * width
	values := buffers[1].Bytes()
	if start < 0 || length < 0 || start > len(values) || length > len(values)-start {
		return nil, stats, moerr.NewInvalidInput(ctx, "Arrow fixed-width value buffer is out of bounds")
	}
	view := values[start : start+length]
	if forceMaterialize {
		vec, stats, err := materializeFixedLayout(ctx, column, target, view, mp, selection)
		stats.EligiblePayloadBytes = int64(length)
		return vec, stats, err
	}
	if len(view) > 0 && uintptr(unsafe.Pointer(unsafe.SliceData(view)))%uintptr(min(width, 8)) != 0 {
		vec, stats, err := materializeFixedLayout(ctx, column, target, view, mp, selection)
		stats.EligiblePayloadBytes = int64(length)
		stats.UnalignedFallbacks = 1
		return vec, stats, err
	}

	lease, err := newArrayDataLease(data, view, int64(buffers[1].Cap()))
	if err != nil {
		return nil, stats, err
	}
	vec, err := vector.NewBorrowedFixedVectorWithAllocation(target, column.Len(), view, lease, selection)
	lease.Release()
	if err != nil {
		return nil, stats, err
	}
	if err = installBorrowedValidity(column, vec, mp); err != nil {
		vec.Free(mp)
		return nil, stats, err
	}
	stats.BorrowedPayloadBytes = int64(length)
	stats.RetainedCapacityBytes = int64(buffers[1].Cap())
	stats.EligiblePayloadBytes = int64(length)
	stats.BorrowedColumns = 1
	return vec, stats, nil
}

func materializeFixedLayout(
	ctx context.Context,
	column arrow.Array,
	target types.Type,
	view []byte,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, ConvertStats, error) {
	stats := ConvertStats{MaterializedPayloadBytes: int64(len(view)), MaterializedColumns: 1}
	vec, err := newOutputVector(target, selection)
	if err != nil {
		return nil, stats, err
	}
	if err := vec.PreExtend(column.Len(), mp); err != nil {
		// PreExtend may have grown one or more vector buffers before a later
		// allocation or account reservation fails. The vector has not been
		// published into the output batch yet, so this helper is its sole owner.
		vec.Free(mp)
		return nil, stats, err
	}
	vec.SetLength(column.Len())
	copy(vec.GetData(), view)
	for row := 0; row < column.Len(); row++ {
		if err := checkConvertContext(ctx, row); err != nil {
			vec.Free(mp)
			return nil, stats, err
		}
		if column.IsNull(row) {
			vec.SetNull(uint64(row))
		}
	}
	return vec, stats, nil
}

func newArrayDataLease(
	data arrow.ArrayData,
	view []byte,
	accounted int64,
) (*bufferlease.RefCounted, error) {
	// Arrow ArrayData is the physical lifetime root visible to this package. A
	// File reader may in turn have attached a RangeLease to that object graph;
	// retaining ArrayData therefore preserves both layers without teaching the
	// container bridge about FileService.
	data.Retain()
	lease, err := bufferlease.NewRefCounted(view, accounted, data.Release)
	if err != nil {
		data.Release()
		return nil, err
	}
	return lease, nil
}

func installBorrowedValidity(column arrow.Array, vec *vector.Vector, mp *mpool.MPool) error {
	if column.NullN() == 0 {
		return nil
	}
	// MO nulls use an inverted bitmap. Reserve the possible legacy-materialized
	// bitmap before publishing the readonly Arrow validity view, so a later
	// compatibility consumer cannot allocate outside statement admission.
	if err := vec.PrepareBorrowedValidity(column.Len(), mp); err != nil {
		return err
	}
	data := column.Data()
	buffers := data.Buffers()
	if len(buffers) == 0 || buffers[0] == nil {
		return moerr.NewInvalidInputNoCtx("Arrow validity buffer is missing")
	}
	validity := buffers[0].Bytes()
	lease, err := newArrayDataLease(data, validity, int64(buffers[0].Cap()))
	if err != nil {
		return err
	}
	err = vec.GetNulls().InstallBorrowedValidity(validity, data.Offset(), column.Len(), column.NullN(), lease)
	lease.Release()
	return err
}

type varlenView struct {
	values           []byte
	offsets32        []int32
	offsets64        []int64
	baseOffset       int64
	fixedWidth       int64
	text             bool
	retainedCapacity int64
}

func convertVarlen(
	ctx context.Context,
	column arrow.Array,
	target types.Type,
	mp *mpool.MPool,
	maxPinAmplification float64,
	selection *vector.AllocationAccountSelection,
	forceMaterialize bool,
) (*vector.Vector, ConvertStats, error) {
	view, err := inspectVarlen(ctx, column)
	if err != nil {
		return nil, ConvertStats{}, err
	}
	var avoided, inlineCopied int64
	for row := 0; row < column.Len(); row++ {
		if err = checkConvertContext(ctx, row); err != nil {
			return nil, ConvertStats{}, err
		}
		// Arrow permits arbitrary, semantically invisible bytes behind a NULL
		// slot. Validate only values that can reach SQL; requiring UTF-8 or a
		// target width for a NULL payload would reject a valid IPC array.
		if column.IsNull(row) {
			continue
		}
		value := view.value(row)
		if err = validateVarlenValue(ctx, row, value, view.text, target); err != nil {
			return nil, ConvertStats{}, err
		}
		if len(value) > types.VarlenaInlineSize {
			avoided += int64(len(value))
		} else {
			inlineCopied += int64(len(value))
		}
	}
	// Short MO varlena values must remain canonical inline descriptors. Borrowing
	// only the long values keeps that invariant while still avoiding their copy.
	if forceMaterialize || avoided == 0 {
		vec, stats, err := materializeVarlen(ctx, column, target, view, mp, selection)
		stats.EligiblePayloadBytes = avoided
		return vec, stats, err
	}
	// Admission follows physical retained capacity, while this policy compares
	// it with the useful bytes. A tiny slice of a large Arrow allocation should
	// not remain pinned merely because its descriptor can be borrowed.
	if float64(view.retainedCapacity)/float64(avoided) > maxPinAmplification {
		vec, stats, err := materializeVarlen(ctx, column, target, view, mp, selection)
		stats.EligiblePayloadBytes = avoided
		stats.PinAmplificationFallbacks = 1
		return vec, stats, err
	}

	vec, err := newOutputVector(target, selection)
	if err != nil {
		return nil, ConvertStats{}, err
	}
	if err = vec.PreExtend(column.Len(), mp); err != nil {
		// Keep failure transactional even when PreExtend performed partial work;
		// Convert cannot clean this vector until it is installed in the batch.
		vec.Free(mp)
		return nil, ConvertStats{}, err
	}
	vec.SetLength(column.Len())
	descriptors := vector.MustFixedColNoTypeCheck[types.Varlena](vec)
	for row := 0; row < column.Len(); row++ {
		if column.IsNull(row) {
			continue
		}
		value := view.value(row)
		if len(value) <= types.VarlenaInlineSize {
			descriptors[row][0] = byte(len(value))
			copy(descriptors[row][1:], value)
			continue
		}
		offset := view.offset(row)
		if offset < 0 || offset > math.MaxUint32 || len(value) > math.MaxUint32 {
			vec.Free(mp)
			return nil, ConvertStats{}, moerr.NewInvalidInputf(ctx, "Arrow value at row %d exceeds MatrixOne varlen limits", row)
		}
		descriptors[row].SetOffsetLen(uint32(offset), uint32(len(value)))
	}

	data := column.Data()
	lease, err := newArrayDataLease(data, view.values, view.retainedCapacity)
	if err != nil {
		vec.Free(mp)
		return nil, ConvertStats{}, err
	}
	err = vec.InstallBorrowedArea(view.values, lease)
	lease.Release()
	if err != nil {
		vec.Free(mp)
		return nil, ConvertStats{}, err
	}
	if err = installBorrowedValidity(column, vec, mp); err != nil {
		vec.Free(mp)
		return nil, ConvertStats{}, err
	}
	return vec, ConvertStats{
		BorrowedPayloadBytes:     avoided,
		MaterializedPayloadBytes: inlineCopied,
		RetainedCapacityBytes:    view.retainedCapacity,
		EligiblePayloadBytes:     avoided,
		BorrowedColumns:          1,
	}, nil
}

func inspectVarlen(ctx context.Context, column arrow.Array) (varlenView, error) {
	var view varlenView
	data := column.Data()
	buffers := data.Buffers()
	if len(buffers) < 2 {
		return view, moerr.NewInvalidInput(ctx, "Arrow varlen buffers are missing")
	}

	switch values := column.(type) {
	case *array.String:
		view.text = true
		view.values = values.ValueBytes()
		view.offsets32 = values.ValueOffsets()
		view.baseOffset = int64(view.offsets32[0])
	case *array.LargeString:
		view.text = true
		view.values = values.ValueBytes()
		view.offsets64 = values.ValueOffsets()
		view.baseOffset = view.offsets64[0]
	case *array.Binary:
		view.values = values.ValueBytes()
		view.offsets32 = values.ValueOffsets()
		view.baseOffset = int64(view.offsets32[0])
	case *array.LargeBinary:
		view.values = values.ValueBytes()
		view.offsets64 = values.ValueOffsets()
		view.baseOffset = view.offsets64[0]
	case *array.FixedSizeBinary:
		widthType, ok := column.DataType().(*arrow.FixedSizeBinaryType)
		if !ok || widthType.ByteWidth <= 0 || data.Offset() > math.MaxInt/int(widthType.ByteWidth) ||
			column.Len() > math.MaxInt/int(widthType.ByteWidth) {
			return view, moerr.NewInvalidInput(ctx, "invalid Arrow FixedSizeBinary width")
		}
		if buffers[1] == nil {
			if column.Len() == 0 {
				return view, nil
			}
			return view, moerr.NewInvalidInput(ctx, "Arrow FixedSizeBinary value buffer is missing")
		}
		start := data.Offset() * int(widthType.ByteWidth)
		length := column.Len() * int(widthType.ByteWidth)
		if start > len(buffers[1].Bytes()) || length > len(buffers[1].Bytes())-start {
			return view, moerr.NewInvalidInput(ctx, "Arrow FixedSizeBinary value buffer is out of bounds")
		}
		view.values = buffers[1].Bytes()[start : start+length]
		view.fixedWidth = int64(widthType.ByteWidth)
	default:
		return view, moerr.NewInvalidInputf(ctx, "invalid Arrow varlen array %T", column)
	}
	if len(buffers) > 2 && buffers[2] != nil {
		view.retainedCapacity = int64(buffers[2].Cap())
	} else if buffers[1] != nil {
		view.retainedCapacity = int64(buffers[1].Cap())
	}
	if len(view.offsets32) != 0 && len(view.offsets32) != column.Len()+1 ||
		len(view.offsets64) != 0 && len(view.offsets64) != column.Len()+1 ||
		len(view.offsets32) == 0 && len(view.offsets64) == 0 && view.fixedWidth == 0 {
		return view, moerr.NewInvalidInput(ctx, "invalid Arrow varlen offsets")
	}
	for row := 0; row < column.Len(); row++ {
		if err := checkConvertContext(ctx, row); err != nil {
			return view, err
		}
		start, end := view.offset(row), view.offset(row+1)
		if start < 0 || start > end || end > int64(len(view.values)) {
			return view, moerr.NewInvalidInputf(ctx, "invalid Arrow varlen offsets at row %d", row)
		}
	}
	return view, nil
}

func (v varlenView) offset(row int) int64 {
	if len(v.offsets32) != 0 {
		return int64(v.offsets32[row]) - v.baseOffset
	}
	if len(v.offsets64) != 0 {
		return v.offsets64[row] - v.baseOffset
	}
	return int64(row) * v.fixedWidth
}

func (v varlenView) value(row int) []byte {
	return v.values[v.offset(row):v.offset(row+1)]
}

func validateVarlenValue(
	ctx context.Context,
	row int,
	value []byte,
	text bool,
	target types.Type,
) error {
	if len(value) > math.MaxUint32 {
		return moerr.NewConstraintViolationf(ctx, "Arrow value at row %d exceeds MatrixOne varlen capacity", row)
	}
	logicalLength := len(value)
	if text {
		if !utf8.Valid(value) {
			return moerr.NewInvalidInputf(ctx, "Arrow UTF-8 value at row %d is invalid", row)
		}
		logicalLength = utf8.RuneCount(value)
	}
	if target.Width > 0 && int64(logicalLength) > int64(target.Width) {
		return moerr.NewConstraintViolationf(ctx, "Arrow value at row %d has length %d, target limit is %d", row, logicalLength, target.Width)
	}
	return nil
}

func materializeVarlen(
	ctx context.Context,
	column arrow.Array,
	target types.Type,
	view varlenView,
	mp *mpool.MPool,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, ConvertStats, error) {
	vec, err := newOutputVector(target, selection)
	if err != nil {
		return nil, ConvertStats{}, err
	}
	var copied int64
	for row := 0; row < column.Len(); row++ {
		if row&1023 == 0 {
			if err := ctx.Err(); err != nil {
				vec.Free(mp)
				return nil, ConvertStats{}, err
			}
		}
		value := view.value(row)
		if err := vector.AppendBytes(vec, value, column.IsNull(row), mp); err != nil {
			vec.Free(mp)
			return nil, ConvertStats{}, err
		}
		if !column.IsNull(row) {
			copied += int64(len(value))
		}
	}
	return vec, ConvertStats{MaterializedPayloadBytes: copied, MaterializedColumns: 1}, nil
}

func materializeConverted(
	ctx context.Context,
	column arrow.Array,
	binding columnPlan,
	mp *mpool.MPool,
	location *time.Location,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, ConvertStats, error) {
	if binding.kind == conversionMaterializeDictionary {
		return materializeDictionary(ctx, column, binding, mp, location, selection)
	}
	vec, err := newOutputVector(binding.target.Type, selection)
	if err != nil {
		return nil, ConvertStats{}, err
	}
	stats := ConvertStats{
		MaterializedPayloadBytes: int64(column.Len() * binding.target.Type.TypeSize()),
		MaterializedColumns:      1,
	}
	fail := func(err error) (*vector.Vector, ConvertStats, error) {
		vec.Free(mp)
		return nil, stats, err
	}

	switch binding.kind {
	case conversionMaterializeBool:
		values, ok := column.(*array.Boolean)
		if !ok {
			return fail(moerr.NewInvalidInput(ctx, "invalid Arrow Boolean array"))
		}
		for row := 0; row < values.Len(); row++ {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			if err := vector.AppendFixed(vec, values.Value(row), values.IsNull(row), mp); err != nil {
				return fail(err)
			}
		}
	case conversionMaterializeDate32:
		values, ok := column.(*array.Date32)
		if !ok {
			return fail(moerr.NewInvalidInput(ctx, "invalid Arrow Date32 array"))
		}
		for row, value := range values.Date32Values() {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			if binding.target.Type.Oid == types.T_date {
				date := types.DaysFromUnixEpochToDate(int32(value))
				if err := vector.AppendFixed(vec, date, values.IsNull(row), mp); err != nil {
					return fail(err)
				}
			} else {
				datetime, err := arrowDaysToDatetime(int64(value))
				if err != nil && !values.IsNull(row) {
					return fail(moerr.NewConstraintViolationf(ctx, "Arrow Date32 value at row %d is out of MatrixOne range", row))
				}
				if err := vector.AppendFixed(vec, datetime, values.IsNull(row), mp); err != nil {
					return fail(err)
				}
			}
		}
	case conversionMaterializeDate64:
		values, ok := column.(*array.Date64)
		if !ok {
			return fail(moerr.NewInvalidInput(ctx, "invalid Arrow Date64 array"))
		}
		const millisPerDay = int64(24 * 60 * 60 * 1000)
		for row, value := range values.Date64Values() {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			days := int64(value) / millisPerDay
			if !values.IsNull(row) && (int64(value)%millisPerDay != 0 || days < math.MinInt32 || days > math.MaxInt32) {
				return fail(moerr.NewConstraintViolationf(ctx, "Arrow Date64 value at row %d is not an integral representable day", row))
			}
			if binding.target.Type.Oid == types.T_date {
				if err := vector.AppendFixed(vec, types.DaysFromUnixEpochToDate(int32(days)), values.IsNull(row), mp); err != nil {
					return fail(err)
				}
			} else {
				datetime, err := arrowDaysToDatetime(days)
				if err != nil && !values.IsNull(row) {
					return fail(moerr.NewConstraintViolationf(ctx, "Arrow Date64 value at row %d is out of MatrixOne range", row))
				}
				if err := vector.AppendFixed(vec, datetime, values.IsNull(row), mp); err != nil {
					return fail(err)
				}
			}
		}
	case conversionMaterializeTimestamp:
		values, ok := column.(*array.Timestamp)
		if !ok {
			return fail(moerr.NewInvalidInput(ctx, "invalid Arrow Timestamp array"))
		}
		timestampType, ok := column.DataType().(*arrow.TimestampType)
		if !ok {
			return fail(moerr.NewInvalidInput(ctx, "invalid Arrow Timestamp type"))
		}
		if _, err := timestampType.GetZone(); err != nil {
			return fail(moerr.NewInvalidInputf(ctx, "invalid Arrow timezone %q: %v", timestampType.TimeZone, err))
		}
		for row, value := range values.TimestampValues() {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			isNull := values.IsNull(row)
			micros, err := timestampToMicros(int64(value), timestampType.Unit)
			if err != nil && !isNull {
				return fail(moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of range", row))
			}
			if !isNull {
				if err := validateTimestampPrecision(ctx, row, micros, binding.target.Type); err != nil {
					return fail(err)
				}
			}
			if binding.target.Type.Oid == types.T_timestamp {
				converted, err := arrowMicrosToTimestamp(micros, timestampType.TimeZone != "", location)
				if err != nil && !isNull {
					return fail(moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of MatrixOne range", row))
				}
				if err := vector.AppendFixed(vec, converted, isNull, mp); err != nil {
					return fail(err)
				}
			} else {
				converted, err := arrowMicrosToDatetime(micros, timestampType.TimeZone != "", location)
				if err != nil && !isNull {
					return fail(moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of MatrixOne range", row))
				}
				if err := vector.AppendFixed(vec, converted, isNull, mp); err != nil {
					return fail(err)
				}
			}
		}
	case conversionMaterializeWiden:
		for row := 0; row < column.Len(); row++ {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			if err := appendWidenedValue(ctx, vec, column, row, column.IsNull(row), binding.target.Type, mp); err != nil {
				return fail(err)
			}
		}
	case conversionMaterializeTime:
		for row := 0; row < column.Len(); row++ {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			if err := appendTimeValue(ctx, vec, column, row, column.IsNull(row), binding.target.Type, mp); err != nil {
				return fail(err)
			}
		}
	case conversionMaterializeNull:
		if err := vec.PreExtend(column.Len(), mp); err != nil {
			return fail(err)
		}
		vec.SetLength(column.Len())
		for row := 0; row < column.Len(); row++ {
			if err := checkConvertContext(ctx, row); err != nil {
				return fail(err)
			}
			vec.SetNull(uint64(row))
		}
	default:
		return fail(moerr.NewInternalErrorNoCtx("unknown Arrow conversion plan"))
	}
	return vec, stats, nil
}

func appendWidenedValue(
	ctx context.Context,
	vec *vector.Vector,
	values arrow.Array,
	row int,
	isNull bool,
	target types.Type,
	mp *mpool.MPool,
) error {
	switch typed := values.(type) {
	case *array.Int8:
		value := typed.Value(row)
		switch target.Oid {
		case types.T_int16:
			return appendDictionaryFixed(vec, int16(value), isNull, mp)
		case types.T_int32:
			return appendDictionaryFixed(vec, int32(value), isNull, mp)
		case types.T_int64:
			return appendDictionaryFixed(vec, int64(value), isNull, mp)
		}
	case *array.Int16:
		value := typed.Value(row)
		switch target.Oid {
		case types.T_int32:
			return appendDictionaryFixed(vec, int32(value), isNull, mp)
		case types.T_int64:
			return appendDictionaryFixed(vec, int64(value), isNull, mp)
		}
	case *array.Int32:
		if target.Oid == types.T_int64 {
			return appendDictionaryFixed(vec, int64(typed.Value(row)), isNull, mp)
		}
	case *array.Uint8:
		value := typed.Value(row)
		switch target.Oid {
		case types.T_uint16:
			return appendDictionaryFixed(vec, uint16(value), isNull, mp)
		case types.T_uint32:
			return appendDictionaryFixed(vec, uint32(value), isNull, mp)
		case types.T_uint64:
			return appendDictionaryFixed(vec, uint64(value), isNull, mp)
		}
	case *array.Uint16:
		value := typed.Value(row)
		switch target.Oid {
		case types.T_uint32:
			return appendDictionaryFixed(vec, uint32(value), isNull, mp)
		case types.T_uint64:
			return appendDictionaryFixed(vec, uint64(value), isNull, mp)
		}
	case *array.Uint32:
		if target.Oid == types.T_uint64 {
			return appendDictionaryFixed(vec, uint64(typed.Value(row)), isNull, mp)
		}
	case *array.Float32:
		if target.Oid == types.T_float64 {
			return appendDictionaryFixed(vec, float64(typed.Value(row)), isNull, mp)
		}
	}
	return moerr.NewInvalidInputf(ctx, "invalid Arrow widening from %s to %s", values.DataType(), target)
}

func appendTimeValue(
	ctx context.Context,
	vec *vector.Vector,
	values arrow.Array,
	row int,
	isNull bool,
	target types.Type,
	mp *mpool.MPool,
) error {
	var value int64
	var unit arrow.TimeUnit
	switch typed := values.(type) {
	case *array.Time32:
		value = int64(typed.Value(row))
		timeType, ok := values.DataType().(*arrow.Time32Type)
		if !ok {
			return moerr.NewInvalidInput(ctx, "invalid Arrow Time32 type")
		}
		unit = timeType.Unit
	case *array.Time64:
		value = int64(typed.Value(row))
		timeType, ok := values.DataType().(*arrow.Time64Type)
		if !ok {
			return moerr.NewInvalidInput(ctx, "invalid Arrow Time64 type")
		}
		unit = timeType.Unit
	default:
		return moerr.NewInvalidInputf(ctx, "invalid Arrow time array %T", values)
	}
	micros, err := timeToMicros(value, unit)
	if err != nil && !isNull {
		return moerr.NewConstraintViolationf(ctx, "Arrow time value at row %d is not representable in microseconds", row)
	}
	const microsPerDay = int64(24 * 60 * 60 * 1_000_000)
	if !isNull && (micros < 0 || micros >= microsPerDay) {
		return moerr.NewConstraintViolationf(ctx, "Arrow time value at row %d is outside [0,24h)", row)
	}
	precisionFactor := int64(1)
	for scale := target.Scale; scale < 6; scale++ {
		precisionFactor *= 10
	}
	if !isNull && micros%precisionFactor != 0 {
		return moerr.NewConstraintViolationf(ctx,
			"Arrow time value at row %d exceeds MatrixOne TIME(%d) precision", row, target.Scale)
	}
	return appendDictionaryFixed(vec, types.Time(micros), isNull, mp)
}

func timeToMicros(value int64, unit arrow.TimeUnit) (int64, error) {
	switch unit {
	case arrow.Second:
		if value > math.MaxInt64/1_000_000 || value < math.MinInt64/1_000_000 {
			return 0, moerr.NewOutOfRangeNoCtx("time", "microsecond")
		}
		return value * 1_000_000, nil
	case arrow.Millisecond:
		if value > math.MaxInt64/1_000 || value < math.MinInt64/1_000 {
			return 0, moerr.NewOutOfRangeNoCtx("time", "microsecond")
		}
		return value * 1_000, nil
	case arrow.Microsecond:
		return value, nil
	case arrow.Nanosecond:
		if value%1_000 != 0 {
			return 0, moerr.NewOutOfRangeNoCtx("time", "microsecond")
		}
		return value / 1_000, nil
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid Arrow time unit")
	}
}

func arrowDaysToDatetime(days int64) (types.Datetime, error) {
	if days > math.MaxInt64/(24*60*60) || days < math.MinInt64/(24*60*60) {
		return 0, moerr.NewOutOfRangeNoCtx("date", "MatrixOne datetime")
	}
	value := types.DatetimeFromUnixWithNsec(time.UTC, days*24*60*60, 0)
	year, _, _, _ := value.ToDate().Calendar(true)
	if year < types.MinDatetimeYear || year > types.MaxDatetimeYear {
		return 0, moerr.NewOutOfRangeNoCtx("date", "MatrixOne datetime")
	}
	return value, nil
}

func materializeDictionary(
	ctx context.Context,
	column arrow.Array,
	binding columnPlan,
	mp *mpool.MPool,
	location *time.Location,
	selection *vector.AllocationAccountSelection,
) (*vector.Vector, ConvertStats, error) {
	dictionary, ok := column.(*array.Dictionary)
	if !ok {
		return nil, ConvertStats{}, moerr.NewInvalidInput(ctx, "invalid Arrow Dictionary array")
	}
	values := dictionary.Dictionary()
	valueKind, err := selectLoadConversion(values.DataType(), binding.target.Type)
	if err != nil || valueKind == conversionMaterializeDictionary {
		return nil, ConvertStats{}, moerr.NewInvalidInputf(ctx, "invalid Arrow Dictionary value type %s", values.DataType())
	}

	vec, err := newOutputVector(binding.target.Type, selection)
	if err != nil {
		return nil, ConvertStats{}, err
	}
	stats := ConvertStats{MaterializedColumns: 1}
	if valueKind != conversionBorrowVarlen {
		stats.MaterializedPayloadBytes = int64(column.Len() * binding.target.Type.TypeSize())
	}
	fail := func(err error) (*vector.Vector, ConvertStats, error) {
		vec.Free(mp)
		return nil, stats, err
	}

	for row := 0; row < dictionary.Len(); row++ {
		if err := checkConvertContext(ctx, row); err != nil {
			return fail(err)
		}
		logicalNull := dictionary.IsNull(row)
		index := 0
		if !logicalNull {
			index, err = checkedDictionaryIndex(ctx, dictionary, row, values.Len())
			if err != nil {
				return fail(err)
			}
			logicalNull = values.IsNull(index)
		}
		if binding.target.NotNull && logicalNull {
			return fail(moerr.NewConstraintViolationf(ctx,
				"Arrow column %q contains NULL for a NOT NULL target", binding.target.Name))
		}
		copied, err := appendDictionaryValue(
			ctx, vec, values, valueKind, index, row, logicalNull, binding.target.Type, mp, location,
		)
		if err != nil {
			return fail(err)
		}
		stats.MaterializedPayloadBytes += copied
	}
	return vec, stats, nil
}

func checkedDictionaryIndex(
	ctx context.Context,
	dictionary *array.Dictionary,
	row int,
	dictionaryLength int,
) (int, error) {
	var index int64
	switch indices := dictionary.Indices().(type) {
	case *array.Int8:
		index = int64(indices.Value(row))
	case *array.Int16:
		index = int64(indices.Value(row))
	case *array.Int32:
		index = int64(indices.Value(row))
	case *array.Int64:
		index = indices.Value(row)
	case *array.Uint8:
		index = int64(indices.Value(row))
	case *array.Uint16:
		index = int64(indices.Value(row))
	case *array.Uint32:
		index = int64(indices.Value(row))
	case *array.Uint64:
		value := indices.Value(row)
		if value > uint64(math.MaxInt) {
			return 0, moerr.NewInvalidInputf(ctx, "Arrow dictionary index at row %d overflows", row)
		}
		index = int64(value)
	default:
		return 0, moerr.NewInvalidInputf(ctx, "invalid Arrow dictionary index array %T", dictionary.Indices())
	}
	if index < 0 || index >= int64(dictionaryLength) {
		return 0, moerr.NewInvalidInputf(ctx,
			"Arrow dictionary index %d at row %d is outside [0,%d)", index, row, dictionaryLength)
	}
	return int(index), nil
}

func appendDictionaryValue(
	ctx context.Context,
	vec *vector.Vector,
	values arrow.Array,
	valueKind conversionKind,
	index int,
	row int,
	isNull bool,
	target types.Type,
	mp *mpool.MPool,
	location *time.Location,
) (int64, error) {
	if valueKind == conversionBorrowVarlen {
		var value []byte
		text := false
		if !isNull {
			switch typed := values.(type) {
			case *array.String:
				value, text = []byte(typed.Value(index)), true
			case *array.LargeString:
				value, text = []byte(typed.Value(index)), true
			case *array.Binary:
				value = typed.Value(index)
			case *array.LargeBinary:
				value = typed.Value(index)
			case *array.FixedSizeBinary:
				value = typed.Value(index)
			default:
				return 0, moerr.NewInvalidInputf(ctx, "invalid Arrow dictionary varlen values %T", values)
			}
			if err := validateVarlenValue(ctx, row, value, text, target); err != nil {
				return 0, err
			}
		}
		if err := vector.AppendBytes(vec, value, isNull, mp); err != nil {
			return 0, err
		}
		if isNull {
			return 0, nil
		}
		return int64(len(value)), nil
	}
	if isNull {
		return 0, appendDictionaryNull(vec, target, mp)
	}
	if valueKind == conversionMaterializeWiden {
		return 0, appendWidenedValue(ctx, vec, values, index, false, target, mp)
	}
	if valueKind == conversionMaterializeTime {
		return 0, appendTimeValue(ctx, vec, values, index, false, target, mp)
	}

	switch typed := values.(type) {
	case *array.Int8:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Int16:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Int32:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Int64:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Uint8:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Uint16:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Uint32:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Uint64:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Float32:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Float64:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Decimal128:
		value := typed.Value(index)
		converted := types.Decimal128{B0_63: value.LowBits(), B64_127: uint64(value.HighBits())}
		return 0, appendDictionaryFixed(vec, converted, isNull, mp)
	case *array.Time64:
		value := typed.Value(index)
		const microsPerDay = arrow.Time64(24 * 60 * 60 * 1_000_000)
		if !isNull && (value < 0 || value >= microsPerDay) {
			return 0, moerr.NewConstraintViolationf(ctx,
				"Arrow Time64 value at row %d is outside [0,24h)", row)
		}
		return 0, appendDictionaryFixed(vec, types.Time(value), isNull, mp)
	case *array.Boolean:
		return 0, appendDictionaryFixed(vec, typed.Value(index), isNull, mp)
	case *array.Date32:
		if target.Oid == types.T_date {
			value := types.DaysFromUnixEpochToDate(int32(typed.Value(index)))
			return 0, appendDictionaryFixed(vec, value, isNull, mp)
		}
		value, err := arrowDaysToDatetime(int64(typed.Value(index)))
		if err != nil {
			return 0, moerr.NewConstraintViolationf(ctx, "Arrow Date32 value at row %d is out of MatrixOne range", row)
		}
		return 0, appendDictionaryFixed(vec, value, isNull, mp)
	case *array.Date64:
		value := int64(typed.Value(index))
		const millisPerDay = int64(24 * 60 * 60 * 1000)
		days := value / millisPerDay
		if !isNull && (value%millisPerDay != 0 || days < math.MinInt32 || days > math.MaxInt32) {
			return 0, moerr.NewConstraintViolationf(ctx,
				"Arrow Date64 value at row %d is not an integral representable day", row)
		}
		if target.Oid == types.T_date {
			return 0, appendDictionaryFixed(vec, types.DaysFromUnixEpochToDate(int32(days)), isNull, mp)
		}
		converted, err := arrowDaysToDatetime(days)
		if err != nil {
			return 0, moerr.NewConstraintViolationf(ctx, "Arrow Date64 value at row %d is out of MatrixOne range", row)
		}
		return 0, appendDictionaryFixed(vec, converted, isNull, mp)
	case *array.Timestamp:
		timestampType, ok := values.DataType().(*arrow.TimestampType)
		if !ok {
			return 0, moerr.NewInvalidInput(ctx, "invalid Arrow Timestamp dictionary type")
		}
		if _, err := timestampType.GetZone(); err != nil {
			return 0, moerr.NewInvalidInputf(ctx, "invalid Arrow timezone %q: %v", timestampType.TimeZone, err)
		}
		micros, err := timestampToMicros(int64(typed.Value(index)), timestampType.Unit)
		if err != nil && !isNull {
			return 0, moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of range", row)
		}
		if !isNull {
			if err := validateTimestampPrecision(ctx, row, micros, target); err != nil {
				return 0, err
			}
		}
		if target.Oid == types.T_timestamp {
			converted, err := arrowMicrosToTimestamp(micros, timestampType.TimeZone != "", location)
			if err != nil && !isNull {
				return 0, moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of MatrixOne range", row)
			}
			return 0, appendDictionaryFixed(vec, converted, isNull, mp)
		}
		converted, err := arrowMicrosToDatetime(micros, timestampType.TimeZone != "", location)
		if err != nil && !isNull {
			return 0, moerr.NewConstraintViolationf(ctx, "Arrow Timestamp at row %d is out of MatrixOne range", row)
		}
		return 0, appendDictionaryFixed(vec, converted, isNull, mp)
	default:
		return 0, moerr.NewInvalidInputf(ctx, "invalid Arrow dictionary values %T", values)
	}
}

func appendDictionaryFixed[T any](vec *vector.Vector, value T, isNull bool, mp *mpool.MPool) error {
	var zero T
	if isNull {
		value = zero
	}
	return vector.AppendFixed(vec, value, isNull, mp)
}

func appendDictionaryNull(vec *vector.Vector, target types.Type, mp *mpool.MPool) error {
	switch target.Oid {
	case types.T_bool:
		return vector.AppendFixed(vec, false, true, mp)
	case types.T_int8:
		return vector.AppendFixed(vec, int8(0), true, mp)
	case types.T_int16:
		return vector.AppendFixed(vec, int16(0), true, mp)
	case types.T_int32:
		return vector.AppendFixed(vec, int32(0), true, mp)
	case types.T_int64:
		return vector.AppendFixed(vec, int64(0), true, mp)
	case types.T_uint8:
		return vector.AppendFixed(vec, uint8(0), true, mp)
	case types.T_uint16:
		return vector.AppendFixed(vec, uint16(0), true, mp)
	case types.T_uint32:
		return vector.AppendFixed(vec, uint32(0), true, mp)
	case types.T_uint64:
		return vector.AppendFixed(vec, uint64(0), true, mp)
	case types.T_float32:
		return vector.AppendFixed(vec, float32(0), true, mp)
	case types.T_float64:
		return vector.AppendFixed(vec, float64(0), true, mp)
	case types.T_decimal128:
		return vector.AppendFixed(vec, types.Decimal128{}, true, mp)
	case types.T_date:
		return vector.AppendFixed(vec, types.Date(0), true, mp)
	case types.T_time:
		return vector.AppendFixed(vec, types.Time(0), true, mp)
	case types.T_datetime:
		return vector.AppendFixed(vec, types.Datetime(0), true, mp)
	case types.T_timestamp:
		return vector.AppendFixed(vec, types.Timestamp(0), true, mp)
	default:
		return moerr.NewInternalErrorNoCtx("unknown MatrixOne dictionary target type")
	}
}

func checkConvertContext(ctx context.Context, row int) error {
	if row&1023 == 0 {
		return ctx.Err()
	}
	return nil
}

func timestampToMicros(value int64, unit arrow.TimeUnit) (int64, error) {
	switch unit {
	case arrow.Second:
		if value > math.MaxInt64/1_000_000 || value < math.MinInt64/1_000_000 {
			return 0, moerr.NewOutOfRangeNoCtx("timestamp", "microsecond")
		}
		return value * 1_000_000, nil
	case arrow.Millisecond:
		if value > math.MaxInt64/1_000 || value < math.MinInt64/1_000 {
			return 0, moerr.NewOutOfRangeNoCtx("timestamp", "microsecond")
		}
		return value * 1_000, nil
	case arrow.Microsecond:
		return value, nil
	case arrow.Nanosecond:
		if value%1_000 != 0 {
			return 0, moerr.NewOutOfRangeNoCtx("timestamp", "microsecond")
		}
		return value / 1_000, nil
	default:
		return 0, moerr.NewInvalidInputNoCtx("invalid Arrow timestamp unit")
	}
}

func validateTimestampPrecision(
	ctx context.Context,
	row int,
	micros int64,
	target types.Type,
) error {
	precisionFactor := int64(1)
	for scale := target.Scale; scale < 6; scale++ {
		precisionFactor *= 10
	}
	if micros%precisionFactor != 0 {
		return moerr.NewConstraintViolationf(ctx,
			"Arrow Timestamp at row %d exceeds MatrixOne %s(%d) precision",
			row, target.Oid, target.Scale)
	}
	return nil
}

var (
	minSupportedUnixMicros = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	maxSupportedUnixMicros = time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC).UnixMicro()
)

func arrowMicrosToTimestamp(micros int64, zoned bool, location *time.Location) (types.Timestamp, error) {
	if !zoned {
		wall := time.UnixMicro(micros).UTC()
		micros = time.Date(
			wall.Year(), wall.Month(), wall.Day(), wall.Hour(), wall.Minute(), wall.Second(), wall.Nanosecond(), location,
		).UnixMicro()
	}
	if micros < minSupportedUnixMicros || micros > maxSupportedUnixMicros {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "MatrixOne")
	}
	value := types.UnixMicroToTimestamp(micros)
	if value < types.TimestampMinValue || value > types.TimestampMaxValue {
		return 0, moerr.NewOutOfRangeNoCtx("timestamp", "MatrixOne")
	}
	return value, nil
}

func arrowMicrosToDatetime(micros int64, zoned bool, location *time.Location) (types.Datetime, error) {
	conversionLocation := time.UTC
	if zoned {
		conversionLocation = location
	}
	if micros < minSupportedUnixMicros || micros > maxSupportedUnixMicros {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "MatrixOne")
	}
	seconds := micros / 1_000_000
	nanos := micros % 1_000_000 * 1_000
	value := types.DatetimeFromUnixWithNsec(conversionLocation, seconds, nanos)
	year, _, _, _ := value.ToDate().Calendar(true)
	if year < types.MinDatetimeYear || year > types.MaxDatetimeYear {
		return 0, moerr.NewOutOfRangeNoCtx("datetime", "MatrixOne")
	}
	return value, nil
}
