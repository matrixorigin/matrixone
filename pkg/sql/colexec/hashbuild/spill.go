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

package hashbuild

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap/keycodec"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	spillNumBuckets = 32
	spillMagic      = 0x12345678DEADBEEF
	spillBufferSize = 8192 // Buffer 8192 rows before flushing
	// Serialized records are accumulated per bucket across source batches.
	// Allocation is admitted lazily against the lifecycle scratch lease and
	// falls back to direct writes when the hard budget has no headroom.
	spillWriteCoalesceSize = 64 << 10
)

type spillMaterializationMode uint8

const (
	// spillDirectMaterialization models UnionInt32 on the current upstream
	// batch. A const varlen source copies its out-of-line payload once and
	// broadcasts the resulting descriptor.
	spillDirectMaterialization spillMaterializationMode = iota
	// spillRetainedMaterialization models the compact non-const batch produced
	// by CopyIntoBatches. A later UnionInt32 treats every retained row as an
	// independent value, even when the ingress vector was const.
	spillRetainedMaterialization
)

func spillCheckedAdd(total, value uint64) (uint64, error) {
	if total > math.MaxUint64-value {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return total + value, nil
}

func spillCheckedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return left * right, nil
}

func spillCapacityReplacementOverlap(rows, keys, hashCap, rowIDCap, keyCap int) (uint64, error) {
	var overlap uint64
	add := func(required, current int, width uint64) error {
		if required < 0 || current < 0 {
			return process.ErrHashBuildBudgetInvalid
		}
		if required <= current {
			return nil
		}
		old, err := spillCheckedMul(uint64(current), width)
		if err != nil {
			return err
		}
		overlap, err = spillCheckedAdd(overlap, old)
		return err
	}
	if err := add(keys, keyCap, 8); err != nil {
		return 0, err
	}
	if err := add(rows, hashCap, 8); err != nil {
		return 0, err
	}
	if err := add(rows, rowIDCap, 4); err != nil {
		return 0, err
	}
	return overlap, nil
}

// spillMaterializedBytes models the batch that spillBatchBounded creates with
// UnionInt32. It follows vector materialization semantics instead of retained
// capacity or stale logical length: fixed-width descriptors are per output
// row, null payload is skipped, and direct const varlen payload is copied once.
func spillMaterializedBytesFor(
	bat *batch.Batch,
	targetRows uint64,
	mode spillMaterializationMode,
) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 || targetRows == 0 {
		return 0, nil
	}
	liveRows := uint64(bat.RowCount())
	var materialized uint64
	for _, vec := range bat.Vecs {
		if vec == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		typeSize := vec.GetType().TypeSize()
		if typeSize < 0 {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		descriptors, err := spillCheckedMul(targetRows, uint64(typeSize))
		if err != nil {
			return 0, err
		}
		if materialized, err = spillCheckedAdd(materialized, descriptors); err != nil {
			return 0, err
		}
		if !vec.GetType().IsVarlen() || vec.IsConstNull() {
			continue
		}

		values, _ := vector.MustVarlenaRawData(vec)
		valueRows := liveRows
		if vec.IsConst() {
			valueRows = 1
		}
		if valueRows == 0 || valueRows > uint64(len(values)) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		var livePayload uint64
		hasNull := !vec.GetNulls().EmptyByFlag()
		for row := uint64(0); row < valueRows; row++ {
			if hasNull && vec.GetNulls().Contains(row) {
				continue
			}
			if values[row].IsSmall() {
				continue
			}
			_, length := values[row].OffsetLen()
			if livePayload, err = spillCheckedAdd(livePayload, uint64(length)); err != nil {
				return 0, err
			}
		}

		projectedPayload := livePayload
		if !(mode == spillDirectMaterialization && vec.IsConst()) {
			// A retained CopyIntoBatches destination is non-const. Repeating
			// the complete live sample is a conservative bound for any compact
			// target batch assembled from ingress batches whose individual
			// high-water estimates were admitted before copying.
			roundedRows, err := spillCheckedAdd(targetRows, valueRows-1)
			if err != nil {
				return 0, err
			}
			repeats := roundedRows / valueRows
			if projectedPayload, err = spillCheckedMul(livePayload, repeats); err != nil {
				return 0, err
			}
		}

		if materialized, err = spillCheckedAdd(materialized, projectedPayload); err != nil {
			return 0, err
		}
	}
	return materialized, nil
}

func spillMaterializedBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	return spillMaterializedBytesFor(
		bat,
		uint64(bat.RowCount()),
		spillDirectMaterialization,
	)
}

func spillMarshalSlack(columns uint64) (uint64, error) {
	const (
		fixedSlack     = uint64(64 << 10)
		perColumnSlack = uint64(128)
	)
	if columns > (math.MaxUint64-fixedSlack)/perColumnSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return fixedSlack + columns*perColumnSlack, nil
}

func spillMaterializationSlack(columns uint64) (uint64, error) {
	const (
		fixedSlack     = uint64(64 << 10)
		perColumnSlack = uint64(16 << 10)
	)
	if columns > (math.MaxUint64-fixedSlack)/perColumnSlack {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return fixedSlack + columns*perColumnSlack, nil
}

// spillPeakBudgetFor accounts each simultaneously live component explicitly.
// inputBytes is zero for a retained batch whose source reservation is already
// owned by HashBuild.
func spillPeakBudgetFor(rows, inputBytes, selectedBytes, columns uint64) (uint64, error) {
	rowScratch, err := spillCheckedMul(rows, 12) // hashes + one row-id array
	if err != nil {
		return 0, err
	}
	total, err := spillCheckedAdd(rowScratch, inputBytes)
	if err != nil {
		return 0, err
	}
	if total, err = spillCheckedAdd(total, selectedBytes); err != nil {
		return 0, err
	}
	// MarshalBinary creates one serialized payload. The selected estimate
	// already includes its fixed-width data and varlen area, so charge that
	// payload once plus bounded framing/allocation slack.
	marshalSlack, err := spillMarshalSlack(columns)
	if err != nil {
		return 0, err
	}
	marshalBytes, err := spillCheckedAdd(selectedBytes, marshalSlack)
	if err != nil {
		return 0, err
	}
	if total, err = spillCheckedAdd(total, marshalBytes); err != nil {
		return 0, err
	}
	if total > uint64(^uint(0)>>1) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return total, nil
}

// spillBudgetBytes admits only the actual direct-spill path for the current
// input. It never projects a hypothetical retained batch.
func spillBudgetBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	rows := uint64(bat.RowCount())
	selected, err := spillMaterializedBytesFor(
		bat,
		rows,
		spillDirectMaterialization,
	)
	if err != nil {
		return 0, err
	}
	materializationSlack, err := spillMaterializationSlack(uint64(len(bat.Vecs)))
	if err != nil {
		return 0, err
	}
	if selected, err = spillCheckedAdd(selected, materializationSlack); err != nil {
		return 0, err
	}
	return spillPeakBudgetFor(rows, uint64(bat.Allocated()), selected, uint64(len(bat.Vecs)))
}

// spillScratchBudgetBytes returns the incremental spill charge. A copied
// build batch remains covered by HashmapBuilder.batchReservations while it is
// drained, so charging its source footprint again would double count it. An
// upstream batch has no HashBuild-owned reservation and keeps the full charge.
func spillScratchBudgetBytes(bat *batch.Batch, sourceAlreadyCharged bool) (uint64, error) {
	need, err := spillBudgetBytes(bat)
	if err != nil || !sourceAlreadyCharged || bat == nil || bat.RowCount() <= 0 {
		return need, err
	}
	// copyBuildBatch reconciles its retained reservation against Allocated
	// (plus metadata), so only that proven charge may be subtracted here.
	source := uint64(bat.Allocated())
	if source > need {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return need - source, nil
}

// spillRetainedBudgetBytes is the future-drain proof required before
// CopyIntoBatches may retain a small input. The destination loses constness,
// so its selected payload follows retained rather than direct semantics.
func spillRetainedBudgetBytes(bat *batch.Batch) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	rows := uint64(bat.RowCount())
	targetRows := rows
	if rows < uint64(colexec.DefaultBatchSize) {
		targetRows = uint64(colexec.DefaultBatchSize)
	}
	selected, err := spillMaterializedBytesFor(
		bat,
		targetRows,
		spillRetainedMaterialization,
	)
	if err != nil {
		return 0, err
	}
	metadata, ok := retainedMetadataAllowance(bat)
	if !ok || metadata > math.MaxUint64/targetRows {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	projectedMetadata := metadata
	if targetRows > rows {
		projectedMetadata, err = spillCheckedMul(metadata, targetRows)
		if err != nil {
			return 0, err
		}
		projectedMetadata, err = spillCheckedAdd(projectedMetadata, rows-1)
		if err != nil {
			return 0, err
		}
		projectedMetadata /= rows
	}
	if selected, err = spillCheckedAdd(selected, projectedMetadata); err != nil {
		return 0, err
	}
	materializationSlack, err := spillMaterializationSlack(uint64(len(bat.Vecs)))
	if err != nil {
		return 0, err
	}
	if selected, err = spillCheckedAdd(selected, materializationSlack); err != nil {
		return 0, err
	}
	// The retained source itself is covered by batchReservations.
	return spillPeakBudgetFor(
		targetRows,
		0,
		selected,
		uint64(len(bat.Vecs)),
	)
}

func (ctr *container) ensureSpillScratchReservationBytes(
	need uint64,
	analyzer process.Analyzer,
) error {
	if ctr.hashmapBuilder.budget == nil || need == 0 {
		return nil
	}
	var err error
	if ctr.spillScratchReservation == nil {
		ctr.spillScratchReservation, err =
			ctr.hashmapBuilder.budget.Reserve(need)
		if err == nil {
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildEmergencyScratchBytes",
				hashBuildStatInt64(need),
			)
			ctr.spillScratchEmergency = true
			ctr.spillScratchBase = need
		}
		return err
	}
	if ctr.spillScratchBase >= need {
		ctr.spillScratchEmergency = true
		return nil
	}
	grow := need - ctr.spillScratchBase
	if err := ctr.spillScratchReservation.Grow(grow); err != nil {
		analyzer.GetOpStats().AddExtraStat(
			"HashBuildEmergencyScratchGrowRejects",
			1,
		)
		return err
	}
	analyzer.GetOpStats().AddExtraStat(
		"HashBuildEmergencyScratchGrowCount",
		1,
	)
	analyzer.GetOpStats().AddExtraStat(
		"HashBuildEmergencyScratchGrowBytes",
		hashBuildStatInt64(grow),
	)
	ctr.spillScratchBase = need
	ctr.spillScratchEmergency = true
	return nil
}

func (ctr *container) growSpillScratchTransient(
	required uint64,
	analyzer process.Analyzer,
) (uint64, bool, error) {
	if ctr.hashmapBuilder.budget == nil || ctr.spillScratchReservation == nil ||
		required <= ctr.spillScratchBase {
		return 0, false, nil
	}
	oldSize := ctr.spillScratchReservation.Size()
	if err := ctr.spillScratchReservation.Grow(required - ctr.spillScratchBase); err != nil {
		return 0, false, err
	}
	analyzer.GetOpStats().SetMaxExtraStat(
		"HashBuildSpillScratchPeakBytes",
		hashBuildStatInt64(ctr.spillScratchReservation.Size()),
	)
	return oldSize, true, nil
}

func (ctr *container) restoreSpillScratchTransient(oldSize uint64, grew bool) error {
	if !grew {
		return nil
	}
	_, err := ctr.spillScratchReservation.ReconcileDown(oldSize)
	return err
}

func (ctr *container) ensureDirectSpillScratchReservation(bat *batch.Batch, analyzer process.Analyzer) error {
	if ctr.spillBatchAllocation != nil {
		// A borrowed upstream batch is already live and cannot be reclaimed by
		// reserving another logical token. Exact scatter/expression allocations
		// admit their physical capacities and adapt the unpublished input. The
		// retained-copy path below still keeps a one-unit future-progress token
		// because choosing to retain is under HashBuild's control.
		return nil
	}
	var (
		need uint64
		err  error
	)
	need, err = spillBudgetBytes(bat)
	if err != nil {
		return err
	}
	return ctr.ensureSpillScratchReservationBytes(need, analyzer)
}

func (ctr *container) ensureRetainedSpillScratchReservation(bat *batch.Batch, analyzer process.Analyzer) error {
	var (
		need uint64
		err  error
	)
	if ctr.spillBatchAllocation != nil {
		need, err = spillMinimumUnitBudgetBytes(bat, ctr.spillConditions)
	} else {
		need, err = spillRetainedBudgetBytes(bat)
	}
	if err != nil {
		return err
	}
	return ctr.ensureSpillScratchReservationBytes(need, analyzer)
}

// spillMinimumUnitBudgetBytes keeps only the headroom for one physical spill
// unit. It derives capacities from actual input values and the closed
// expression family; it neither scales the whole batch nor applies a safety
// multiplier. The token is converted into exact allocations on spill entry.
func spillMinimumUnitBudgetBytes(
	bat *batch.Batch,
	exprs []*plan.Expr,
) (uint64, error) {
	if bat == nil || bat.RowCount() <= 0 {
		return 0, nil
	}
	selected, wire, err := spillMinimumSelectedAndWireBytes(bat)
	if err != nil {
		return 0, err
	}
	if wire > math.MaxUint64-24 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	marshal, err := initialAllocationCapacity(wire + 24)
	if err != nil {
		return 0, err
	}
	expression, err := spillMinimumExpressionBytes(exprs, bat)
	if err != nil {
		return 0, err
	}
	total := uint64(12) // one hash plus one row id
	for _, value := range []uint64{selected, marshal, expression} {
		if total > math.MaxUint64-value {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += value
	}
	return total, nil
}

func spillMinimumSelectedAndWireBytes(
	bat *batch.Batch,
) (selected uint64, wire uint64, err error) {
	// Batch framing plus Attr/ExtraBuf length prefixes.
	wire = 8 + 4 + 4 + 4 + 4 + 4 + uint64(len(bat.ExtraBuf))
	for _, attr := range bat.Attrs {
		if wire > math.MaxUint64-4-uint64(len(attr)) {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		wire += 4 + uint64(len(attr))
	}
	for _, vec := range bat.Vecs {
		if vec == nil || vec.GetType().TypeSize() < 0 {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		data, capErr := initialAllocationCapacity(
			uint64(vec.GetType().TypeSize()),
		)
		if capErr != nil {
			return 0, 0, capErr
		}
		areaPayload, payloadErr := maxVectorValueBytes(vec)
		if payloadErr != nil {
			return 0, 0, payloadErr
		}
		var area uint64
		if areaPayload > types.VarlenaInlineSize {
			area, capErr = initialAllocationCapacity(areaPayload)
			if capErr != nil {
				return 0, 0, capErr
			}
		}
		// Accounted vectors install one null and one grouping word before
		// extending their first row.
		physical := data + area + 16
		if selected > math.MaxUint64-physical {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		selected += physical
		// Physical capacities upper-bound one-row logical data, area, and null
		// payload; only the fixed wire framing is added separately.
		const vectorFraming = uint64(4 + 1 + types.TSize + 4 + 4 + 4 + 4 + 1)
		if wire > math.MaxUint64-vectorFraming-physical {
			return 0, 0, process.ErrHashBuildBudgetInvalid
		}
		wire += vectorFraming + physical
	}
	return selected, wire, nil
}

func maxVectorValueBytes(vec *vector.Vector) (uint64, error) {
	if vec == nil || !vec.GetType().IsVarlen() || vec.IsConstNull() {
		return 0, nil
	}
	values, _ := vector.MustVarlenaRawData(vec)
	rows := vec.Length()
	if vec.IsConst() && rows > 0 {
		rows = 1
	}
	if rows > len(values) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var maximum uint64
	for row := 0; row < rows; row++ {
		if vec.GetNulls().Contains(uint64(row)) {
			continue
		}
		var length uint64
		if values[row].IsSmall() {
			length = uint64(len(values[row].GetByteSlice(nil)))
		} else {
			_, valueLen := values[row].OffsetLen()
			length = uint64(valueLen)
		}
		if length > maximum {
			maximum = length
		}
	}
	return maximum, nil
}

func spillMinimumExpressionBytes(
	exprs []*plan.Expr,
	bat *batch.Batch,
) (uint64, error) {
	if len(exprs) == 0 {
		return 0, nil
	}
	if !AllocationAccountedExpressionSetSupported(exprs) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var total uint64
	for _, expr := range exprs {
		bytes, err := spillMinimumExpressionTreeBytes(expr, bat)
		if err != nil || total > math.MaxUint64-bytes {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += bytes
	}
	return total, nil
}

func spillMinimumExpressionTreeBytes(
	expr *plan.Expr,
	bat *batch.Batch,
) (uint64, error) {
	if expr == nil {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_Col:
		return 0, nil
	case *plan.Expr_Lit:
		return expressionInitialOwnedBytes(expr)
	case *plan.Expr_F:
		if node.F == nil || node.F.Func == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		var total uint64
		for _, arg := range node.F.Args {
			child, err := spillMinimumExpressionTreeBytes(arg, bat)
			if err != nil || total > math.MaxUint64-child {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			total += child
		}
		result, err := spillMinimumExpressionResultBytes(expr, node.F.Args, bat)
		if err != nil || total > math.MaxUint64-result {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		total += result
		functionID, _ := function.DecodeOverloadID(node.F.Func.Obj)
		if functionID == function.CASE {
			if total > math.MaxUint64-8 {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			total += 8
		}
		return total, nil
	default:
		return expressionInitialOwnedBytes(expr)
	}
}

func spillMinimumExpressionResultBytes(
	expr *plan.Expr,
	args []*plan.Expr,
	bat *batch.Batch,
) (uint64, error) {
	oid := types.T(expr.Typ.Id)
	typ := oid.ToType()
	if typ.TypeSize() < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	data, err := initialAllocationCapacity(uint64(typ.TypeSize()))
	if err != nil {
		return 0, err
	}
	result := data + 16
	if !typ.IsVarlen() {
		return result, nil
	}
	payload, err := spillExpressionPayloadBytes(expr, args, bat)
	if err != nil {
		return 0, err
	}
	if payload > types.VarlenaInlineSize {
		area, err := initialAllocationCapacity(payload)
		if err != nil || result > math.MaxUint64-area {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		result += area
	}
	return result, nil
}

func spillExpressionPayloadBytes(
	expr *plan.Expr,
	args []*plan.Expr,
	bat *batch.Batch,
) (uint64, error) {
	if bat == nil || bat.RowCount() < 0 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	var maximum uint64
	for row := 0; row < bat.RowCount(); row++ {
		value, err := spillExpressionPayloadBytesAt(expr, args, bat, row)
		if err != nil {
			return 0, err
		}
		if value > maximum {
			maximum = value
		}
	}
	return maximum, nil
}

func spillExpressionPayloadBytesAt(
	expr *plan.Expr,
	args []*plan.Expr,
	bat *batch.Batch,
	row int,
) (uint64, error) {
	node, ok := expr.Expr.(*plan.Expr_F)
	if !ok || node.F == nil || node.F.Func == nil {
		return 0, nil
	}
	functionID, _ := function.DecodeOverloadID(node.F.Func.Obj)
	switch functionID {
	case function.CONCAT:
		var total uint64
		for _, arg := range args {
			value, err := spillExpressionArgPayloadBytesAt(arg, bat, row)
			if err != nil || total > math.MaxUint64-value {
				return 0, process.ErrHashBuildBudgetInvalid
			}
			total += value
		}
		return total, nil
	case function.CASE:
		// CASE's exact selected branch is evaluated later. The largest varlen
		// branch value at this same row is a safe one-row bound without combining
		// maxima taken from different rows.
		var maximum uint64
		for _, arg := range args {
			if arg == nil || !types.T(arg.Typ.Id).ToType().IsVarlen() {
				continue
			}
			value, err := spillExpressionArgPayloadBytesAt(arg, bat, row)
			if err != nil {
				return 0, err
			}
			if value > maximum {
				maximum = value
			}
		}
		return maximum, nil
	case function.CAST:
		if len(args) == 0 || args[0] == nil {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		if types.T(args[0].Typ.Id).ToType().IsIntOrUint() {
			return 20, nil
		}
		return spillExpressionArgPayloadBytesAt(args[0], bat, row)
	default:
		return 0, nil
	}
}

func spillExpressionArgPayloadBytesAt(
	expr *plan.Expr,
	bat *batch.Batch,
	row int,
) (uint64, error) {
	if expr == nil || bat == nil || row < 0 || row >= bat.RowCount() {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	switch node := expr.Expr.(type) {
	case *plan.Expr_Col:
		if node.Col == nil || node.Col.ColPos < 0 ||
			int(node.Col.ColPos) >= len(bat.Vecs) {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		return vectorValueBytesAt(bat.Vecs[node.Col.ColPos], row)
	case *plan.Expr_Lit:
		if node.Lit == nil || node.Lit.GetIsnull() {
			return 0, nil
		}
		return uint64(len(node.Lit.GetSval())), nil
	case *plan.Expr_F:
		return spillExpressionPayloadBytesAt(expr, node.F.GetArgs(), bat, row)
	default:
		return 0, nil
	}
}

func vectorValueBytesAt(vec *vector.Vector, row int) (uint64, error) {
	if vec == nil || row < 0 || row >= vec.Length() ||
		!vec.GetType().IsVarlen() || vec.IsConstNull() {
		if vec == nil || row < 0 || row >= vec.Length() {
			return 0, process.ErrHashBuildBudgetInvalid
		}
		return 0, nil
	}
	index := row
	if vec.IsConst() {
		index = 0
	}
	if vec.GetNulls().Contains(uint64(index)) {
		return 0, nil
	}
	values, _ := vector.MustVarlenaRawData(vec)
	if index >= len(values) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if values[index].IsSmall() {
		return uint64(len(values[index].GetByteSlice(nil))), nil
	}
	_, length := values[index].OffsetLen()
	return uint64(length), nil
}

func (ctr *container) releaseSpillScratchReservation() {
	if ctr.spillScratchReservation != nil {
		ctr.spillScratchReservation.Release()
		ctr.spillScratchReservation = nil
	}
	ctr.spillScratchEmergency = false
	ctr.spillScratchBase = 0
}

func (ctr *container) dropSpillScratchBuffers() {
	if ctr.spillBatchAllocation != nil {
		if cap(ctr.spillHashValues) > 0 {
			mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillHashValues)
		}
		if cap(ctr.spillBucketRowIds) > 0 {
			mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillBucketRowIds)
		}
		if ctr.spillAccountedWrite != nil {
			ctr.spillAccountedWrite.Free()
			ctr.spillAccountedWrite = nil
		}
		for i := range ctr.spillAccountedBuckets {
			if ctr.spillAccountedBuckets[i] != nil {
				ctr.spillAccountedBuckets[i].Free()
				ctr.spillAccountedBuckets[i] = nil
			}
		}
	}
	for bucket := range ctr.spillBucketWriteBufs {
		ctr.spillBucketWriteBufs[bucket] = bytes.Buffer{}
		ctr.spillBucketWriteRows[bucket] = 0
	}
	ctr.spillHashValues = nil
	ctr.spillBucketRowIds = nil
	for i := range ctr.spillBucketCounts {
		ctr.spillBucketCounts[i] = 0
	}
	for i := range ctr.spillBucketOffsets {
		ctr.spillBucketOffsets[i] = 0
	}
	ctr.spillKeyVecs = nil
	ctr.spillWriteBuf = bytes.Buffer{}
	ctr.spillAllocationMP = nil
	ctr.spillCoalesceDisabled = false
}

func growHashBuildSpillSlice[T any](
	values []T,
	length int,
	mp *mpool.MPool,
	account *mpool.AllocationAccount,
	site mpool.AllocationSite,
) ([]T, error) {
	if length < 0 || account == nil {
		return nil, mpool.ErrAllocationAccountInvalid
	}
	if length <= cap(values) {
		return values[:length], nil
	}
	capacity := cap(values)
	if capacity == 0 {
		capacity = 1
	}
	for capacity < length {
		if capacity > math.MaxInt/2 {
			capacity = length
			break
		}
		capacity *= 2
	}
	next, err := mpool.MakeSliceAccounted[T](
		capacity,
		mp,
		account,
		HashBuildAllocationOwner,
		site,
	)
	if err != nil {
		return nil, err
	}
	copy(next, values)
	if cap(values) > 0 {
		mpool.FreeSlice(mp, values)
	}
	return next[:length], nil
}

func spillMarshalGrowBytes(bat *batch.Batch) (uint64, error) {
	base := uint64(bat.Allocated())
	if size := uint64(bat.Size()); size > base {
		base = size
	}
	columns := uint64(len(bat.Vecs))
	if columns > (math.MaxUint64-24)/128 {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	return spillCheckedAdd(base, columns*128+24)
}

func marshalSpillRecord(bat *batch.Batch, buf *bytes.Buffer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}

	cnt := int64(bat.RowCount())
	buf.Reset()
	grow, err := spillMarshalGrowBytes(bat)
	if err != nil {
		return 0, err
	}
	if grow > uint64(math.MaxInt) {
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if uint64(buf.Cap()) < grow {
		// Drop a smaller retained buffer before allocating the final capacity;
		// otherwise bytes.Buffer's geometric growth recreates the multiplier
		// that admission intentionally removed.
		*buf = *bytes.NewBuffer(make([]byte, 0, int(grow)))
	}
	buf.Write(types.EncodeInt64(&cnt))
	// Reserve space for batchSize (filled in after marshalling)
	batchSizePos := buf.Len()
	var zero int64
	buf.Write(types.EncodeInt64(&zero))

	// Write batch data directly to spillWriteBuf.  The bounded partition path
	// reserves this buffer's conservative upper bound before entering here.
	batchStartPos := buf.Len()
	if _, err := bat.MarshalBinaryWithBuffer(buf, false); err != nil {
		return 0, err
	}
	batchSize := int64(buf.Len() - batchStartPos)

	// Write batchSize at reserved position
	batchSizeBytes := types.EncodeInt64(&batchSize)
	copy(buf.Bytes()[batchSizePos:batchSizePos+len(batchSizeBytes)], batchSizeBytes)

	magic := uint64(spillMagic)
	buf.Write(types.EncodeUint64(&magic))
	return cnt, nil
}

func marshalSpillRecordAccounted(
	bat *batch.Batch,
	buf *mpool.AccountedBuffer,
) (int64, error) {
	if bat == nil || bat.RowCount() == 0 || buf == nil {
		return 0, nil
	}
	cnt := int64(bat.RowCount())
	buf.Reset()
	batchSize, err := bat.MarshalBinarySize()
	if err != nil || batchSize > math.MaxInt-24 {
		if err != nil {
			return 0, err
		}
		return 0, process.ErrHashBuildBudgetInvalid
	}
	if err := buf.EnsureCapacity(batchSize + 24); err != nil {
		return 0, err
	}
	if _, err := buf.Write(types.EncodeInt64(&cnt)); err != nil {
		return 0, err
	}
	batchSizePos := buf.Len()
	var zero int64
	if _, err := buf.Write(types.EncodeInt64(&zero)); err != nil {
		return 0, err
	}
	batchStart := buf.Len()
	if err := bat.MarshalBinaryTo(buf); err != nil {
		return 0, err
	}
	serializedSize := int64(buf.Len() - batchStart)
	copy(
		buf.Bytes()[batchSizePos:batchSizePos+8],
		types.EncodeInt64(&serializedSize),
	)
	magic := uint64(spillMagic)
	if _, err := buf.Write(types.EncodeUint64(&magic)); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (ctr *container) writeSpillPayload(
	proc *process.Process,
	file *os.File,
	payload []byte,
	rows int64,
	analyzer process.Analyzer,
) error {
	if file == nil || len(payload) == 0 {
		return process.ErrHashBuildBudgetInvalid
	}
	// All initial-spill writes funnel through this helper. Check after any
	// vector projection/marshal and immediately before the physical write so a
	// cancellation that raced lazy file creation or serialization does not
	// start stale I/O. An already-running os.File.Write is not interruptible.
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}

	var err error
	if ctr.hashmapBuilder.budget != nil {
		if ctr.spillBundle == nil {
			return process.ErrHashBuildBudgetInvalid
		}
		_, _, err = ctr.spillBundle.growDisk(file, ctr.hashmapBuilder.budget, uint64(len(payload)))
		if err != nil {
			return err
		}
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	written, err := file.Write(payload)
	if err != nil {
		return err
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	if ctr.hashmapBuilder.budget != nil {
		// The exact payload length was admitted. Record logical ownership only
		// after the full write; partial writes retain the conservative charge
		// until the enclosing bundle closes the file.
		ctr.spillBundle.recordDiskWrite(file, rows, uint64(written))
	}
	if analyzer != nil {
		analyzer.Spill(int64(written))
		analyzer.SpillRows(rows)
	}

	return nil
}

func (ctr *container) flushBucketBuffer(proc *process.Process, bat *batch.Batch, file *os.File, analyzer process.Analyzer) (int64, error) {
	if bat == nil || bat.RowCount() == 0 {
		return 0, nil
	}
	cnt, err := marshalSpillRecord(bat, &ctr.spillWriteBuf)
	if err != nil {
		return 0, err
	}
	if err := ctr.writeSpillPayload(proc, file, ctr.spillWriteBuf.Bytes(), cnt, analyzer); err != nil {
		return 0, err
	}
	return cnt, nil
}

func (ctr *container) getSpillFS(proc *process.Process) (fileservice.MutableFileService, error) {
	if ctr.spillFS != nil {
		return ctr.spillFS, nil
	}
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	ctr.spillFS = fs
	return fs, nil
}

// ensureSpillFile lazily creates an anonymous spill file for the given bucket.
func (ctr *container) ensureSpillFile(proc *process.Process, files []*os.File, bucket int) (*os.File, error) {
	if bucket < 0 || bucket >= len(files) {
		return nil, process.ErrHashBuildBudgetInvalid
	}
	if files[bucket] != nil {
		return files[bucket], nil
	}
	if ctr.spillUUID == "" {
		return nil, moerr.NewInternalErrorNoCtx("ensureSpillFile: spillUUID not initialized")
	}
	spillfs, err := ctr.getSpillFS(proc)
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("join_%s_%d_build", ctr.spillUUID, bucket)
	var fdToken *process.HashBuildSpillFDReservation
	if ctr.hashmapBuilder.budget != nil {
		fdToken, err = ctr.hashmapBuilder.budget.ReserveSpillFD(1)
		if err != nil {
			return nil, err
		}
	}
	f, err := spillfs.CreateAndRemoveFile(proc.Ctx, name)
	if err != nil {
		if fdToken != nil {
			fdToken.Release()
		}
		return nil, err
	}
	files[bucket] = f
	if fdToken != nil {
		if ctr.spillBundle == nil {
			ctr.spillBundle = &spillFileBundle{}
		}
		ctr.spillBundle.addFD(f, bucket, fdToken)
	}
	return f, nil
}

// spillBatchBounded partitions one input batch without retaining 32 bucket
// vectors. Hash values are classified with two linear passes (count, then
// scatter after prefix offsets), and a single row-id array describes every
// bucket. One selected batch is reused as each bucket is materialized and
// marshaled before advancing; serialized records are coalesced until the
// bounded buffers or final handoff flush.
func (ctr *container) spillBatchBounded(proc *process.Process, bat *batch.Batch, files []*os.File, executors []colexec.ExpressionExecutor, analyzer process.Analyzer, sourceAlreadyCharged bool) error {
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	exact := ctr.spillBatchAllocation != nil
	var (
		need uint64
		err  error
	)
	if exact {
		if ctr.hashmapBuilder.mapAllocationAccount == nil {
			return mpool.ErrAllocationAccountInvalid
		}
		if ctr.spillAllocationMP != nil && ctr.spillAllocationMP != proc.Mp() {
			return mpool.ErrAllocationAccountInvalid
		}
		ctr.spillAllocationMP = proc.Mp()
		// A pre-spill token is headroom, not physical ownership. Release it
		// immediately before the exact scratch allocations consume that space.
		ctr.releaseSpillScratchReservation()
		if !sourceAlreadyCharged {
			// The upstream batch is borrowed, already physically live, and cannot
			// be made smaller by rejecting a new logical token. Record it as
			// observation only; every new HashBuild-owned byte below is admitted
			// by the exact account and the process MPool remains the global guard.
			externalBytes := bat.Allocated()
			if size := bat.Size(); size > externalBytes {
				externalBytes = size
			}
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildSpillBorrowedSourceBytes",
				int64(externalBytes),
			)
		}
	} else {
		need, err = spillScratchBudgetBytes(bat, sourceAlreadyCharged)
		if err != nil {
			return err
		}
	}
	// Scratch belongs to the execution generation, not to one batch. Build
	// normally pre-admits the emergency lease before calling us; direct callers
	// (including recovery/error paths and unit tests) establish the same lease
	// here. Keep it live while capacities are retained and release it from
	// Reset/Free/build cleanup exactly once.
	if ctr.hashmapBuilder.budget != nil && !exact {
		if ctr.spillScratchReservation == nil {
			ctr.spillScratchReservation, err = ctr.hashmapBuilder.budget.Reserve(need)
			if err != nil {
				analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchReserveRejects", 1)
				return err
			}
			ctr.spillScratchBase = need
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildSpillScratchPeakBytes",
				hashBuildStatInt64(ctr.spillScratchReservation.Size()),
			)
		} else if need > ctr.spillScratchBase {
			grow := need - ctr.spillScratchBase
			if err := ctr.spillScratchReservation.Grow(grow); err != nil {
				analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowRejects", 1)
				return err
			}
			analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowCount", 1)
			analyzer.GetOpStats().AddExtraStat("HashBuildSpillScratchGrowBytes", hashBuildStatInt64(grow))
			ctr.spillScratchBase = need
			analyzer.GetOpStats().SetMaxExtraStat(
				"HashBuildSpillScratchPeakBytes",
				hashBuildStatInt64(ctr.spillScratchReservation.Size()),
			)
		}
	}

	rows := bat.RowCount()
	var oldScratchSize uint64
	var grewScratch bool
	if !exact {
		replacementOverlap, overlapErr := spillCapacityReplacementOverlap(
			rows,
			len(executors),
			cap(ctr.spillHashValues),
			cap(ctr.spillBucketRowIds),
			cap(ctr.spillKeyVecs),
		)
		if overlapErr != nil {
			return overlapErr
		}
		replacementPeak, addErr := spillCheckedAdd(need, replacementOverlap)
		if addErr != nil {
			return addErr
		}
		oldScratchSize, grewScratch, err = ctr.growSpillScratchTransient(
			replacementPeak,
			analyzer,
		)
		if err != nil {
			return err
		}
	}

	if cap(ctr.spillKeyVecs) < len(executors) {
		ctr.spillKeyVecs = make([]*vector.Vector, len(executors))
	}
	if exact {
		ctr.spillHashValues, err = growHashBuildSpillSlice(
			ctr.spillHashValues,
			rows,
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildSpillAllocationSiteHashValues,
		)
	} else if cap(ctr.spillHashValues) < rows {
		ctr.spillHashValues = make([]uint64, rows)
	}
	if err != nil {
		return err
	}
	if exact {
		ctr.spillBucketRowIds, err = growHashBuildSpillSlice(
			ctr.spillBucketRowIds,
			rows,
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildSpillAllocationSiteRowIDs,
		)
	} else if cap(ctr.spillBucketRowIds) < rows {
		ctr.spillBucketRowIds = make([]int32, rows)
	}
	if err != nil {
		return err
	}
	if err := ctr.restoreSpillScratchTransient(oldScratchSize, grewScratch); err != nil {
		return err
	}
	keyVecs := ctr.spillKeyVecs[:len(executors)]
	var selected *batch.Batch
	defer func() {
		if selected != nil {
			selected.Clean(proc.Mp())
		}
		for i := range ctr.spillKeyVecs {
			ctr.spillKeyVecs[i] = nil
		}
	}()
	evalOne := func(i int) error {
		vec, evalErr := executors[i].Eval(proc, []*batch.Batch{bat}, nil)
		if evalErr == nil {
			keyVecs[i] = vec
		}
		return evalErr
	}
	if ctr.spillExprLease != nil {
		if ctr.spillExprLease.Len() != len(executors) {
			return process.ErrHashBuildBudgetInvalid
		}
		err = ctr.spillExprLease.Run(proc, bat.RowCount(), evalOne)
	} else {
		for i := range executors {
			if err = evalOne(i); err != nil {
				break
			}
		}
	}
	if err != nil {
		// Eval may leave newly allocated child/result vectors cached in the
		// executor tree. Destroy that tree while both the previous and
		// candidate reservations are still charged.
		if !exact {
			ctr.freeSpillExprExecs()
		}
		return err
	}
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	ctr.hashmapBuilder.observeNullKeys(keyVecs)

	// Reuse hashValues buffer
	hashes := ctr.spillHashValues[:rows]
	computeXXHash(keyVecs, hashes)
	if err := checkHashBuildCanceled(proc); err != nil {
		return err
	}
	counts := ctr.spillBucketCounts[:]
	for i := range counts {
		counts[i] = 0
	}
	for _, hash := range hashes {
		counts[int(hash&(spillNumBuckets-1))]++
	}
	offsets := ctr.spillBucketOffsets[:]
	offsets[0] = 0
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		offsets[bucket+1] = offsets[bucket] + counts[bucket]
	}
	// Reuse counts as write cursors only through a stack-local copy. The
	// original prefix offsets remain stable while each bucket is materialized.
	var writePos [spillNumBuckets]int32
	copy(writePos[:], offsets[:spillNumBuckets])
	for row, hash := range hashes {
		bucket := int(hash & (spillNumBuckets - 1))
		pos := writePos[bucket]
		ctr.spillBucketRowIds[pos] = int32(row)
		writePos[bucket] = pos + 1
	}

	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		if err := checkHashBuildCanceled(proc); err != nil {
			return err
		}
		start, end := offsets[bucket], offsets[bucket+1]
		if start == end {
			continue
		}
		if selected == nil {
			selected = batch.NewOffHeapWithSize(len(bat.Vecs))
			if exact {
				if err := selected.SetAllocationAccount(
					ctr.spillBatchAllocation,
				); err != nil {
					return err
				}
			}
			selected.Attrs = bat.Attrs
			for i, vec := range bat.Vecs {
				if vec == nil {
					return process.ErrHashBuildBudgetInvalid
				}
				if exact {
					selected.Vecs[i], err =
						vector.NewOffHeapVecWithTypeAndAllocation(
							*vec.GetType(),
							ctr.spillBatchAllocation,
						)
					if err != nil {
						return err
					}
				} else {
					selected.Vecs[i] = vector.NewOffHeapVecWithType(*vec.GetType())
				}
			}
		}
		cursor := start
		for cursor < end {
			attemptEnd := end
			reclaimedMinimum := false
			for {
				selected.CleanOnlyData()
				sels := ctr.spillBucketRowIds[cursor:attemptEnd]
				n := int(attemptEnd - cursor)
				var spillErr error
				for i, vec := range bat.Vecs {
					if vec == nil {
						spillErr = process.ErrHashBuildBudgetInvalid
						break
					}
					if spillErr = selected.Vecs[i].PreExtend(n, proc.Mp()); spillErr != nil {
						break
					}
					if spillErr = selected.Vecs[i].UnionInt32(vec, sels, proc.Mp()); spillErr != nil {
						break
					}
				}
				if spillErr == nil {
					selected.SetRowCount(n)
					var file *os.File
					file, spillErr = ctr.ensureSpillFile(proc, files, int(bucket))
					if spillErr == nil {
						spillErr = ctr.appendSpillRecord(
							proc,
							file,
							int(bucket),
							selected,
							need,
							analyzer,
						)
					}
				}
				selected.CleanOnlyData()
				if spillErr == nil {
					cursor = attemptEnd
					break
				}
				if !exact || !IsRetryableMemoryCapacity(spillErr) {
					return spillErr
				}
				if err := checkHashBuildCanceled(proc); err != nil {
					return err
				}
				if n > 1 {
					attemptEnd = cursor + int32((n+1)/2)
					analyzer.GetOpStats().AddExtraStat(
						"HashBuildSpillBatchReductions",
						1,
					)
					continue
				}
				if !reclaimedMinimum {
					before := ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used
					if err := ctr.reclaimOptionalSpillBuffers(
						proc,
						files,
						analyzer,
					); err != nil {
						return err
					}
					reclaimedMinimum = true
					after := ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used
					if after >= before {
						analyzer.GetOpStats().AddExtraStat(
							"HashBuildSpillMinimumUnitErrors",
							1,
						)
						return NewMinimumAllocationPressureError(
							"hashbuild",
							"spill-selected-or-codec",
							ctr.hashmapBuilder.mapAllocationAccount,
						)
					}
					analyzer.GetOpStats().AddExtraStat(
						"HashBuildSpillOptionalReclaims",
						1,
					)
					continue
				}
				analyzer.GetOpStats().AddExtraStat(
					"HashBuildSpillMinimumUnitErrors",
					1,
				)
				return NewMinimumAllocationPressureError(
					"hashbuild",
					"spill-selected-or-codec",
					ctr.hashmapBuilder.mapAllocationAccount,
				)
			}
		}
	}
	return nil
}

// reclaimOptionalSpillBuffers publishes already completed coalesced records,
// then drops codec/coalesce capacity. The current selected record has not been
// published when this is called, so retrying that one record is idempotent.
func (ctr *container) reclaimOptionalSpillBuffers(
	proc *process.Process,
	files []*os.File,
	analyzer process.Analyzer,
) error {
	for bucket, buffer := range ctr.spillAccountedBuckets {
		if buffer == nil {
			continue
		}
		if buffer.Len() > 0 {
			if bucket >= len(files) || files[bucket] == nil {
				return process.ErrHashBuildBudgetInvalid
			}
			if err := ctr.flushPendingSpillBucket(
				proc,
				files[bucket],
				bucket,
				analyzer,
			); err != nil {
				return err
			}
		}
		buffer.Free()
		ctr.spillAccountedBuckets[bucket] = nil
	}
	ctr.spillCoalesceDisabled = true
	if ctr.spillAccountedWrite != nil {
		ctr.spillAccountedWrite.Free()
		ctr.spillAccountedWrite = nil
	}
	return nil
}

func (ctr *container) releaseSpillComputeScratch() {
	if ctr.spillBatchAllocation == nil || ctr.spillAllocationMP == nil {
		return
	}
	if cap(ctr.spillHashValues) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillHashValues)
	}
	if cap(ctr.spillBucketRowIds) > 0 {
		mpool.FreeSlice(ctr.spillAllocationMP, ctr.spillBucketRowIds)
	}
	ctr.spillHashValues = nil
	ctr.spillBucketRowIds = nil
}

// spillBatchWithPressure retries only the unpublished prefix of an exact
// spill operation. Hash/expression capacity failures happen before any bucket
// write; selected/codec failures are handled transactionally inside
// spillBatchBounded. Each retry halves the input or reclaims memory, and a
// one-row failure becomes a controlled minimum-unit error.
func (ctr *container) spillBatchWithPressure(
	proc *process.Process,
	bat *batch.Batch,
	files []*os.File,
	executors []colexec.ExpressionExecutor,
	analyzer process.Analyzer,
	sourceAlreadyCharged bool,
) error {
	if ctr.spillBatchAllocation == nil || bat == nil || bat.RowCount() == 0 {
		return ctr.spillBatchBounded(
			proc,
			bat,
			files,
			executors,
			analyzer,
			sourceAlreadyCharged,
		)
	}
	rows := bat.RowCount()
	chunk := rows
	minimumRetried := false
	guard := NewPressureRetryGuard(PressureProgress{
		Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
		InputUnits:       chunk,
		OptionalDisabled: ctr.spillCoalesceDisabled,
	}, 64)
	for start := 0; start < rows; {
		end := rows
		if chunk < rows-start {
			end = start + chunk
		}
		current := bat
		if start != 0 || end != rows {
			var err error
			current, err = bat.Window(start, end)
			if err != nil {
				return err
			}
		}
		err := ctr.spillBatchBounded(
			proc,
			current,
			files,
			executors,
			analyzer,
			sourceAlreadyCharged,
		)
		if current != bat {
			current.Clean(proc.Mp())
		}
		if err == nil {
			start = end
			minimumRetried = false
			nextUnits := chunk
			if remaining := rows - start; remaining < nextUnits {
				nextUnits = remaining
			}
			guard = NewPressureRetryGuard(PressureProgress{
				Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
				InputUnits:       nextUnits,
				OptionalDisabled: ctr.spillCoalesceDisabled,
			}, 64)
			continue
		}
		if !IsRetryableMemoryCapacity(err) {
			return err
		}
		if cancelErr := checkHashBuildCanceled(proc); cancelErr != nil {
			return cancelErr
		}
		ctr.releaseSpillComputeScratch()
		attempted := end - start
		if attempted <= 1 {
			if !minimumRetried {
				if reclaimErr := ctr.reclaimOptionalSpillBuffers(
					proc,
					files,
					analyzer,
				); reclaimErr != nil {
					return reclaimErr
				}
				next := PressureProgress{
					Used:             ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
					InputUnits:       attempted,
					OptionalDisabled: ctr.spillCoalesceDisabled,
				}
				if guard.Advance(next) != nil {
					return NewMinimumAllocationPressureError(
						"hashbuild",
						"spill-hash-or-expression",
						ctr.hashmapBuilder.mapAllocationAccount,
					)
				}
				minimumRetried = true
				analyzer.GetOpStats().AddExtraStat(
					"HashBuildSpillMinimumRetries",
					1,
				)
				continue
			}
			return NewMinimumAllocationPressureError(
				"hashbuild",
				"spill-hash-or-expression",
				ctr.hashmapBuilder.mapAllocationAccount,
			)
		}
		chunk = (attempted + 1) / 2
		if err := guard.Advance(PressureProgress{
			Used:       ctr.hashmapBuilder.mapAllocationAccount.Snapshot().Used,
			InputUnits: chunk,
		}); err != nil {
			return err
		}
		analyzer.GetOpStats().AddExtraStat("HashBuildSpillInputReductions", 1)
	}
	return nil
}

// appendSpillRecord appends one framed record to the bucket's bounded write
// buffer. Full buffers are written before accepting the next record. A record
// larger than the coalescing target is written directly, so no unbounded
// temporary copy can be retained.
func (ctr *container) appendSpillRecord(
	proc *process.Process,
	file *os.File,
	bucket int,
	bat *batch.Batch,
	scratchNeed uint64,
	analyzer process.Analyzer,
) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	if ctr.spillBatchAllocation != nil {
		return ctr.appendAccountedSpillRecord(
			proc,
			file,
			bucket,
			bat,
			analyzer,
		)
	}
	grow, err := spillMarshalGrowBytes(bat)
	if err != nil {
		return err
	}
	var oldScratchSize uint64
	var grewScratch bool
	if old := uint64(ctr.spillWriteBuf.Cap()); ctr.hashmapBuilder.budget != nil && old > 0 && old < grow {
		peak, addErr := spillCheckedAdd(scratchNeed, old)
		if addErr != nil {
			return addErr
		}
		oldScratchSize, grewScratch, err = ctr.growSpillScratchTransient(peak, analyzer)
		if err != nil {
			return err
		}
	}
	cnt, err := marshalSpillRecord(bat, &ctr.spillWriteBuf)
	if restoreErr := ctr.restoreSpillScratchTransient(oldScratchSize, grewScratch); restoreErr != nil {
		return restoreErr
	}
	if err != nil {
		return err
	}
	payload := ctr.spillWriteBuf.Bytes()
	buf := &ctr.spillBucketWriteBufs[bucket]
	if buf.Len() > 0 && buf.Len()+len(payload) > spillWriteCoalesceSize {
		if err := ctr.flushPendingSpillBucket(proc, file, bucket, analyzer); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
	}
	if buf.Len() == 0 {
		if !ctr.ensureSpillCoalesceCapacity(buf, analyzer) {
			return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
		}
		if buf.Cap() < spillWriteCoalesceSize {
			*buf = *bytes.NewBuffer(make([]byte, 0, spillWriteCoalesceSize))
		}
	}
	_, _ = buf.Write(payload)
	ctr.spillBucketWriteRows[bucket] += cnt
	if buf.Len() >= spillWriteCoalesceSize {
		return ctr.flushPendingSpillBucket(proc, file, bucket, analyzer)
	}
	return nil
}

func (ctr *container) appendAccountedSpillRecord(
	proc *process.Process,
	file *os.File,
	bucket int,
	bat *batch.Batch,
	analyzer process.Analyzer,
) error {
	if ctr.spillAllocationMP != proc.Mp() ||
		ctr.hashmapBuilder.mapAllocationAccount == nil {
		return mpool.ErrAllocationAccountInvalid
	}
	if ctr.spillAccountedWrite == nil {
		var err error
		ctr.spillAccountedWrite, err = mpool.NewAccountedBuffer(
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildAllocationOwner,
			HashBuildSpillAllocationSiteMarshalBuffer,
		)
		if err != nil {
			return err
		}
	}
	cnt, err := marshalSpillRecordAccounted(bat, ctr.spillAccountedWrite)
	if err != nil {
		return err
	}
	payload := ctr.spillAccountedWrite.Bytes()
	if ctr.spillCoalesceDisabled {
		return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
	}
	buffer := ctr.spillAccountedBuckets[bucket]
	if buffer != nil && buffer.Len() > 0 &&
		buffer.Len()+len(payload) > spillWriteCoalesceSize {
		if err := ctr.flushPendingSpillBucket(
			proc,
			file,
			bucket,
			analyzer,
		); err != nil {
			return err
		}
	}
	if len(payload) > spillWriteCoalesceSize {
		return ctr.writeSpillPayload(proc, file, payload, cnt, analyzer)
	}
	if buffer == nil {
		buffer, err = mpool.NewAccountedBuffer(
			proc.Mp(),
			ctr.hashmapBuilder.mapAllocationAccount,
			HashBuildAllocationOwner,
			HashBuildSpillAllocationSiteCoalesceBuffer,
		)
		if err != nil {
			return err
		}
		ctr.spillAccountedBuckets[bucket] = buffer
	}
	if buffer.Len() == 0 && buffer.Cap() < spillWriteCoalesceSize {
		if err := buffer.EnsureCapacity(spillWriteCoalesceSize); err != nil {
			if mpool.IsRetryableAllocationCapacity(err) {
				return ctr.writeSpillPayload(
					proc,
					file,
					payload,
					cnt,
					analyzer,
				)
			}
			return err
		}
	}
	if _, err := buffer.Write(payload); err != nil {
		return err
	}
	ctr.spillBucketWriteRows[bucket] += cnt
	if buffer.Len() >= spillWriteCoalesceSize {
		return ctr.flushPendingSpillBucket(
			proc,
			file,
			bucket,
			analyzer,
		)
	}
	return nil
}

func (ctr *container) ensureSpillCoalesceCapacity(buf *bytes.Buffer, analyzer process.Analyzer) bool {
	if buf == nil || buf.Cap() >= spillWriteCoalesceSize {
		return true
	}
	if ctr.hashmapBuilder.budget == nil || ctr.spillScratchReservation == nil {
		return ctr.hashmapBuilder.budget == nil
	}
	additional := uint64(spillWriteCoalesceSize - buf.Cap())
	if err := ctr.spillScratchReservation.Grow(additional); err != nil {
		analyzer.GetOpStats().AddExtraStat("HashBuildCoalesceGrowRejects", 1)
		return false
	}
	analyzer.GetOpStats().AddExtraStat("HashBuildCoalesceGrowCount", 1)
	analyzer.GetOpStats().AddExtraStat("HashBuildCoalesceGrowBytes", hashBuildStatInt64(additional))
	analyzer.GetOpStats().SetMaxExtraStat(
		"HashBuildSpillScratchPeakBytes",
		hashBuildStatInt64(ctr.spillScratchReservation.Size()),
	)
	return true
}

func (ctr *container) flushPendingSpillBucket(
	proc *process.Process,
	file *os.File,
	bucket int,
	analyzer process.Analyzer,
) error {
	if bucket < 0 || bucket >= spillNumBuckets {
		return process.ErrHashBuildBudgetInvalid
	}
	rows := ctr.spillBucketWriteRows[bucket]
	var payload []byte
	if ctr.spillBatchAllocation != nil {
		buffer := ctr.spillAccountedBuckets[bucket]
		if buffer == nil || buffer.Len() == 0 {
			return nil
		}
		payload = buffer.Bytes()
	} else {
		buf := &ctr.spillBucketWriteBufs[bucket]
		if buf.Len() == 0 {
			return nil
		}
		payload = buf.Bytes()
	}
	err := ctr.writeSpillPayload(proc, file, payload, rows, analyzer)
	// Clear even on a failed/partial write. A caller's enclosing failure path
	// owns cleanup, and retrying the same bytes could duplicate records.
	if ctr.spillBatchAllocation != nil {
		ctr.spillAccountedBuckets[bucket].Reset()
	} else {
		ctr.spillBucketWriteBufs[bucket].Reset()
	}
	ctr.spillBucketWriteRows[bucket] = 0
	return err
}

// flushSpillBuffers writes all pending bucket records before files are rewound
// or handed to JoinMap. Cancellation is checked between physical writes. After
// the first error, the remaining buffers are discarded rather than written, so
// every buffer still reaches a terminal state without doing doomed I/O.
func (ctr *container) flushSpillBuffers(proc *process.Process, files []*os.File, analyzer process.Analyzer) error {
	var firstErr error
	for bucket := 0; bucket < spillNumBuckets; bucket++ {
		pending := ctr.spillBucketWriteBufs[bucket].Len()
		if ctr.spillBatchAllocation != nil &&
			ctr.spillAccountedBuckets[bucket] != nil {
			pending = ctr.spillAccountedBuckets[bucket].Len()
		}
		if pending == 0 {
			continue
		}
		if firstErr != nil {
			ctr.spillBucketWriteBufs[bucket].Reset()
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		if err := checkHashBuildCanceled(proc); err != nil {
			firstErr = err
			ctr.spillBucketWriteBufs[bucket].Reset()
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		var file *os.File
		if bucket < len(files) {
			file = files[bucket]
		}
		if file == nil {
			firstErr = process.ErrHashBuildBudgetInvalid
			ctr.spillBucketWriteBufs[bucket].Reset()
			if ctr.spillAccountedBuckets[bucket] != nil {
				ctr.spillAccountedBuckets[bucket].Reset()
			}
			ctr.spillBucketWriteRows[bucket] = 0
			continue
		}
		if err := ctr.flushPendingSpillBucket(proc, file, bucket, analyzer); err != nil {
			firstErr = err
		}
	}
	return firstErr
}

// initSpillExprExecs initializes or validates spill expression executors.
// Returns the executors slice ready for use. Called once when entering spill mode.
func (ctr *container) initSpillExprExecs(proc *process.Process, conditions []*plan.Expr) ([]colexec.ExpressionExecutor, error) {
	for _, condition := range conditions {
		if condition == nil {
			return nil, &process.HashBuildBudgetError{Kind: process.HashBuildBudgetErrorInvalid, Message: "nil shuffle spill key"}
		}
	}
	wantAccounted := ctr.hashmapBuilder.expressionAllocation != nil &&
		expressionSetAllocationClosed(conditions)
	if len(ctr.spillExprExecs) != len(conditions) ||
		ctr.spillExprAccounted != wantAccounted {
		var (
			execs []colexec.ExpressionExecutor
			lease *ExpressionMemoryLease
			err   error
		)
		if wantAccounted {
			execs, err = NewAllocationAccountedExpressionExecutors(
				proc,
				conditions,
				ctr.hashmapBuilder.expressionAllocation,
			)
		} else {
			execs, lease, err = NewBudgetedExpressionExecutors(
				proc,
				ctr.hashmapBuilder.budget,
				conditions,
				false,
			)
		}
		if err != nil {
			return nil, err
		}
		ctr.freeSpillExprExecs()
		ctr.spillExprExecs = execs
		ctr.spillExprLease = lease
		ctr.spillExprAccounted = wantAccounted
	} else if !ctr.spillExprAccounted && ctr.spillExprLease == nil {
		lease, err := NewExpressionMemoryLease(
			ctr.hashmapBuilder.budget,
			conditions,
			ctr.spillExprExecs,
			false,
		)
		if err != nil {
			return nil, err
		}
		ctr.spillExprLease = lease
	}
	return ctr.spillExprExecs, nil
}

// freeSpillExprExecs frees all cached spill expression executors.
func (ctr *container) freeSpillExprExecs() {
	for _, exec := range ctr.spillExprExecs {
		if exec != nil {
			exec.Free()
		}
	}
	ctr.spillExprExecs = nil
	ctr.spillExprAccounted = false
	if ctr.spillExprLease != nil {
		ctr.spillExprLease.Release()
		ctr.spillExprLease = nil
	}
}

func (ctr *container) memUsed() int64 {
	sz := ctr.hashmapBuilder.GetSize() + ctr.hashmapBuilder.Batches.MemSize
	batches := ctr.hashmapBuilder.Batches.Buf
	// MemSize tracks completed fixed-size batches. Include the one permitted
	// partial tail so a threshold decision cannot lag by almost one batch. If
	// MemSize is zero (including directly assembled test state), sum all batches.
	if ctr.hashmapBuilder.Batches.MemSize == 0 {
		for _, bat := range ctr.hashmapBuilder.Batches.Buf {
			sz += int64(bat.Size())
		}
	} else if len(batches) > 0 {
		tail := batches[len(batches)-1]
		if tail != nil && tail.RowCount() != colexec.DefaultBatchSize {
			sz += int64(tail.Size())
		}
	}
	return sz
}

func (hashBuild *HashBuild) shouldSpillBatches() bool {
	if !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	return colexec.ShouldSpill(ctr.memUsed(), int64(ctr.hashmapBuilder.InputBatchRowCount), ctr.spillThreshold)
}

// shouldSpillBeforeRetain applies the configured threshold to the retained
// state plus the current upstream batch. InputBatchRowCount already includes
// that batch when this is called. Moving the existing decision before the copy
// prevents the threshold-crossing reservation from consuming the scratch
// headroom needed to start spill; it does not size or reserve spill scratch.
func (hashBuild *HashBuild) shouldSpillBeforeRetain(inputBatchSize int64) bool {
	if !hashBuild.IsShuffle || !hashBuild.NeedHashMap {
		return false
	}
	ctr := &hashBuild.ctr
	predicted := ctr.memUsed()
	if inputBatchSize < 0 || predicted > math.MaxInt64-inputBatchSize {
		predicted = math.MaxInt64
	} else {
		predicted += inputBatchSize
	}
	return colexec.ShouldSpill(
		predicted,
		int64(ctr.hashmapBuilder.InputBatchRowCount),
		ctr.spillThreshold,
	)
}

// computeXXHash computes hash values for spill-partitioning using
// column-at-a-time processing for better cache locality.
// Each column is processed in a tight loop over all rows, avoiding
// per-row buffer concatenation and giving sequential vector access.
func computeXXHash(keyVecs []*vector.Vector, hashValues []uint64) {
	if len(keyVecs) == 0 || len(hashValues) == 0 {
		return
	}
	keycodec.ComputeXXHash(keyVecs, hashValues, 0)
}
