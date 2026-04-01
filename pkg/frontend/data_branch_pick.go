// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"bytes"
	"context"
	"errors"
	"math"
	"runtime"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

func handleBranchPick(
	execCtx *ExecCtx,
	ses *Session,
	stmt *tree.DataBranchPick,
) (err error) {
	if stmt.ConflictOpt == nil {
		stmt.ConflictOpt = &tree.ConflictOpt{
			Opt: tree.CONFLICT_FAIL,
		}
	}

	return diffMergeAgency(ses, execCtx, stmt)
}

// pickMergeDiffs is the consumer goroutine for PICK. It receives diff batches
// from the hashDiff producer (which uses ACCEPT internally), filters rows whose
// PK is in the KEYS set, and applies INSERT/DELETE SQL to the destination table.
//
// The user's conflict option is enforced here:
//   - ACCEPT: base DELETE + target INSERT are both applied (source value wins).
//   - SKIP:   base DELETE rows mark keys to skip; corresponding target INSERTs are ignored.
//   - FAIL:   base DELETE for a picked key means conflict → return error.
//
// Within each hashDiff shard, base DELETE batches are emitted before target INSERT
// batches, so the skipSet is populated before the corresponding INSERTs arrive.
func pickMergeDiffs(
	ctx context.Context,
	cancel context.CancelFunc,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	dagInfo branchMetaInfo,
	tblStuff tableStuff,
	retCh chan batchWithKind,
) (err error) {

	var (
		insertCnt int
		deleteCnt int

		insertIntoVals = acquireBuffer(tblStuff.bufPool)
		deleteFromVals = acquireBuffer(tblStuff.bufPool)
		firstErr       error
		tmpValsBuffer  = acquireBuffer(tblStuff.bufPool)
	)

	defer func() {
		releaseBuffer(tblStuff.bufPool, insertIntoVals)
		releaseBuffer(tblStuff.bufPool, deleteFromVals)
		releaseBuffer(tblStuff.bufPool, tmpValsBuffer)
	}()

	defer func() {
		cancel()
	}()

	// PK membership filtering is now handled upstream in buildHashmapForTable
	// via pickKeyHashmap.  All rows arriving through retCh are guaranteed to
	// be in the KEYS set.  The consumer only needs to handle conflict semantics.

	// skipSet tracks picked keys that should be skipped (SKIP conflict mode).
	var skipSet map[string]struct{}
	if stmt.ConflictOpt != nil && stmt.ConflictOpt.Opt == tree.CONFLICT_SKIP {
		skipSet = make(map[string]struct{})
	}

	appender := sqlValuesAppender{
		ctx:             ctx,
		ses:             ses,
		bh:              bh,
		tblStuff:        tblStuff,
		deleteByFullRow: tblStuff.def.pkKind == fakeKind,
		pkInfo:          newPKBatchInfo(ctx, ses, tblStuff),
		deleteCnt:       &deleteCnt,
		deleteBuf:       deleteFromVals,
		insertCnt:       &insertCnt,
		insertBuf:       insertIntoVals,
	}
	if err = initPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err != nil {
		return err
	}
	defer func() {
		if err2 := dropPKTables(ctx, ses, bh, appender.pkInfo, appender.writeFile); err2 != nil && err == nil {
			err = err2
		}
	}()

	for wrapped := range retCh {
		if firstErr != nil || ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		if err = appendPickedBatchRows(
			ctx, ses, tblStuff, wrapped, tmpValsBuffer, appender,
			stmt.ConflictOpt, skipSet,
		); err != nil {
			firstErr = err
			cancel()
			tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
			continue
		}

		tblStuff.retPool.releaseRetBatch(wrapped.batch, false)
	}

	if err = appender.flushAll(); err != nil {
		if firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return firstErr
	}
	return
}

// appendPickedBatchRows applies the user's conflict semantics to each row
// in a diff batch and appends surviving rows to the SQL appender.
//
// PK membership filtering is already done upstream in buildHashmapForTable
// via pickKeyHashmap — every row arriving here is guaranteed to belong to
// the KEYS set.  This function only needs to handle conflict logic.
//
// hashDiff uses ACCEPT internally, so:
//   - base DELETE batches (side=base, kind=DELETE) represent conflict victims
//     (old base rows being replaced by source values).
//   - target INSERT batches (side=target, kind=INSERT) contain both new rows
//     and conflict-winning source rows.
//
// The consumer enforces the user's actual conflict choice:
//   - ACCEPT: apply both DELETE and INSERT (source wins). Same as hashDiff output.
//   - SKIP:   record conflicting keys in skipSet; skip both DELETE and INSERT.
//   - FAIL:   return error when a base DELETE exists for a picked key.
func appendPickedBatchRows(
	ctx context.Context,
	ses *Session,
	tblStuff tableStuff,
	wrapped batchWithKind,
	tmpValsBuffer *bytes.Buffer,
	appender sqlValuesAppender,
	userConflictOpt *tree.ConflictOpt,
	skipSet map[string]struct{},
) (err error) {
	// PICK only cares about two kinds of batches from hashDiff:
	//   1. target INSERT (side=target, kind=INSERT) — source rows to add/replace
	//   2. base DELETE  (side=base, kind=DELETE)   — conflict victims in destination
	//
	// base INSERT batches represent destination-only new rows that hashDiff
	// would keep in a MERGE result.  For PICK these are irrelevant — the
	// rows already exist in the destination.  Applying them would cause a
	// duplicate-key error.
	if wrapped.side == diffSideBase && wrapped.kind != diffDelete {
		return nil
	}

	row := make([]any, len(tblStuff.def.colNames))

	for rowIdx := range wrapped.batch.RowCount() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Extract PK for conflict handling (FAIL error message, SKIP set).
		pkKey, err2 := extractPKAsString(ses, tblStuff, wrapped.batch, rowIdx)
		if err2 != nil {
			return err2
		}

		// Handle conflict semantics for picked keys.
		if wrapped.side == diffSideBase && wrapped.kind == diffDelete {
			// A base DELETE for a picked key means the key exists in both
			// source and destination with different values (a conflict).
			if userConflictOpt != nil {
				switch userConflictOpt.Opt {
				case tree.CONFLICT_FAIL:
					return moerr.NewInternalErrorNoCtxf(
						"conflict: %s %s and %s %s on pk(%v) with different values",
						tblStuff.tarRel.GetTableName(), diffInsert,
						tblStuff.baseRel.GetTableName(), diffInsert,
						pkKey,
					)
				case tree.CONFLICT_SKIP:
					skipSet[pkKey] = struct{}{}
					continue // do not apply the DELETE
				case tree.CONFLICT_ACCEPT:
					// fall through — apply the DELETE (source value wins)
				}
			}
		}

		// For SKIP mode: skip target INSERTs for conflicting keys.
		if skipSet != nil {
			if _, skipped := skipSet[pkKey]; skipped {
				continue
			}
		}

		// Row is in KEYS set and passed conflict checks — extract all visible column values.
		for _, colIdx := range tblStuff.def.visibleIdxes {
			vec := wrapped.batch.Vecs[colIdx]
			if rowIdx >= vec.Length() {
				return moerr.NewInternalErrorNoCtxf(
					"data branch pick batch shape mismatch: row=%d batchRows=%d col=%d vecLen=%d",
					rowIdx, wrapped.batch.RowCount(), colIdx, vec.Length(),
				)
			}
			if vec.GetNulls().Contains(uint64(rowIdx)) {
				row[colIdx] = nil
				continue
			}

			switch vec.GetType().Oid {
			case types.T_datetime, types.T_timestamp, types.T_decimal64,
				types.T_decimal128, types.T_time:
				row[colIdx] = types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)
			default:
				if err = extractRowFromVector(
					ctx, ses, vec, colIdx, row, rowIdx, false,
				); err != nil {
					return
				}
			}
		}

		tmpValsBuffer.Reset()
		if wrapped.kind == diffDelete {
			if appender.deleteByFullRow {
				if err = writeDeleteRowSQLFull(ctx, ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else if appender.pkInfo != nil {
				if err = writeDeleteRowValuesAsTuple(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			} else {
				if err = writeDeleteRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
					return
				}
			}
		} else {
			if err = writeInsertRowValues(ses, tblStuff, row, tmpValsBuffer); err != nil {
				return
			}
		}

		if tmpValsBuffer.Len() == 0 {
			continue
		}

		if err = appender.appendRow(wrapped.kind, tmpValsBuffer.Bytes()); err != nil {
			return
		}
	}

	return nil
}

// extractPKAsString serializes the PK column(s) of a batch row into a
// canonical string for map-based key matching.
func extractPKAsString(
	ses *Session,
	tblStuff tableStuff,
	bat *batch.Batch,
	rowIdx int,
) (string, error) {
	var buf bytes.Buffer

	for i, colIdx := range tblStuff.def.pkColIdxes {
		vec := bat.Vecs[colIdx]
		if vec.GetNulls().Contains(uint64(rowIdx)) {
			buf.WriteString("NULL")
		} else {
			val := extractPKVal(vec, rowIdx)
			if err := formatValIntoString(ses, val, tblStuff.def.colTypes[colIdx], &buf); err != nil {
				return "", err
			}
		}
		if i < len(tblStuff.def.pkColIdxes)-1 {
			buf.WriteByte(',')
		}
	}

	return buf.String(), nil
}

// extractPKVal extracts the Go value from a vector at the given row index.
func extractPKVal(vec *vector.Vector, rowIdx int) any {
	switch vec.GetType().Oid {
	case types.T_datetime, types.T_timestamp, types.T_decimal64,
		types.T_decimal128, types.T_time:
		return types.DecodeValue(vec.GetRawBytesAt(rowIdx), vec.GetType().Oid)
	case types.T_bool:
		return vector.GetFixedAtNoTypeCheck[bool](vec, rowIdx)
	case types.T_bit:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, rowIdx)
	case types.T_int8:
		return vector.GetFixedAtNoTypeCheck[int8](vec, rowIdx)
	case types.T_int16:
		return vector.GetFixedAtNoTypeCheck[int16](vec, rowIdx)
	case types.T_int32:
		return vector.GetFixedAtNoTypeCheck[int32](vec, rowIdx)
	case types.T_int64:
		return vector.GetFixedAtNoTypeCheck[int64](vec, rowIdx)
	case types.T_uint8:
		return vector.GetFixedAtNoTypeCheck[uint8](vec, rowIdx)
	case types.T_uint16:
		return vector.GetFixedAtNoTypeCheck[uint16](vec, rowIdx)
	case types.T_uint32:
		return vector.GetFixedAtNoTypeCheck[uint32](vec, rowIdx)
	case types.T_uint64:
		return vector.GetFixedAtNoTypeCheck[uint64](vec, rowIdx)
	case types.T_float32:
		return vector.GetFixedAtNoTypeCheck[float32](vec, rowIdx)
	case types.T_float64:
		return vector.GetFixedAtNoTypeCheck[float64](vec, rowIdx)
	case types.T_date:
		return vector.GetFixedAtNoTypeCheck[types.Date](vec, rowIdx)
	case types.T_uuid:
		return vector.GetFixedAtNoTypeCheck[types.Uuid](vec, rowIdx)
	case types.T_enum:
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, rowIdx)
	default:
		return vec.GetBytesAt(rowIdx)
	}
}

// materializePickKeysAndFilter is the unified entry point for PICK key
// materialization.  It produces two outputs:
//
//   - pickKeyHashmap: BranchHashmap storing all PK values for O(1) batch-level
//     filtering inside buildHashmapForTable.  Memory-controlled via the
//     allocator + spill-to-disk mechanism of BranchHashmap.
//   - pkFilter: engine.PKFilter containing multi-segment ZoneMap ranges for
//     object/block pruning inside logtailreplay/change_handle.go.  Nil when
//     segments cannot be built (composite PK, exotic types, etc).
//
// Both literal KEYS and subquery KEYS converge on the same pipeline:
//
//  1. Build a sorted typed Vec of PK values
//  2. PutByVectors into pickKeyHashmap
//  3. Iterate sorted Vec through segmentBuilder → ZoneMap segments
//
// For subquery KEYS, the subquery is stored in a temp table and read back
// via ORDER BY for globally sorted streaming.
//
// For composite PK (__mo_cpkey_col), only the hashmap is built — segment
// construction is skipped because the opaque composite encoding cannot
// be meaningfully range-pruned.
func materializePickKeysAndFilter(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
) (pickKeyHashmap databranchutils.BranchHashmap, pkFilter *engine.PKFilter, err error) {
	if stmt.Keys == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx("DATA BRANCH PICK requires a KEYS clause")
	}

	// Create the pick key hashmap with the shared allocator.
	pickKeyHashmap, err = databranchutils.NewBranchHashmap(
		databranchutils.WithBranchHashmapAllocator(tblStuff.hashmapAllocator),
	)
	if err != nil {
		return nil, nil, err
	}

	// Segment construction only works for single-column, non-composite PK
	// with types we can put into a vector and decode back to raw bytes.
	canBuildSegments := tblStuff.def.pkKind == normalKind

	switch stmt.Keys.Type {
	case tree.PickKeysValues:
		pkFilter, err = materializeValuesUnified(ses, stmt, tblStuff, canBuildSegments, pickKeyHashmap)
	case tree.PickKeysSubquery:
		pkFilter, err = materializeSubqueryUnified(ctx, ses, bh, stmt, tblStuff, canBuildSegments, pickKeyHashmap)
	default:
		err = moerr.NewInvalidInputNoCtxf("unsupported KEYS type: %d", stmt.Keys.Type)
	}
	if err != nil {
		pickKeyHashmap.Close()
		return nil, nil, err
	}
	return pickKeyHashmap, pkFilter, nil
}

// materializeValuesUnified processes literal KEYS (e.g. KEYS (1, 2, 3)):
//
//  1. Parse AST expressions → typed Vec (via appendExprToVec)
//  2. Sort Vec
//  3. PutByVectors into pickKeyHashmap
//  4. Iterate sorted Vec → segmentBuilder → ZoneMap segments
//
// If any expression cannot be converted to a typed value, segment
// construction is abandoned (graceful degradation — hashmap still works).
func materializeValuesUnified(
	ses *Session,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
	canBuildSegments bool,
	pickKeyHashmap databranchutils.BranchHashmap,
) (pkFilter *engine.PKFilter, err error) {
	mp := ses.proc.Mp()
	pkType := tblStuff.def.colTypes[tblStuff.def.pkColIdx]

	// Build a typed Vec from AST expressions.
	vec := vector.NewVec(pkType)
	for _, expr := range stmt.Keys.KeyExprs {
		if appendErr := appendExprToVec(vec, expr, pkType, mp); appendErr != nil {
			vec.Free(mp)
			return nil, appendErr
		}
	}

	if vec.Length() == 0 {
		vec.Free(mp)
		return nil, nil
	}

	// Sort for segment construction (and deterministic hashmap ordering).
	vec.InplaceSort()

	// Put all PK values into the hashmap.  keyCols=[0] because the Vec has
	// exactly one column (the PK).
	if err = pickKeyHashmap.PutByVectors([]*vector.Vector{vec}, []int{0}); err != nil {
		vec.Free(mp)
		return nil, err
	}

	// Build ZoneMap segments from the sorted Vec.
	if canBuildSegments {
		pkFilter = buildPKFilterFromVec(vec, pkType, tblStuff.def.pkColIdx)
	}
	vec.Free(mp)
	return pkFilter, nil
}

// materializeSubqueryUnified handles KEYS (SELECT ...):
//
//  1. Execute the user's subquery into a temp table
//  2. Stream "SELECT pk_col FROM temp ORDER BY pk_col" via runSql streaming
//  3. For each streamed batch: PutByVectors into pickKeyHashmap + segmentBuilder
//  4. Finalize segments, drop temp table
//
// The ORDER BY ensures globally sorted streaming, which the segmentBuilder
// requires.  Streaming avoids buffering the entire result set in memory.
func materializeSubqueryUnified(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
	stmt *tree.DataBranchPick,
	tblStuff tableStuff,
	canBuildSegments bool,
	pickKeyHashmap databranchutils.BranchHashmap,
) (pkFilter *engine.PKFilter, err error) {
	if stmt.Keys.Select == nil {
		return nil, moerr.NewInvalidInputNoCtx("KEYS subquery is nil")
	}

	pkType := tblStuff.def.colTypes[tblStuff.def.pkColIdx]
	pkColName := tblStuff.def.colNames[tblStuff.def.pkColIdx]

	// Step 1: Store subquery results in a temp table.
	fmtCtx := tree.NewFmtCtx(dialect.MYSQL)
	stmt.Keys.Select.Format(fmtCtx)
	subquerySQL := fmtCtx.String()

	const tempTable = "__mo_pick_keys"
	createSQL := "CREATE TEMPORARY TABLE " + tempTable + " AS (" + subquerySQL + ")"
	if err = bh.Exec(ctx, createSQL); err != nil {
		return nil, errors.New("failed to create temp table for KEYS subquery: " + err.Error())
	}
	defer func() {
		// Best-effort cleanup.
		_ = bh.Exec(ctx, "DROP TEMPORARY TABLE IF EXISTS "+tempTable)
	}()

	// Step 2: Stream sorted results.
	orderSQL := "SELECT `" + pkColName + "` FROM " + tempTable + " ORDER BY `" + pkColName + "`"

	var sb *segmentBuilder
	if canBuildSegments {
		sb = newSegmentBuilder(pkType)
	}

	streamChan := make(chan executor.Result, runtime.NumCPU())
	errChan := make(chan error, 1)

	// Launch the streaming SQL in a goroutine.
	go func() {
		defer close(streamChan)
		defer close(errChan)
		if _, err2 := runSql(ctx, ses, bh, orderSQL, streamChan, errChan); err2 != nil {
			select {
			case errChan <- err2:
			default:
			}
		}
	}()

	// Consume streamed batches.
	streamOpen, errOpen := true, true
	for streamOpen || errOpen {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return nil, err

		case e, ok := <-errChan:
			if !ok {
				errOpen = false
				continue
			}
			return nil, errors.New("KEYS subquery streaming error: " + e.Error())

		case sqlRet, ok := <-streamChan:
			if !ok {
				streamOpen = false
				continue
			}
			for _, bat := range sqlRet.Batches {
				if bat == nil || bat.VectorCount() == 0 || bat.RowCount() == 0 {
					continue
				}
				pkVec := bat.Vecs[0]

				// Put into hashmap.
				if putErr := pickKeyHashmap.PutByVectors(
					[]*vector.Vector{pkVec}, []int{0},
				); putErr != nil {
					sqlRet.Close()
					return nil, putErr
				}

				// Feed segmentBuilder (data arrives sorted from ORDER BY).
				if sb != nil {
					for i := 0; i < pkVec.Length(); i++ {
						sb.observe(pkVec.GetRawBytesAt(i))
					}
				}
			}
			sqlRet.Close()
		}
	}

	// Finalize ZoneMap segments.
	if sb != nil {
		segments := sb.finalize()
		if len(segments) > 0 {
			pkFilter = &engine.PKFilter{
				Segments:      segments,
				PrimarySeqnum: tblStuff.def.pkColIdx,
			}
		}
	}

	return pkFilter, nil
}

// buildPKFilterFromVec iterates a pre-sorted vector through the segmentBuilder
// and returns a PKFilter.  Returns nil if the vector produces no segments.
// Does NOT free the vector — the caller is responsible.
func buildPKFilterFromVec(vec *vector.Vector, pkType types.Type, pkColIdx int) *engine.PKFilter {
	if vec == nil || vec.Length() == 0 {
		return nil
	}
	segments := buildSegmentsFromSortedVec(vec, pkType)
	if len(segments) == 0 {
		return nil
	}
	return &engine.PKFilter{
		Segments:      segments,
		PrimarySeqnum: pkColIdx,
	}
}

func unescapeMySQLString(s string) string {
	if strings.IndexByte(s, '\\') < 0 && strings.Index(s, "''") < 0 {
		return s
	}
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case '0':
				buf = append(buf, 0)
			case 'b':
				buf = append(buf, '\b')
			case 'n':
				buf = append(buf, '\n')
			case 'r':
				buf = append(buf, '\r')
			case 't':
				buf = append(buf, '\t')
			case 'Z':
				buf = append(buf, 0x1a)
			default:
				buf = append(buf, s[i])
			}
			continue
		}
		if s[i] == '\'' && i+1 < len(s) && s[i+1] == '\'' {
			buf = append(buf, '\'')
			i++
			continue
		}
		buf = append(buf, s[i])
	}
	return string(buf)
}

// ---------------------------------------------------------------------------
// Segment builder: adaptive gap-based streaming segmentation
// ---------------------------------------------------------------------------
//
// The segment builder groups a stream of SORTED PK raw bytes into non-
// overlapping ZoneMap segments.  It is fed one PK at a time (via observe)
// and produces a compact [][]byte of 64-byte ZoneMap ranges (via finalize).
//
// Algorithm:
//
//  1. For numeric PKs — adaptive gap-based splitting.
//     Track a running segment [curMin, curMax] and a running gap average.
//     When the distance from curMax to the next PK exceeds
//     GAP_RATIO × (average gap so far), start a new segment.
//     This naturally clusters nearby values while splitting on
//     sparse regions.
//
//  2. For string PKs — count-based splitting.
//     Distance metrics for variable-length strings are unreliable, so
//     we simply cap each segment at MAX_SEGMENT_SIZE values.
//
//  3. Hard cap: every segment is capped at MAX_SEGMENT_SIZE values
//     regardless of PK type, preventing a single enormous ZoneMap that
//     spans the entire key space and provides no pruning benefit.
//
// Constants:
//   - maxSegmentSize (8192): hard cap on values per segment.
//   - minGapSample   (4):    minimum gap observations before adaptive
//     splitting activates (avoids splitting on noise).
//   - gapRatio       (5.0):  gap > gapRatio × avgGap triggers a split.

const (
	maxSegmentSize = 8192
	minGapSample   = 4
	gapRatio       = 5.0
)

type segmentBuilder struct {
	pkType types.Type

	// Current segment state.
	curMin   []byte  // min PK raw bytes (owned copy)
	curMax   []byte  // max PK raw bytes (owned copy)
	curCount int     // values in current segment

	// Gap statistics (numeric PKs only).
	gapSum    float64
	gapCount  int
	isNumeric bool

	// Completed segments.
	segments [][]byte
}

func newSegmentBuilder(pkType types.Type) *segmentBuilder {
	return &segmentBuilder{
		pkType:    pkType,
		isNumeric: isNumericType(pkType.Oid),
	}
}

// observe ingests one sorted PK value (raw bytes from vec.GetRawBytesAt).
// MUST be called in ascending sort order.
func (sb *segmentBuilder) observe(pkBytes []byte) {
	// Copy — the source vector's memory is not guaranteed stable after sort.
	pk := make([]byte, len(pkBytes))
	copy(pk, pkBytes)

	if sb.curMin == nil {
		// First value: start a new segment.
		sb.curMin = pk
		sb.curMax = pk
		sb.curCount = 1
		return
	}

	shouldSplit := false

	if sb.curCount >= maxSegmentSize {
		// Hard cap — prevents one segment spanning the entire key space.
		shouldSplit = true
	} else if sb.isNumeric && sb.gapCount >= minGapSample {
		// Adaptive gap-based split: the distance from curMax to this PK
		// is compared against GAP_RATIO × the running average gap.
		// A large gap indicates a sparse region between clusters.
		gap := numericGap(sb.curMax, pk, sb.pkType)
		avgGap := sb.gapSum / float64(sb.gapCount)
		if gap > gapRatio*avgGap {
			shouldSplit = true
		}
	}

	if shouldSplit {
		sb.flushSegment()
		sb.curMin = pk
		sb.curMax = pk
		sb.curCount = 1
		return
	}

	// Extend the current segment.
	if sb.isNumeric {
		gap := numericGap(sb.curMax, pk, sb.pkType)
		sb.gapSum += gap
		sb.gapCount++
	}
	sb.curMax = pk
	sb.curCount++
}

// flushSegment writes the current [curMin, curMax] range as a 64-byte ZM
// and appends it to the segments list.
func (sb *segmentBuilder) flushSegment() {
	if sb.curMin == nil {
		return
	}
	zm := index.NewZM(sb.pkType.Oid, sb.pkType.Scale)
	index.UpdateZM(zm, sb.curMin)
	index.UpdateZM(zm, sb.curMax)
	sb.segments = append(sb.segments, []byte(zm))
}

// finalize flushes any pending segment and returns the complete list.
func (sb *segmentBuilder) finalize() [][]byte {
	sb.flushSegment()
	return sb.segments
}

// buildSegmentsFromSortedVec iterates a pre-sorted vector of PK values and
// produces ZoneMap segments via the segmentBuilder.
func buildSegmentsFromSortedVec(vec *vector.Vector, pkType types.Type) [][]byte {
	n := vec.Length()
	if n == 0 {
		return nil
	}
	sb := newSegmentBuilder(pkType)
	for i := 0; i < n; i++ {
		sb.observe(vec.GetRawBytesAt(i))
	}
	return sb.finalize()
}

// numericGap computes |b − a| for two PK values encoded as raw bytes,
// returned as float64 for averaging in the gap heuristic.
func numericGap(a, b []byte, pkType types.Type) float64 {
	switch pkType.Oid {
	case types.T_int8:
		return math.Abs(float64(types.DecodeInt8(b)) - float64(types.DecodeInt8(a)))
	case types.T_int16:
		return math.Abs(float64(types.DecodeInt16(b)) - float64(types.DecodeInt16(a)))
	case types.T_int32:
		return math.Abs(float64(types.DecodeInt32(b)) - float64(types.DecodeInt32(a)))
	case types.T_int64:
		return math.Abs(float64(types.DecodeInt64(b)) - float64(types.DecodeInt64(a)))
	case types.T_uint8:
		va, vb := types.DecodeUint8(a), types.DecodeUint8(b)
		if vb >= va {
			return float64(vb - va)
		}
		return float64(va - vb)
	case types.T_uint16:
		va, vb := types.DecodeUint16(a), types.DecodeUint16(b)
		if vb >= va {
			return float64(vb - va)
		}
		return float64(va - vb)
	case types.T_uint32:
		va, vb := types.DecodeUint32(a), types.DecodeUint32(b)
		if vb >= va {
			return float64(vb - va)
		}
		return float64(va - vb)
	case types.T_uint64:
		va, vb := types.DecodeUint64(a), types.DecodeUint64(b)
		if vb >= va {
			return float64(vb - va)
		}
		return float64(va - vb)
	case types.T_float32:
		return math.Abs(float64(types.DecodeFloat32(b)) - float64(types.DecodeFloat32(a)))
	case types.T_float64:
		return math.Abs(types.DecodeFloat64(b) - types.DecodeFloat64(a))
	default:
		return 0
	}
}

func isNumericType(oid types.T) bool {
	switch oid {
	case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64:
		return true
	default:
		return false
	}
}

// appendExprToVec appends a literal AST expression value to a typed vector.
func appendExprToVec(vec *vector.Vector, expr tree.Expr, pkType types.Type, mp *mpool.MPool) error {
	switch e := expr.(type) {
	case *tree.NumVal:
		return appendNumValToVec(vec, e, pkType, mp)
	case *tree.UnaryExpr:
		num, ok := e.Expr.(*tree.NumVal)
		if !ok {
			return moerr.NewInvalidInputNoCtxf("unsupported unary expression type for PK filter: %T", e.Expr)
		}
		s := num.String()
		switch e.Op {
		case tree.UNARY_MINUS:
			s = "-" + s
		case tree.UNARY_PLUS:
			s = "+" + s
		default:
			return moerr.NewInvalidInputNoCtxf("unsupported unary operator for PK filter: %v", e.Op)
		}
		return appendNumericStringToVec(vec, s, pkType, mp)
	case *tree.StrVal:
		return vector.AppendBytes(vec, []byte(unescapeMySQLString(e.String())), false, mp)
	default:
		// For complex expressions, skip ZoneMap pruning in CollectChanges.
		return moerr.NewInvalidInputNoCtxf("unsupported expression type for PK filter: %T", expr)
	}
}

// appendNumValToVec converts a numeric literal to the correct typed value
// and appends it to the vector.
func appendNumValToVec(vec *vector.Vector, val *tree.NumVal, pkType types.Type, mp *mpool.MPool) error {
	return appendNumericStringToVec(vec, val.String(), pkType, mp)
}

func appendNumericStringToVec(vec *vector.Vector, s string, pkType types.Type, mp *mpool.MPool) error {
	switch pkType.Oid {
	case types.T_int8:
		v, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int8(v), false, mp)
	case types.T_int16:
		v, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int16(v), false, mp)
	case types.T_int32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, int32(v), false, mp)
	case types.T_int64:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_uint8:
		v, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint8(v), false, mp)
	case types.T_uint16:
		v, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint16(v), false, mp)
	case types.T_uint32:
		v, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, uint32(v), false, mp)
	case types.T_uint64:
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_float32:
		v, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, float32(v), false, mp)
	case types.T_float64:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		return vector.AppendFixed(vec, v, false, mp)
	case types.T_varchar, types.T_char, types.T_text, types.T_blob:
		return vector.AppendBytes(vec, []byte(s), false, mp)
	default:
		// For other types (decimal, date, etc.), skip ZoneMap pruning in CollectChanges.
		return moerr.NewInvalidInputNoCtxf("unsupported PK type for engine filter: %s", pkType.Oid.String())
	}
}

// freePKFilter is a no-op retained for call-site compatibility.
// The new PKFilter.Segments are plain [][]byte slices that require no
// explicit deallocation — they are garbage-collected normally.
func freePKFilter(_ *engine.PKFilter, _ *mpool.MPool) {}
