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

package group

import (
	"bytes"
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	prepareParamKindTrailerMagic0 = byte('P')
	prepareParamKindTrailerMagic1 = byte('P')
	prepareParamKindTrailerMagic2 = byte('K')
	// Version 1 is the original one-byte aggregate summary. Version 2 keeps
	// that representation for uniform states and adds an explicit row form for
	// winner provenance across grouped/partial state boundaries. Readers accept
	// both; writers select v2 only when exact rows are present.
	prepareParamKindTrailerVersion     = byte(1)
	prepareParamKindTrailerRowsVersion = byte(2)
	prepareParamKindTrailerRowsMarker  = byte(0x80)
	prepareParamKindTrailerMaxRows     = int32(1 << 24)
)

type prepareParamKindSummary struct {
	kind vector.PrepareParamKind
	seen bool
}

func prepareParamKindWireV1Enabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	// Deployment raises the shared protocol only after every participating CN
	// can emit and consume the trailer, and lowers it before rollback.
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion11
}

func hasPrepareParamKindPreservingAgg(aggs []aggexec.AggFuncExecExpression) bool {
	for i := range aggs {
		if aggs[i].PreservesFirstArgPrepareParamKind() {
			return true
		}
	}
	return false
}

// writePrepareParamKindTrailer appends an optional, self-identifying extension
// after the legacy aggregate payload. Keeping the legacy prefix byte-for-byte
// intact lets old readers consume aggregate state and ignore this trailer.
func writePrepareParamKindTrailer(
	ctx context.Context,
	buf *bytes.Buffer,
	aggs []aggexec.AggFuncExecExpression,
	states *aggexec.PrepareParamKindStates,
	rows [][]vector.PrepareParamKind,
	summaries []prepareParamKindSummary,
) error {
	rowsVersion := false
	for i := range rows {
		if len(rows[i]) != 0 {
			rowsVersion = true
			break
		}
	}
	version := prepareParamKindTrailerVersion
	if rowsVersion {
		version = prepareParamKindTrailerRowsVersion
	}
	buf.WriteByte(prepareParamKindTrailerMagic0)
	buf.WriteByte(prepareParamKindTrailerMagic1)
	buf.WriteByte(prepareParamKindTrailerMagic2)
	buf.WriteByte(version)
	nAggs := int32(len(aggs))
	buf.Write(types.EncodeInt32(&nAggs))
	for i := range aggs {
		kind, seen := states.GetState(i)
		// Validate the execution-wide compatibility state even when a
		// preserving aggregate supplies a more precise per-chunk summary
		// below.  This keeps malformed internal state from being silently
		// hidden by the exact-row fast path.
		if seen && kind > vector.PrepareParamBoolean {
			return moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter kind %d", kind)
		}
		if rowsVersion && i < len(rows) && len(rows[i]) != 0 {
			if int64(len(rows[i])) > int64(prepareParamKindTrailerMaxRows) {
				return moerr.NewInternalErrorf(ctx,
					"aggregate prepared parameter row count %d exceeds limit", len(rows[i]))
			}
			buf.WriteByte(prepareParamKindTrailerRowsMarker)
			rowCount := int32(len(rows[i]))
			buf.Write(types.EncodeInt32(&rowCount))
			for _, kind := range rows[i] {
				if kind > vector.PrepareParamBoolean {
					return moerr.NewInternalErrorf(ctx,
						"invalid aggregate prepared parameter row kind %d", kind)
				}
				buf.WriteByte(byte(kind))
			}
			continue
		}
		// Preserving aggregate accessors provide a summary for the exact
		// chunk/selection being serialized.  Use an unseen summary as well:
		// falling back to the cumulative operator state here can leak a prior
		// chunk's Float/Integer category onto an ordinary winner in this one.
		if i < len(summaries) && aggs[i].PreservesFirstArgPrepareParamKind() {
			kind, seen = summaries[i].kind, summaries[i].seen
		}
		encoded, ok := encodePrepareParamKindState(kind, seen)
		if !ok {
			return moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter kind %d", kind)
		}
		buf.WriteByte(encoded)
	}
	return nil
}

// readPrepareParamKindTrailer consumes the extension after every aggregate has
// read its legacy state. A missing trailer is a valid legacy payload.
func readPrepareParamKindTrailer(
	ctx context.Context,
	reader io.Reader,
	nAggs int32,
	states *aggexec.PrepareParamKindStates,
	expectedRows []int,
) ([][]vector.PrepareParamKind, []prepareParamKindSummary, error) {
	magic0, err := types.ReadByte(reader)
	if err != nil {
		return nil, nil, err
	}
	magic1, err := types.ReadByte(reader)
	if err != nil {
		return nil, nil, err
	}
	magic2, err := types.ReadByte(reader)
	if err != nil {
		return nil, nil, err
	}
	if magic0 != prepareParamKindTrailerMagic0 ||
		magic1 != prepareParamKindTrailerMagic1 ||
		magic2 != prepareParamKindTrailerMagic2 {
		return nil, nil, moerr.NewInternalErrorNoCtx("invalid aggregate prepared parameter trailer")
	}
	version, err := types.ReadByte(reader)
	if err != nil {
		return nil, nil, err
	}
	if version != prepareParamKindTrailerVersion && version != prepareParamKindTrailerRowsVersion {
		return nil, nil, moerr.NewInternalErrorf(ctx,
			"unsupported aggregate prepared parameter trailer version %d", version)
	}
	encodedAggs, err := types.ReadInt32(reader)
	if err != nil {
		return nil, nil, err
	}
	if encodedAggs != nAggs {
		return nil, nil, moerr.NewInternalErrorf(ctx,
			"aggregate prepared parameter count %d does not match %d", encodedAggs, nAggs)
	}
	rows := make([][]vector.PrepareParamKind, nAggs)
	// Keep the incoming record separate from the cumulative state.  The
	// cumulative state is only an execution-wide compatibility summary; using
	// it to restore this partial can relabel a later winning value after a
	// mixed-category partial has already been observed.
	summaries := make([]prepareParamKindSummary, nAggs)
	for i := int32(0); i < nAggs; i++ {
		encoded, err := types.ReadByte(reader)
		if err != nil {
			return nil, nil, err
		}
		if version == prepareParamKindTrailerRowsVersion && encoded == prepareParamKindTrailerRowsMarker {
			rowCount, err := types.ReadInt32(reader)
			if err != nil {
				return nil, nil, err
			}
			if rowCount <= 0 || rowCount > prepareParamKindTrailerMaxRows {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"invalid aggregate prepared parameter row count %d", rowCount)
			}
			if i < int32(len(expectedRows)) && expectedRows[i] >= 0 &&
				rowCount != int32(expectedRows[i]) {
				return nil, nil, moerr.NewInternalErrorf(ctx,
					"aggregate prepared parameter row count %d does not match %d",
					rowCount, expectedRows[i])
			}
			// bytes.Reader exposes the complete remaining partial payload. Check
			// the bound before make so a truncated/corrupt record cannot trigger
			// a data-scaled allocation. Local spill uses an io.BufferedReader and
			// is protected by expectedRows above; its read then remains streaming.
			if remaining, ok := prepareParamKindReaderLen(reader); ok &&
				int64(rowCount) > int64(remaining) {
				return nil, nil, io.ErrUnexpectedEOF
			}
			kinds := make([]vector.PrepareParamKind, int(rowCount))
			for row := range kinds {
				kind, err := types.ReadByte(reader)
				if err != nil {
					return nil, nil, err
				}
				if vector.PrepareParamKind(kind) > vector.PrepareParamBoolean {
					return nil, nil, moerr.NewInternalErrorf(ctx,
						"invalid aggregate prepared parameter row kind %d", kind)
				}
				kinds[row] = vector.PrepareParamKind(kind)
			}
			rows[i] = kinds
			kind, seen := summarizePrepareParamKinds(kinds)
			summaries[i] = prepareParamKindSummary{kind: kind, seen: seen}
			states.ObserveState(int(i), kind, seen)
			continue
		}
		kind, seen, ok := decodePrepareParamKindState(encoded)
		if !ok {
			return nil, nil, moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter state %d", encoded)
		}
		summaries[i] = prepareParamKindSummary{kind: kind, seen: seen}
		states.ObserveState(int(i), kind, seen)
	}
	return rows, summaries, nil
}

func prepareParamKindReaderLen(reader io.Reader) (int, bool) {
	type lenReader interface{ Len() int }
	if r, ok := reader.(lenReader); ok {
		return r.Len(), true
	}
	return 0, false
}

func summarizePrepareParamKinds(kinds []vector.PrepareParamKind) (vector.PrepareParamKind, bool) {
	if len(kinds) == 0 {
		return vector.PrepareParamNone, false
	}
	kind := kinds[0]
	for _, current := range kinds[1:] {
		if current != kind {
			return vector.PrepareParamNone, true
		}
	}
	return kind, true
}

// Zero is reserved for an unobserved aggregate input. Observed kinds are
// shifted by one so the complete state remains one byte per aggregate.
func encodePrepareParamKindState(kind vector.PrepareParamKind, seen bool) (byte, bool) {
	if !seen {
		return 0, true
	}
	if kind > vector.PrepareParamBoolean {
		return 0, false
	}
	return byte(kind) + 1, true
}

func decodePrepareParamKindState(encoded byte) (
	vector.PrepareParamKind,
	bool,
	bool,
) {
	if encoded == 0 {
		return vector.PrepareParamNone, false, true
	}
	kind := vector.PrepareParamKind(encoded - 1)
	if kind > vector.PrepareParamBoolean {
		return vector.PrepareParamNone, false, false
	}
	return kind, true, true
}
