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
	"context"
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
	prepareParamKindTrailerVersion       = byte(1)
	prepareParamKindTrailerRowsVersion   = byte(2)
	prepareParamKindTrailerBinaryVersion = byte(3)
	prepareParamKindTrailerDomainVersion = byte(4)
	prepareParamKindTrailerRowsMarker    = byte(0x80)
	prepareParamKindTrailerTextMarker    = byte(0x40)
	prepareParamKindTrailerMaxRows       = int32(1 << 24)
)

type prepareParamKindSummary struct {
	kind         vector.PrepareParamKind
	seen         bool
	rows         bool
	binaryString bool
	textString   bool
}

func (s *prepareParamKindSummary) observe(kind vector.PrepareParamKind) {
	if !s.seen {
		s.kind, s.seen = kind, true
	} else if s.kind != kind {
		s.kind = vector.PrepareParamNone
	}
}

type prepareParamKindRowsSource struct {
	vec      *vector.Vector
	flags    []uint8
	rows     []int32
	rowCount int
	summary  prepareParamKindSummary
}

type prepareParamKindRowState struct {
	kind   vector.PrepareParamKind
	domain types.RuntimeStringDomain
	seen   bool
	mixed  bool
}

func (s *prepareParamKindRowState) observe(
	kind vector.PrepareParamKind, domain types.RuntimeStringDomain,
) {
	if !s.seen {
		s.kind, s.domain, s.seen = kind, domain, true
		return
	}
	s.mixed = s.mixed || s.kind != kind || s.domain != domain
}

func newPrepareParamKindRowsSource(
	vec *vector.Vector,
	flags []uint8,
) (prepareParamKindRowsSource, error) {
	source := prepareParamKindRowsSource{vec: vec, flags: flags}
	if vec == nil {
		return source, nil
	}
	if flags != nil && len(flags) != vec.Length() {
		return source, moerr.NewInvalidInputNoCtxf(
			"prepared parameter selection length %d does not match vector rows %d",
			len(flags), vec.Length())
	}
	hasExactRows := len(vec.GetPrepareParamKinds()) != 0 || vec.HasBinaryStringRows()
	if flags == nil && !hasExactRows {
		if vec.Length() != 0 && vec.HasPrepareParamKind() && !vec.AllNull() {
			source.summary.observe(vec.GetPrepareParamKind())
			source.summary.binaryString = vec.GetIsBinaryString()
		}
		return source, nil
	}
	selectedRows := 0
	var rowState prepareParamKindRowState
	for row := 0; row < vec.Length(); row++ {
		if flags != nil {
			if flags[row] == 0 {
				continue
			}
			if flags[row] != 1 {
				return source, moerr.NewInvalidInputNoCtx(
					"prepared parameter selection flag must be zero or one")
			}
		}
		selectedRows++
		if vec.IsNull(uint64(row)) {
			continue
		}
		kind := vec.GetPrepareParamKindAt(row)
		source.summary.observe(kind)
		domain := vec.GetRuntimeStringDomainAt(row)
		rowState.observe(kind, domain)
		binaryString := domain == types.RuntimeStringBinary
		source.summary.binaryString = source.summary.binaryString || binaryString
		source.summary.textString = source.summary.textString || domain == types.RuntimeStringText
	}
	source.summary.rows = hasExactRows && rowState.mixed
	if source.summary.rows {
		source.rowCount = selectedRows
	}
	return source, nil
}

func newPrepareParamKindSelectedRowsSource(
	vec *vector.Vector,
	rows []int32,
) (prepareParamKindRowsSource, error) {
	source := prepareParamKindRowsSource{vec: vec, rows: rows}
	if vec == nil {
		return source, nil
	}
	hasExactRows := len(vec.GetPrepareParamKinds()) != 0 || vec.HasBinaryStringRows()
	var rowState prepareParamKindRowState
	for _, row := range rows {
		if row < 0 || int(row) >= vec.Length() {
			return source, moerr.NewInvalidInputNoCtxf(
				"prepared parameter row %d exceeds vector rows %d",
				row, vec.Length())
		}
		if vec.IsNull(uint64(row)) {
			continue
		}
		kind := vec.GetPrepareParamKindAt(int(row))
		source.summary.observe(kind)
		domain := vec.GetRuntimeStringDomainAt(int(row))
		rowState.observe(kind, domain)
		binaryString := domain == types.RuntimeStringBinary
		source.summary.binaryString = source.summary.binaryString || binaryString
		source.summary.textString = source.summary.textString || domain == types.RuntimeStringText
	}
	source.summary.rows = hasExactRows && rowState.mixed
	if source.summary.rows {
		source.rowCount = len(rows)
	}
	return source, nil
}

func (source *prepareParamKindRowsSource) writeRows(writer io.Writer, domainVersion bool) error {
	if source == nil || source.rowCount == 0 {
		return nil
	}
	if source.vec == nil || writer == nil {
		return moerr.NewInvalidInputNoCtx("invalid prepared parameter row source")
	}
	var buffer [256]byte
	buffered := 0
	written := 0
	flush := func() error {
		if buffered == 0 {
			return nil
		}
		_, err := writeGroupSpillBytes(writer, buffer[:buffered])
		buffered = 0
		return err
	}
	writeRow := func(row int) error {
		kind := vector.PrepareParamNone
		nullValue := source.vec.IsNull(uint64(row))
		if !nullValue {
			kind = source.vec.GetPrepareParamKindAt(row)
		}
		if kind > vector.PrepareParamBoolean {
			return moerr.NewInvalidInputNoCtxf(
				"invalid aggregate prepared parameter row kind %d", kind)
		}
		encoded := byte(kind)
		if domainVersion && !nullValue {
			switch source.vec.GetRuntimeStringDomainAt(row) {
			case types.RuntimeStringBinary:
				encoded |= prepareParamKindTrailerRowsMarker
			case types.RuntimeStringText:
				encoded |= prepareParamKindTrailerTextMarker
			}
		}
		buffer[buffered] = encoded
		buffered++
		written++
		if buffered == len(buffer) {
			return flush()
		}
		return nil
	}
	if source.rows != nil {
		for _, row := range source.rows {
			if err := writeRow(int(row)); err != nil {
				return err
			}
		}
	} else {
		for row := 0; row < source.vec.Length(); row++ {
			if source.flags != nil && source.flags[row] == 0 {
				continue
			}
			if err := writeRow(row); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if written != source.rowCount {
		return moerr.NewInternalErrorNoCtx(
			"prepared parameter row source changed during serialization")
	}
	return nil
}

type prepareParamKindRowsTarget struct {
	accessor     aggexec.PrepareParamKindStateAccessor
	chunk        int
	flat         bool
	expectedRows int
}

func prepareParamKindChunkTarget(
	accessor aggexec.PrepareParamKindStateAccessor,
	chunk int,
) prepareParamKindRowsTarget {
	target := prepareParamKindRowsTarget{
		accessor:     accessor,
		chunk:        chunk,
		expectedRows: -1,
	}
	if accessor != nil {
		if vec := accessor.PrepareParamKindVectorForChunk(chunk); vec != nil {
			target.expectedRows = vec.Length()
		}
	}
	return target
}

func prepareParamKindFlatTarget(
	accessor aggexec.PrepareParamKindStateAccessor,
) prepareParamKindRowsTarget {
	target := prepareParamKindRowsTarget{
		accessor:     accessor,
		flat:         true,
		expectedRows: -1,
	}
	if accessor == nil {
		return target
	}
	rows := 0
	for chunk := 0; chunk < accessor.PrepareParamKindChunkCount(); chunk++ {
		vec := accessor.PrepareParamKindVectorForChunk(chunk)
		if vec == nil || vec.Length() < 0 || rows > math.MaxInt-vec.Length() {
			return target
		}
		rows += vec.Length()
	}
	target.expectedRows = rows
	return target
}

type prepareParamKindObservingReader struct {
	reader        io.Reader
	summary       prepareParamKindSummary
	binaryVersion bool
	textVersion   bool
}

func (r *prepareParamKindObservingReader) Read(value []byte) (int, error) {
	n, err := r.reader.Read(value)
	for _, encoded := range value[:n] {
		if r.binaryVersion {
			r.summary.binaryString = r.summary.binaryString || encoded&prepareParamKindTrailerRowsMarker != 0
			if r.textVersion {
				r.summary.textString = r.summary.textString || encoded&prepareParamKindTrailerTextMarker != 0
				encoded &^= prepareParamKindTrailerTextMarker
			}
			encoded &^= prepareParamKindTrailerRowsMarker
		}
		r.summary.observe(vector.PrepareParamKind(encoded))
	}
	return n, err
}

func (target *prepareParamKindRowsTarget) restore(
	reader io.Reader,
	rows int,
	mp *mpool.MPool,
	binaryVersion bool,
	textVersions ...bool,
) (prepareParamKindSummary, error) {
	if target == nil || reader == nil || rows <= 0 || target.expectedRows != rows {
		return prepareParamKindSummary{}, moerr.NewInvalidInputNoCtx(
			"invalid prepared parameter row target")
	}
	textVersion := len(textVersions) == 1 && textVersions[0]
	observed := &prepareParamKindObservingReader{
		reader: reader, binaryVersion: binaryVersion, textVersion: textVersion,
	}
	if target.accessor == nil {
		var buffer [256]byte
		remaining := rows
		for remaining > 0 {
			n := min(remaining, len(buffer))
			if _, err := io.ReadFull(observed, buffer[:n]); err != nil {
				return prepareParamKindSummary{}, err
			}
			for _, encoded := range buffer[:n] {
				if binaryVersion {
					encoded &^= prepareParamKindTrailerRowsMarker
					if textVersion {
						encoded &^= prepareParamKindTrailerTextMarker
					}
				}
				if vector.PrepareParamKind(encoded) > vector.PrepareParamBoolean {
					return prepareParamKindSummary{}, moerr.NewInvalidInputNoCtxf(
						"invalid aggregate prepared parameter row kind %d", encoded)
				}
			}
			remaining -= n
		}
		return observed.summary, nil
	}
	restoreVector := func(vec *vector.Vector, count int) error {
		if vec == nil || vec.Length() != count {
			return moerr.NewInvalidInputNoCtx(
				"prepared parameter target vector row count changed")
		}
		if binaryVersion {
			if !textVersion {
				return vec.SetPrepareParamKindsAndBinaryStringFromReader(
					observed, count, mp, prepareParamKindTrailerRowsMarker)
			}
			return vec.SetPrepareParamKindsAndBinaryStringFromReader(
				observed, count, mp, prepareParamKindTrailerRowsMarker, prepareParamKindTrailerTextMarker)
		}
		return vec.SetPrepareParamKindsFromReader(observed, count, mp)
	}
	if !target.flat {
		if err := restoreVector(
			target.accessor.PrepareParamKindVectorForChunk(target.chunk), rows,
		); err != nil {
			return prepareParamKindSummary{}, err
		}
		return observed.summary, nil
	}
	remaining := rows
	for chunk := 0; chunk < target.accessor.PrepareParamKindChunkCount(); chunk++ {
		vec := target.accessor.PrepareParamKindVectorForChunk(chunk)
		if vec == nil || vec.Length() > remaining {
			return prepareParamKindSummary{}, moerr.NewInvalidInputNoCtx(
				"prepared parameter flat target does not match row count")
		}
		if err := restoreVector(vec, vec.Length()); err != nil {
			return prepareParamKindSummary{}, err
		}
		remaining -= vec.Length()
	}
	if remaining != 0 {
		return prepareParamKindSummary{}, moerr.NewInvalidInputNoCtx(
			"prepared parameter flat target is shorter than row count")
	}
	return observed.summary, nil
}

func (target *prepareParamKindRowsTarget) setBinarySummary(binaryString bool) {
	if target == nil || target.accessor == nil || !binaryString {
		return
	}
	if !target.flat {
		if vec := target.accessor.PrepareParamKindVectorForChunk(target.chunk); vec != nil {
			vec.SetIsBinaryString(true)
		}
		return
	}
	for chunk := 0; chunk < target.accessor.PrepareParamKindChunkCount(); chunk++ {
		if vec := target.accessor.PrepareParamKindVectorForChunk(chunk); vec != nil {
			vec.SetIsBinaryString(true)
		}
	}
}

func (target *prepareParamKindRowsTarget) setStringDomainSummary(
	domain types.RuntimeStringDomain, mp *mpool.MPool,
) error {
	if target == nil || target.accessor == nil {
		return nil
	}
	set := func(vec *vector.Vector) error {
		if vec != nil {
			return vec.SetRuntimeStringDomainWithMP(domain, mp)
		}
		return nil
	}
	if !target.flat {
		return set(target.accessor.PrepareParamKindVectorForChunk(target.chunk))
	}
	for chunk := 0; chunk < target.accessor.PrepareParamKindChunkCount(); chunk++ {
		if err := set(target.accessor.PrepareParamKindVectorForChunk(chunk)); err != nil {
			return err
		}
	}
	return nil
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
	return ok && version >= defines.MORPCVersion12
}

func binaryStringWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion18
}

func explicitTextWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion23
}

func stringSourceWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion27
}

type aggregateStringSourceProtocolWriter interface {
	SaveIntermediateResultOfChunkWithStringSource(
		chunk int,
		writer io.Writer,
		includeStringSource bool,
	) error
}

func saveAggregateChunkForProtocol(
	agg aggexec.AggFuncExec,
	chunk int,
	writer io.Writer,
	includeStringSource bool,
) error {
	if includeStringSource {
		return agg.SaveIntermediateResultOfChunk(chunk, writer)
	}
	protocolWriter, ok := agg.(aggregateStringSourceProtocolWriter)
	if !ok {
		if accessor, ok := agg.(aggexec.PrepareParamKindStateAccessor); ok {
			if vec := accessor.PrepareParamKindVectorForChunk(chunk); vec != nil && vec.HasStringSourceMetadata() {
				return moerr.NewInternalErrorNoCtx(
					"aggregate cannot omit string source for an older peer")
			}
		}
		return agg.SaveIntermediateResultOfChunk(chunk, writer)
	}
	return protocolWriter.SaveIntermediateResultOfChunkWithStringSource(
		chunk, writer, false)
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
	writer io.Writer,
	aggs []aggexec.AggFuncExecExpression,
	states *aggexec.PrepareParamKindStates,
	sources []prepareParamKindRowsSource,
) error {
	rowsVersion := false
	binaryVersion := false
	for i := range sources {
		if sources[i].rowCount != 0 {
			rowsVersion = true
		}
		if sources[i].summary.binaryString {
			binaryVersion = true
		}
	}
	version := prepareParamKindTrailerVersion
	if binaryVersion {
		version = prepareParamKindTrailerBinaryVersion
	} else if rowsVersion {
		version = prepareParamKindTrailerRowsVersion
	}
	textVersion := false
	for i := range sources {
		textVersion = textVersion || sources[i].summary.textString
	}
	if textVersion {
		version = prepareParamKindTrailerDomainVersion
		binaryVersion = true
	}
	if _, err := writeGroupSpillBytes(writer, []byte{
		prepareParamKindTrailerMagic0,
		prepareParamKindTrailerMagic1,
		prepareParamKindTrailerMagic2,
		version,
	}); err != nil {
		return err
	}
	nAggs := int32(len(aggs))
	if err := types.WriteInt32(writer, nAggs); err != nil {
		return err
	}
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
		if rowsVersion && i < len(sources) && sources[i].rowCount != 0 {
			if int64(sources[i].rowCount) > int64(prepareParamKindTrailerMaxRows) {
				return moerr.NewInternalErrorf(ctx,
					"aggregate prepared parameter row count %d exceeds limit", sources[i].rowCount)
			}
			if _, err := writeGroupSpillBytes(writer, []byte{prepareParamKindTrailerRowsMarker}); err != nil {
				return err
			}
			rowCount := int32(sources[i].rowCount)
			if err := types.WriteInt32(writer, rowCount); err != nil {
				return err
			}
			if err := sources[i].writeRows(writer, binaryVersion); err != nil {
				return err
			}
			continue
		}
		// Preserving aggregate accessors provide a summary for the exact
		// chunk/selection being serialized.  Use an unseen summary as well:
		// falling back to the cumulative operator state here can leak a prior
		// chunk's Float/Integer category onto an ordinary winner in this one.
		if i < len(sources) && aggs[i].PreservesFirstArgPrepareParamKind() {
			kind, seen = sources[i].summary.kind, sources[i].summary.seen
		}
		encoded, ok := encodePrepareParamKindState(kind, seen)
		if !ok {
			return moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter kind %d", kind)
		}
		if _, err := writeGroupSpillBytes(writer, []byte{encoded}); err != nil {
			return err
		}
		if binaryVersion {
			domain := byte(0)
			if version == prepareParamKindTrailerDomainVersion {
				if i < len(sources) && sources[i].summary.binaryString {
					domain = byte(types.RuntimeStringBinary)
				} else if i < len(sources) && sources[i].summary.textString {
					domain = byte(types.RuntimeStringText)
				}
			} else if i < len(sources) && sources[i].summary.binaryString {
				domain = 1
			}
			if _, err := writeGroupSpillBytes(writer, []byte{domain}); err != nil {
				return err
			}
		}
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
	targets []prepareParamKindRowsTarget,
	mp *mpool.MPool,
	allowBinaryString bool,
	allowExplicitText ...bool,
) ([]prepareParamKindSummary, error) {
	if nAggs < 0 || states == nil {
		return nil, moerr.NewInvalidInputNoCtx(
			"invalid aggregate prepared parameter destination")
	}
	magic0, err := types.ReadByte(reader)
	if err != nil {
		return nil, err
	}
	magic1, err := types.ReadByte(reader)
	if err != nil {
		return nil, err
	}
	magic2, err := types.ReadByte(reader)
	if err != nil {
		return nil, err
	}
	if magic0 != prepareParamKindTrailerMagic0 ||
		magic1 != prepareParamKindTrailerMagic1 ||
		magic2 != prepareParamKindTrailerMagic2 {
		return nil, moerr.NewInternalErrorNoCtx("invalid aggregate prepared parameter trailer")
	}
	version, err := types.ReadByte(reader)
	if err != nil {
		return nil, err
	}
	if version != prepareParamKindTrailerVersion && version != prepareParamKindTrailerRowsVersion &&
		version != prepareParamKindTrailerBinaryVersion && version != prepareParamKindTrailerDomainVersion {
		return nil, moerr.NewInternalErrorf(ctx,
			"unsupported aggregate prepared parameter trailer version %d", version)
	}
	if (version == prepareParamKindTrailerBinaryVersion || version == prepareParamKindTrailerDomainVersion) && !allowBinaryString {
		return nil, moerr.NewInvalidStateNoCtx(
			"aggregate binary-string metadata requires MORPCVersion18")
	}
	textAllowed := len(allowExplicitText) == 1 && allowExplicitText[0]
	if version == prepareParamKindTrailerDomainVersion && !textAllowed {
		return nil, moerr.NewInvalidStateNoCtx(
			"aggregate explicit-text metadata requires MORPCVersion23")
	}
	encodedAggs, err := types.ReadInt32(reader)
	if err != nil {
		return nil, err
	}
	if encodedAggs != nAggs {
		return nil, moerr.NewInternalErrorf(ctx,
			"aggregate prepared parameter count %d does not match %d", encodedAggs, nAggs)
	}
	// Keep the incoming record separate from the cumulative state.  The
	// cumulative state is only an execution-wide compatibility summary; using
	// it to restore this partial can relabel a later winning value after a
	// mixed-category partial has already been observed.
	summaries := make([]prepareParamKindSummary, nAggs)
	for i := int32(0); i < nAggs; i++ {
		encoded, err := types.ReadByte(reader)
		if err != nil {
			return nil, err
		}
		if (version == prepareParamKindTrailerRowsVersion ||
			version == prepareParamKindTrailerBinaryVersion || version == prepareParamKindTrailerDomainVersion) &&
			encoded == prepareParamKindTrailerRowsMarker {
			rowCount, err := types.ReadInt32(reader)
			if err != nil {
				return nil, err
			}
			if rowCount <= 0 || rowCount > prepareParamKindTrailerMaxRows {
				return nil, moerr.NewInternalErrorf(ctx,
					"invalid aggregate prepared parameter row count %d", rowCount)
			}
			if i >= int32(len(targets)) || targets[i].expectedRows < 0 {
				return nil, moerr.NewInternalErrorf(ctx,
					"aggregate %d does not expose a prepared parameter row count", i)
			}
			if rowCount != int32(targets[i].expectedRows) {
				return nil, moerr.NewInternalErrorf(ctx,
					"aggregate prepared parameter row count %d does not match %d",
					rowCount, targets[i].expectedRows)
			}
			// bytes.Reader exposes the complete remaining partial payload. Check
			// the bound before make so a truncated/corrupt record cannot trigger
			// a data-scaled allocation. Local spill uses an io.BufferedReader and
			// is protected by expectedRows above; its read then remains streaming.
			if remaining, ok := prepareParamKindReaderLen(reader); ok &&
				int64(rowCount) > int64(remaining) {
				return nil, io.ErrUnexpectedEOF
			}
			summary, err := targets[i].restore(
				reader, int(rowCount), mp,
				version == prepareParamKindTrailerBinaryVersion || version == prepareParamKindTrailerDomainVersion,
				version == prepareParamKindTrailerDomainVersion,
			)
			if err != nil {
				return nil, err
			}
			summary.rows = true
			summaries[i] = summary
			continue
		}
		kind, seen, ok := decodePrepareParamKindState(encoded)
		if !ok {
			return nil, moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter state %d", encoded)
		}
		summaries[i] = prepareParamKindSummary{kind: kind, seen: seen}
		if version == prepareParamKindTrailerBinaryVersion || version == prepareParamKindTrailerDomainVersion {
			domain, err := types.ReadByte(reader)
			if err != nil {
				return nil, err
			}
			if version == prepareParamKindTrailerBinaryVersion && domain > 1 ||
				version == prepareParamKindTrailerDomainVersion && types.RuntimeStringDomain(domain) > types.RuntimeStringBinary {
				return nil, moerr.NewInternalErrorNoCtx(
					"invalid aggregate binary provenance summary")
			}
			if version == prepareParamKindTrailerBinaryVersion {
				summaries[i].binaryString = domain == 1
			} else {
				summaries[i].binaryString = types.RuntimeStringDomain(domain) == types.RuntimeStringBinary
				summaries[i].textString = types.RuntimeStringDomain(domain) == types.RuntimeStringText
			}
			if i < int32(len(targets)) {
				targets[i].setBinarySummary(summaries[i].binaryString)
				if summaries[i].textString {
					if err := targets[i].setStringDomainSummary(types.RuntimeStringText, mp); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	for i := range summaries {
		states.ObserveState(i, summaries[i].kind, summaries[i].seen)
	}
	return summaries, nil
}

func prepareParamKindReaderLen(reader io.Reader) (int, bool) {
	type lenReader interface{ Len() int }
	if r, ok := reader.(lenReader); ok {
		return r.Len(), true
	}
	return 0, false
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
