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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// The row-metadata trailer is a retired, self-identifying extension that older
// writers appended after the aggregate payload of a remote partial or a spill
// record. It carried a per-aggregate conversion category (retired) together
// with a copy of the aggregate result's binary/text string domain. The domain
// copy duplicates the aggregate state's own string-domain trailer, so current
// writers no longer emit this extension. Readers still accept and validate it
// so that partials and spill records from older writers remain decodable.
//
// Wire values are frozen; do not renumber them.
const (
	rowMetadataTrailerMagic0 = byte('P')
	rowMetadataTrailerMagic1 = byte('P')
	rowMetadataTrailerMagic2 = byte('K')
	// Version 1 has one summary byte per aggregate. Version 2 adds an explicit
	// per-row form. Version 3 adds a binary-string summary byte (0/1) and a
	// binary row bit. Version 4 widens the summary byte to a
	// RuntimeStringDomain and adds a text row bit.
	rowMetadataTrailerVersion       = byte(1)
	rowMetadataTrailerRowsVersion   = byte(2)
	rowMetadataTrailerBinaryVersion = byte(3)
	rowMetadataTrailerDomainVersion = byte(4)
	rowMetadataTrailerRowsMarker    = byte(0x80)
	rowMetadataTrailerTextMarker    = byte(0x40)
	rowMetadataTrailerMaxRows       = int32(1 << 24)
	// rowMetadataTrailerMaxRetired is the largest value of the retired
	// per-aggregate/per-row field (five categories, 0..4).
	rowMetadataTrailerMaxRetired = byte(4)
)

// rowMetadataTrailerWireEnabled reports whether peers may append the retired
// row-metadata trailer to a partial. A receiver rejects trailer bytes when the
// shared protocol predates it.
func rowMetadataTrailerWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
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
	return ok && version >= defines.MORPCVersion37
}

func groupConcatSourceRowWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	if !proc.GroupConcatSourceRowProvenanceTrusted() {
		return false
	}
	return groupConcatSourceRowProvenanceWireEnabled(proc)
}

// groupConcatSourceRowProvenanceWireEnabled gates the v66 state-level marker.
// It remains enabled for remote producers even though their payload rows are
// legacy: the marker is how a coordinator learns that an empty/NULL-only
// partial consumed an independent source-row namespace.
func groupConcatSourceRowProvenanceWireEnabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	if rt == nil {
		return false
	}
	value, _ := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	version, ok := value.(int64)
	return ok && version >= defines.MORPCVersion66
}

// aggResultVectorAccessor is an optional aggregate capability exposing the
// state vector of one chunk, used to enforce the protocol gates of the string
// metadata it carries before that state crosses a peer boundary.
type aggResultVectorAccessor interface {
	ResultVectorForChunk(chunk int) *vector.Vector
}

func aggResultVectorForChunk(agg aggexec.AggFuncExec, chunk int) *vector.Vector {
	if accessor, ok := agg.(aggResultVectorAccessor); ok {
		return accessor.ResultVectorForChunk(chunk)
	}
	return nil
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
		if vec := aggResultVectorForChunk(agg, chunk); vec != nil && vec.HasStringSourceMetadata() {
			return moerr.NewInternalErrorNoCtx(
				"aggregate cannot omit string source for an older peer")
		}
		return agg.SaveIntermediateResultOfChunk(chunk, writer)
	}
	return protocolWriter.SaveIntermediateResultOfChunkWithStringSource(
		chunk, writer, false)
}

// readRowMetadataTrailer consumes one retired row-metadata trailer written by
// an older peer after every aggregate has read its state. Everything it
// carries is validated and discarded: the retired category has no consumer,
// and the string-domain copy duplicates what each aggregate already restored
// from its own state trailer. expectedRows[i] is the decoded group count of
// aggregate i; a per-row section must match it exactly.
func readRowMetadataTrailer(
	ctx context.Context,
	reader io.Reader,
	expectedRows []int,
	allowBinaryString bool,
	allowExplicitText bool,
) error {
	if reader == nil {
		return moerr.NewInvalidInputNoCtx("invalid aggregate row metadata reader")
	}
	var header [4]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return err
	}
	if header[0] != rowMetadataTrailerMagic0 ||
		header[1] != rowMetadataTrailerMagic1 ||
		header[2] != rowMetadataTrailerMagic2 {
		return moerr.NewInternalErrorNoCtx("invalid aggregate row metadata trailer")
	}
	version := header[3]
	if version != rowMetadataTrailerVersion && version != rowMetadataTrailerRowsVersion &&
		version != rowMetadataTrailerBinaryVersion && version != rowMetadataTrailerDomainVersion {
		return moerr.NewInternalErrorf(ctx,
			"unsupported aggregate row metadata trailer version %d", version)
	}
	binaryVersion := version == rowMetadataTrailerBinaryVersion ||
		version == rowMetadataTrailerDomainVersion
	domainVersion := version == rowMetadataTrailerDomainVersion
	if binaryVersion && !allowBinaryString {
		return moerr.NewInvalidStateNoCtx(
			"aggregate binary-string metadata requires MORPCVersion18")
	}
	if domainVersion && !allowExplicitText {
		return moerr.NewInvalidStateNoCtx(
			"aggregate explicit-text metadata requires MORPCVersion23")
	}
	nAggs, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if int(nAggs) != len(expectedRows) {
		return moerr.NewInternalErrorf(ctx,
			"aggregate row metadata count %d does not match %d", nAggs, len(expectedRows))
	}
	for i := 0; i < len(expectedRows); i++ {
		encoded, err := types.ReadByte(reader)
		if err != nil {
			return err
		}
		if version != rowMetadataTrailerVersion && encoded == rowMetadataTrailerRowsMarker {
			rowCount, err := types.ReadInt32(reader)
			if err != nil {
				return err
			}
			if rowCount <= 0 || rowCount > rowMetadataTrailerMaxRows {
				return moerr.NewInternalErrorf(ctx,
					"invalid aggregate row metadata row count %d", rowCount)
			}
			if int(rowCount) != expectedRows[i] {
				return moerr.NewInternalErrorf(ctx,
					"aggregate row metadata row count %d does not match %d",
					rowCount, expectedRows[i])
			}
			if err := discardRowMetadataRows(
				reader, int(rowCount), binaryVersion, domainVersion); err != nil {
				return err
			}
			continue
		}
		// Zero is an unobserved aggregate; observed categories are shifted by
		// one.
		if encoded > rowMetadataTrailerMaxRetired+1 {
			return moerr.NewInternalErrorf(ctx,
				"invalid aggregate row metadata state %d", encoded)
		}
		if !binaryVersion {
			continue
		}
		domain, err := types.ReadByte(reader)
		if err != nil {
			return err
		}
		if !domainVersion && domain > 1 ||
			domainVersion && types.RuntimeStringDomain(domain) > types.RuntimeStringBinary {
			return moerr.NewInternalErrorNoCtx(
				"invalid aggregate binary provenance summary")
		}
	}
	return nil
}

// discardRowMetadataRows streams and validates one per-row section without
// allocating a row-sized buffer.
func discardRowMetadataRows(
	reader io.Reader,
	rows int,
	binaryVersion bool,
	domainVersion bool,
) error {
	var mask byte
	if binaryVersion {
		mask |= rowMetadataTrailerRowsMarker
	}
	if domainVersion {
		mask |= rowMetadataTrailerTextMarker
	}
	var buffer [256]byte
	for remaining := rows; remaining > 0; {
		n := min(remaining, len(buffer))
		if _, err := io.ReadFull(reader, buffer[:n]); err != nil {
			if err == io.EOF {
				// The row count has been read; a missing body is truncation.
				err = io.ErrUnexpectedEOF
			}
			return err
		}
		for _, encoded := range buffer[:n] {
			if encoded&rowMetadataTrailerRowsMarker != 0 &&
				encoded&rowMetadataTrailerTextMarker != 0 && domainVersion {
				return moerr.NewInvalidInputNoCtx(
					"binary and text row flags are mutually exclusive")
			}
			if encoded&^mask > rowMetadataTrailerMaxRetired {
				return moerr.NewInvalidInputNoCtxf(
					"invalid aggregate row metadata byte %d", encoded)
			}
		}
		remaining -= n
	}
	return nil
}
