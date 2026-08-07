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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	prepareParamKindTrailerMagic0  = byte('P')
	prepareParamKindTrailerMagic1  = byte('P')
	prepareParamKindTrailerMagic2  = byte('K')
	prepareParamKindTrailerVersion = byte(1)
)

func prepareParamKindWireV1Enabled(proc *process.Process) bool {
	if proc == nil {
		return false
	}
	// Deployment raises the shared protocol only after every participating CN
	// can emit and consume the trailer, and lowers it before rollback.
	value, ok := moruntime.ServiceRuntime(proc.GetService()).
		GetGlobalVariables(moruntime.MOProtocolVersion)
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
) error {
	buf.WriteByte(prepareParamKindTrailerMagic0)
	buf.WriteByte(prepareParamKindTrailerMagic1)
	buf.WriteByte(prepareParamKindTrailerMagic2)
	buf.WriteByte(prepareParamKindTrailerVersion)
	nAggs := int32(len(aggs))
	buf.Write(types.EncodeInt32(&nAggs))
	for i := range aggs {
		kind, seen := states.GetState(i)
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
	reader *bytes.Reader,
	nAggs int32,
	states *aggexec.PrepareParamKindStates,
) error {
	magic0, err := types.ReadByte(reader)
	if err != nil {
		return err
	}
	magic1, err := types.ReadByte(reader)
	if err != nil {
		return err
	}
	magic2, err := types.ReadByte(reader)
	if err != nil {
		return err
	}
	if magic0 != prepareParamKindTrailerMagic0 ||
		magic1 != prepareParamKindTrailerMagic1 ||
		magic2 != prepareParamKindTrailerMagic2 {
		return moerr.NewInternalErrorNoCtx("invalid aggregate prepared parameter trailer")
	}
	version, err := types.ReadByte(reader)
	if err != nil {
		return err
	}
	if version != prepareParamKindTrailerVersion {
		return moerr.NewInternalErrorf(ctx,
			"unsupported aggregate prepared parameter trailer version %d", version)
	}
	encodedAggs, err := types.ReadInt32(reader)
	if err != nil {
		return err
	}
	if encodedAggs != nAggs {
		return moerr.NewInternalErrorf(ctx,
			"aggregate prepared parameter count %d does not match %d", encodedAggs, nAggs)
	}
	for i := int32(0); i < nAggs; i++ {
		encoded, err := types.ReadByte(reader)
		if err != nil {
			return err
		}
		kind, seen, ok := decodePrepareParamKindState(encoded)
		if !ok {
			return moerr.NewInternalErrorf(ctx,
				"invalid aggregate prepared parameter state %d", encoded)
		}
		states.ObserveState(int(i), kind, seen)
	}
	if reader.Len() != 0 {
		return moerr.NewInternalErrorNoCtx("unexpected aggregate prepared parameter trailer bytes")
	}
	return nil
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
