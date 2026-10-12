// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// The footer frames an optional pipeline-only extension after the stable batch
// and parameter provenance. Neither the persisted vector format nor spill
// framing changes. Its last byte versions the grouping extension.
const pipelineGroupingMagic = "MOGROUP\x01"
const pipelineGroupingFooterSize = 16

func (bat *Batch) HasGrouping() bool {
	if bat != nil {
		for _, vec := range bat.Vecs {
			if vec != nil && vec.HasGrouping() {
				return true
			}
		}
	}
	return false
}

// MarshalBinaryForPipeline preserves aggregate ExtraBuf as well as expression
// provenance. Callers must gate grouping-bearing batches on MORPCVersion87.
// Batches without grouping retain the previous transport bytes exactly.
func (bat *Batch) MarshalBinaryForPipeline(w *bytes.Buffer, reset, includeStringSources bool) ([]byte, error) {
	if _, err := bat.MarshalBinaryWithPrepareParamKindsForProtocol(w, reset, includeStringSources); err != nil {
		return nil, err
	}
	if !bat.HasGrouping() {
		return w.Bytes(), nil
	}
	if err := bat.CheckLength(); err != nil {
		return nil, err
	}
	start := w.Len()
	if err := writeBatchMarshalInt64(w, int64(bat.RowCount())); err != nil {
		return nil, err
	}
	if err := bat.marshalGroupingTo(w); err != nil {
		return nil, err
	}
	var footer [pipelineGroupingFooterSize]byte
	copy(footer[:8], pipelineGroupingMagic)
	binary.LittleEndian.PutUint64(footer[8:], uint64(w.Len()-start))
	_, _ = w.Write(footer[:])
	return w.Bytes(), nil
}

// UnmarshalBinaryForPipeline accepts both legacy batches and the grouping
// extension. The caller owns cleanup after errors, as with the stable decoder.
func (bat *Batch) UnmarshalBinaryForPipeline(data []byte, mp *mpool.MPool) error {
	prefix, err := stableBatchPayloadLength(data)
	if err != nil {
		return err
	}
	var grouping []byte
	end := len(data) - pipelineGroupingFooterSize
	// Inspect only bytes outside the stable payload: user values or ExtraBuf
	// may legitimately end with the footer magic.
	if end >= prefix && string(data[end:end+8]) == pipelineGroupingMagic {
		size := binary.LittleEndian.Uint64(data[end+8:])
		if size > uint64(end-prefix) || size < 12 {
			return moerr.NewInvalidInputNoCtx("invalid pipeline grouping payload size")
		}
		start := end - int(size)
		grouping = data[start:end]
		data = data[:start]
	}
	if err := bat.UnmarshalBinaryWithPrepareParamKinds(data, mp); err != nil {
		return err
	}
	// A receiver reused after a grouping batch must not inherit its markers.
	for _, vec := range bat.Vecs {
		vec.GetGrouping().Reset()
	}
	if grouping == nil {
		return nil
	}
	r := &io.LimitedReader{R: bytes.NewReader(grouping), N: int64(len(grouping))}
	rows, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if rows != int64(bat.RowCount()) {
		return moerr.NewInvalidInputNoCtx("pipeline grouping row count mismatch")
	}
	if err := bat.unmarshalGroupingFromReader(r, mp); err != nil {
		return err
	}
	if r.N != 0 {
		return moerr.NewInvalidInputNoCtx("pipeline grouping payload was not fully consumed")
	}
	for _, vec := range bat.Vecs {
		if vec.GetGrouping().Count() != vec.GetGrouping().CountRange(0, uint64(vec.Length())) {
			return moerr.NewInvalidInputNoCtx("pipeline grouping bit exceeds vector length")
		}
	}
	return nil
}
