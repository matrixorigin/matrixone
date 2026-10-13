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

package batch

import (
	"io"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// MarshalBinaryWithGroupingSize returns the spill-only Batch wire size. Spill
// records retain vectors and grouping provenance but deliberately omit Attrs
// and ExtraBuf: joins do not consume either field, and ExtraBuf would otherwise
// create an unaccounted data-scaled Go-heap owner while decoding.
func (bat *Batch) MarshalBinaryWithGroupingSize() (int, error) {
	metadataFree := *bat
	metadataFree.Attrs = nil
	metadataFree.ExtraBuf = nil
	metadataFree.extraBufMP = nil
	size, err := metadataFree.MarshalBinarySize()
	if err != nil {
		return 0, err
	}
	metadataSize, err := bat.PrepareParamKindMetadataSize()
	if err != nil {
		return 0, err
	}
	if len(bat.Vecs) > math.MaxInt32 || size > math.MaxInt-8-metadataSize {
		return 0, moerr.NewInvalidInputNoCtx("batch grouping payload exceeds marshal format")
	}
	size += 8 + metadataSize + 4
	for _, vec := range bat.Vecs {
		groupingSize := vec.GroupingMarshalBinarySize()
		if groupingSize < 0 || groupingSize > math.MaxInt32 ||
			size > math.MaxInt-4-groupingSize {
			return 0, moerr.NewInvalidInputNoCtx("batch grouping payload exceeds marshal format")
		}
		size += 4 + groupingSize
	}
	return size, nil
}

func (bat *Batch) MarshalBinaryWithGroupingTo(w io.Writer) error {
	if bat == nil || w == nil {
		return io.ErrClosedPipe
	}
	metadataFree := *bat
	metadataFree.Attrs = nil
	metadataFree.ExtraBuf = nil
	metadataFree.extraBufMP = nil
	payloadSize, err := metadataFree.MarshalBinarySize()
	if err != nil {
		return err
	}
	metadataSize, err := bat.PrepareParamKindMetadataSize()
	if err != nil {
		return err
	}
	if payloadSize > math.MaxInt-metadataSize {
		return moerr.NewInvalidInputNoCtx("batch grouping payload exceeds marshal format")
	}
	if err := writeBatchMarshalInt64(w, int64(payloadSize+metadataSize)); err != nil {
		return err
	}
	if err := metadataFree.MarshalBinaryTo(w); err != nil {
		return err
	}
	if err := bat.AppendPrepareParamKindMetadata(w); err != nil {
		return err
	}
	return bat.marshalGroupingTo(w)
}

func (bat *Batch) marshalGroupingTo(w io.Writer) error {
	if err := writeBatchMarshalInt32(w, int32(len(bat.Vecs))); err != nil {
		return err
	}
	for _, vec := range bat.Vecs {
		size := vec.GroupingMarshalBinarySize()
		if size > math.MaxInt32 {
			return moerr.NewInvalidInputNoCtx("vector grouping payload exceeds marshal format")
		}
		if err := writeBatchMarshalInt32(w, int32(size)); err != nil {
			return err
		}
		if size > 0 {
			if err := vec.MarshalGroupingTo(w); err != nil {
				return err
			}
		}
	}
	return nil
}

func (bat *Batch) UnmarshalFromReaderWithGrouping(
	r io.Reader,
	mp *mpool.MPool,
) error {
	payloadSize, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if payloadSize < 0 {
		return moerr.NewInvalidInputNoCtx("negative batch grouping payload size")
	}
	limited := &io.LimitedReader{R: r, N: payloadSize}
	if err := bat.unmarshalFromReaderWithPrepareParamKinds(limited, payloadSize, mp, false); err != nil {
		return err
	}
	if limited.N != 0 {
		return moerr.NewInvalidInputNoCtx("batch grouping payload was not fully consumed")
	}
	return bat.unmarshalGroupingFromReader(r, mp)
}

func (bat *Batch) unmarshalGroupingFromReader(
	r io.Reader,
	mp *mpool.MPool,
) error {
	if err := bat.CheckLength(); err != nil {
		return moerr.NewInvalidInputNoCtx("spill batch vector length does not match row count")
	}
	count, err := types.ReadInt32AsInt(r)
	if err != nil {
		return err
	}
	if count != len(bat.Vecs) {
		return moerr.NewInvalidInputNoCtx("batch grouping vector count mismatch")
	}
	for _, vec := range bat.Vecs {
		size, err := types.ReadInt32AsInt(r)
		if err != nil {
			return err
		}
		if size < 0 {
			return moerr.NewInvalidInputNoCtx("invalid vector grouping payload size")
		}
		if remaining, ok := r.(*io.LimitedReader); ok && int64(size) > remaining.N {
			return io.ErrUnexpectedEOF
		}
		limited := &io.LimitedReader{R: r, N: int64(size)}
		if err = vec.UnmarshalGroupingFromReader(limited, size, mp); err != nil {
			return err
		}
		if limited.N != 0 {
			return moerr.NewInvalidInputNoCtx("vector grouping payload was not fully consumed")
		}
	}
	return nil
}
