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

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// MarshalBinaryForSpillSize returns the spill-only Batch wire size: an int64
// payload size followed by the stable batch. Spill records deliberately omit
// Attrs and ExtraBuf: joins do not consume either field, and ExtraBuf would
// otherwise create an unaccounted data-scaled Go-heap owner while decoding.
func (bat *Batch) MarshalBinaryForSpillSize() (int, error) {
	metadataFree := bat.spillView()
	size, err := metadataFree.MarshalBinarySize()
	if err != nil {
		return 0, err
	}
	if size > math.MaxInt-8 {
		return 0, moerr.NewInvalidInputNoCtx("batch spill payload exceeds marshal format")
	}
	return size + 8, nil
}

// MarshalBinaryForSpillTo writes the spill-only Batch wire format described by
// MarshalBinaryForSpillSize.
func (bat *Batch) MarshalBinaryForSpillTo(w io.Writer) error {
	if bat == nil || w == nil {
		return io.ErrClosedPipe
	}
	metadataFree := bat.spillView()
	payloadSize, err := metadataFree.MarshalBinarySize()
	if err != nil {
		return err
	}
	if err := writeBatchMarshalInt64(w, int64(payloadSize)); err != nil {
		return err
	}
	return metadataFree.MarshalBinaryTo(w)
}

func (bat *Batch) spillView() Batch {
	metadataFree := *bat
	metadataFree.Attrs = nil
	metadataFree.extraBuf = nil
	metadataFree.mp = nil
	// The view may read bat's selection but never owns its storage.
	metadataFree.ownSel = bitmap.Bitmap{}
	return metadataFree
}

// UnmarshalFromReaderForSpill reads one record written by
// MarshalBinaryForSpillTo.
func (bat *Batch) UnmarshalFromReaderForSpill(
	r io.Reader,
	mp *mpool.MPool,
) error {
	payloadSize, err := types.ReadInt64(r)
	if err != nil {
		return err
	}
	if payloadSize < 0 {
		return moerr.NewInvalidInputNoCtx("negative batch spill payload size")
	}
	limited := &io.LimitedReader{R: r, N: payloadSize}
	if err := bat.unmarshalFromReaderWithMetadata(limited, payloadSize, mp, false); err != nil {
		return err
	}
	if limited.N != 0 {
		return moerr.NewInvalidInputNoCtx("batch spill payload was not fully consumed")
	}
	if err := bat.CheckLength(); err != nil {
		return moerr.NewInvalidInputNoCtx("spill batch vector length does not match row count")
	}
	return nil
}
