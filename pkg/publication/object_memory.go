// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"
	"errors"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

type publicationMemoryKey struct{}
type publicationFetchMemoryKey struct{}

type objectMemoryGrowth struct{ bytes int64 }

func (e *objectMemoryGrowth) Error() string {
	return "publication requires a larger object reservation"
}

// The final buffer, returned chunks, and driver's decoding backing can coexist.
// Each generation is class-rounded; chunk totals are bounded by the object size.
func publicationBufferBytes(size int64) (int64, error) {
	if size < 0 || size > math.MaxUint32 || size > math.MaxInt {
		return 0, moerr.NewInternalErrorNoCtx("invalid GETOBJECT size")
	}
	if size == 0 {
		return 0, nil
	}
	backing, ok := malloc.ClassAllocationSize(uint64(size))
	if !ok || backing > math.MaxInt64/3 {
		return 0, moerr.NewInternalErrorNoCtx("publication buffer size overflow")
	}
	return int64(backing) * 3, nil
}

func reservePublicationObject(ctx context.Context, stats *objectio.ObjectStats, minimum int64, rewrite bool) (*rscthrottler.InputLease, func(), error) {
	owner, ok := ctx.Value(publicationMemoryKey{}).(rscthrottler.WorkingSetThrottler)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("publication object requires CN memory admission")
	}
	peak, err := publicationBufferBytes(int64(stats.Size()))
	if err != nil {
		return nil, nil, err
	}
	if rewrite {
		peak, err = ioutil.IOEnvelope(nil, 1, max(int64(stats.Size()), int64(stats.OriginSize()), int64(stats.Extent().OriginSize())))
		if err != nil {
			return nil, nil, err
		}
	}
	peak = max(int64(1), peak, minimum)
	ticket, err := owner.ReserveWorkingSetCandidatesUntilFit(ctx, []rscthrottler.WorkingSetSpec{{UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Name: "publication-object", Count: 1, Bytes: peak}}}})
	if err != nil {
		return nil, nil, err
	}
	lease, err := ticket.AcquireInput(ctx, 0)
	if err != nil {
		ticket.Close()
		return nil, nil, err
	}
	return lease, func() { lease.Release(); ticket.Close() }, nil
}

func growPublicationObject(ctx context.Context, memory *rscthrottler.InputLease, peak int64) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	err := memory.TryGrow(ctx, peak)
	var capacity *rscthrottler.WorkingSetCapacityError
	if errors.As(err, &capacity) {
		return &objectMemoryGrowth{bytes: peak}
	}
	return err
}

// An empty file still has one data chunk in the GETOBJECT protocol.
func objectChunkCount(size, chunkSize int64) (int64, error) {
	if _, err := publicationBufferBytes(size); err != nil {
		return 0, err
	}
	if chunkSize <= 0 {
		return 0, moerr.NewInternalErrorNoCtx("invalid GETOBJECT chunk size")
	}
	count := size / chunkSize
	if size%chunkSize != 0 {
		count++
	}
	return max(int64(1), count), nil
}
