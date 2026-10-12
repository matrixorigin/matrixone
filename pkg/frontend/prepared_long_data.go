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

package frontend

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// appendLongData keeps only the latest value, rather than placing every
// cumulative prefix in the parameter vector. MPool.Grow provides amortized
// linear copying and enforces the pool/global limits for off-heap allocations.
func (prepareStmt *PrepareStmt) appendLongData(
	ctx context.Context, proc *process.Process, index int, chunk []byte, limit int64,
) error {
	old := prepareStmt.longDataBuffers[index]
	if limit < 0 || int64(len(chunk)) > limit || int64(len(old)) > limit-int64(len(chunk)) {
		return moerr.NewInvalidInputf(ctx,
			"long data parameter exceeds max_allowed_packet (%d bytes)", limit)
	}
	if len(chunk) != 0 {
		pool := prepareStmt.longDataPool
		if pool == nil {
			pool = proc.Mp()
		}
		grown, err := pool.Grow(old, len(old)+len(chunk), true)
		if err != nil {
			return err
		}
		copy(grown[len(old):], chunk)
		if prepareStmt.longDataBuffers == nil {
			prepareStmt.longDataBuffers = make(map[int][]byte)
		}
		prepareStmt.longDataBuffers[index] = grown
		prepareStmt.longDataPool = pool
	}
	if prepareStmt.getFromSendLongData == nil {
		prepareStmt.getFromSendLongData = make(map[int]struct{})
	}
	prepareStmt.getFromSendLongData[index] = struct{}{}
	return nil
}

func (prepareStmt *PrepareStmt) releaseLongDataBuffer(index int) {
	if prepareStmt.longDataBuffers == nil {
		return
	}
	if data := prepareStmt.longDataBuffers[index]; data != nil {
		prepareStmt.longDataPool.Free(data)
	}
	delete(prepareStmt.longDataBuffers, index)
	if len(prepareStmt.longDataBuffers) == 0 {
		prepareStmt.longDataBuffers = nil
		prepareStmt.longDataPool = nil
	}
}

func (prepareStmt *PrepareStmt) releaseLongDataBuffers() {
	if prepareStmt == nil {
		return
	}
	for index := range prepareStmt.longDataBuffers {
		prepareStmt.releaseLongDataBuffer(index)
	}
}

// SEND_LONG_DATA has no response. Retain the first failure until RESET or
// CLOSE, and reject EXECUTE instead of running a partially received value.
func (prepareStmt *PrepareStmt) latchLongDataError(err error) {
	if prepareStmt.longDataErr == nil {
		prepareStmt.longDataErr = err
	}
	prepareStmt.releaseLongDataBuffers()
}
