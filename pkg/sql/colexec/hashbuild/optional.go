// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashbuild

import (
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/hashtable"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Match this sentinel by identity, not ErrInternal or its message: unrelated
// internal errors must still propagate instead of becoming index-free success.
var errOptionalIndexBudget = moerr.NewInternalErrorNoCtx("optional membership index budget exceeded")

// overhead bounds key/executor scratch independently of distinct-key count.
// limit is the initial incremental budget; admit also checks current headroom.
// This state is local to one build and is never published with the JoinMap.
type optionalIndexBudget struct {
	overhead, limit int64
}

func (b *optionalIndexBudget) admit(hb *HashmapBuilder, proc *process.Process, rows uint64) error {
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	_, peak, ok := hashtable.BuildMemoryBound(rows, hb.UsesStringKeys())
	if !ok || b.overhead > b.limit || peak > b.limit-b.overhead {
		return errOptionalIndexBudget
	}
	var retained int64
	if hb.IntHashMap != nil {
		retained = hb.IntHashMap.Size()
	}
	if hb.StrHashMap != nil {
		retained = hb.StrHashMap.Size()
	}
	// The peak includes the old allocation during growth. Credit only the
	// table already owned by this build, not another operator's allocations.
	remaining := min(proc.Mp().Cap()-proc.Mp().CurrNB(), mpool.GlobalCap()-mpool.GlobalStats().NumCurrBytes.Load())
	if peak-retained+b.overhead > remaining {
		return errOptionalIndexBudget
	}
	return nil
}

func (hashBuild *HashBuild) buildOptionalIndex(proc *process.Process) error {
	hb := &hashBuild.ctr.hashmapBuilder
	bound, known := hb.buildMemoryBound(proc.Ctx, hashBuild.Conditions, false, false, 0)
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	// Prove key/executor scratch once. Table capacity is instead admitted
	// incrementally, so duplicate input rows do not count as distinct keys.
	available := min(proc.Mp().Cap()-proc.Mp().CurrNB(), mpool.GlobalCap()-mpool.GlobalStats().NumCurrBytes.Load())
	if hashBuild.ctr.spillThreshold > 100000 {
		available = min(available, hashBuild.ctr.spillThreshold-hb.Batches.Allocated())
	}
	_, initialPeak, ok := hashtable.BuildMemoryBound(0, hb.UsesStringKeys())
	if !known || !ok || bound < initialPeak {
		hb.discardOptionalIndex(proc)
		return nil
	}
	budget := &optionalIndexBudget{overhead: bound - initialPeak, limit: available}
	err := hb.buildHashmap(false, false, false, false, proc, budget)
	if canceled := proc.Ctx.Err(); canceled != nil {
		return canceled
	}
	if errors.Is(err, errOptionalIndexBudget) || moerr.IsMoErrCode(err, moerr.ErrOOM) ||
		(moerr.IsMoErrCode(err, moerr.ErrInternal) && strings.HasPrefix(err.Error(), "internal error: mpool out of space, alloc ")) {
		// Admission is not an exclusive reservation: another operator may consume
		// memory meanwhile. Retire partial index state, but keep all RHS batches.
		hb.discardOptionalIndex(proc)
		return nil
	}
	return err
}

func (hb *HashmapBuilder) discardOptionalIndex(proc *process.Process) {
	hb.cachedIntIterator = nil
	hb.cachedStrIterator = nil
	if hb.IntHashMap != nil {
		hb.IntHashMap.Free()
		hb.IntHashMap = nil
	}
	if hb.StrHashMap != nil {
		hb.StrHashMap.Free()
		hb.StrHashMap = nil
	}
	if hb.needDupVec {
		for _, vec := range hb.curVecs {
			if vec != nil {
				vec.Free(proc.Mp())
			}
		}
	}
	hb.curVecs = nil
	hb.FreeExecutors()
	hb.hasNullKey = false
}
