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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (b *HashBuild) buildOptionalIndex(proc *process.Process) error {
	hb := &b.ctr.hashmapBuilder
	bound, known := hb.BuildMemoryBound(b.Conditions, false, false)
	if err := proc.Ctx.Err(); err != nil {
		return err
	}
	// The bound includes reallocation overlap and key-executor scratch. Unknown
	// expressions or insufficient headroom retain the original loop evaluator.
	available := min(proc.Mp().Cap()-proc.Mp().CurrNB(), mpool.GlobalCap()-mpool.GlobalStats().NumCurrBytes.Load())
	if b.ctr.spillThreshold > 100000 {
		available = min(available, b.ctr.spillThreshold-hb.Batches.Allocated())
	}
	if !known || bound > available {
		hb.discardOptionalIndex(proc)
		return nil
	}
	err := hb.BuildHashmap(false, false, false, proc)
	if canceled := proc.Ctx.Err(); canceled != nil {
		return canceled
	}
	if moerr.IsMoErrCode(err, moerr.ErrOOM) ||
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
