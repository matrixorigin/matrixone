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

package projection

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// ActivatesAllocationAccountLifecycle reports that Projection never binds a
// statement allocation account: its output vectors are executor-owned and
// flat, so executors stay reusable across Reset.
func (projection *Projection) ActivatesAllocationAccountLifecycle() bool {
	return false
}

func (projection *Projection) SetAllocationAccount(account *mpool.AllocationAccount) error {
	if account == nil || account.Handle() == 0 {
		return mpool.ErrAllocationAccountInvalid
	}
	return nil
}

func (projection *Projection) ClearAllocationAccount(account *mpool.AllocationAccount) error {
	return nil
}
