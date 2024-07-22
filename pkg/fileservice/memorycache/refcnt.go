// Copyright 2022 Matrix Origin
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

package memorycache

import (
	"fmt"
	"sync/atomic"
)

// refcnt is an atomic reference counter
type refcnt struct {
	val atomic.Int32
}

func (r *refcnt) init(val int32) {
	r.val.Store(val)
}

func (r *refcnt) refs() int32 {
	return r.val.Load()
}

func (r *refcnt) acquire() {
	switch v := r.val.Add(1); {
	case v <= 1:
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	}
}

func (r *refcnt) release() bool {
	switch v := r.val.Add(-1); {
	case v < 0:
		panic(fmt.Sprintf("inconsistent refcnt count: %d", v))
	case v == 0:
		return true
	default:
		return false
	}
}
