// Copyright 2021-2024 Matrix Origin
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

package runtime

import (
	"sync"
)

func InTesting(
	sid string,
) bool {
	_, ok := ServiceRuntime(sid).GetGlobalVariables(TestingContextKey)
	return ok
}

func SetupServiceRuntimeTestingContext(
	sid string,
) {
	ServiceRuntime(sid).SetGlobalVariables(TestingContextKey, &TestingContext{})
}

func MustGetTestingContext(
	sid string,
) *TestingContext {
	v, ok := ServiceRuntime(sid).GetGlobalVariables(TestingContextKey)
	if !ok {
		panic("testing context not found")
	}
	return v.(*TestingContext)
}

type TestingContext struct {
	sync.RWMutex

	adjustCheckDataChangedAfterLocked func(txnID []byte, tableID uint64, changed bool) bool
}

func (tc *TestingContext) SetAdjustCheckDataChangedAfterLocked(
	fn func(txnID []byte, tableID uint64, changed bool) bool,
) {
	tc.Lock()
	defer tc.Unlock()
	tc.adjustCheckDataChangedAfterLocked = fn
}

func (tc *TestingContext) GetAdjustCheckDataChangedAfterLocked() func(txnID []byte, tableID uint64, changed bool) bool {
	tc.RLock()
	defer tc.RUnlock()
	if tc.adjustCheckDataChangedAfterLocked == nil {
		return func(txnID []byte, tableID uint64, changed bool) bool {
			return changed
		}
	}
	return tc.adjustCheckDataChangedAfterLocked
}
