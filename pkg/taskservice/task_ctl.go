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

package taskservice

import (
	"sync/atomic"
)

// TODO: refactor to use Export function
var (
	disableTaskFramework atomic.Value // bool
)

// DebugCtlTaskFramwork disable task framework
func DebugCtlTaskFramwork(disable bool) {
	disableTaskFramework.Store(disable)
}

func taskFrameworkDisabled() bool {
	if v := disableTaskFramework.Load(); v != nil {
		return v.(bool)
	}
	return false
}
