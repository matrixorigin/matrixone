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

package docfilter

// AcquireMemoryForTest lets the external consumer test preserve the exact
// reservation oracle without adding a production API or importing the CN
// throttler (which depends on ObjectIO and docfilter) into this package's tests.
func AcquireMemoryForTest(admission MemoryAdmission, bytes int64) (func(), error) {
	return acquireMemory(admission, bytes)
}
