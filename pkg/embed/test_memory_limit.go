// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embed

import (
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// testProcessMemoryLimit is the private memory limit of a process that runs
// embedded test clusters. CI runs several test binaries on one runner, so the
// host's available memory mostly reflects the other binaries; measuring
// admission against it made queries fail at random. Against a private limit,
// execution admission still rejects when this process itself grows too large,
// but other processes no longer decide the outcome.
const testProcessMemoryLimit = uint64(8 << 30)

// useTestProcessMemoryLimit is called before a test cluster starts. Every test
// cluster in the process wants the same limit, so storing it again is
// harmless.
func useTestProcessMemoryLimit() {
	process.SetExecutionPrivateMemoryLimit(testProcessMemoryLimit)
}
