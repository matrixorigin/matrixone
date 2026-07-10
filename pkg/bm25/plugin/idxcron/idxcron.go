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

// Package idxcron implements the bm25 plugin's cron-side gating hook.
package idxcron

import (
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
)

// Compile-time interface check.
var _ idxcronplugin.Hooks = Hooks{}

// Hooks implements plugin/idxcron.Hooks for bm25.
type Hooks struct{}

// Updatable — bm25 has no minimum-size constraint; the scheduled
// merge-compaction may always fire once the executor's time-cadence checks
// pass. The tag=1 tail-threshold gating (skip when the CDC tail is small)
// lands in Phase 4.
func (Hooks) Updatable(idxcronplugin.UpdatableInput) (bool, string, error) {
	return true, "", nil
}
