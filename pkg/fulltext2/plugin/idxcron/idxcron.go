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

// Package idxcron holds the fulltext2 index's scheduled-reindex hook. No
// scheduled reindex yet (SyncDescriptor().IdxcronAction == ""), so Updatable is
// inert; it activates with the CDC/idxcron step.
package idxcron

import (
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
)

var _ idxcronplugin.Hooks = Hooks{}

type Hooks struct{}

// Updatable — fulltext2 has no scheduled reindex yet.
func (Hooks) Updatable(_ idxcronplugin.UpdatableInput) (bool, string, error) {
	return false, "", nil
}
