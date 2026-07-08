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

// Package idxcron is HNSW's idxcron hook implementation. HNSW does
// not participate in scheduled rebuilds today (SyncDescriptor.IdxcronAction
// is empty in HNSW's runtime), so the hook is unreachable in
// practice — it satisfies the AlgoPlugin contract with a trivial
// "always rebuild" body.
package idxcron

import (
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

// Updatable — HNSW has no minimum-size constraint and no idxcron
// action wired today. Returns true unconditionally so the (unreached)
// cron path doesn't surprise-skip if anyone wires HNSW into idxcron.
func (Hooks) Updatable(in idxcronplugin.UpdatableInput) (bool, string, error) {
	logutil.Infof("[plugin] hnsw Updatable: index=%s", in.IndexName)
	return true, "", nil
}
