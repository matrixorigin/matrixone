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

// Package idxcron is fulltext's idxcron hook implementation. Fulltext
// does not participate in scheduled rebuilds (no IdxcronAction in its
// SyncDescriptor); the hook is unreachable in practice.
package idxcron

import (
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

func (Hooks) Updatable(_ *sqlexec.SqlProcess, _ *plan.TableDef, _ string) (bool, string, error) {
	return true, "", nil
}
