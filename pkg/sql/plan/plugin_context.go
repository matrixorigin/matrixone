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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"

	// Blank-import vector-index plugins so their init() registrations fire
	// any time this package is loaded.
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/hnsw/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin"
	_ "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin"
)

// exportMultiTableIndex copies a package-private *MultiTableIndex into the
// exported *vectorplan.MultiTableIndexRef so it can cross the plugin
// boundary without leaking pkg/sql/plan internals.
func exportMultiTableIndex(m *MultiTableIndex) *vectorplan.MultiTableIndexRef {
	if m == nil {
		return nil
	}
	return &vectorplan.MultiTableIndexRef{
		IndexAlgo:       m.IndexAlgo,
		IndexAlgoParams: m.IndexAlgoParams,
		IndexDefs:       m.IndexDefs,
	}
}
