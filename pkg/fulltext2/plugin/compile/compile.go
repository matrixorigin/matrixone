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

// Package compile holds the fulltext2 index's compile-layer (DDL) hooks.
package compile

import (
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ compileplugin.Hooks = Hooks{}

type Hooks struct{}

// HandleCreateIndex creates the storage + metadata hidden tables. The
// build-from-source and CDC-task registration land in later steps; until then a
// freshly created fulltext2 index is empty (MATCH returns no rows).
func (Hooks) HandleCreateIndex(ctx compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	if info := ctx.IndexInfo(); info != nil {
		for _, table := range info.GetIndexTables() {
			if err := ctx.BuildIndexTable(table); err != nil {
				return err
			}
		}
	}
	return nil
}

// HandleReindex — ALTER … REINDEX (build/merge) lands with the build path.
func (Hooks) HandleReindex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef, _, _ bool) error {
	return nil
}

// RestoreInitSQL — the clone copies the storage+metadata rows; register CDC from
// the post-clone TS. A non-empty InitSQL is required for startFromNow.
func (Hooks) RestoreInitSQL(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) (bool, string, error) {
	return true, "SELECT 1", nil
}

// ValidateReindexParams — no reindex-time params.
func (Hooks) ValidateReindexParams(old map[string]string, _ compileplugin.ReindexParamUpdate) (map[string]string, error) {
	return old, nil
}

// HandleDropIndex — no algorithm-specific cleanup beyond the generic hidden-table
// deletion the SQL layer performs.
func (Hooks) HandleDropIndex(_ compileplugin.CompileContext, _ map[string]*plan.IndexDef) error {
	return nil
}

// IdxcronMetadata — no idxcron action yet.
func (Hooks) IdxcronMetadata(_ compileplugin.CompileContext) ([]byte, error) {
	return nil, nil
}
