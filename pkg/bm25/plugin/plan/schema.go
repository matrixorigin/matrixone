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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BuildSecondaryIndexDefs constructs the bm25 index def + its two hidden
// tables (storage + metadata) from CREATE INDEX ... USING bm25. bm25 parses
// to *tree.Index and is dispatched here (the vector-plugin path), NOT through
// BuildFullTextIndexDefs.
//
// STUB (Phase 1b): the real builder (validate single TEXT/VARCHAR column,
// emit the storage+metadata TableDefs) lands in Phase 2.
func (Hooks) BuildSecondaryIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.Index,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNYINoCtx("bm25 BuildSecondaryIndexDefs (Phase 2)")
}

// BuildFullTextIndexDefs — bm25 is not reached via CREATE FULLTEXT INDEX
// (*tree.FullTextIndex); it uses BuildSecondaryIndexDefs instead.
func (Hooks) BuildFullTextIndexDefs(
	_ planplugin.CompilerContext,
	_ *tree.FullTextIndex,
	_ map[string]*plan.ColDef,
	_ []*plan.IndexDef,
	_ string,
) ([]*plan.IndexDef, []*plan.TableDef, error) {
	return nil, nil, moerr.NewNotSupportedNoCtx("bm25 plugin does not build fulltext indexes")
}
