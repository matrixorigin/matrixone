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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var _ planplugin.AlterColumnHooks = Hooks{}
var _ planplugin.RenameColumnRebuildHook = Hooks{}

func (Hooks) HandleAlterDropColumn(_ *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, colName), nil
}

func (Hooks) HandleAlterRenameColumn(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error) {
	return planplugin.RenameIncludedColumnsForAlgo(tableDef, catalog.MoIndexIvfFlatAlgo.ToString(), oldColName, newColName, false)
}

func (Hooks) RenameColumnRequiresIndexRebuild(_ *planpb.TableDef, indexDef *planpb.IndexDef, oldColName string) (bool, error) {
	return planplugin.IncludedColumnAffected(indexDef, oldColName), nil
}
