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

import planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"

// AlterColumnHooks is an optional plan-layer hook for algorithm-specific
// metadata maintenance during ALTER TABLE column mutations.
type AlterColumnHooks interface {
	HandleAlterDropColumn(tableDef *planpb.TableDef, indexDef *planpb.IndexDef, colName string) (bool, error)
	HandleAlterRenameColumn(tableDef *planpb.TableDef, oldColName, newColName string) ([]string, error)
}

type RenameColumnRebuildHook interface {
	RenameColumnRequiresIndexRebuild(tableDef *planpb.TableDef, indexDef *planpb.IndexDef, oldColName string) (bool, error)
}

var (
	IncludedColumnAffected       func(indexDef *planpb.IndexDef, colName string) bool
	RenameIncludedColumnsForAlgo func(tableDef *planpb.TableDef, algo, oldColName, newColName string, syncAlgoParams bool) ([]string, error)
)
