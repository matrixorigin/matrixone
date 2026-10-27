// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func validateM0CopyExitTarget(source, target *plan.TableDef) error {
	if source == nil || source.AppendMissingState == nil {
		return nil
	}
	if err := plan.ValidateAppendMissingTable(source); err != nil {
		return err
	}
	invalid := func() error {
		return moerr.NewInternalErrorNoCtx("M0 COPY exit: target is not a complete ordinary layout")
	}
	if target == nil || target.AppendMissingState != nil || target.Version != 0 || len(target.Cols) < 2 {
		return invalid()
	}
	ordinary := uint32(0)
	for _, col := range target.Cols {
		if col == nil {
			return invalid()
		}
		if col.Name == catalog.Row_ID {
			continue
		}
		if col.Seqnum != ordinary || col.ColId != uint64(ordinary)+1 {
			return invalid()
		}
		ordinary++
	}
	if ordinary == 0 {
		return invalid()
	}
	return nil
}

func m0CopyExitRenameSQL(database string, rename *plan.AlterTableName) string {
	return "alter table " + sqlquote.QualifiedIdent(database, rename.OldName) + " rename to " + sqlquote.Ident(rename.NewName)
}
