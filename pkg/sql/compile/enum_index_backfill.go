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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// The source is the relation whose DDL owns this build; resolve the just-created
// hidden table in the same transaction and carry its physical identity too.
func newEnumIndexBackfillSpec(c *Compile, sourceID uint64, db engine.Database, database string, source, target *plan.TableDef, index *plan.IndexDef) (*executor.EnumIndexBackfillSpec, error) {
	if source == nil || target == nil || index == nil {
		return nil, moerr.NewInternalErrorNoCtx("missing index source")
	}
	usesNewIdentity, err := sqlplan.EnumIndexUsesNewIdentity(source, index)
	if err != nil {
		return nil, err
	}
	if !usesNewIdentity {
		return nil, nil
	}
	wanted := make(map[string]bool)
	for _, name := range index.Parts {
		wanted[strings.ToLower(catalog.ResolveAlias(name))] = true
	}
	if source.Pkey != nil {
		wanted[strings.ToLower(source.Pkey.PkeyColName)] = true
		for _, name := range source.Pkey.Names {
			wanted[strings.ToLower(name)] = true
		}
	}
	hasNewIdentity := false
	for _, col := range source.Cols {
		if col != nil && wanted[strings.ToLower(col.Name)] &&
			col.Typ.Id == int32(types.T_enum) && col.Typ.EnumCollation != 0 {
			hasNewIdentity = true
			break
		}
	}
	if !hasNewIdentity {
		return nil, moerr.NewNotSupportedNoCtx("index has an unverified derived ENUM identity dependency")
	}
	account, err := defines.GetAccountId(c.proc.Ctx)
	if err != nil {
		return nil, err
	}
	rel, err := db.Relation(c.proc.Ctx, index.IndexTableName, nil)
	if err != nil {
		return nil, err
	}
	targetID := rel.GetTableID(c.proc.Ctx)
	if sourceID == 0 || targetID == 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid ENUM index relation identity")
	}
	return &executor.EnumIndexBackfillSpec{AccountID: account, Database: database, SourceID: sourceID, TargetID: targetID, Source: source, Target: target, Index: index}, nil
}

func enumIndexStatementOption(opt executor.StatementOption, spec *executor.EnumIndexBackfillSpec, sql string, duplicateCheck bool) executor.StatementOption {
	if spec == nil {
		return opt
	}
	statement := *spec
	statement.SQL, statement.DuplicateCheck = sql, duplicateCheck
	return opt.WithEnumIndexBackfill(&statement)
}
