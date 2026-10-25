// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// V1物理格式的旧表将CollationVersion用作UCA unique proof，而不是
// legacy general-ci/bin默认值的语义代际。不能重新解释历史双用途字段。
func tableColumnCollationRevision(table *TableDef) uint32 {
	if table.KeyFormat == collation.KeyFormatV1 && !types.IsUnicodeCollation(uint8(table.DefaultCharset)) {
		return 0
	}
	return table.CollationVersion
}

func rejectNativePADColumnRelabel(ctx context.Context, old, next Type) error {
	if (planpb.HasNativePADCollation(old) || planpb.HasNativePADCollation(next)) && !old.SameCollation(next) {
		return moerr.NewNotSupported(ctx, "online native PAD collation conversion is disabled; use explicit copy/reload")
	}
	return nil
}

// requireViewCollationMarker不从结果Type或floor推断绑定来源；即使结果
// 折叠成bool，revision1也必须携带native能力的durable要求。
func requireViewCollationMarker(ctx context.Context, data ViewData) error {
	if err := persistedCollationBindingRevision(ctx, data.CollationBindingRevision); err != nil {
		return err
	}
	if data.CollationBindingRevision == 1 && (data.RequiredProtocolVersion == nil || *data.RequiredProtocolVersion < defines.MORPCVersion132) {
		return moerr.NewInvalidInput(ctx, "collation binding revision 1 requires persisted protocol version 132")
	}
	return nil
}

// RequireCreateTableCollationsForAuthoring同时检查schema和CTAS fold前来源。
// 即使输出全是bool，写门回落也必须先于建表、锁、catalog修改而拒绝。
func RequireCreateTableCollationsForAuthoring(ctx context.Context, proc *process.Process, create *planpb.CreateTable) error {
	if create == nil {
		return nil
	}
	if err := requireViewCollationMarker(ctx, ViewData{CollationBindingRevision: create.CollationBindingRevision, RequiredProtocolVersion: &create.RequiredProtocolVersion}); err != nil {
		return err
	}
	if create.RequiredProtocolVersion < 0 {
		return moerr.NewInvalidInput(ctx, "CTAS protocol version must not be negative")
	}
	if err := RequirePersistedProtocolVersionForAuthoring(ctx, proc, create.RequiredProtocolVersion); err != nil {
		return err
	}
	if err := RequireCatalogCollationsForAuthoring(ctx, proc, create.TableDef); err != nil {
		return err
	}
	for _, index := range create.IndexTables {
		if err := RequireCatalogCollationsForAuthoring(ctx, proc, index); err != nil {
			return err
		}
	}
	return nil
}

// RequireCreateIndexCollationsForAuthoring uses the same complete-schema
// validation as ALTER ADD INDEX, rather than admitting a partial index schema.
func RequireCreateIndexCollationsForAuthoring(ctx context.Context, proc *process.Process, create *planpb.CreateIndex) error {
	if create == nil {
		return nil
	}
	return RequireAlterTableCollationsForAuthoring(ctx, proc, &planpb.AlterTable{
		TableDef: create.TableDef,
		Actions: []*planpb.AlterTable_Action{{Action: &planpb.AlterTable_Action_AddIndex{
			AddIndex: &planpb.AlterTableAddIndex{IndexInfo: create.Index},
		}}},
	})
}

// RequireAlterTableCollationsForAuthoring validates replacement schemas and
// action-only key additions together. CopyTableDef is optional for INPLACE;
// checking it alone would miss a new key over an existing native column.
// The validation view borrows columns but never mutates the source or plan.
func RequireAlterTableCollationsForAuthoring(ctx context.Context, proc *process.Process, alter *planpb.AlterTable) error {
	if alter == nil {
		return nil
	}
	if err := planpb.RequirePersistedCollations(alter); err != nil {
		return err
	}
	if err := RequirePersistedExpressionProtocolForAuthoring(ctx, proc, alter); err != nil {
		return err
	}
	// COPY must also retire its source, even when the replacement drops the
	// last native column. Do not defer that rejection to the internal DROP.
	if err := RequireCatalogCollationsForAuthoring(ctx, proc, alter.TableDef); err != nil {
		return err
	}
	target := alter.CopyTableDef
	if target == nil {
		target = alter.TableDef
	}
	if target == nil {
		return nil
	}
	if alter.CopyTableDef != nil && alter.TableDef != nil {
		byID := make(map[uint64]*planpb.ColDef, len(alter.TableDef.Cols))
		byName := make(map[string]*planpb.ColDef, len(alter.TableDef.Cols))
		nativeSource := false
		for _, col := range alter.TableDef.Cols {
			if col != nil {
				// Zero is a valid column ID. Duplicate IDs in old synthetic
				// definitions are not identity proof for a renamed column.
				if _, exists := byID[col.ColId]; exists {
					byID[col.ColId] = nil
				} else {
					byID[col.ColId] = col
				}
				byName[col.Name] = col
				nativeSource = nativeSource || planpb.HasNativePADCollation(col.Typ)
			}
		}
		for _, col := range target.Cols {
			if col == nil {
				continue
			}
			old, exists := byID[col.ColId]
			if exists && old == nil {
				old = byName[col.Name]
				if old == nil && (nativeSource || planpb.HasNativePADCollation(col.Typ)) {
					return moerr.NewInvalidInput(ctx, "ambiguous native PAD collation ALTER column identity")
				}
			}
			// A fresh ID (including DROP+ADD with the same name) is a new
			// column, not a semantic relabel of the source column.
			if old != nil {
				if err := rejectNativePADColumnRelabel(ctx, old.Typ, col.Typ); err != nil {
					return err
				}
			}
		}
	}
	if err := RequireCatalogCollationsForAuthoring(ctx, proc, target); err != nil {
		return err
	}
	for _, action := range alter.Actions {
		if action == nil {
			continue
		}
		// Validate each addition against the complete column set, not the
		// action's partial TableDef. An unsupported key may not be transiently
		// created either, even if a later action drops it.
		view := *target
		if add := action.GetAddIndex(); add != nil && add.IndexInfo != nil {
			if err := RequireCreateTableCollationsForAuthoring(ctx, proc, add.IndexInfo); err != nil {
				return err
			}
			if definition := add.IndexInfo.TableDef; definition != nil {
				view.Cols = append(append([]*planpb.ColDef(nil), target.Cols...), definition.Cols...)
				view.Indexes = append(append([]*planpb.IndexDef(nil), target.Indexes...), definition.Indexes...)
			}
		} else if add := action.GetAddFk(); add != nil {
			view.Fkeys = append(append([]*planpb.ForeignKeyDef(nil), target.Fkeys...), add.Fkey)
		} else {
			continue
		}
		if err := planpb.RequirePersistedCollations(&view); err != nil {
			return err
		}
	}
	return nil
}

// RequireCloneTableCollationsForAuthoring keeps the source read contract and
// target publication contract independent, including clones with no CREATE
// subplan used by internal restore owners.
func RequireCloneTableCollationsForAuthoring(ctx context.Context, proc *process.Process, clone *planpb.CloneTable) error {
	if clone == nil {
		return nil
	}
	if err := planpb.RequirePersistedCollations(clone); err != nil {
		return err
	}
	if err := RequireCatalogCollations(ctx, proc, clone.SrcTableDef); err != nil {
		return err
	}
	if err := RequireCatalogCollationsForAuthoring(ctx, proc, clone.SrcTableDef); err != nil {
		return err
	}
	return RequireCreateTableCollationsForAuthoring(ctx, proc, clone.GetCreateTable().GetDdl().GetCreateTable())
}

// RequireCatalogCollations用于完整catalog定义，必须先于恢复/裁剪等重写。
func RequireCatalogCollations(ctx context.Context, proc *process.Process, table *TableDef) error {
	return requireCatalogCollations(ctx, proc, table, false)
}

// RequireCatalogCollationsForAuthoring在实际DDL发布前再次检查，不能复用
// 仅表示读取能力的floor，也不能相信编译时已检查的快照。
func RequireCatalogCollationsForAuthoring(ctx context.Context, proc *process.Process, table *TableDef) error {
	return requireCatalogCollations(ctx, proc, table, true)
}

func requireCatalogCollations(ctx context.Context, proc *process.Process, table *TableDef, authoring bool) error {
	if err := planpb.RequirePersistedCollations(table); err != nil {
		return err
	}
	required, err := RequiredPersistedExpressionProtocolVersion(table)
	if err != nil {
		return err
	}
	if table != nil && table.ViewSql != nil {
		var data ViewData
		if err = json.Unmarshal([]byte(table.ViewSql.View), &data); err != nil {
			return err
		}
		if err = requireViewCollationMarker(ctx, data); err != nil {
			return err
		}
		if data.RequiredProtocolVersion != nil {
			if *data.RequiredProtocolVersion < 0 {
				return moerr.NewInvalidInput(ctx, "persisted view protocol version must not be negative")
			}
			required = max(required, *data.RequiredProtocolVersion)
		}
	}
	if authoring {
		return RequirePersistedProtocolVersionForAuthoring(ctx, proc, required)
	}
	return RequirePersistedProtocolVersion(ctx, proc, required)
}
