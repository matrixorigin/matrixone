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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// requireViewCollationMarker不从结果Type或floor推断绑定来源；即使结果
// 折叠成bool，revision1也必须携带119的durable要求。
func requireViewCollationMarker(ctx context.Context, data ViewData) error {
	if err := persistedCollationBindingRevision(ctx, data.CollationBindingRevision); err != nil {
		return err
	}
	if data.CollationBindingRevision == 1 && (data.RequiredProtocolVersion == nil || *data.RequiredProtocolVersion < defines.MORPCVersion119) {
		return moerr.NewInvalidInput(ctx, "collation binding revision 1 requires persisted protocol version 119")
	}
	return nil
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
