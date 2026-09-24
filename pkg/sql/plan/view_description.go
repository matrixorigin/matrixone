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

package plan

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

const MaxViewMetadataColumns = 4096

// viewDescriptionDependencyContext records every relation resolved while
// regenerating the View (including sources of nested Views). The resulting
// VALUES plan has no scan nodes from which PREPARE could infer dependencies.
type viewDescriptionDependencyContext struct {
	CompilerContext
	refs               []*ObjectRef
	publisherBinding   bool
	publisherAccountID uint32
}

func (c *viewDescriptionDependencyContext) record(obj *ObjectRef, def *TableDef, snapshot *Snapshot) {
	ref := prepareSchemaRefWithSnapshot(obj, def, snapshot)
	// The isolated subscription binder runs as the publisher. Its source
	// ObjectRefs need that account identity on the subscriber's prepared plan;
	// otherwise validation incorrectly looks in the subscriber's catalog.
	if c.publisherBinding && ref.PubInfo == nil &&
		!slices.Contains(catalog.SystemDatabases, strings.ToLower(ref.SchemaName)) {
		ref.PubInfo = &planpb.PubInfo{TenantId: int32(c.publisherAccountID)}
	}
	c.refs = appendPrepareSchemas(c.refs, ref)
}

func (c *viewDescriptionDependencyContext) Resolve(db, name string, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	obj, def, err := c.CompilerContext.Resolve(db, name, snapshot)
	if err == nil && obj != nil && def != nil {
		c.record(obj, def, snapshot)
	}
	return obj, def, err
}

func (c *viewDescriptionDependencyContext) ResolveById(id uint64, snapshot *Snapshot) (*ObjectRef, *TableDef, error) {
	obj, def, err := c.CompilerContext.ResolveById(id, snapshot)
	if err == nil && obj != nil && def != nil {
		c.record(obj, def, snapshot)
	}
	return obj, def, err
}

func (c *viewDescriptionDependencyContext) ResolveViewDependencyAccount(
	obj *ObjectRef, def *TableDef, snapshot *Snapshot,
) (uint32, error) {
	if resolver, ok := c.CompilerContext.(ViewDependencyIdentityResolver); ok {
		return resolver.ResolveViewDependencyAccount(obj, def, snapshot)
	}
	accountID, err := c.GetAccountId()
	if obj.PubInfo != nil {
		accountID = uint32(obj.PubInfo.TenantId)
	} else if snapshot != nil && snapshot.Tenant != nil {
		accountID = snapshot.Tenant.TenantID
	}
	return accountID, err
}

// viewDescriptionRelation returns the column row source used by SHOW's existing
// formatting and filtering expressions. No rows are read from the persisted
// View column snapshot, and no generated definition is written back.
func viewDescriptionRelation(
	ctx CompilerContext, def *TableDef, accountID uint32, databaseName, viewName string,
) (string, []*ObjectRef, error) {
	if sub := ctx.GetQueryingSubscription(); sub != nil {
		provider, ok := ctx.(ViewDescriptionContextProvider)
		if !ok {
			return "", nil, moerr.NewNotSupported(ctx.GetContext(), "subscription View description requires an isolated binding context")
		}
		child, cleanup, err := provider.NewViewDescriptionCompilerContext(ctx.GetContext())
		if err != nil {
			return "", nil, err
		}
		defer cleanup()
		child.SetContext(defines.AttachAccountId(child.GetContext(), accountID))
		child.SetQueryingSubscription(sub)
		ctx = child
	}
	dependencies := &viewDescriptionDependencyContext{CompilerContext: ctx}
	if ctx.GetQueryingSubscription() != nil {
		dependencies.publisherBinding = true
		dependencies.publisherAccountID = accountID
	}
	cols, err := DescribeViewColumns(dependencies, def.ViewSql.View)
	if err != nil {
		return "", nil, err
	}
	if len(cols) > MaxViewMetadataColumns {
		return "", nil, moerr.NewInternalError(ctx.GetContext(), "View metadata exceeds its column budget")
	}
	if len(cols) == 0 {
		return "", nil, moerr.NewInternalError(ctx.GetContext(), "View has no output columns")
	}
	rows := make([]string, 0, len(cols))
	for i, col := range cols {
		if err := ctx.GetContext().Err(); err != nil {
			return "", nil, err
		}
		typ := MakeTypeByPlan2Type(col.Typ)
		typeBytes, err := types.Encode(&typ)
		if err != nil {
			return "", nil, err
		}
		defaultDef := col.Default
		if defaultDef == nil {
			defaultDef = &planpb.Default{NullAbility: !col.Typ.NotNullable}
		}
		defaultBytes, err := types.Encode(defaultDef)
		if err != nil {
			return "", nil, err
		}
		notNull := 0
		if col.Typ.NotNullable {
			notNull = 1
		}
		rows = append(rows, fmt.Sprintf(
			"row(%d,%d,%s,%s,%s,%d,unhex('%s'),%s,%d,unhex('%s'),0,0,0,cast(null as varchar),'')",
			accountID, def.TblId, formatStrLit(databaseName), formatStrLit(viewName),
			formatStrLit(col.GetOriginCaseName()), i+1, hex.EncodeToString(typeBytes), formatStrLit(col.Typ.Enumvalues), notNull,
			hex.EncodeToString(defaultBytes)))
	}
	return "(select * from (values " + strings.Join(rows, ",") + ") as view_columns(" +
		"account_id,att_relname_id,att_database,att_relname,attname,attnum,atttyp,attr_enum,attnotnull," +
		"att_default,att_is_hidden,att_is_auto_increment,attr_has_generated,attr_generated,att_comment))", dependencies.refs, nil
}
