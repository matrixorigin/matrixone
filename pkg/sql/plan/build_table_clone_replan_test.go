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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// subscriberOnlyCompilerContext mimics the engine for a subscriber: the
// subscription alias resolves to the publisher's table (whose DbName is the
// publisher's physical database), while the physical database itself is not
// visible to the subscriber.
type subscriberOnlyCompilerContext struct {
	*subscriptionScopeCompilerContext
	hiddenDB string
}

func (c *subscriberOnlyCompilerContext) Resolve(
	dbName string, tableName string, snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	if dbName == c.hiddenDB {
		return nil, nil, nil
	}
	obj, def, err := c.subscriptionScopeCompilerContext.Resolve(dbName, tableName, snapshot)
	if def != nil && dbName == c.subscription.SubName {
		def.DbName = c.subscription.DbName
	}
	return obj, def, err
}

// A CLONE of a subscribed table must plan the same way when the same AST is
// planned again (e.g. compile retry after an RC "def changed" error).
func TestBuildCloneTableSubscriptionReplanKeepsAlias(t *testing.T) {
	const rootSQL = "CREATE TABLE localdb.copy CLONE subdb.source"
	base := NewMockCompilerContext(false, newPlanTestProcess(t))
	base.dbs["localdb"] = true
	base.dbs["subdb"] = true
	base.tables["source"] = &plan.TableDef{
		Name:      "source",
		DbName:    "publisherdb",
		TableType: catalog.SystemOrdinaryRel,
		Cols: []*plan.ColDef{{
			Name: "id", OriginName: "id",
			Typ:     plan.Type{Id: int32(types.T_int32)},
			Default: &plan.Default{NullAbility: true},
		}},
	}
	base.objects["source"] = &plan.ObjectRef{
		SchemaName:       "publisherdb",
		ObjName:          "source",
		SubscriptionName: "subdb",
		PubInfo:          &plan.PubInfo{TenantId: 7},
	}
	ctx := &subscriberOnlyCompilerContext{
		subscriptionScopeCompilerContext: &subscriptionScopeCompilerContext{
			MockCompilerContext: base,
			subscription: &SubscriptionMeta{
				AccountId: 7, DbName: "publisherdb", SubName: "subdb", Tables: "*",
			},
		},
		hiddenDB: "publisherdb",
	}

	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, rootSQL, 1)
	require.NoError(t, err)
	defer stmt.Free()

	first, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)

	// Planning the same AST again must still resolve via the subscription.
	second, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	clone := stmt.(*tree.CloneTable)
	require.Equal(t, "subdb", string(clone.SrcTable.SchemaName))
	require.Equal(t, "source", string(clone.SrcTable.ObjectName))
	require.Equal(t,
		first.GetDdl().GetCloneTable().GetSrcTableDef().GetDbName(),
		second.GetDdl().GetCloneTable().GetSrcTableDef().GetDbName())
	require.Equal(t, "publisherdb", second.GetDdl().GetCloneTable().GetSrcTableDef().GetDbName())
	require.Equal(t, "localdb", second.GetDdl().GetCloneTable().GetDstDatabaseName())
	require.Equal(t, "copy", second.GetDdl().GetCloneTable().GetDstTableName())
}
