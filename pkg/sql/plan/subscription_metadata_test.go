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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

type subscriptionMetadataTestContext struct {
	*MockCompilerContext
	subscription  *SubscriptionMeta
	subscriptions map[string]*SubscriptionMeta
	querying      *SubscriptionMeta
	defaultDB     string
	metadata      []*SubscriptionMeta
}

func (c *subscriptionMetadataTestContext) subscriptionFor(databaseName string) *SubscriptionMeta {
	if strings.EqualFold(databaseName, c.subscription.SubName) {
		return c.subscription
	}
	for name, subscription := range c.subscriptions {
		if strings.EqualFold(databaseName, name) {
			return subscription
		}
	}
	return nil
}

func (c *subscriptionMetadataTestContext) GetSubscriptionMeta(
	databaseName string,
	_ *Snapshot,
) (*SubscriptionMeta, error) {
	return c.subscriptionFor(databaseName), nil
}

func (c *subscriptionMetadataTestContext) GetSubscriptionMetas(
	_ *Snapshot,
) ([]*SubscriptionMeta, error) {
	if c.metadata != nil {
		return c.metadata, nil
	}
	metas := []*SubscriptionMeta{c.subscription}
	for _, subscription := range c.subscriptions {
		metas = append(metas, subscription)
	}
	return metas, nil
}

func (c *subscriptionMetadataTestContext) SetQueryingSubscription(subscription *SubscriptionMeta) {
	c.querying = subscription
}

func (c *subscriptionMetadataTestContext) GetQueryingSubscription() *SubscriptionMeta {
	return c.querying
}

func (c *subscriptionMetadataTestContext) DefaultDatabase() string {
	if c.defaultDB != "" {
		return c.defaultDB
	}
	return c.MockCompilerContext.DefaultDatabase()
}

func (c *subscriptionMetadataTestContext) DatabaseExists(
	databaseName string,
	snapshot *Snapshot,
) bool {
	return c.subscriptionFor(databaseName) != nil ||
		c.MockCompilerContext.DatabaseExists(databaseName, snapshot)
}

func (c *subscriptionMetadataTestContext) Resolve(
	databaseName string,
	tableName string,
	snapshot *Snapshot,
) (*ObjectRef, *TableDef, error) {
	if strings.EqualFold(databaseName, INFORMATION_SCHEMA) &&
		strings.EqualFold(tableName, informationSchemaStatistics) {
		obj, tableDef := subscriptionStatisticsTestView()
		return obj, tableDef, nil
	}
	subscription := c.subscriptionFor(databaseName)
	if subscription == nil {
		return c.MockCompilerContext.Resolve(databaseName, tableName, snapshot)
	}
	obj, tableDef, err := c.MockCompilerContext.Resolve(subscription.DbName, tableName, snapshot)
	if err != nil || tableDef == nil {
		return obj, tableDef, err
	}
	objectID := int64(tableDef.TblId)
	if obj != nil {
		objectID = obj.Obj
	}
	obj = &ObjectRef{
		SchemaName:       subscription.DbName,
		ObjName:          tableName,
		Obj:              objectID,
		SubscriptionName: subscription.SubName,
		PubInfo:          &plan.PubInfo{TenantId: subscription.AccountId},
	}
	return obj, tableDef, nil
}

func subscriptionStatisticsTestView() (*ObjectRef, *TableDef) {
	viewData, _ := json.Marshal(ViewData{
		Stmt:            sysview.InformationSchemaStatisticsDDL,
		DefaultDatabase: INFORMATION_SCHEMA,
		SecurityType:    "DEFINER",
	})
	columnNames := []string{
		"table_catalog", "table_schema", "table_name", "non_unique", "index_schema",
		"index_name", "seq_in_index", "column_name", "collation", "cardinality",
		"sub_part", "packed", "nullable", "index_type", "comment", "index_comment",
		"is_visible", "expression",
	}
	columns := make([]*ColDef, 0, len(columnNames))
	for _, columnName := range columnNames {
		columns = append(columns, &ColDef{
			Name: columnName,
			Typ:  Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		})
	}
	return &ObjectRef{SchemaName: INFORMATION_SCHEMA, ObjName: informationSchemaStatistics}, &TableDef{
		Name:      informationSchemaStatistics,
		DbName:    INFORMATION_SCHEMA,
		TableType: catalog.SystemViewRel,
		Cols:      columns,
		ViewSql:   &plan.ViewDef{View: string(viewData)},
	}
}

type subscriptionMetadataTestOptimizer struct {
	ctx CompilerContext
}

func (o subscriptionMetadataTestOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	built, err := BuildPlan(o.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return built.GetQuery(), nil
}

func (o subscriptionMetadataTestOptimizer) CurrentContext() CompilerContext {
	return o.ctx
}

func newSubscriptionMetadataTestOptimizer() (Optimizer, *subscriptionMetadataTestContext) {
	ctx := &subscriptionMetadataTestContext{
		MockCompilerContext: NewMockCompilerContext(true),
		subscription: &SubscriptionMeta{
			AccountId: 0,
			DbName:    "tpch",
			SubName:   "sub_db",
			Tables:    "nation",
		},
		subscriptions: map[string]*SubscriptionMeta{
			"sub_b": {
				AccountId: 0,
				DbName:    "tpch",
				SubName:   "sub_b",
				Tables:    "nation",
			},
		},
	}
	return subscriptionMetadataTestOptimizer{ctx: ctx}, ctx
}

func TestSubscriptionStatisticsRoutesCatalogScansToPublisher(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	queryPlan, err := runOneStmt(optimizer, t,
		"select table_schema, index_schema, table_name, index_name "+
			"from information_schema.statistics "+
			"where table_schema = 'sub_db' and table_name = 'nation'")
	require.NoError(t, err)
	require.Nil(t, ctx.GetQueryingSubscription(), "planning must restore the session compiler context")

	seen := make(map[string]map[string]bool)
	seenLocal := make(map[string]bool)
	for _, node := range queryPlan.GetQuery().GetNodes() {
		if node.GetNodeType() != plan.Node_TABLE_SCAN || node.GetObjRef().GetSchemaName() != "mo_catalog" {
			continue
		}
		name := node.GetObjRef().GetObjName()
		if name != "mo_indexes" && name != "mo_tables" && name != "mo_columns" {
			continue
		}
		if node.GetObjRef().GetPubInfo() == nil {
			seenLocal[name] = true
			continue
		}
		subscriptionName := node.GetObjRef().GetSubscriptionName()
		if seen[subscriptionName] == nil {
			seen[subscriptionName] = make(map[string]bool)
		}
		seen[subscriptionName][name] = true
		require.Equal(t, int32(0), node.GetObjRef().GetPubInfo().GetTenantId(), name)
		require.True(t, node.GetNotCacheable(), name)
		if name == catalog.MO_TABLES {
			require.NotEmpty(t, node.GetFilterList(), "subscription mo_tables scan must be constrained to the publication")
		}
	}
	wantCatalogTables := map[string]bool{
		"mo_indexes": true,
		"mo_tables":  true,
		"mo_columns": true,
	}
	require.Equal(t, wantCatalogTables, seenLocal)
	require.Equal(t, map[string]map[string]bool{
		"sub_b":  wantCatalogTables,
		"sub_db": wantCatalogTables,
	}, seen)
}

func TestSubscriptionShowIndexRoutesCatalogScansToPublisher(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	queryPlan, err := runOneStmt(optimizer, t, "show index from sub_db.nation")
	require.NoError(t, err)
	require.Nil(t, ctx.GetQueryingSubscription(), "SHOW INDEX must restore the session compiler context")

	seen := make(map[string]bool)
	for _, node := range queryPlan.GetQuery().GetNodes() {
		if node.GetNodeType() != plan.Node_TABLE_SCAN || node.GetObjRef().GetSchemaName() != "mo_catalog" {
			continue
		}
		name := node.GetObjRef().GetObjName()
		if name != "mo_indexes" && name != "mo_columns" {
			continue
		}
		seen[name] = true
		require.NotNil(t, node.GetObjRef().GetPubInfo(), name)
		require.Equal(t, int32(0), node.GetObjRef().GetPubInfo().GetTenantId(), name)
		require.True(t, node.GetNotCacheable(), name)
	}
	require.Equal(t, map[string]bool{"mo_indexes": true, "mo_columns": true}, seen)
}

func TestSubscriptionStatisticsPreservesSourcesAcrossQueryShapes(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "account wide",
			sql:  "select table_schema, index_name from information_schema.statistics where table_name = 'nation'",
		},
		{
			name: "join on",
			sql: "select s.index_name from information_schema.statistics s " +
				"join mo_catalog.mo_database d on s.table_schema = 'sub_db' and d.datname = s.table_schema",
		},
		{
			name: "derived outer predicate",
			sql: "select index_name from (select * from information_schema.statistics) s " +
				"where table_schema = 'sub_db'",
		},
		{
			name: "ambiguous schemas",
			sql: "select index_name from information_schema.statistics " +
				"where table_schema = 'sub_db' or table_schema = 'tpch'",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			queryPlan, err := runOneStmt(optimizer, t, test.sql)
			require.NoError(t, err)
			requireStatisticsPublisherScopes(t, queryPlan.GetQuery(), map[string]int32{
				"sub_db": 0,
				"sub_b":  0,
			})
			require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()),
				"the subscriber-local catalog must remain in the account-wide source")
			require.Nil(t, ctx.GetQueryingSubscription())
		})
	}

	filter := tree.String(subscriptionMoTablesFilter(ctx.subscription), dialect.MYSQL)
	require.Contains(t, filter, "reldatabase = tpch")
	require.Contains(t, filter, "relname in (nation)")
	require.NotContains(t, filter, "region")
}

func TestSubscriptionStatisticsExpandsEachOccurrence(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()

	nestedPlan, err := runOneStmt(optimizer, t,
		"select s.index_name from information_schema.statistics s "+
			"where s.table_schema = 'sub_db' and exists ("+
			"select 1 from information_schema.statistics t where t.table_schema = 'sub_b')")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"sub_db": 2, "sub_b": 2},
		statisticsPublisherScanCounts(nestedPlan.GetQuery()))
	require.Nil(t, ctx.GetQueryingSubscription())

	siblingPlan, err := runOneStmt(optimizer, t,
		"select a.index_name from information_schema.statistics a "+
			"join information_schema.statistics b on a.table_name = b.table_name "+
			"where a.table_schema = 'sub_db' and b.table_schema = 'sub_b'")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"sub_db": 2, "sub_b": 2},
		statisticsPublisherScanCounts(siblingPlan.GetQuery()))
	require.Nil(t, ctx.GetQueryingSubscription())
}

func TestPreparedSubscriptionMetadataPreservesAllSources(t *testing.T) {
	_, ctx := newSubscriptionMetadataTestOptimizer()
	statements, err := mysql.Parse(context.Background(),
		"select index_name from information_schema.statistics "+
			"where table_schema = ? and table_name = ?", 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	queryPlan, err := BuildPlan(ctx, statements[0], true)
	require.NoError(t, err)
	requireStatisticsPublisherScopes(t, queryPlan.GetQuery(), map[string]int32{
		"sub_db": 0,
		"sub_b":  0,
	})
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))
}

func TestSubscriptionStatisticsSupportsManyVisibleSubscriptions(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = make([]*SubscriptionMeta, 0, 67)
	for i := 63; i >= 0; i-- {
		ctx.metadata = append(ctx.metadata, &SubscriptionMeta{
			AccountId: 0,
			DbName:    "tpch",
			SubName:   fmt.Sprintf("sub_%02d", i),
			Tables:    "nation",
		})
	}
	ctx.metadata = append(ctx.metadata, nil, ctx.metadata[10])

	queryPlan, err := runOneStmt(optimizer, t,
		"select count(*) from information_schema.statistics where table_name = 'nation'")
	require.NoError(t, err)
	counts := statisticsPublisherScanCounts(queryPlan.GetQuery())
	require.Len(t, counts, 64)
	for i := 0; i < 64; i++ {
		require.Equal(t, 1, counts[fmt.Sprintf("sub_%02d", i)])
	}
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))
	require.Nil(t, ctx.GetQueryingSubscription())
}

func hasLocalStatisticsCatalogScan(query *Query) bool {
	for _, node := range query.GetNodes() {
		if node.GetNodeType() == plan.Node_TABLE_SCAN &&
			node.GetObjRef().GetSchemaName() == catalog.MO_CATALOG &&
			node.GetObjRef().GetObjName() == catalog.MO_INDEXES &&
			node.GetObjRef().GetPubInfo() == nil {
			return true
		}
	}
	return false
}

func statisticsPublisherScanCounts(query *Query) map[string]int {
	counts := make(map[string]int)
	for _, node := range query.GetNodes() {
		if node.GetNodeType() == plan.Node_TABLE_SCAN &&
			node.GetObjRef().GetObjName() == catalog.MO_INDEXES &&
			node.GetObjRef().GetPubInfo() != nil {
			counts[node.GetObjRef().GetSubscriptionName()]++
		}
	}
	return counts
}

func requireStatisticsPublisherScopes(
	t *testing.T,
	query *Query,
	want map[string]int32,
) {
	t.Helper()
	got := make(map[string]int32)
	for _, node := range query.GetNodes() {
		if node.GetNodeType() != plan.Node_TABLE_SCAN ||
			node.GetObjRef().GetObjName() != catalog.MO_INDEXES ||
			node.GetObjRef().GetPubInfo() == nil {
			continue
		}
		got[node.GetObjRef().GetSubscriptionName()] = node.GetObjRef().GetPubInfo().GetTenantId()
	}
	require.Equal(t, want, got)
}
