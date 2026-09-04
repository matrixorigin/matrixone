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
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

type subscriptionMetadataTestContext struct {
	*MockCompilerContext
	subscription        *SubscriptionMeta
	subscriptions       map[string]*SubscriptionMeta
	querying            *SubscriptionMeta
	defaultDB           string
	metadata            []*SubscriptionMetadata
	metadataBySnapshot  map[string][]*SubscriptionMetadata
	lowerCaseTableNames int64
	metadataCalls       int
	metadataLimits      []int
	publisherBindCount  int
	cancelPublisherBind int
	cancelPlanning      context.CancelFunc
}

func (c *subscriptionMetadataTestContext) GetLowerCaseTableNames() int64 {
	return c.lowerCaseTableNames
}

func (c *subscriptionMetadataTestContext) subscriptionFor(databaseName string) *SubscriptionMeta {
	if subscriptionMetadataNameKey(databaseName, c.lowerCaseTableNames) ==
		subscriptionMetadataNameKey(c.subscription.SubName, c.lowerCaseTableNames) {
		return c.subscription
	}
	for name, subscription := range c.subscriptions {
		if subscriptionMetadataNameKey(databaseName, c.lowerCaseTableNames) ==
			subscriptionMetadataNameKey(name, c.lowerCaseTableNames) {
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

func (c *subscriptionMetadataTestContext) GetSubscriptionMetadata(
	snapshot *Snapshot,
	maxCandidates int,
) ([]*SubscriptionMetadata, error) {
	c.metadataCalls++
	c.metadataLimits = append(c.metadataLimits, maxCandidates)
	if c.metadataBySnapshot != nil {
		key, err := subscriptionMetadataSnapshotKey(snapshot)
		if err != nil {
			return nil, err
		}
		return c.metadataBySnapshot[key], nil
	}
	if c.metadata != nil {
		return c.metadata, nil
	}
	metas := []*SubscriptionMetadata{{Meta: c.subscription, AllTablesVisible: true}}
	for _, subscription := range c.subscriptions {
		metas = append(metas, &SubscriptionMetadata{Meta: subscription, AllTablesVisible: true})
	}
	return metas, nil
}

func (c *subscriptionMetadataTestContext) SetQueryingSubscription(subscription *SubscriptionMeta) {
	c.querying = subscription
	if subscription == nil {
		return
	}
	c.publisherBindCount++
	if c.cancelPlanning != nil && c.publisherBindCount == c.cancelPublisherBind {
		c.cancelPlanning()
		c.cancelPlanning = nil
	}
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
		lowerCaseTableNames: 1,
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

func TestSubscriptionStatisticsUsesResolvedPublisherAccountIdentity(t *testing.T) {
	// Production initializes this list while bootstrapping the frontend. The
	// lightweight planner context does not, but a non-system publisher causes
	// the normal mo_columns tenant filter to be bound and therefore needs a
	// non-empty predefined-table set too.
	util.InitPredefinedTables([]string{catalog.MO_USER})
	defer util.InitPredefinedTables(nil)

	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = []*SubscriptionMetadata{{
		Meta: &SubscriptionMeta{
			Name:        "publication",
			AccountId:   42,
			AccountName: "publisher",
			DbName:      "tpch",
			SubName:     "legacy_sub",
			Tables:      "nation",
		},
		AllTablesVisible: true,
	}}

	queryPlan, err := runOneStmt(optimizer, t,
		"select table_schema, index_schema, table_name, index_name "+
			"from information_schema.statistics "+
			"where table_schema = 'legacy_sub' and table_name = 'nation'")
	require.NoError(t, err)
	requireStatisticsPublisherScopes(t, queryPlan.GetQuery(), map[string]int32{
		"legacy_sub": 42,
	})

	publisherCatalogScans := 0
	accountFilteredScans := make(map[string]bool)
	for _, node := range queryPlan.GetQuery().GetNodes() {
		if node.GetNodeType() != plan.Node_TABLE_SCAN ||
			node.GetObjRef().GetSchemaName() != catalog.MO_CATALOG ||
			node.GetObjRef().GetSubscriptionName() != "legacy_sub" {
			continue
		}
		name := node.GetObjRef().GetObjName()
		if name != catalog.MO_INDEXES && name != catalog.MO_TABLES && name != catalog.MO_COLUMNS {
			continue
		}
		publisherCatalogScans++
		require.Equal(t, int32(42), node.GetObjRef().GetPubInfo().GetTenantId(), name)
		if name == catalog.MO_TABLES || name == catalog.MO_COLUMNS {
			require.Contains(t, FormatExprs(node.GetFilterList(), FormatOption{}), "u32val:42", name)
			accountFilteredScans[name] = true
		}
	}
	require.Equal(t, 3, publisherCatalogScans)
	require.Equal(t, map[string]bool{
		catalog.MO_TABLES:  true,
		catalog.MO_COLUMNS: true,
	}, accountFilteredScans)
	require.Nil(t, ctx.GetQueryingSubscription())
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

	filter := tree.String(subscriptionMoTablesFilter(&SubscriptionMetadata{
		Meta: ctx.subscription, AllTablesVisible: true,
	}), dialect.MYSQL)
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
	require.True(t, PreparedPlanDependsOnSubscriptionMetadata(queryPlan))
	requireStatisticsPublisherScopes(t, queryPlan.GetQuery(), map[string]int32{
		"sub_db": 0,
		"sub_b":  0,
	})
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))
}

func TestSubscriptionStatisticsZeroSubscriptionsRetainsMetadataDependency(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = []*SubscriptionMetadata{}

	queryPlan, err := runOneStmt(optimizer, t,
		"select count(*) from information_schema.statistics where table_name = 'nation'")
	require.NoError(t, err)
	require.True(t, PreparedPlanDependsOnSubscriptionMetadata(queryPlan))
	require.Empty(t, statisticsPublisherScanCounts(queryPlan.GetQuery()))
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))

	// There is deliberately no publisher node to carry the existing node-level
	// cache markers. Ordinary cache admission must therefore use the preserved
	// STATISTICS origin-view dependency instead.
	for _, node := range queryPlan.GetQuery().GetNodes() {
		require.False(t, node.GetNotCacheable())
		require.Empty(t, node.GetObjRef().GetSubscriptionName())
	}
}

func TestSubscriptionStatisticsSupportsManyVisibleSubscriptions(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = subscriptionMetadataTestSet(64)
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

func TestSubscriptionStatisticsPlanningBudget(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = subscriptionMetadataTestSet(64)

	queryPlan, err := runOneStmt(optimizer, t,
		"select count(*) from information_schema.statistics a "+
			"join information_schema.statistics b on a.table_name = b.table_name "+
			"join information_schema.statistics c on b.table_name = c.table_name "+
			"join information_schema.statistics d on c.table_name = d.table_name "+
			"where a.table_name = 'nation'")
	require.NoError(t, err)

	counts := statisticsPublisherScanCounts(queryPlan.GetQuery())
	require.Len(t, counts, 64)
	for i := 0; i < 64; i++ {
		require.Equal(t, 4, counts[fmt.Sprintf("sub_%02d", i)])
	}
	require.Nil(t, ctx.GetQueryingSubscription())
	require.Equal(t, 1, ctx.metadataCalls,
		"sibling occurrences must reuse one bounded visibility enumeration")
	require.Equal(t, []int{maxSubscriptionStatisticsPublisherBranches}, ctx.metadataLimits)
}

func TestSubscriptionStatisticsMetadataCacheIsSnapshotScoped(t *testing.T) {
	_, ctx := newSubscriptionMetadataTestOptimizer()
	snapshotA := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 1}}
	snapshotB := &Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 2}}
	keyA, err := subscriptionMetadataSnapshotKey(snapshotA)
	require.NoError(t, err)
	keyB, err := subscriptionMetadataSnapshotKey(snapshotB)
	require.NoError(t, err)
	ctx.metadataBySnapshot = map[string][]*SubscriptionMetadata{
		keyA: subscriptionMetadataTestSet(2),
		keyB: subscriptionMetadataTestSet(3),
	}
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, false)

	first, err := builder.visibleSubscriptionMetadata(snapshotA)
	require.NoError(t, err)
	second, err := builder.visibleSubscriptionMetadata(snapshotA)
	require.NoError(t, err)
	third, err := builder.visibleSubscriptionMetadata(snapshotB)
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Len(t, second, 2)
	require.Len(t, third, 3)
	require.Equal(t, 2, ctx.metadataCalls,
		"equivalent sibling snapshots reuse enumeration while distinct snapshots do not")
	require.Equal(t, []int{256, 252}, ctx.metadataLimits,
		"a new snapshot receives only the statement budget remaining after cached occurrences")
}

func TestSubscriptionStatisticsBudgetIgnoresRejectedAndDuplicateMetadata(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = subscriptionMetadataTestSet(maxSubscriptionStatisticsPublisherBranches)
	ctx.metadata = append(ctx.metadata,
		nil,
		ctx.metadata[0],
		&SubscriptionMetadata{Meta: nil, AllTablesVisible: true},
		&SubscriptionMetadata{Meta: &SubscriptionMeta{
			AccountId: 0,
			DbName:    "",
			SubName:   "empty_database",
			Tables:    "nation",
		}, AllTablesVisible: true},
		&SubscriptionMetadata{Meta: &SubscriptionMeta{
			AccountId: 0,
			DbName:    "tpch",
			SubName:   "empty_tables",
			Tables:    " , ",
		}, AllTablesVisible: true},
		&SubscriptionMetadata{Meta: &SubscriptionMeta{
			AccountId: 0,
			DbName:    "tpch",
			SubName:   "not_visible",
			Tables:    "nation",
		}},
	)

	queryPlan, err := runOneStmt(optimizer, t,
		"select count(*) from information_schema.statistics where table_name = 'nation'")
	require.NoError(t, err)
	require.Len(t, statisticsPublisherScanCounts(queryPlan.GetQuery()),
		maxSubscriptionStatisticsPublisherBranches)
	require.Equal(t, maxSubscriptionStatisticsPublisherBranches, ctx.publisherBindCount,
		"only distinct, valid, visible publisher branches may consume the budget")
	require.Nil(t, ctx.GetQueryingSubscription())
}

func TestSubscriptionStatisticsBudgetUsesIdentifierCaseMode(t *testing.T) {
	metadata := subscriptionMetadataTestSet(maxSubscriptionStatisticsPublisherBranches - 1)
	metadata = append(metadata,
		&SubscriptionMetadata{Meta: &SubscriptionMeta{
			AccountId: 0, DbName: "tpch", SubName: "SubCase", Tables: "nation",
		}, AllTablesVisible: true},
		&SubscriptionMetadata{Meta: &SubscriptionMeta{
			AccountId: 0, DbName: "tpch", SubName: "subcase", Tables: "nation",
		}, AllTablesVisible: true},
	)

	t.Run("case-sensitive names are distinct", func(t *testing.T) {
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.lowerCaseTableNames = 0
		ctx.metadata = metadata
		statements, err := mysql.Parse(context.Background(),
			"select count(*) from information_schema.statistics", 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()

		_, err = BuildPlan(ctx, statements[0], false)
		require.ErrorContains(t, err, "publisher expansion exceeds planning budget of 256 branches")
		require.Zero(t, ctx.publisherBindCount)
		require.Nil(t, ctx.GetQueryingSubscription())
	})

	for _, mode := range []int64{1, 2} {
		t.Run(fmt.Sprintf("case-insensitive mode %d deduplicates", mode), func(t *testing.T) {
			optimizer, ctx := newSubscriptionMetadataTestOptimizer()
			ctx.lowerCaseTableNames = mode
			ctx.metadata = metadata

			queryPlan, err := runOneStmt(optimizer, t,
				"select count(*) from information_schema.statistics")
			require.NoError(t, err)
			require.Len(t, statisticsPublisherScanCounts(queryPlan.GetQuery()),
				maxSubscriptionStatisticsPublisherBranches)
			require.Equal(t, maxSubscriptionStatisticsPublisherBranches, ctx.publisherBindCount)
			require.Nil(t, ctx.GetQueryingSubscription())
		})
	}
}

func TestSubscriptionStatisticsBudgetDoesNotLeakAcrossBuilds(t *testing.T) {
	_, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = subscriptionMetadataTestSet(129)
	statements, err := mysql.Parse(context.Background(),
		"select count(*) from information_schema.statistics", 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()

	for build := 0; build < 2; build++ {
		queryPlan, buildErr := BuildPlan(ctx, statements[0], false)
		require.NoError(t, buildErr)
		require.Len(t, statisticsPublisherScanCounts(queryPlan.GetQuery()), 129)
		require.Nil(t, ctx.GetQueryingSubscription())
	}
	require.Equal(t, 258, ctx.publisherBindCount,
		"the statement-wide budget must be fresh for each independent build")
}

func TestSubscriptionStatisticsPublicationTableBudget(t *testing.T) {
	build := func(t *testing.T, tables, sql string) (*Plan, error, *subscriptionMetadataTestContext) {
		t.Helper()
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.metadata = []*SubscriptionMetadata{{Meta: &SubscriptionMeta{
			AccountId: 0, DbName: "tpch", SubName: "table_budget", Tables: tables,
		}, AllTablesVisible: true}}
		statements, err := mysql.Parse(context.Background(), sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()
		plan, err := BuildPlan(ctx, statements[0], false)
		return plan, err, ctx
	}

	t.Run("deduplicates persisted table names before expansion", func(t *testing.T) {
		plan, err, ctx := build(t,
			strings.TrimSuffix(strings.Repeat("nation,", maxSubscriptionStatisticsPublicationTableEntries+1), ","),
			"select count(*) from information_schema.statistics")
		require.NoError(t, err)
		require.Equal(t, 1, ctx.publisherBindCount)
		require.Equal(t, map[string]int{"table_budget": 1},
			statisticsPublisherScanCounts(plan.GetQuery()))
	})

	t.Run("rejects too many explicit table entries before binding", func(t *testing.T) {
		_, err, ctx := build(t,
			subscriptionMetadataTestTableList(maxSubscriptionStatisticsPublicationTableEntries+1),
			"select count(*) from information_schema.statistics")
		require.ErrorContains(t, err, "publication table expansion exceeds planning budget of 4096 table entries")
		require.Zero(t, ctx.publisherBindCount)
	})

	t.Run("charges every statistics occurrence", func(t *testing.T) {
		_, err, ctx := build(t,
			subscriptionMetadataTestTableList(1025),
			"select count(*) from information_schema.statistics a "+
				"join information_schema.statistics b on a.table_name = b.table_name "+
				"join information_schema.statistics c on b.table_name = c.table_name "+
				"join information_schema.statistics d on c.table_name = d.table_name")
		require.ErrorContains(t, err, "publication table expansion exceeds planning budget of 4096 table entries")
		require.Equal(t, 3, ctx.publisherBindCount,
			"the over-budget occurrence must fail before its publisher branch is bound")
	})

	t.Run("rejects oversized encoded table names before binding", func(t *testing.T) {
		_, err, ctx := build(t,
			strings.Repeat("t", maxSubscriptionStatisticsPublicationTableLiteralBytes),
			"select count(*) from information_schema.statistics")
		require.ErrorContains(t, err, "publication table expansion exceeds planning budget of 1048576 encoded table-name bytes")
		require.Zero(t, ctx.publisherBindCount)
	})
}

func TestSubscriptionPublicationTableScopeIsDeterministicAndCancelable(t *testing.T) {
	scope, err := subscriptionPublicationTableScope(" beta,alpha,beta, alpha ", 1, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta"}, scope.tableNames)
	require.Equal(t, len("alpha")+2+len("beta")+2, scope.literalBytes)
	caseInsensitive, err := subscriptionPublicationTableScope("Table,table", 1, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"Table"}, caseInsensitive.tableNames)
	caseSensitive, err := subscriptionPublicationTableScope("Table,table", 0, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"Table", "table"}, caseSensitive.tableNames)

	wantErr := errors.New("stop publication table list processing")
	checks := 0
	_, err = subscriptionPublicationTableScope("t1,t2,t3,t4", 1, func() error {
		checks++
		if checks == 3 {
			return wantErr
		}
		return nil
	})
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 3, checks)
}

func subscriptionMetadataTestTableList(count int) string {
	var tables strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			tables.WriteByte(',')
		}
		fmt.Fprintf(&tables, "t_%d", i)
	}
	return tables.String()
}

func TestSubscriptionStatisticsRejectsPublisherExpansionOverBudget(t *testing.T) {
	t.Run("single occurrence", func(t *testing.T) {
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.metadata = subscriptionMetadataTestSet(maxSubscriptionStatisticsPublisherBranches + 1)
		statements, err := mysql.Parse(context.Background(),
			"select count(*) from information_schema.statistics where table_name = 'nation'", 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()

		_, err = BuildPlan(ctx, statements[0], false)
		require.ErrorContains(t, err, "publisher expansion exceeds planning budget of 256 branches")
		require.Zero(t, ctx.publisherBindCount,
			"admission must reject the occurrence before binding any publisher view")
		require.Nil(t, ctx.GetQueryingSubscription())
	})

	t.Run("cumulative occurrences", func(t *testing.T) {
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.metadata = subscriptionMetadataTestSet(64)
		statements, err := mysql.Parse(context.Background(),
			"select count(*) from information_schema.statistics a "+
				"join information_schema.statistics b on a.table_name = b.table_name "+
				"join information_schema.statistics c on b.table_name = c.table_name "+
				"join information_schema.statistics d on c.table_name = d.table_name "+
				"join information_schema.statistics e on d.table_name = e.table_name "+
				"where a.table_name = 'nation'", 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()

		_, err = BuildPlan(ctx, statements[0], false)
		require.ErrorContains(t, err, "publisher expansion exceeds planning budget of 256 branches")
		require.Equal(t, maxSubscriptionStatisticsPublisherBranches, ctx.publisherBindCount,
			"the fifth occurrence must fail admission before binding another publisher view")
		require.Nil(t, ctx.GetQueryingSubscription())
	})
}

func TestSubscriptionStatisticsPlanningObservesCancellation(t *testing.T) {
	const sql = "select count(*) from information_schema.statistics where table_name = 'nation'"

	t.Run("pre-canceled", func(t *testing.T) {
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.metadata = subscriptionMetadataTestSet(4)
		statements, err := mysql.Parse(context.Background(), sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()

		wantErr := errors.New("stop planning")
		canceledCtx, cancel := context.WithCancelCause(context.Background())
		cancel(wantErr)
		ctx.SetContext(canceledCtx)
		_, err = BuildPlan(ctx, statements[0], false)
		require.ErrorIs(t, err, wantErr)
		require.Zero(t, ctx.metadataCalls,
			"pre-canceled planning must stop before subscription enumeration")
		require.Zero(t, ctx.publisherBindCount)
		require.Nil(t, ctx.GetQueryingSubscription())
	})

	t.Run("during publisher binding", func(t *testing.T) {
		_, ctx := newSubscriptionMetadataTestOptimizer()
		ctx.metadata = subscriptionMetadataTestSet(4)
		statements, err := mysql.Parse(context.Background(), sql, 1)
		require.NoError(t, err)
		require.Len(t, statements, 1)
		defer statements[0].Free()

		planningCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx.SetContext(planningCtx)
		ctx.cancelPublisherBind = 1
		ctx.cancelPlanning = cancel
		_, err = BuildPlan(ctx, statements[0], false)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, ctx.publisherBindCount,
			"the loop must observe cancellation before binding the next publisher view")
		require.Nil(t, ctx.GetQueryingSubscription())
	})
}

func BenchmarkSubscriptionStatisticsPlanning(b *testing.B) {
	queries := []struct {
		name string
		sql  string
	}{
		{
			name: "one-occurrence",
			sql: "select count(*) from information_schema.statistics " +
				"where table_name = 'nation'",
		},
		{
			name: "four-occurrences",
			sql: "select count(*) from information_schema.statistics a " +
				"join information_schema.statistics b on a.table_name = b.table_name " +
				"join information_schema.statistics c on b.table_name = c.table_name " +
				"join information_schema.statistics d on c.table_name = d.table_name " +
				"where a.table_name = 'nation'",
		},
	}

	for _, subscriptionCount := range []int{0, 16, 64} {
		for _, query := range queries {
			b.Run(fmt.Sprintf("subscriptions-%d/%s", subscriptionCount, query.name), func(b *testing.B) {
				_, ctx := newSubscriptionMetadataTestOptimizer()
				ctx.metadata = subscriptionMetadataTestSet(subscriptionCount)
				b.ReportMetric(float64(subscriptionCount), "subscriptions")
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					statements, err := mysql.Parse(ctx.GetContext(), query.sql, 1)
					if err != nil {
						b.Fatal(err)
					}
					if len(statements) != 1 {
						b.Fatalf("expected one statement, got %d", len(statements))
					}
					_, err = BuildPlan(ctx, statements[0], false)
					statements[0].Free()
					if err != nil {
						b.Fatal(err)
					}
					if ctx.GetQueryingSubscription() != nil {
						b.Fatal("planning leaked the querying-subscription context")
					}
				}
			})
		}
	}
}

func subscriptionMetadataTestSet(count int) []*SubscriptionMetadata {
	metas := make([]*SubscriptionMetadata, 0, count)
	for i := count - 1; i >= 0; i-- {
		metas = append(metas, &SubscriptionMetadata{
			Meta: &SubscriptionMeta{
				AccountId: 0,
				DbName:    "tpch",
				SubName:   fmt.Sprintf("sub_%02d", i),
				Tables:    "nation",
			},
			AllTablesVisible: true,
		})
	}
	return metas
}

func TestSubscriptionStatisticsHonorsSubscriptionNameCaseMode(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = []*SubscriptionMetadata{
		{Meta: &SubscriptionMeta{AccountId: 0, DbName: "tpch", SubName: "SubCase", Tables: "nation"}, AllTablesVisible: true},
		{Meta: &SubscriptionMeta{AccountId: 0, DbName: "tpch", SubName: "subcase", Tables: "nation"}, AllTablesVisible: true},
	}

	t.Run("case sensitive", func(t *testing.T) {
		ctx.lowerCaseTableNames = 0
		queryPlan, err := runOneStmt(optimizer, t,
			"select count(*) from information_schema.statistics where table_name = 'nation'")
		require.NoError(t, err)
		require.Equal(t, map[string]int{"SubCase": 1, "subcase": 1},
			statisticsPublisherScanCounts(queryPlan.GetQuery()))
		require.Nil(t, ctx.GetQueryingSubscription())
	})

	t.Run("case insensitive", func(t *testing.T) {
		ctx.lowerCaseTableNames = 1
		queryPlan, err := runOneStmt(optimizer, t,
			"select count(*) from information_schema.statistics where table_name = 'nation'")
		require.NoError(t, err)
		require.Equal(t, map[string]int{"SubCase": 1},
			statisticsPublisherScanCounts(queryPlan.GetQuery()))
		require.Nil(t, ctx.GetQueryingSubscription())
	})

	t.Run("case insensitive preserve spelling", func(t *testing.T) {
		ctx.lowerCaseTableNames = 2
		queryPlan, err := runOneStmt(optimizer, t,
			"select count(*) from information_schema.statistics where table_name = 'nation'")
		require.NoError(t, err)
		require.Equal(t, map[string]int{"SubCase": 1},
			statisticsPublisherScanCounts(queryPlan.GetQuery()))
		require.Nil(t, ctx.GetQueryingSubscription())
	})
}

func TestSubscriptionStatisticsAppliesSubscriberRBACBeforePublisherScan(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()

	queryPlan, err := runOneStmt(optimizer, t,
		"select index_name from information_schema.statistics "+
			"where table_schema in ('sub_db', 'sub_b') and table_name = 'nation'")
	require.NoError(t, err)
	requireStatisticsPublisherScopes(t, queryPlan.GetQuery(), map[string]int32{
		"sub_db": 0,
		"sub_b":  0,
	})

	// Subscriber visibility was computed in the subscriber account before these
	// branches were built. Publisher catalog scans must therefore contain only
	// publication and logical-table filters, never publisher-side comparisons
	// against subscriber role IDs.
	for _, node := range queryPlan.GetQuery().GetNodes() {
		obj := node.GetObjRef()
		if obj.GetPubInfo() == nil {
			continue
		}
		require.NotEqual(t, "mo_role_privs", obj.GetObjName())
		require.NotEqual(t, catalog.MO_DATABASE, obj.GetObjName())
	}
	require.Nil(t, ctx.GetQueryingSubscription())
}

func TestSubscriptionStatisticsOmitsSubscriberInvisibleBranches(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = []*SubscriptionMetadata{
		{Meta: ctx.subscription},
		{Meta: ctx.subscriptions["sub_b"], AllTablesVisible: true},
	}

	queryPlan, err := runOneStmt(optimizer, t,
		"select index_name from information_schema.statistics where table_name = 'nation'")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"sub_b": 1},
		statisticsPublisherScanCounts(queryPlan.GetQuery()))
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))
	require.Nil(t, ctx.GetQueryingSubscription())
}

func TestSubscriptionStatisticsOmitsInvalidPublicationScopes(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.metadata = []*SubscriptionMetadata{
		{Meta: &SubscriptionMeta{AccountId: 7, SubName: "missing_db", Tables: "*"}, AllTablesVisible: true},
		{Meta: &SubscriptionMeta{AccountId: 7, DbName: "publisher_db", SubName: "missing_tables"}, AllTablesVisible: true},
		{Meta: &SubscriptionMeta{AccountId: 7, DbName: "publisher_db", SubName: "blank_tables", Tables: " , "}, AllTablesVisible: true},
		{Meta: ctx.subscriptions["sub_b"], AllTablesVisible: true},
	}

	queryPlan, err := runOneStmt(optimizer, t,
		"select index_name from information_schema.statistics where table_name = 'nation'")
	require.NoError(t, err)
	require.Equal(t, map[string]int{"sub_b": 1},
		statisticsPublisherScanCounts(queryPlan.GetQuery()))
	require.True(t, hasLocalStatisticsCatalogScan(queryPlan.GetQuery()))
}

func TestInvalidDirectSubscriptionScopeFailsClosed(t *testing.T) {
	optimizer, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.SetQueryingSubscription(&SubscriptionMeta{
		AccountId: 7,
		SubName:   "invalid_subscription",
		Tables:    "*",
	})

	queryPlan, err := runOneStmt(optimizer, t,
		"select relname from mo_catalog.mo_tables")
	require.NoError(t, err)

	found := false
	for _, node := range queryPlan.GetQuery().GetNodes() {
		if node.GetObjRef().GetObjName() != catalog.MO_TABLES {
			continue
		}
		found = true
		require.Contains(t, FormatExprs(node.GetFilterList(), FormatOption{}),
			"bval:false")
	}
	require.True(t, found)
}

func TestSubscriptionMoTablesFilterIntersectsPublicationAndSubscriberRBAC(t *testing.T) {
	meta := &SubscriptionMeta{
		AccountId: 7,
		DbName:    "publisher_db",
		SubName:   "subscriber_db",
		Tables:    "published_t,other_t",
	}

	partial := tree.String(subscriptionMoTablesFilter(&SubscriptionMetadata{
		Meta: meta, VisibleTableIDs: []uint64{42, 7, 42, 0},
	}), dialect.MYSQL)
	require.Contains(t, partial, "reldatabase = publisher_db")
	require.Contains(t, partial, "relname in (other_t, published_t)")
	require.Contains(t, partial, "rel_logical_id in (7, 42)")

	all := tree.String(subscriptionMoTablesFilter(&SubscriptionMetadata{
		Meta: meta, AllTablesVisible: true,
	}), dialect.MYSQL)
	require.NotContains(t, all, "rel_logical_id")
	require.Contains(t, all, "relname in (other_t, published_t)")

	none := tree.String(subscriptionMoTablesFilter(&SubscriptionMetadata{Meta: meta}), dialect.MYSQL)
	require.Contains(t, none, "reldatabase = __mo_invalid_subscription_scope__")
	require.Contains(t, none, "reldatabase != __mo_invalid_subscription_scope__")

	for _, invalid := range []*SubscriptionMetadata{
		{},
		{Meta: &SubscriptionMeta{SubName: "sub", Tables: "*"}},
		{Meta: &SubscriptionMeta{SubName: "sub", DbName: "publisher_db"}},
		{Meta: &SubscriptionMeta{SubName: "sub", DbName: "publisher_db", Tables: " , "}},
	} {
		filter := tree.String(subscriptionMoTablesFilter(invalid), dialect.MYSQL)
		require.Contains(t, filter, "reldatabase = __mo_invalid_subscription_scope__")
		require.Contains(t, filter, "reldatabase != __mo_invalid_subscription_scope__")
	}
	require.Nil(t, subscriptionMoTablesFilter(nil))

	_, ctx := newSubscriptionMetadataTestOptimizer()
	ctx.SetQueryingSubscription(meta)
	builder := NewQueryBuilder(plan.Query_SELECT, ctx, false, true)
	directFilter, err := builder.currentSubscriptionMoTablesFilter()
	require.NoError(t, err)
	direct := tree.String(directFilter, dialect.MYSQL)
	require.Contains(t, direct, "reldatabase = publisher_db")
	require.Contains(t, direct, "relname in (other_t, published_t)")
	require.NotContains(t, direct, "rel_logical_id")
}

func TestRewriteSubscriptionStatisticsAccountScopesVisibilityCTE(t *testing.T) {
	statements, err := mysql.Parse(context.Background(), sysview.InformationSchemaStatisticsDDL, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()

	view, ok := statements[0].(*tree.CreateView)
	require.True(t, ok)
	require.True(t, rewriteSubscriptionStatisticsAccount(view.AsSource, 42))

	outerClause := selectClauseOf(view.AsSource)
	require.NotNil(t, outerClause)
	require.NotNil(t, outerClause.Where)
	outerFilter := tree.String(outerClause.Where.Expr, dialect.MYSQL)
	require.Contains(t, outerFilter, "tbl.account_id = 42")
	require.NotContains(t, outerFilter, "current_account_id")

	var visibleTables *tree.SelectClause
	for _, cte := range view.AsSource.With.CTEs {
		if cte != nil && cte.Name != nil &&
			strings.EqualFold(string(cte.Name.Alias), "__mo_visible_tables") {
			cteSelect, cteOK := cte.Stmt.(*tree.Select)
			require.True(t, cteOK)
			visibleTables = selectClauseOf(cteSelect)
			break
		}
	}
	require.NotNil(t, visibleTables)
	require.NotNil(t, visibleTables.Where)
	visibilityFilter := tree.String(visibleTables.Where.Expr, dialect.MYSQL)
	require.Equal(t, "tbl.account_id = 42", visibilityFilter)
	require.NotContains(t, visibilityFilter, "mo_current_roles")
	require.NotContains(t, visibilityFilter, "mo_role_privs")
}

func TestRewriteSubscriptionStatisticsAccountSupportsLegacyView(t *testing.T) {
	statements, err := mysql.Parse(context.Background(),
		"select tbl.relname from mo_catalog.mo_tables tbl "+
			"where tbl.account_id = current_account_id()", 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()

	stmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)
	require.True(t, rewriteSubscriptionStatisticsAccount(stmt, 42))
	clause := selectClauseOf(stmt)
	require.NotNil(t, clause)
	require.NotNil(t, clause.Where)
	filter := tree.String(clause.Where.Expr, dialect.MYSQL)
	require.Contains(t, filter, "tbl.account_id = 42")
	require.NotContains(t, filter, "current_account_id")
}

func TestRewriteSubscriptionStatisticsAccountRejectsUnsupportedVisibilityCTE(t *testing.T) {
	statements, err := mysql.Parse(context.Background(),
		"with __mo_visible_tables as (select 1 union all select 2) "+
			"select tbl.relname from mo_catalog.mo_tables tbl "+
			"where tbl.account_id = current_account_id()", 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()

	stmt, ok := statements[0].(*tree.Select)
	require.True(t, ok)
	require.False(t, rewriteSubscriptionStatisticsAccount(stmt, 42),
		"an unrecognized visibility CTE must fail closed")
}

func TestSubscriptionMetadataNameKeyUsesIdentifierCanonicalization(t *testing.T) {
	malformed := "Sub\xe9A"
	require.Equal(t, malformed, subscriptionMetadataNameKey(malformed, 0))
	require.Equal(t, "sub\xe9a", subscriptionMetadataNameKey(malformed, 1))
	require.Equal(t, "sub\xe9a", subscriptionMetadataNameKey(malformed, 2))
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
