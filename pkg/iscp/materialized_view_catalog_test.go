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

package iscp

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog/mvdefinition"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
)

// A minimal catalog snapshot exercises the real envelope loader. SQL execution
// remains a routing oracle here; public BVT proves transactional data effects.
type mvTestCatalog struct {
	engine.Engine
	relations  map[string]*materializedViewTestRelation
	definition *mvdefinition.Definition
	target     *planpb.TableDef
	txn        client.TxnOperator
	sqls       []string
	failSQL    func(string) error
}
type mvTestDatabase struct {
	engine.Database
	catalog *mvTestCatalog
	name    string
}

func (c *mvTestCatalog) Database(_ context.Context, name string, _ client.TxnOperator) (engine.Database, error) {
	return &mvTestDatabase{catalog: c, name: name}, nil
}
func (d *mvTestDatabase) Relation(_ context.Context, name string, _ any) (engine.Relation, error) {
	if rel := d.catalog.relations[d.name+"."+name]; rel != nil {
		return rel, nil
	}
	return nil, errors.New("relation is absent")
}
func (r *materializedViewTestRelation) GetTableDef(ctx context.Context) *planpb.TableDef {
	if r.def == nil && r.Relation != nil {
		return r.Relation.GetTableDef(ctx)
	}
	return r.def
}
func (r *materializedViewTestRelation) GetTableID(ctx context.Context) uint64 {
	if r.def == nil {
		return r.Relation.GetTableID(ctx)
	}
	return r.def.TblId
}
func newMVTestCatalog(t *testing.T, info *ConsumerInfo, sources ...*planpb.TableDef) *mvTestCatalog {
	t.Helper()
	c := &mvTestCatalog{relations: make(map[string]*materializedViewTestRelation), txn: mock_frontend.NewMockTxnOperator(gomock.NewController(t))}
	d := &mvdefinition.Definition{Format: 1, RequiredCapability: mvdefinition.RequiredCapability, Target: mvdefinition.Relation{Database: info.DBName, Name: info.TableName, DatabaseID: 1, ID: 100}, Generation: 1,
		CreateSQL: "create materialized view " + info.TableName + " as " + info.RefreshSQL, RefreshSQL: info.RefreshSQL, Method: info.RefreshMethod, Timing: "change", Columns: info.Columns, Incremental: info.IncrementalSpec}
	if d.Method == "" {
		d.Method = "force"
	}
	if len(d.Columns) == 0 {
		d.Columns = []string{"service", "requests"}
	}
	if len(sources) == 0 {
		sources = []*planpb.TableDef{{DbName: "db", Name: "events", DbId: 1, TblId: 11, TableType: "r", Cols: []*planpb.ColDef{{Name: "service"}}}}
	}
	for i, src := range sources {
		if src.TblId == 0 {
			src.TblId = uint64(11 + i)
		}
		if src.DbId == 0 {
			src.DbId = uint64(1 + i)
		}
		if src.TableType == "" {
			src.TableType = "r"
		}
		version := src.Version
		d.Sources = append(d.Sources, mvdefinition.Source{Relation: mvdefinition.Relation{Database: src.DbName, Name: src.Name, DatabaseID: src.DbId, ID: src.TblId}, Version: &version})
		c.relations[src.DbName+"."+src.Name] = &materializedViewTestRelation{def: src}
	}
	encoded, err := mvdefinition.Encode(d)
	require.NoError(t, err)
	c.target = &planpb.TableDef{DbName: d.Target.Database, Name: d.Target.Name, DbId: d.Target.DatabaseID, TblId: d.Target.ID, TableType: "m", Props: []*planpb.PropertyDef{{Key: mvdefinition.Property, Value: encoded}}}
	c.relations[info.DBName+"."+info.TableName] = &materializedViewTestRelation{def: c.target}
	projected, err := MaterializedViewInfo(d)
	require.NoError(t, err)
	*info = *projected
	c.definition = d
	old := ExecWithResult
	ExecWithResult = c.exec
	t.Cleanup(func() { ExecWithResult = old })
	return c
}
func (c *mvTestCatalog) exec(ctx context.Context, sql, _ string, txn client.TxnOperator) (executor.Result, error) {
	if txn != c.txn {
		return executor.Result{}, errors.New("refresh changed caller transaction")
	}
	count := 0
	if strings.HasPrefix(sql, "SELECT dat_id") {
		dbs := map[string]bool{c.definition.Target.Database: true}
		for _, s := range c.definition.Sources {
			dbs[s.Database] = true
		}
		count = len(dbs)
	} else if strings.HasPrefix(sql, "SELECT rel_id") {
		count = 1 + len(c.definition.Sources)
	}
	if count > 0 {
		b := batch.NewWithSize(0)
		b.SetRowCount(count)
		return executor.Result{Batches: []*batch.Batch{b}}, nil
	}
	if !mvdefinition.CanWrite(ctx, c.target) {
		return executor.Result{}, errors.New("refresh lacks exact target authority")
	}
	c.sqls = append(c.sqls, sql)
	if c.failSQL != nil {
		return executor.Result{}, c.failSQL(sql)
	}
	return executor.Result{}, nil
}
func (c *mvTestCatalog) stubTransactions(t *testing.T) {
	t.Helper()
	stub := gostub.Stub(&runTxnWithSqlContext, func(ctx context.Context, _ engine.Engine, _ client.TxnClient, service string, account uint32, timeout time.Duration, resolve func(string, bool, bool) (any, error), data any, fn func(*sqlexec.SqlProcess, any) error) error {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, account)
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return fn(sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, service, c.txn, account, resolve)), data)
	})
	t.Cleanup(stub.Reset)
}

func TestMaterializedViewForceFallbackRequiresKnownRollback(t *testing.T) {
	deltaErr := errors.New("delta watermark write failed")
	rollbackErr := errors.New("rollback failed")
	commitErr := errors.New("commit outcome unknown")
	for _, tc := range []struct {
		name                        string
		operation, rollback, commit error
		wantTransactions            int
	}{
		{"rolled back", deltaErr, nil, nil, 2},
		{"rollback failed", deltaErr, rollbackErr, nil, 1},
		{"commit uncertain", nil, nil, commitErr, 1},
		{"generation CAS lost", newISCPStatusCASLostError("watermark", "mv", 1, 0), nil, nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &ConsumerInfo{DBName: "db", TableName: "mv", RefreshMethod: "force",
				RefreshSQL: "select service, count(*) requests from db.events group by service",
				IncrementalSpec: encodeMaterializedViewIncrementalDescription(t, incrementalDescription{
					Version: 2, Strategy: "direct-delta", SourceAlias: "e", SourceColumns: []string{"service"},
					Groups:         []incrementalGroup{{Expression: "e.service", OutputColumn: "service", NotNullable: true}},
					Aggregates:     []incrementalAggregate{{Kind: "count_star", OutputColumn: "requests"}},
					GroupKeyColumn: "__group_key", RowCountColumn: "__row_count",
					StateColumns: []string{"__row_count", "__group_key"},
				}),
			}
			catalog := newMVTestCatalog(t, info)
			transactions := 0
			stub := gostub.Stub(&runTxnWithSqlContext, func(ctx context.Context, _ engine.Engine, _ client.TxnClient, service string, account uint32, _ time.Duration, resolve func(string, bool, bool) (any, error), data any, fn func(*sqlexec.SqlProcess, any) error) error {
				transactions++
				ctx = defines.AttachAccountId(ctx, account)
				err := fn(sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(ctx, service, catalog.txn, account, resolve)), data)
				if transactions == 1 {
					require.ErrorIs(t, err, tc.operation)
					if err != nil {
						return errors.Join(err, tc.rollback)
					}
					return tc.commit
				}
				return err
			})
			t.Cleanup(stub.Reset)
			r := &materializedViewBoundaryRetriever{MockRetriever: MockRetriever{dtype: ISCPDataType_Tail,
				updateWatermark: func(context.Context, string, client.TxnOperator) error {
					if transactions == 1 {
						return tc.operation
					}
					return nil
				}}, from: types.BuildTS(10, 0), to: types.BuildTS(20, 0)}
			consumer := &MaterializedViewConsumer{cnEngine: catalog, info: info, jobID: JobID{DBName: "db", TableName: "events"}}
			err := consumer.Consume(t.Context(), r)
			require.Equal(t, tc.wantTransactions, transactions)
			if tc.wantTransactions == 2 {
				require.NoError(t, err)
				require.Len(t, catalog.sqls, 2)
				require.Contains(t, strings.ToLower(catalog.sqls[0]), "delete from `db`.`mv`")
				require.Contains(t, strings.ToLower(catalog.sqls[1]), "insert into `db`.`mv`")
			} else {
				require.Error(t, err)
				require.Empty(t, catalog.sqls, "terminal failures must not replace the target")
			}
		})
	}
}

func TestLoadMaterializedViewDefinitionValidatesCatalogEnvelope(t *testing.T) {
	ctx := defines.AttachAccountId(t.Context(), 0)
	_, err := MaterializedViewInfo(&mvdefinition.Definition{})
	require.Error(t, err)
	_, err = LoadMaterializedViewDefinition(t.Context(), nil, "cn", nil, nil, false)
	require.Error(t, err)
	_, err = LoadMaterializedViewDefinition(ctx, nil, "cn", nil, nil, false)
	require.Error(t, err)

	info := &ConsumerInfo{DBName: "db", TableName: "mv", MVReference: &mvdefinition.Reference{}}
	_, err = LoadMaterializedViewDefinition(ctx, nil, "cn", nil, info, false)
	require.Error(t, err)

	info = &ConsumerInfo{DBName: "db", TableName: "mv", RefreshSQL: "select service, count(*) requests from db.events group by service"}
	catalog := newMVTestCatalog(t, info)
	_, err = LoadMaterializedViewDefinition(ctx, nil, "cn", catalog.txn, info, false)
	require.Error(t, err)

	loaded, err := LoadMaterializedViewDefinition(ctx, catalog, "cn", catalog.txn, info, true)
	require.NoError(t, err)
	require.Equal(t, catalog.definition.Target.ID, loaded.Target.ID)

	lockFailure := gostub.Stub(&ExecWithResult, func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, errors.New("lock failed")
	})
	require.ErrorContains(t, lockMaterializedViewDefinition(ctx, "cn", catalog.txn, catalog.definition), "lock failed")
	lockFailure.Reset()
	rowMismatch := gostub.Stub(&ExecWithResult, func(context.Context, string, string, client.TxnOperator) (executor.Result, error) {
		return executor.Result{}, nil
	})
	require.ErrorContains(t, lockMaterializedViewDefinition(ctx, "cn", catalog.txn, catalog.definition), "catalog identity disappeared")
	rowMismatch.Reset()
	_, err = LoadMaterializedViewDefinition(t.Context(), catalog, "cn", catalog.txn, info, false)
	require.Error(t, err)

	badSources := *info
	badSources.SrcTables = append([]TableInfo(nil), info.SrcTables...)
	badSources.SrcTables[0].TableID++
	_, err = LoadMaterializedViewDefinition(ctx, catalog, "cn", catalog.txn, &badSources, false)
	require.ErrorContains(t, err, "job source projection changed")

	missingTarget := *info
	missingTarget.TableName = "missing_mv"
	_, err = LoadMaterializedViewDefinition(ctx, catalog, "cn", catalog.txn, &missingTarget, false)
	require.ErrorContains(t, err, "target is unavailable")

	missingSource := *catalog
	missingSource.relations = make(map[string]*materializedViewTestRelation, len(catalog.relations))
	for name, rel := range catalog.relations {
		missingSource.relations[name] = rel
	}
	delete(missingSource.relations, "db.events")
	_, err = LoadMaterializedViewDefinition(ctx, &missingSource, "cn", catalog.txn, info, false)
	require.Error(t, err)

	otherAccount := defines.AttachAccountId(t.Context(), 1)
	_, err = LoadMaterializedViewDefinition(otherAccount, catalog, "cn", catalog.txn, info, false)
	require.ErrorContains(t, err, "account identity changed")

	stateInfo := &ConsumerInfo{DBName: "db", TableName: "mv_stateful", RefreshMethod: "force",
		RefreshSQL: "select service, count(*) requests from db.events group by service"}
	stateCatalog := newMVTestCatalog(t, stateInfo)
	state := &mvdefinition.Relation{Database: "db", Name: "__mo_mv_state_mv_stateful", DatabaseID: 1, ID: 101}
	stateCatalog.definition.State = state
	stateOwner := mvdefinition.Owner{Format: mvdefinition.Format, AccountID: stateCatalog.definition.AccountID,
		TargetID: stateCatalog.definition.Target.ID, Generation: stateCatalog.definition.Generation, StateID: state.ID}
	stateDef := &planpb.TableDef{DbName: state.Database, Name: state.Name, DbId: state.DatabaseID, TblId: state.ID,
		TableType: "i", Props: []*planpb.PropertyDef{{Key: mvdefinition.OwnerProperty, Value: mvdefinition.EncodeOwner(stateOwner)}}}
	stateCatalog.relations[state.Database+"."+state.Name] = &materializedViewTestRelation{def: stateDef}
	encoded, encodeErr := mvdefinition.Encode(stateCatalog.definition)
	require.NoError(t, encodeErr)
	stateCatalog.target.Props[0].Value = encoded
	projected, projectErr := MaterializedViewInfo(stateCatalog.definition)
	require.NoError(t, projectErr)
	*stateInfo = *projected
	stateLocks := gostub.Stub(&ExecWithResult, func(_ context.Context, sql, _ string, _ client.TxnOperator) (executor.Result, error) {
		rows := 1
		if strings.HasPrefix(sql, "SELECT rel_id") {
			rows = 3
		}
		b := batch.NewWithSize(0)
		b.SetRowCount(rows)
		return executor.Result{Batches: []*batch.Batch{b}}, nil
	})
	loaded, err = LoadMaterializedViewDefinition(ctx, stateCatalog, "cn", stateCatalog.txn, stateInfo, true)
	require.NoError(t, err)
	require.Equal(t, state.Name, loaded.State.Name)
	stateLocks.Reset()
}
