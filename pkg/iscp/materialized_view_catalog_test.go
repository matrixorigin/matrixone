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
