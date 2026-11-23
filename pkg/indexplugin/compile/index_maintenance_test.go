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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type maintenanceTestDB struct {
	engine.Database
	relations map[string]*maintenanceTestRelation
}

func (db *maintenanceTestDB) GetDatabaseId(context.Context) string { return "17" }
func (db *maintenanceTestDB) Relation(_ context.Context, name string, _ any) (engine.Relation, error) {
	if rel := db.relations[name]; rel != nil {
		return rel, nil
	}
	return nil, moerr.NewInvalidInputNoCtx("missing relation")
}

type maintenanceTestRelation struct {
	engine.Relation
	name  string
	id    uint64
	def   *plan.TableDef
	extra *api.SchemaExtra
}

func (r *maintenanceTestRelation) GetTableName() string                       { return r.name }
func (r *maintenanceTestRelation) GetTableID(context.Context) uint64          { return r.id }
func (r *maintenanceTestRelation) GetTableDef(context.Context) *plan.TableDef { return r.def }
func (r *maintenanceTestRelation) GetExtraInfo() *api.SchemaExtra             { return r.extra }

func TestResolveIndexMaintenanceSpec(t *testing.T) {
	ctx := defines.AttachAccountId(context.Background(), 0)
	base := &maintenanceTestRelation{name: "base", id: 20, def: &plan.TableDef{Name: "base"}}
	meta := &maintenanceTestRelation{name: "renamed_meta", id: 21, def: &plan.TableDef{Name: "renamed_meta", TableType: "meta"}, extra: &api.SchemaExtra{ParentTableID: 20}}
	store := &maintenanceTestRelation{name: "renamed_store", id: 22, def: &plan.TableDef{Name: "renamed_store", TableType: catalog.SystemTemporaryTable, FeatureFlag: features.IndexTable}, extra: &api.SchemaExtra{ParentTableID: 20}}
	db := &maintenanceTestDB{relations: map[string]*maintenanceTestRelation{"base": base, "renamed_meta": meta, "renamed_store": store}}
	_, err := ResolveIndexMaintenanceSpec(ctx, "db", db, "base", 20, "build", map[string]*plan.IndexDef{})
	require.ErrorContains(t, err, "missing index maintenance owner")
	defs := map[string]*plan.IndexDef{
		"meta":  {IndexName: "ix", IndexAlgo: "test", IndexAlgoTableType: "meta", IndexTableName: "renamed_meta"},
		"store": {IndexName: "ix", IndexAlgo: "test", IndexAlgoTableType: "store", IndexTableName: "renamed_store"},
	}
	resolve := func() error {
		_, err := ResolveIndexMaintenanceSpec(ctx, "db", db, "base", 20, "build", defs)
		return err
	}
	spec, err := ResolveIndexMaintenanceSpec(ctx, "db", db, "base", 20, "build", defs)
	require.NoError(t, err)
	require.Zero(t, spec.AccountID, "system account zero is valid")
	require.Equal(t, uint64(17), spec.DatabaseID)
	require.Equal(t, "meta", spec.Targets[0].Role, "role ordering is deterministic")

	meta.extra.ParentTableID = 99
	require.Error(t, resolve())
	meta.extra.ParentTableID = 20
	store.def.FeatureFlag = 0
	require.Error(t, resolve(), "temporary children require the durable feature")
	store.def.FeatureFlag = features.IndexTable
	defs["store"].IndexName = "different"
	require.Error(t, resolve())
	defs["store"].IndexName = "ix"
	store.id = meta.id
	require.Error(t, resolve(), "two roles cannot attest one relation")
	store.id = 22
	base.id = 23
	require.Error(t, resolve(), "base replacement invalidates the owner")
}

func TestIndexMaintenanceFunctionNames(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want []string
	}{
		{"select * from hnsw_create('a','b') f", []string{"hnsw_create"}},
		{"insert into d.t select * from (select * from ivf_create('a','b') f) q", []string{"ivf_create"}},
		{"select 1", nil},
	} {
		stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
		require.NoError(t, err)
		require.Equal(t, tc.want, IndexMaintenanceFunctionNames(stmt))
		stmt.Free()
	}
}
