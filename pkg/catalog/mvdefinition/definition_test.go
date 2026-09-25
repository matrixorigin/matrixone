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

package mvdefinition

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func definitionFixture(t *testing.T) (*Definition, *plan.TableDef) {
	t.Helper()
	v := uint32(0)
	d := &Definition{Format: Format, RequiredCapability: RequiredCapability, Target: Relation{Database: "db", Name: "mv", DatabaseID: 1, ID: 100}, Generation: 1, CreateSQL: "create materialized view mv as select k,count(*) c from src group by k", RefreshSQL: "select k,count(*) c from src group by k", Method: "complete", Timing: "demand", Columns: []string{"k", "c"}, Sources: []Source{{Relation: Relation{Database: "db", Name: "src", DatabaseID: 1, ID: 11}, Version: &v}}}
	encoded, err := Encode(d)
	require.NoError(t, err)
	return d, &plan.TableDef{DbName: "db", Name: "mv", DbId: 1, TblId: 100, TableType: "m", Props: []*plan.PropertyDef{{Key: Property, Value: encoded}}}
}
func TestDefinitionIdentityAndAuthority(t *testing.T) {
	d, target := definitionFixture(t)
	ref, err := d.Reference()
	require.NoError(t, err)
	decoded, err := FromTable(target)
	require.NoError(t, err)
	require.NoError(t, decoded.Match(ref))
	source := &plan.TableDef{DbName: "db", Name: "src", DbId: 1, TblId: 11, TableType: "r"}
	resolve := func(Source) (*plan.TableDef, error) { return source, nil }
	require.NoError(t, d.ValidateSources(resolve), "schema version zero has presence")
	for _, change := range []func(){func() { source.TblId++ }, func() { source.Version++ }, func() { source.Name = "renamed" }, func() { source.DbId++ }} {
		copy := *source
		change()
		require.Error(t, d.ValidateSources(resolve))
		*source = copy
	}
	ctx := WithAuthority(defines.AttachAccountId(context.Background(), 0), d)
	require.True(t, CanWrite(ctx, target))
	require.False(t, CanWrite(defines.AttachAccountId(ctx, 1), target))
	require.False(t, CanWrite(context.WithValue(context.Background(), "materialized-refresh", true), target))
	replacement := *target
	replacement.TblId++
	require.False(t, CanWrite(ctx, &replacement))
	other := *d
	other.Generation++
	require.Error(t, other.Match(ref))
	require.False(t, CanWrite(WithAuthority(defines.AttachAccountId(context.Background(), 0), &other), target))
	owner := Owner{Format: 1, TargetID: 100, Generation: 1, StateID: 101}
	state := &plan.TableDef{TblId: 101, TableType: "i", Props: []*plan.PropertyDef{{Key: OwnerProperty, Value: EncodeOwner(owner)}}}
	d.State = &Relation{Database: "db", Name: StatePrefix + "test", DatabaseID: 1, ID: 101}
	ctx = WithAuthority(defines.AttachAccountId(context.Background(), 0), d)
	require.True(t, CanWrite(ctx, state))
	owner.TargetID++
	state.Props[0].Value = EncodeOwner(owner)
	require.False(t, CanWrite(ctx, state))
}
func TestDefinitionRejectsUnknownOrIncompleteCatalog(t *testing.T) {
	for _, change := range []func(*Definition){func(d *Definition) { d.Format++ }, func(d *Definition) { d.RequiredCapability-- }, func(d *Definition) { d.Sources[0].Version = nil }, func(d *Definition) { d.Sources = append(d.Sources, d.Sources[0]) }, func(d *Definition) { d.Target.ID = 0 }, func(d *Definition) { d.Method = "fast" }} {
		d, _ := definitionFixture(t)
		change(d)
		encoded, err := Encode(d)
		require.NoError(t, err)
		_, err = Decode(encoded, true)
		require.Error(t, err)
	}
	_, err := Decode(strings.Repeat("A", 2*MaxDefinitionBytes), true)
	require.Error(t, err)
	d, target := definitionFixture(t)
	target.TableType = "r"
	require.False(t, CanWrite(WithAuthority(defines.AttachAccountId(context.Background(), 0), d), target))
	_, err = FromTable(target)
	require.Error(t, err)
	target.TableType = "v"
	target.ViewSql = &plan.ViewDef{View: `{"Stmt":"create materialized view mv as select k,count(*) c from src group by k"}`}
	PlannerKind(target)
	require.Equal(t, "m", target.TableType)
	require.Nil(t, target.ViewSql)
	_, err = FromTable(target)
	require.NoError(t, err)
	ordinary := &plan.TableDef{TableType: "r", ViewSql: &plan.ViewDef{View: `{"Stmt":"create materialized view fake as select 1"}`}}
	PlannerKind(ordinary)
	require.Equal(t, "r", ordinary.TableType)
}
