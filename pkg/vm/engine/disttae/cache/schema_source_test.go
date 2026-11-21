// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cache

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCanonicalSchemaAttributeIsolation(t *testing.T) {
	typ := types.T_decimal64.ToType()
	typ.Width = 15
	typ.Scale = 3
	typ.SetNotNull(true)
	original := engine.Attribute{ID: 42, Name: "Mixed", Type: typ, IsRowId: true, IsHidden: true, Primary: true, ClusterBy: true, AutoIncrement: true, Seqnum: 9, EnumVlaues: "x,y", NumericAttributes: 3, Comment: "comment", Default: &plan.Default{OriginString: "default"}, OnUpdate: &plan.OnUpdate{OriginString: "update"}, GeneratedCol: &plan.GeneratedCol{OriginString: "generated"}}
	item := &TableItem{Name: "t", Defs: []engine.TableDef{&engine.AttributeDef{Attr: original}}}
	expected, _ := getTableDef(item, item.Defs)
	s := newCandidateSchema(item)
	require.NotNil(t, s)
	require.NotNil(t, s.planner.Cols)
	require.True(t, proto.Equal(expected, s.planner))
	source := SchemaSource{schema: s}
	a, b := source.Columns(false), source.Columns(false)
	require.Equal(t, original, *a[0])
	a[0].Default.OriginString = "changed"
	a[0].OnUpdate.OriginString = "changed"
	a[0].GeneratedCol.OriginString = "changed"
	require.Equal(t, "default", s.planner.Cols[0].Default.OriginString)
	require.Equal(t, original, *b[0])
	require.Equal(t, original, *source.Columns(false)[0])
	require.Equal(t, "default", item.Defs[0].(*engine.AttributeDef).Attr.Default.OriginString)

	defs, err := s.engineDefs(item)
	require.NoError(t, err)
	require.Equal(t, buildCanonicalDefs(item, item.Defs), defs)
	defs[len(defs)-1].(*engine.AttributeDef).Attr.Default.OriginString = "engine change"
	again, err := s.engineDefs(item)
	require.NoError(t, err)
	require.Equal(t, "default", again[len(again)-1].(*engine.AttributeDef).Attr.Default.OriginString)
}

func TestPublishedSchemaCopySharesPlanner(t *testing.T) {
	_, item, _ := columnUpdateFixture(t, 3)
	require.Nil(t, item.Defs)
	require.NotNil(t, item.schema)
	var copied TableItem
	copyTableItem(&copied, item)
	require.Same(t, item.schema, copied.schema)
	require.Same(t, item.ReadPlanTableDef(), copied.ReadPlanTableDef())
	defs, err := copied.BuildEngineDefs()
	require.NoError(t, err)
	require.NotEmpty(t, defs)
}

func TestSchemaSourceColumnsIsolation(t *testing.T) {
	item := &TableItem{schema: newCandidateSchema(&TableItem{Defs: []engine.TableDef{
		&engine.AttributeDef{Attr: engine.Attribute{Name: "a", Primary: true, Default: &plan.Default{OriginString: "one"}}},
		&engine.AttributeDef{Attr: engine.Attribute{Name: "rowid", IsRowId: true}},
		&engine.AttributeDef{Attr: engine.Attribute{Name: "b", Primary: true, OnUpdate: &plan.OnUpdate{OriginString: "update"}}},
	}})}
	source := item.SchemaSource()
	require.True(t, source.Valid())
	all := source.Columns(false)
	keys := source.Columns(true)
	require.Len(t, all, 3)
	require.True(t, all[1].IsRowId)
	require.Len(t, keys, 2)
	require.Equal(t, "a", keys[0].Name)
	require.Equal(t, "b", keys[1].Name)
	keys[0].Default.OriginString = "changed"
	keys[1].OnUpdate.OriginString = "changed"
	require.Equal(t, "one", all[0].Default.OriginString)
	require.Equal(t, "update", source.Columns(true)[1].OnUpdate.OriginString)
	require.False(t, (&TableItem{}).SchemaSource().Valid())
}

func TestSchemaSourceReconstructsPrivateViews(t *testing.T) {
	_, item, _ := columnUpdateFixture(t, 3)
	source := item.SchemaSource()
	require.True(t, source.Valid())
	expected, err := item.BuildEngineDefs()
	require.NoError(t, err)
	actual, err := source.EngineDefs(item)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
