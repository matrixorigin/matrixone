//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"bytes"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func birthAddReq(s *Schema, name string) *api.AlterTableReq {
	next := s.Extra.NextColSeqnum
	req := api.NewAddColumnReq(1, 42, name, types.NewProtoType(types.T_bool), -1)
	a := req.GetAddColumn()
	a.Column.ColId = uint64(next) + 1
	a.Column.Seqnum = next
	a.Column.Default = &plan.Default{NullAbility: true, OriginString: "false", Expr: &plan.Expr{Typ: a.Column.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: false}}}}}
	a.ExpectedSchemaVersion = s.Version
	a.ExpectedNextColSeqnum = next
	a.AppendMissingState = plan.CloneAppendMissingState(s.Extra.AppendMissingState)
	if a.AppendMissingState == nil {
		a.AppendMissingState = &plan.AppendMissingState{FormatVersion: 1, LayoutId: []byte("0123456789abcdef"), BaseWidth: next, PkSeqnum: next - 1}
	}
	a.AppendMissingState.Columns = append(a.AppendMissingState.Columns, &plan.ColumnBirthFill{Seqnum: next, BirthAppendOrdinal: s.Version + 1, Type: a.Column.Typ, Kind: plan.MissingFillKind_MISSING_FILL_BOOL_FALSE_V1})
	return req
}

func TestM0BirthAddAtomicSchemaHistoryAndRecovery(t *testing.T) {
	s := appendMissingSchema(t)
	s.Relkind = pkgcatalog.SystemOrdinaryRel
	old := s.Clone()
	req := birthAddReq(s, "second")
	require.NoError(t, s.ApplyAlterTable(req))
	require.Equal(t, uint32(2), s.Version)
	require.Equal(t, uint32(5), s.Extra.NextColSeqnum)
	require.Len(t, old.Extra.AppendMissingState.Columns, 1)
	require.Len(t, s.Extra.AppendMissingState.Columns, 2)
	req.GetAddColumn().AppendMissingState.LayoutId[0] = 'x'
	require.Equal(t, byte('0'), s.Extra.AppendMissingState.LayoutId[0])
	wire, err := s.Marshal()
	require.NoError(t, err)
	restored := NewEmptySchema("recovery")
	_, err = restored.ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	require.Equal(t, s.Extra.AppendMissingState, restored.Extra.AppendMissingState)
	for _, mutate := range []func(*api.AlterTableAddColumn){
		func(a *api.AlterTableAddColumn) { a.ExpectedSchemaVersion-- },
		func(a *api.AlterTableAddColumn) { a.ExpectedNextColSeqnum-- },
		func(a *api.AlterTableAddColumn) {
			a.AppendMissingState.Columns[0].Kind = plan.MissingFillKind_MISSING_FILL_NULL_V1
		},
		func(a *api.AlterTableAddColumn) { a.AppendMissingState = nil },
		func(a *api.AlterTableAddColumn) { a.Column.Default.OriginString = "true" },
	} {
		before, err := s.Marshal()
		require.NoError(t, err)
		bad := birthAddReq(s, "rejected")
		mutate(bad.GetAddColumn())
		require.Error(t, s.ApplyAlterTable(bad))
		after, err := s.Marshal()
		require.NoError(t, err)
		require.Equal(t, before, after, "validation must precede schema mutation")
	}
}

func TestM0BirthAddOrdinaryCatalogProperties(t *testing.T) {
	for _, invalidProperty := range []bool{false, true} {
		s := appendMissingSchema(t)
		s.Relkind = pkgcatalog.SystemOrdinaryRel
		properties := []*plan.Property{{Key: pkgcatalog.SystemRelAttr_Kind, Value: pkgcatalog.SystemOrdinaryRel}, {Key: pkgcatalog.SystemRelAttr_CreateSQL, Value: "create table customer (...)"}}
		if invalidProperty {
			properties = append(properties, &plan.Property{Key: "stream.connector", Value: "unsupported"})
		}
		table := &plan.TableDef{Defs: []*plan.TableDef_DefType{{Def: &plan.TableDef_DefType_Properties{Properties: &plan.PropertiesDef{Properties: properties}}}}}
		constraint := engine.PlanDefToCstrDef(table)
		var err error
		s.Constraint, err = constraint.MarshalBinary()
		require.NoError(t, err)
		before, err := s.Marshal()
		require.NoError(t, err)
		err = s.ApplyAlterTable(birthAddReq(s, "ordinary_flag"))
		if invalidProperty {
			require.Error(t, err)
			after, e := s.Marshal()
			require.NoError(t, e)
			require.Equal(t, before, after)
		} else {
			require.NoError(t, err)
		}
	}
}
