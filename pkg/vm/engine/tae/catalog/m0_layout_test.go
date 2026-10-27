//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	"bytes"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func m0CustomerLayout(t *testing.T) (*plan.TableDef, *Schema) {
	t.Helper()
	s := NewEmptySchema("customer")
	for _, name := range []string{"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance"} {
		require.NoError(t, s.AppendCol(name, types.T_int32.ToType()))
	}
	require.NoError(t, s.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	s.ColDefs[6].Hidden = true
	require.NoError(t, s.Finalize(false))
	// Real TN append primitive preserves the hidden key at 6 and appends at 7.
	require.NoError(t, s.ApplyAlterTable(api.NewAddColumnReq(0, 0, "x", plan.Type{Id: int32(types.T_int32)}, -1)))
	s.Version = 1
	return m0PlanFromSchema(s), s
}

func m0PlanFromSchema(s *Schema) *plan.TableDef {
	cn := &plan.TableDef{Name: s.Name, Version: s.Version, Name2ColIndex: make(map[string]int32)}
	for i, col := range s.ColDefs {
		def := &plan.ColDef{
			Name: col.Name, ColId: uint64(i + 1), Seqnum: uint32(i), Hidden: col.Hidden, Primary: col.Primary,
			Typ: plan.Type{Id: int32(col.Type.Oid), Width: col.Type.Width, Scale: col.Type.Scale,
				Charset: uint32(col.Type.Charset), CollationVersion: uint32(col.Type.CollationVersion)},
		}
		cn.Cols = append(cn.Cols, def)
		cn.Name2ColIndex[col.Name] = int32(i)
		if col.Primary {
			cn.Pkey = &plan.PrimaryKeyDef{PkeyColName: col.Name, CompPkeyCol: def}
		}
	}
	return cn
}

func TestM0LayoutRejectsPermutationAndStaleMaps(t *testing.T) {
	require.IsType(t, (*error)(nil), &ErrM0Layout)
	for _, test := range []struct {
		name string
		edit func(*plan.TableDef, *Schema)
	}{
		{"CN permutation", func(cn *plan.TableDef, _ *Schema) { cn.Cols[6], cn.Cols[7] = cn.Cols[7], cn.Cols[6] }},
		{"CN identity", func(cn *plan.TableDef, _ *Schema) { cn.Cols[7].ColId = 7 }},
		{"CN sequence", func(cn *plan.TableDef, _ *Schema) { cn.Cols[7].Seqnum = 6 }},
		{"CN name map", func(cn *plan.TableDef, _ *Schema) { cn.Name2ColIndex["x"] = 6 }},
		{"CN visibility", func(cn *plan.TableDef, _ *Schema) { cn.Cols[6].Hidden = false }},
		{"CN primary key", func(cn *plan.TableDef, _ *Schema) { cn.Pkey.PkeyColName = "x" }},
		{"CN composite key reference", func(cn *plan.TableDef, _ *Schema) {
			key := *cn.Pkey.CompPkeyCol
			key.Seqnum = 7
			cn.Pkey.CompPkeyCol = &key
		}},
		{"CN version", func(cn *plan.TableDef, _ *Schema) { cn.Version++ }},
		{"TN permutation", func(_ *plan.TableDef, s *Schema) {
			s.ColDefs[6], s.ColDefs[7] = s.ColDefs[7], s.ColDefs[6]
		}},
		{"TN primary sequence", func(_ *plan.TableDef, s *Schema) { s.ColDefs[6].SeqNum = 7 }},
		{"TN name map", func(_ *plan.TableDef, s *Schema) { s.NameMap["x"] = 6 }},
		{"TN sequence map", func(_ *plan.TableDef, s *Schema) { s.SeqnumMap[7] = 6 }},
		{"TN allocation counter", func(_ *plan.TableDef, s *Schema) { s.Extra.NextColSeqnum++ }},
		{"TN rowid", func(_ *plan.TableDef, s *Schema) { s.ColDefs[len(s.ColDefs)-1].SeqNum = 8 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			cn, tn := m0CustomerLayout(t)
			require.NoError(t, ValidateM0Layout(cn, tn))
			test.edit(cn, tn)
			require.ErrorIs(t, ValidateM0Layout(cn, tn), ErrM0Layout)
		})
	}
}

func TestM0LayoutSchemaSerializationAndClone(t *testing.T) {
	cn, tn := m0CustomerLayout(t)
	data, err := tn.Marshal()
	require.NoError(t, err)
	reopened := NewEmptySchema("customer")
	_, err = reopened.ReadFromWithVersion(bytes.NewReader(data), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	require.NoError(t, ValidateM0Layout(cn, reopened))
	require.NoError(t, ValidateM0CloneLayout(cn, tn, m0PlanFromSchema(reopened), reopened))
	var visible []string
	for _, col := range cn.Cols {
		if !col.Hidden {
			visible = append(visible, col.Name)
		}
	}
	require.Equal(t, []string{"c_id", "c_d_id", "c_w_id", "c_credit_lim", "c_ytd_payment", "c_balance", "x"}, visible)
	// This validates schema encoding only, not WAL/checkpoint engine recovery.
}

func TestM0DerivedAppendVersionUsesCompleteLayout(t *testing.T) {
	_, schema := m0CustomerLayout(t)
	require.Equal(t, uint32(1), schema.Version)
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "y", plan.Type{Id: int32(types.T_int32)}, -1,
	)))
	require.Equal(t, uint32(1), schema.Version)

	version, err := M0DerivedAppendVersion(schema)
	require.NoError(t, err)
	require.Equal(t, uint32(2), version)

	schema.Extra.NextColSeqnum++
	_, err = M0DerivedAppendVersion(schema)
	require.ErrorIs(t, err, ErrM0Layout)
}

func TestM0DerivedAppendLayoutUsesObjectProofVersion(t *testing.T) {
	_, schema := m0CustomerLayout(t)
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "y", plan.Type{Id: int32(types.T_int32)}, -1,
	)))
	// Relation creation can restore complete inherited columns while leaving
	// the local catalog version at zero. The derived layout, not that marker,
	// is the merge-read qualification.
	schema.Version = 0
	layout, err := M0DerivedAppendLayout(schema)
	require.NoError(t, err)
	require.Equal(t, uint32(2), layout.Version)
	require.Equal(t, uint32(9), layout.NextColSeqnum)
	require.Equal(t, uint16(6), layout.PKSeqnum)
}

func TestM0CloneRejectsActualCatalogRenumbering(t *testing.T) {
	cn, tn := m0CustomerLayout(t)
	// A CREATE reconstruction puts visible x before the regenerated hidden PK.
	rebuilt := tn.Clone()
	rebuilt.ColDefs[6], rebuilt.ColDefs[7] = rebuilt.ColDefs[7], rebuilt.ColDefs[6]
	defs := make([]engine.TableDef, 0, len(rebuilt.ColDefs))
	for _, col := range rebuilt.ColDefs {
		defs = append(defs, &engine.AttributeDef{Attr: engine.Attribute{
			Name: col.Name, Type: col.Type, ID: uint64(col.SeqNum) + 1, Seqnum: col.SeqNum,
			IsHidden: col.Hidden, Primary: col.Primary,
		}})
	}
	_, err := pkgcatalog.GenColumnsFromDefs(0, "clone", "db", 2, 1, defs)
	require.NoError(t, err)
	rebuilt.NameMap = make(map[string]int)
	rebuilt.SeqnumMap = make(map[uint16]int)
	for i, def := range defs {
		attr := def.(*engine.AttributeDef).Attr
		rebuilt.ColDefs[i].Idx = i
		rebuilt.NameMap[attr.Name] = i
		if !rebuilt.ColDefs[i].IsPhyAddr() {
			rebuilt.ColDefs[i].SeqNum = attr.Seqnum
		}
	}
	require.NoError(t, rebuilt.Finalize(true))
	target := m0PlanFromSchema(rebuilt)
	// A local-only dense-layout check is insufficient: both sides pass it.
	require.NoError(t, ValidateM0Layout(target, rebuilt))
	require.ErrorIs(t, ValidateM0CloneLayout(cn, tn, target, rebuilt), ErrM0Layout)
	require.Equal(t, uint16(6), tn.GetPrimaryKey().SeqNum)
	require.Equal(t, uint16(7), rebuilt.GetPrimaryKey().SeqNum)
}

func TestM0SchemaIdentityDoesNotGuessDestinationVersion(t *testing.T) {
	_, schema := m0CustomerLayout(t)
	c := MockCatalog(nil)
	defer c.Close()
	db, err := c.CreateDBEntry("m0", "", "", nil)
	require.NoError(t, err)
	owner, err := db.CreateTableEntry(schema, nil, nil)
	require.NoError(t, err)
	h := objectio.BuildHeader()
	h.SetSchemaVersion(schema.Version)
	// Use the object metadata representation returned by the real writer test
	// for zero-origin coverage. Bound identities below are synthetic fixtures.
	data := make(objectio.ObjectDataMeta, 1024)
	data.BlockHeader().SetTableID(owner.ID)
	resolved, err := resolveM0ObjectSchema(owner, h, data)
	require.NoError(t, err)
	require.Same(t, schema, resolved)
	data.BlockHeader().SetTableID(owner.ID + 1)
	_, err = resolveM0ObjectSchema(owner, h, data)
	require.ErrorIs(t, err, objectio.ErrM0SchemaIdentityUnavailable)
	data.BlockHeader().SetTableID(owner.ID)
	h.SetSchemaVersion(schema.Version + 1)
	_, err = resolveM0ObjectSchema(owner, h, data)
	require.ErrorIs(t, err, objectio.ErrM0SchemaIdentityUnavailable)
	data.BlockHeader().SetTableID(0)
	_, err = resolveM0ObjectSchema(owner, h, data)
	require.ErrorIs(t, err, objectio.ErrM0SchemaIdentityUnavailable)
}
