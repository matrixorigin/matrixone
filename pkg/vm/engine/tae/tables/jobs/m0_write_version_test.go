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

package jobs

import (
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/internal/m0read"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/stretchr/testify/require"
)

func TestM0WriteSchemaVersionUsesCurrentCompletePrefix(t *testing.T) {
	schema := catalog.NewEmptySchema("customer")
	for _, name := range []string{"a", "b"} {
		require.NoError(t, schema.AppendCol(name, types.T_int32.ToType()))
	}
	require.NoError(t, schema.AppendPKCol(pkgcatalog.CPrimaryKeyColName, types.T_varchar.ToType(), 0))
	schema.ColDefs[2].Hidden = true
	require.NoError(t, schema.Finalize(false))
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "tier", plan.Type{Id: int32(types.T_varchar)}, -1,
	)))
	require.NoError(t, schema.ApplyAlterTable(api.NewAddColumnReq(
		0, 0, "credit", plan.Type{Id: int32(types.T_int32)}, -1,
	)))
	schema.Version = 1

	complete := []uint16{0, 1, 2, 3, 4, objectio.SEQNUM_COMMITTS}
	ordinary, err := m0WriteSchemaVersion(schema, complete, 7, 42, schema.Version)
	require.NoError(t, err)
	require.Equal(t, schema.Version, ordinary, "uncontracted objects keep ordinary version semantics")
	c := &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), DatabaseId: 7, RelationId: 42, RootColumns: 3, LayoutGeneration: 2}
	schema.Extra.M0Evolution = c
	constraints := &engine.ConstraintDef{Cts: []engine.Constraint{&engine.PrimaryKeyDef{Pkey: &plan.PrimaryKeyDef{PkeyColName: pkgcatalog.CPrimaryKeyColName, Names: []string{"a", "b"}}}}}
	schema.Constraint, err = constraints.MarshalBinary()
	require.NoError(t, err)
	for _, col := range schema.ColDefs {
		if col.IsPhyAddr() {
			continue
		}
		sem, err := catalog.EvolutionSchemaColumnSemantic(schema, col, uint64(col.SeqNum)+1)
		require.NoError(t, err)
		g := uint32(0)
		if col.SeqNum >= 3 {
			g = uint32(col.SeqNum) - 2
		}
		c.Columns = append(c.Columns, &api.M0EvolutionColumn{ColumnId: uint64(col.SeqNum) + 1, Seqnum: uint32(col.SeqNum), IntroducedLayout: g, Semantics: sem})
	}
	c.SchemaDigest = m0read.EvolutionDigest(c.Columns)
	schema.Extra.M0Evolution = c
	for _, tc := range []struct {
		seqs []uint16
		want uint32
	}{
		{complete, 2}, {[]uint16{0, 1, 2, objectio.SEQNUM_COMMITTS}, 0}, {[]uint16{0, 1, 2, 3}, 1},
	} {
		version, err := m0WriteSchemaVersion(schema, tc.seqs, 7, 42, schema.Version)
		require.NoError(t, err)
		require.Equal(t, tc.want, version)
	}
	_, err = m0WriteSchemaVersion(schema, []uint16{0, 1, 2, 4, objectio.SEQNUM_COMMITTS}, 7, 42, schema.Version)
	require.Error(t, err)
	_, err = m0WriteSchemaVersion(schema, complete, 7, 43, schema.Version)
	require.Error(t, err, "a writer cannot adopt another relation's contract")
	trimmed := schema.Clone()
	trimmed.ColDefs = append(trimmed.ColDefs[:4], trimmed.ColDefs[5:]...)
	_, err = m0WriteSchemaVersion(trimmed, []uint16{0, 1, 2, 3}, 7, 42, schema.Version)
	require.Error(t, err, "an incomplete schema cannot masquerade as a pinned older batch")
	c.FormatVersion++
	_, err = m0WriteSchemaVersion(schema, complete, 7, 42, schema.Version)
	require.Error(t, err)
}
