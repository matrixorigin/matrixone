// Copyright 2021 Matrix Origin
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

package catalog

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestEnumDomainSchemaRoundTrip(t *testing.T) {
	typ := types.T_enum.ToType()
	typ.SetEnumEncoding(1)
	schema := NewEmptySchema("enum_domain")
	def, err := ColDefFromAttribute(engine.Attribute{Name: "e", Type: typ, EnumVlaues: "2#3:a,b1:c"})
	require.NoError(t, err)
	def.Primary = true
	def.SortKey = true
	def.SortIdx = 0
	require.NoError(t, schema.AppendColDef(def))
	require.NoError(t, schema.Finalize(false))
	data, err := schema.Marshal()
	require.NoError(t, err)
	restored := NewEmptySchema("")
	_, err = restored.ReadFromWithVersion(bytes.NewReader(data), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	require.Equal(t, typ.GetEnumEncoding(), restored.ColDefs[0].Type.GetEnumEncoding())
	require.Equal(t, "2#3:a,b1:c", restored.ColDefs[0].EnumValues)
	var tombstone *Schema
	require.NotPanics(t, func() { tombstone = GetTombstoneSchema(schema) })
	require.NotNil(t, tombstone)
	data, err = tombstone.Marshal()
	require.NoError(t, err)
	physical := NewEmptySchema("")
	_, err = physical.ReadFromWithVersion(bytes.NewReader(data), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	require.Equal(t, types.T_enum, physical.ColDefs[1].Type.Oid)
	require.Empty(t, physical.ColDefs[1].EnumValues)
	_, err = ColDefFromAttribute(engine.Attribute{Name: "bad", Type: typ})
	require.Error(t, err)
}
