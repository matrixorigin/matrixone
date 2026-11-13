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

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/stretchr/testify/require"
)

func TestEvolutionContractSchemaCodecOwnership(t *testing.T) {
	s := MockSchemaAll(3, 1)
	s.FromPublication = true
	s.Extra.FromPublication = true
	s.Extra.M0Evolution = &api.M0EvolutionContract{FormatVersion: 1, RootId: []byte("0123456789abcdef"), DatabaseId: 7, RelationId: 42,
		RootColumns: 1, Columns: []*api.M0EvolutionColumn{{ColumnId: 1, Semantics: []byte("descriptor")}}, SchemaDigest: []byte("digest")}
	wire, err := s.Marshal()
	require.NoError(t, err)
	restored := NewEmptySchema("restored")
	_, err = restored.ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
	require.NoError(t, err)
	clones := []*api.SchemaExtra{api.CloneExtra(s.Extra), s.Clone().Extra, restored.Extra}
	for _, extra := range clones {
		require.True(t, extra.FromPublication, "transaction-owned copies must retain ordinary metadata")
		require.Equal(t, s.Extra.M0Evolution, extra.M0Evolution)
		extra.M0Evolution.RootId[0] = 'X'
		extra.M0Evolution.Columns[0].Semantics[0] = 'X'
		require.Equal(t, byte('0'), s.Extra.M0Evolution.RootId[0])
		require.Equal(t, byte('d'), s.Extra.M0Evolution.Columns[0].Semantics[0])
	}
}
