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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataSchemaRecovery(t *testing.T) {
	for _, version := range []uint8{0, 1} {
		source := MockSchemaAll(3, 1)
		identity := uint8(3)
		if version == 1 {
			identity = 4
		}
		typ := types.NewWithCharset(types.T_varchar, 32, 0, identity)
		typ.CollationVersion = version
		source.ColDefs[0].Type = typ
		source.Extra.DefaultCharset = uint32(identity)
		source.Extra.CollationVersion = uint32(version)
		source.Extra.KeyFormat = uint32(version)
		cloned := source.Clone()
		require.Equal(t, source.Extra, cloned.Extra)
		require.Equal(t, typ, cloned.ColDefs[0].Type)
		// Exercise the actual catalog WAL reader, not just SchemaExtra.Unmarshal.
		wire, err := source.Marshal()
		require.NoError(t, err)
		restored := NewEmptySchema("restored")
		_, err = restored.ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
		require.NoError(t, err)
		require.Equal(t, source.Extra, restored.Extra)
		require.Equal(t, typ, restored.ColDefs[0].Type)
		source.Extra.KeyFormat = 257
		wire, err = source.Marshal()
		require.NoError(t, err)
		require.NotPanics(t, func() {
			_, err = NewEmptySchema("bad").ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
		})
		require.Error(t, err)
		source.Extra.KeyFormat = uint32(version)
		source.ColDefs[0].Type.CollationVersion = 2
		wire, err = source.Marshal()
		require.NoError(t, err)
		_, err = NewEmptySchema("bad-type").ReadFromWithVersion(bytes.NewReader(wire), IOET_WALTxnCommand_Table_CurrVer)
		require.Error(t, err)
	}
}
