// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataLegacyObjectReader(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	for _, revision := range []uint8{0, 1} {
		typ := types.NewWithCharset(types.T_varchar, 8, 0, 3)
		typ.CollationVersion = revision
		source := vector.NewVec(typ)
		require.NoError(t, vector.AppendBytes(source, []byte("x"), false, mp))
		encoded := marshalLegacyTestColumn(t, source)
		layout, err := readLegacyColumnLayout(bytes.NewReader(encoded), int64(len(encoded)))
		require.NoError(t, err)
		require.Equal(t, typ, layout.typ)
		encoded[IOEntryHeaderSize+1+3] = 2
		_, err = readLegacyColumnLayout(bytes.NewReader(encoded), int64(len(encoded)))
		require.ErrorContains(t, err, "collation")
		source.Free(mp)
	}
	require.Zero(t, mp.CurrNB())
}
