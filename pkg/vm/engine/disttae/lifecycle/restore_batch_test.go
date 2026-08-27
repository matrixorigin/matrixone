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

package lifecycle

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestCanonicalRowsToBatchBuildsOrdinaryMOVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	schema := SchemaDescriptor{
		FormatVersion: 1,
		Columns: []SchemaColumn{
			{Ordinal: 0, Name: "id", TypeID: int32(types.T_int64)},
			{Ordinal: 1, Name: "doc", TypeID: int32(types.T_json)},
		},
	}
	rows := [][]CanonicalCell{{
		{Type: types.T_int64.ToType(), Value: int64(7)},
		{Type: types.T_json.ToType(), Value: []byte(`{"k": 1}`)},
	}}
	value, err := CanonicalRowsToBatch(context.Background(), schema, rows, mp)
	require.NoError(t, err)
	defer value.Clean(mp)
	require.Equal(t, []string{"id", "doc"}, value.Attrs)
	require.Equal(t, 1, value.RowCount())
	require.Equal(t, int64(7), vector.GetFixedAtWithTypeCheck[int64](value.Vecs[0], 0))
	require.Equal(t, `{"k": 1}`, types.DecodeJson(value.Vecs[1].GetBytesAt(0)).String())

	schemaDigest, err := schema.Digest()
	require.NoError(t, err)
	encoder := NewCanonicalValueEncoder(schemaDigest)
	require.NoError(t, encoder.WriteRow(context.Background(), rows[0]))
	require.NoError(t, VerifyRestoreBatch(
		context.Background(),
		schemaDigest,
		value,
		encoder.RowCount(),
		encoder.LogicalBytes(),
		encoder.Sum(),
	))
	require.Error(t, VerifyRestoreBatch(
		context.Background(),
		schemaDigest,
		value,
		encoder.RowCount(),
		encoder.LogicalBytes(),
		[32]byte{9},
	))
}
