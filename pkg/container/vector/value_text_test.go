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

package vector

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// zeroRowVector returns a one-row vector of oid holding the zero value (an
// empty value for a varlena type, a small object for json).
func zeroRowVector(t *testing.T, oid types.T, mp *mpool.MPool) *Vector {
	typ := oid.ToType()
	vec := NewVec(typ, mp)
	raw := make([]byte, typ.TypeSize())
	if typ.IsVarlen() {
		raw = nil
	}
	if oid == types.T_json {
		j, err := types.ParseStringToByteJson(`{"a":1}`)
		require.NoError(t, err)
		raw, err = types.EncodeJson(j)
		require.NoError(t, err)
	}
	require.NoError(t, AppendRaw(vec, raw, false, mp))
	return vec
}

// Every type id the vector table supports has a text form, a SQL literal
// and a row key, and its raw bytes round-trip.
func TestValueTextCoversTypeOps(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, row := range typeOpsRows {
		oid := row.oid
		vec := zeroRowVector(t, oid, mp)
		typ := *vec.GetType()
		val := GetAny(vec, 0, false)

		text := types.ValueText(typ, val)
		require.NotContains(t, text, "<unsupported", "%s has no ValueText case", oid)
		require.Equal(t, text, vec.RowToString(0), oid.String())
		require.Equal(t, text, types.ValueText(typ, ExportValue(vec, 0)), "%s: the exported form has the same text", oid)

		if !oid.IsBlockScaledArray() { // an empty cell is not a valid block-scaled vector
			lit, err := types.AppendSQLLiteral(nil, typ, val, time.UTC)
			require.NoError(t, err, oid.String())
			require.NotEmpty(t, lit, oid.String())
		}

		require.Equal(t, RowKey(vec, 0), RowKey(vec, 0), oid.String())
		raw := RawFromValue(oid, val)
		require.Equal(t, vec.GetRawBytesAt(0), raw, oid.String())
		require.Equal(t, val, ValueFromRaw(oid, raw), oid.String())
		vec.Free(mp)
	}
}

func TestValueTextAndSQLLiteral(t *testing.T) {
	dec := types.New(types.T_decimal256, 50, 2)
	d256, err := types.ParseDecimal256("-12345678901234567890123456789012345678901.25", 50, 2)
	require.NoError(t, err)
	require.Equal(t, "-12345678901234567890123456789012345678901.25", types.ValueText(dec, d256))
	lit, err := types.AppendSQLLiteral(nil, dec, d256, nil)
	require.NoError(t, err)
	require.Equal(t, "'-12345678901234567890123456789012345678901.25'", string(lit))

	str := types.T_varchar.ToType()
	lit, err = types.AppendSQLLiteral(nil, str, []byte(`it's a \ test`), nil)
	require.NoError(t, err)
	require.Equal(t, `'it\'s a \\ test'`, string(lit))

	lit, err = types.AppendSQLLiteral(nil, types.T_varbinary.ToType(), []byte{0xde, 0xad}, nil)
	require.NoError(t, err)
	require.Equal(t, "x'dead'", string(lit))

	bit := types.New(types.T_bit, 12, 0)
	lit, err = types.AppendSQLLiteral(nil, bit, uint64(0xabc), nil)
	require.NoError(t, err)
	require.Equal(t, "x'0abc'", string(lit))

	lit, err = types.AppendSQLLiteral(nil, str, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "NULL", string(lit))

	require.Equal(t, "1.5", types.ValueText(types.T_float32.ToType(), float32(1.5)))
	require.Equal(t, "true", types.ValueText(types.T_bool.ToType(), true))

	ts := types.New(types.T_timestamp, 0, 3)
	tsv, err := types.ParseTimestamp(time.UTC, "2026-11-27 10:11:12.345", 3)
	require.NoError(t, err)
	lit, err = types.AppendSQLLiteral(nil, ts, tsv, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "'2026-11-27 10:11:12.345'", string(lit))
}

func TestRawToFloat64(t *testing.T) {
	dec := types.New(types.T_decimal256, 50, 2)
	d, err := types.ParseDecimal256("-3.25", 50, 2)
	require.NoError(t, err)
	f, ok := types.RawToFloat64(dec, types.EncodeDecimal256(&d))
	require.True(t, ok)
	require.Equal(t, -3.25, f)

	f, ok = types.RawToFloat64(types.T_varchar.ToType(), []byte("ab"))
	require.True(t, ok)
	require.Equal(t, float64(uint64('a')<<56|uint64('b')<<48), f)

	_, ok = types.RawToFloat64(types.T_json.ToType(), nil)
	require.False(t, ok)
}
