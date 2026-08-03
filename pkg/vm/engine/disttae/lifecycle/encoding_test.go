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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestCanonicalFramingDistinguishesAdjacentValuesAndNull(t *testing.T) {
	left := hashCanonicalCells(t,
		canonicalTestCell(types.T_varchar.ToType(), []byte("ab"), false),
		canonicalTestCell(types.T_varchar.ToType(), []byte("c"), false),
	)
	right := hashCanonicalCells(t,
		canonicalTestCell(types.T_varchar.ToType(), []byte("a"), false),
		canonicalTestCell(types.T_varchar.ToType(), []byte("bc"), false),
	)
	require.NotEqual(t, left, right)

	empty := hashCanonicalCells(t,
		canonicalTestCell(types.T_varchar.ToType(), []byte{}, false),
	)
	nullValue := hashCanonicalCells(t,
		canonicalTestCell(types.T_varchar.ToType(), nil, true),
	)
	require.NotEqual(t, empty, nullValue)
}

func TestCanonicalFloatNormalizesNaNAndNegativeZero(t *testing.T) {
	firstNaN := math.Float64frombits(0x7ff8000000000001)
	secondNaN := math.Float64frombits(0x7ff8000000000042)
	require.Equal(t,
		hashCanonicalCells(t, canonicalTestCell(types.T_float64.ToType(), firstNaN, false)),
		hashCanonicalCells(t, canonicalTestCell(types.T_float64.ToType(), secondNaN, false)),
	)
	require.Equal(t,
		hashCanonicalCells(t, canonicalTestCell(types.T_float64.ToType(), float64(0), false)),
		hashCanonicalCells(t, canonicalTestCell(types.T_float64.ToType(), math.Copysign(0, -1), false)),
	)
}

func TestCanonicalBatchEncoderDeterministic(t *testing.T) {
	mp := mpool.MustNewZero()
	first := batch.New([]string{"id", "name", "created_at"})
	first.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	first.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	first.Vecs[2] = vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixed(first.Vecs[0], int64(7), false, mp))
	require.NoError(t, vector.AppendBytes(first.Vecs[1], []byte("event"), false, mp))
	require.NoError(t, vector.AppendFixed(first.Vecs[2], types.Timestamp(123456), false, mp))
	first.SetRowCount(1)
	defer first.Clean(mp)

	schemaDigest := [32]byte{1, 2, 3}
	encoderA := NewCanonicalBatchEncoder(schemaDigest)
	require.NoError(t, encoderA.WriteBatch(context.Background(), first, nil))
	encoderB := NewCanonicalBatchEncoder(schemaDigest)
	require.NoError(t, encoderB.WriteBatch(context.Background(), first, nil))

	require.Equal(t, encoderA.Sum(), encoderB.Sum())
	require.Equal(t, uint64(1), encoderA.RowCount())
	require.Positive(t, encoderA.LogicalBytes())
}

func TestCanonicalBatchEncoderRejectsUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	value := batch.New([]string{"value"})
	value.Vecs[0] = vector.NewVec(types.T_array_float32.ToType())
	value.SetRowCount(0)
	defer value.Clean(mp)

	encoder := NewCanonicalBatchEncoder([32]byte{})
	err := encoder.WriteBatch(context.Background(), value, nil)
	require.Error(t, err)
}

func canonicalTestCell(typ types.Type, value any, nullValue bool) CanonicalCell {
	return CanonicalCell{Type: typ, Value: value, Null: nullValue}
}

func hashCanonicalCells(t *testing.T, cells ...CanonicalCell) [32]byte {
	t.Helper()
	encoder := NewCanonicalValueEncoder([32]byte{})
	require.NoError(t, encoder.WriteRow(context.Background(), cells))
	return encoder.Sum()
}
