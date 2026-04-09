// Copyright 2024 Matrix Origin
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

package publication

import (
	"database/sql"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- truncateSQL ----

func TestTruncateSQL_Short(t *testing.T) {
	assert.Equal(t, "SELECT 1", truncateSQL("SELECT 1"))
}

func TestTruncateSQL_ExactLimit(t *testing.T) {
	s := make([]byte, 200)
	for i := range s {
		s[i] = 'a'
	}
	assert.Equal(t, string(s), truncateSQL(string(s)))
}

func TestTruncateSQL_Long(t *testing.T) {
	s := make([]byte, 300)
	for i := range s {
		s[i] = 'b'
	}
	result := truncateSQL(string(s))
	assert.Len(t, result, 203) // 200 + "..."
	assert.Equal(t, "...", result[200:])
}

// ---- helpers to build executor.Result with batch+vector ----

func newMPool(t *testing.T) *mpool.MPool {
	mp, err := mpool.NewMPool("test_internal_sql", 0, mpool.NoFixed)
	require.NoError(t, err)
	return mp
}

func makeStringBatch(t *testing.T, mp *mpool.MPool, vals []string) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendBytes(vec, []byte(v), false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeBoolBatch(t *testing.T, mp *mpool.MPool, vals []bool) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_bool.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeInt8Batch(t *testing.T, mp *mpool.MPool, vals []int8) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_int8.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeInt64Batch(t *testing.T, mp *mpool.MPool, vals []int64) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_int64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeUint32Batch(t *testing.T, mp *mpool.MPool, vals []uint32) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_uint32.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeUint64Batch(t *testing.T, mp *mpool.MPool, vals []uint64) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_uint64.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeTSBatch(t *testing.T, mp *mpool.MPool, vals []types.TS) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_TS.ToType())
	for _, v := range vals {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(vals))
	return bat
}

func makeJsonBatch(t *testing.T, mp *mpool.MPool, jsonStrs []string) *batch.Batch {
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_json.ToType())
	for _, s := range jsonStrs {
		bj, err := types.ParseStringToByteJson(s)
		require.NoError(t, err)
		require.NoError(t, vector.AppendByteJson(vec, bj, false, mp))
	}
	bat.Vecs[0] = vec
	bat.SetRowCount(len(jsonStrs))
	return bat
}

func buildResult(mp *mpool.MPool, batches ...*batch.Batch) executor.Result {
	return executor.Result{
		Batches: batches,
		Mp:      mp,
	}
}

// ---- InternalResult.Close ----

func TestInternalResult_Close_NilBatches(t *testing.T) {
	r := &InternalResult{}
	assert.NoError(t, r.Close())
}

func TestInternalResult_Close_WithBatches(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"hello"})
	r := &InternalResult{
		executorResult: buildResult(mp, bat),
	}
	assert.NoError(t, r.Close())
}

// ---- InternalResult.Next ----

func TestInternalResult_Next_EmptyBatches(t *testing.T) {
	r := &InternalResult{}
	assert.False(t, r.Next())
}

func TestInternalResult_Next_WithError(t *testing.T) {
	r := &InternalResult{err: assert.AnError}
	assert.False(t, r.Next())
}

func TestInternalResult_Next_NilBatchSkipped(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"a"})
	r := &InternalResult{
		executorResult: buildResult(mp, nil, bat),
	}
	// First call should skip nil batch and find row in second batch
	assert.True(t, r.Next())
	assert.False(t, r.Next())
}

func TestInternalResult_Next_MultipleBatches(t *testing.T) {
	mp := newMPool(t)
	bat1 := makeStringBatch(t, mp, []string{"a"})
	bat2 := makeStringBatch(t, mp, []string{"b", "c"})
	r := &InternalResult{
		executorResult: buildResult(mp, bat1, bat2),
	}
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.True(t, r.Next())
	assert.False(t, r.Next())
}

// ---- InternalResult.Scan ----

func TestInternalResult_Scan_NoBatches(t *testing.T) {
	r := &InternalResult{}
	var s string
	assert.Error(t, r.Scan(&s))
}

func TestInternalResult_Scan_ColumnCountMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"hello"})
	r := &InternalResult{
		executorResult: buildResult(mp, bat),
	}
	require.True(t, r.Next())
	var s1, s2 string
	err := r.Scan(&s1, &s2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "column count mismatch")
}

// ---- extractVectorValue: string types ----

func TestExtractVectorValue_String(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"hello"})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	require.NoError(t, r.Scan(&s))
	assert.Equal(t, "hello", s)
}

func TestExtractVectorValue_StringToBytes(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"bytes"})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var b []byte
	require.NoError(t, r.Scan(&b))
	assert.Equal(t, []byte("bytes"), b)
}

func TestExtractVectorValue_StringToNullString(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"ns"})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var ns sql.NullString
	require.NoError(t, r.Scan(&ns))
	assert.True(t, ns.Valid)
	assert.Equal(t, "ns", ns.String)
}

func TestExtractVectorValue_StringTypeMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeStringBatch(t, mp, []string{"x"})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var i int64
	err := r.Scan(&i)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: bool ----

func TestExtractVectorValue_Bool(t *testing.T) {
	mp := newMPool(t)
	bat := makeBoolBatch(t, mp, []bool{true})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var b bool
	require.NoError(t, r.Scan(&b))
	assert.True(t, b)
}

func TestExtractVectorValue_BoolToNullBool(t *testing.T) {
	mp := newMPool(t)
	bat := makeBoolBatch(t, mp, []bool{false})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var nb sql.NullBool
	require.NoError(t, r.Scan(&nb))
	assert.True(t, nb.Valid)
	assert.False(t, nb.Bool)
}

func TestExtractVectorValue_BoolTypeMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeBoolBatch(t, mp, []bool{true})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: int8 ----

func TestExtractVectorValue_Int8(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt8Batch(t, mp, []int8{42})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var v int8
	require.NoError(t, r.Scan(&v))
	assert.Equal(t, int8(42), v)
}

func TestExtractVectorValue_Int8TypeMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt8Batch(t, mp, []int8{1})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: int64 ----

func TestExtractVectorValue_Int64(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt64Batch(t, mp, []int64{999})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var v int64
	require.NoError(t, r.Scan(&v))
	assert.Equal(t, int64(999), v)
}

func TestExtractVectorValue_Int64ToNullInt64(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt64Batch(t, mp, []int64{123})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var ni sql.NullInt64
	require.NoError(t, r.Scan(&ni))
	assert.True(t, ni.Valid)
	assert.Equal(t, int64(123), ni.Int64)
}

func TestExtractVectorValue_Int64ToUint64(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt64Batch(t, mp, []int64{100})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var u uint64
	require.NoError(t, r.Scan(&u))
	assert.Equal(t, uint64(100), u)
}

func TestExtractVectorValue_Int64ToUint64_Negative(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt64Batch(t, mp, []int64{-1})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var u uint64
	err := r.Scan(&u)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "negative")
}

func TestExtractVectorValue_Int64TypeMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeInt64Batch(t, mp, []int64{1})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: uint32 ----

func TestExtractVectorValue_Uint32(t *testing.T) {
	mp := newMPool(t)
	bat := makeUint32Batch(t, mp, []uint32{42})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var v uint32
	require.NoError(t, r.Scan(&v))
	assert.Equal(t, uint32(42), v)
}

func TestExtractVectorValue_Uint32ToNullInt64(t *testing.T) {
	mp := newMPool(t)
	bat := makeUint32Batch(t, mp, []uint32{100})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var ni sql.NullInt64
	require.NoError(t, r.Scan(&ni))
	assert.True(t, ni.Valid)
	assert.Equal(t, int64(100), ni.Int64)
}

func TestExtractVectorValue_Uint32TypeMismatch(t *testing.T) {
	mp := newMPool(t)
	bat := makeUint32Batch(t, mp, []uint32{1})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: uint64 ----

func TestExtractVectorValue_Uint64(t *testing.T) {
	mp := newMPool(t)
	bat := makeUint64Batch(t, mp, []uint64{999})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var v uint64
	require.NoError(t, r.Scan(&v))
	assert.Equal(t, uint64(999), v)
}

// ---- extractVectorValue: TS ----

func TestExtractVectorValue_TS(t *testing.T) {
	mp := newMPool(t)
	ts := types.BuildTS(100, 1)
	bat := makeTSBatch(t, mp, []types.TS{ts})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var v types.TS
	require.NoError(t, r.Scan(&v))
	assert.Equal(t, ts, v)
}

func TestExtractVectorValue_TSTypeMismatch(t *testing.T) {
	mp := newMPool(t)
	ts := types.BuildTS(100, 1)
	bat := makeTSBatch(t, mp, []types.TS{ts})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type mismatch")
}

// ---- extractVectorValue: json ----

func TestExtractVectorValue_JsonToString(t *testing.T) {
	mp := newMPool(t)
	bat := makeJsonBatch(t, mp, []string{`{"key":"val"}`})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var s string
	require.NoError(t, r.Scan(&s))
	assert.Contains(t, s, "key")
}

func TestExtractVectorValue_JsonToNullString(t *testing.T) {
	mp := newMPool(t)
	bat := makeJsonBatch(t, mp, []string{`{"a":1}`})
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var ns sql.NullString
	require.NoError(t, r.Scan(&ns))
	assert.True(t, ns.Valid)
	assert.Contains(t, ns.String, "a")
}

// ---- extractVectorValue: unsupported type ----

func TestExtractVectorValue_UnsupportedType(t *testing.T) {
	mp := newMPool(t)
	// Use float32 which is not handled
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_float32.ToType())
	require.NoError(t, vector.AppendFixed(vec, float32(1.0), false, mp))
	bat.Vecs[0] = vec
	bat.SetRowCount(1)

	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var f float32
	err := r.Scan(&f)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported vector type")
}

// ---- InternalResult.Err ----

func TestInternalResult_Err(t *testing.T) {
	r := &InternalResult{}
	assert.Nil(t, r.Err())

	r.err = assert.AnError
	assert.Equal(t, assert.AnError, r.Err())
}

// ---- Scan with NULL value ----

func TestInternalResult_Scan_NullBytes(t *testing.T) {
	mp := newMPool(t)
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, nil, true, mp)) // null
	bat.Vecs[0] = vec
	bat.SetRowCount(1)

	r := &InternalResult{executorResult: buildResult(mp, bat)}
	require.True(t, r.Next())

	var b []byte
	require.NoError(t, r.Scan(&b))
	assert.Nil(t, b)
}
