// Copyright 2022 Matrix Origin
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

package containers

import (
	"bytes"
	"math/rand"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
)

func TestBatch1a(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()[2:4] // int32, int64
	attrs := []string{"attr1", "attr2"}
	opts := Options{}
	opts.Capacity = 0
	bat := BuildBatch(attrs, vecTypes, opts)
	bat.Vecs[0].Append(int32(1), false)
	bat.Vecs[0].Append(int32(2), false)
	bat.Vecs[0].Append(int32(3), false)
	bat.Vecs[1].Append(int64(11), false)
	bat.Vecs[1].Append(int64(12), false)
	bat.Vecs[1].Append(int64(13), false)

	assert.Equal(t, 3, bat.Length())
	assert.False(t, bat.HasDelete())
	bat.Delete(1)
	assert.Equal(t, 3, bat.Length())
	assert.True(t, bat.HasDelete())
	assert.True(t, bat.IsDeleted(1))

	w := new(bytes.Buffer)
	_, err := bat.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())
	bat2 := NewEmptyBatch()
	_, err = bat2.ReadFrom(r)
	assert.NoError(t, err)
	assert.True(t, bat.Equals(bat2))

	bat.Close()
}

func TestBatch1b(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()[12:14] // Varchar, Char
	attrs := []string{"attr1", "attr2"}
	opts := Options{}
	opts.Capacity = 0
	bat := BuildBatch(attrs, vecTypes, opts)
	bat.Vecs[0].Append([]byte("a"), false)
	bat.Vecs[0].Append([]byte("b"), false)
	bat.Vecs[0].Append([]byte("c"), false)
	bat.Vecs[1].Append([]byte("1"), false)
	bat.Vecs[1].Append([]byte("2"), false)
	bat.Vecs[1].Append([]byte("3"), false)

	assert.Equal(t, 3, bat.Length())
	assert.False(t, bat.HasDelete())
	bat.Delete(1)
	assert.Equal(t, 3, bat.Length())
	assert.True(t, bat.HasDelete())
	assert.True(t, bat.IsDeleted(1))

	w := new(bytes.Buffer)
	_, err := bat.WriteTo(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())
	bat2 := NewEmptyBatch()
	_, err = bat2.ReadFrom(r)
	assert.NoError(t, err)
	assert.True(t, bat.Equals(bat2))

	bat.Close()
}
func TestBatch2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	bat := MockBatch(vecTypes, 10, 3, nil)
	assert.Equal(t, 10, bat.Length())

	cloned := bat.CloneWindow(0, 5)
	assert.Equal(t, 5, cloned.Length())
	t.Log(cloned.Allocated())
	cloned.Close()
	cloned = bat.CloneWindow(0, bat.Length())
	assert.True(t, bat.Equals(cloned))
	cloned.Close()
	bat.Close()
}

func TestBatch3(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	bat := MockBatch(vecTypes, 101, 3, nil)
	defer bat.Close()
	bats := bat.Split(5)
	assert.Equal(t, 5, len(bats))
	row := 0
	for _, b := range bats {
		row += b.Length()
	}
	assert.Equal(t, bat.Length(), row)

	bat2 := MockBatch(vecTypes, 20, 3, nil)
	bats = bat2.Split(2)
	t.Log(bats[0].Vecs[3].Length())
	t.Log(bats[1].Vecs[3].Length())
}

func TestBatchWithPool(t *testing.T) {
	defer testutils.AfterTest(t)()
	colTypes := types.MockColTypes()
	seedBat := MockBatch(colTypes, 10, 3, nil)
	defer seedBat.Close()

	uid := uuid.New()
	pool := NewVectorPool(uid.String(), 200)

	bat := BuildBatchWithPool(seedBat.Attrs, colTypes, 0, pool)

	err := bat.Append(seedBat)
	assert.NoError(t, err)
	assert.True(t, seedBat.Equals(bat))

	bat.Reset()

	usedCnt, _ := pool.FixedSizeUsed(false)
	assert.True(t, usedCnt > 0)

	assert.Equal(t, 0, bat.Length())
	err = bat.Append(seedBat)
	assert.NoError(t, err)
	assert.True(t, seedBat.Equals(bat))
	bat.Close()

	usedCnt, _ = pool.FixedSizeUsed(false)
	assert.Equal(t, 0, usedCnt)
	usedCnt, _ = pool.VarlenUsed(false)
	assert.Equal(t, 0, usedCnt)

	pool.Destory()
}

func TestBatchSpliter(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()
	bat := MockBatch(vecTypes, 11, 3, nil)
	defer bat.Close()
	spliter := NewBatchSplitter(bat, 5)
	expects_1 := []int{5, 5, 1}
	var actuals_1 []int
	for {
		bat, err := spliter.Next()
		if err != nil {
			break
		}
		actuals_1 = append(actuals_1, bat.Length())
	}
	assert.Equal(t, expects_1, actuals_1)

	bat2 := MockBatch(vecTypes, 10, 3, nil)
	defer bat2.Close()
	spliter = NewBatchSplitter(bat2, 5)
	expects_2 := []int{5, 5}
	var actuals_2 []int
	for {
		bat, err := spliter.Next()
		if err != nil {
			break
		}
		actuals_2 = append(actuals_2, bat.Length())
	}
	assert.Equal(t, expects_2, actuals_2)
}

func TestApproxSize(t *testing.T) {
	defer testutils.AfterTest(t)()
	vec := MockVector(types.T_uint8.ToType(), 1024, false, nil)
	t.Log(vec.ApproxSize())
	defer vec.Close()

	svec := MakeVector(types.T_varchar.ToType(), common.DefaultAllocator)
	defer svec.Close()
	sizeCnt := 0
	for i := 0; i < 1024; i++ {
		l := rand.Intn(100)
		sizeCnt += l
		svec.Append([]byte(strings.Repeat("x", l)), false)
	}
	t.Log(svec.ApproxSize(), sizeCnt)
}

func TestCompatibilityV2(t *testing.T) {
	defer testutils.AfterTest(t)()
	vecTypes := types.MockColTypes()[2:4] // int32, int64
	attrs := []string{"attr1", "attr2"}
	opts := Options{}
	opts.Capacity = 0
	bat := BuildBatch(attrs, vecTypes, opts)
	bat.Vecs[0].Append(int32(1), false)
	bat.Vecs[0].Append(int32(2), false)
	bat.Vecs[0].Append(int32(3), true)
	bat.Vecs[1].Append(int64(11), false)
	bat.Vecs[1].Append(int64(12), true)
	bat.Vecs[1].Append(int64(13), false)

	assert.Equal(t, 3, bat.Length())
	assert.False(t, bat.HasDelete())
	bat.Delete(1)
	assert.Equal(t, 3, bat.Length())
	assert.True(t, bat.HasDelete())
	assert.True(t, bat.IsDeleted(1))

	w := new(bytes.Buffer)
	_, err := bat.WriteToV2(w)
	assert.NoError(t, err)

	r := bytes.NewBuffer(w.Bytes())
	bat2 := NewEmptyBatch()
	_, err = bat2.ReadFromV2(r)
	assert.NoError(t, err)
	assert.True(t, bat.Equals(bat2))

	bat.Close()
}
