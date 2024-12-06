// Copyright 2022 Matrix Origin
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

package engine_util

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/assert"
)

func TestNewMemPKFilter(t *testing.T) {

	lb, ub := 10, 20

	baseFilters := []BasePKFilter{
		{Op: function.BETWEEN, Valid: true, Oid: types.T_int8, LB: types.EncodeFixed(int8(lb)), UB: types.EncodeFixed(int8(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_int16, LB: types.EncodeFixed(int16(lb)), UB: types.EncodeFixed(int16(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(lb)), UB: types.EncodeFixed(int32(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_int64, LB: types.EncodeFixed(int64(lb)), UB: types.EncodeFixed(int64(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_uint8, LB: types.EncodeFixed(uint8(lb)), UB: types.EncodeFixed(uint8(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_uint16, LB: types.EncodeFixed(uint16(lb)), UB: types.EncodeFixed(uint16(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_uint32, LB: types.EncodeFixed(uint32(lb)), UB: types.EncodeFixed(uint32(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_uint64, LB: types.EncodeFixed(uint64(lb)), UB: types.EncodeFixed(uint64(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_float32, LB: types.EncodeFixed(float32(lb)), UB: types.EncodeFixed(float32(ub))},
		{Op: function.BETWEEN, Valid: true, Oid: types.T_float64, LB: types.EncodeFixed(float64(lb)), UB: types.EncodeFixed(float64(ub))},

		{Op: function.BETWEEN, Valid: true, Oid: types.T_enum, LB: types.EncodeFixed(types.Enum(lb)), UB: types.EncodeFixed(types.Enum(ub))},
	}

	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a"},
		},
		Cols: []*plan.ColDef{
			{
				Name: "a",
				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
			},
		},
	}

	ts := types.MaxTs().ToTimestamp()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer {
			return types.NewPacker()
		},
		func(packer *types.Packer) {
			packer.Reset()
		},
		func(packer *types.Packer) {
			packer.Close()
		},
	)

	for i := range baseFilters {
		tableDef.Cols[0].Typ.Id = int32(baseFilters[i].Oid)
		filter, err := NewMemPKFilter(
			tableDef,
			ts,
			packerPool,
			baseFilters[i],
			engine.FilterHint{})
		assert.Nil(t, err)
		assert.True(t, filter.isValid)
		assert.Equal(t, function.BETWEEN, filter.op)
	}
}

func TestMemPKFilter_FilterVector(t *testing.T) {
	lb, ub := 10, 20

	baseFilters := []BasePKFilter{
		{Op: function.BETWEEN, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(lb)), UB: types.EncodeFixed(int32(ub))},
		{Op: RangeBothOpen, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(lb)), UB: types.EncodeFixed(int32(ub))},
		{Op: RangeLeftOpen, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(lb)), UB: types.EncodeFixed(int32(ub))},
		{Op: RangeRightOpen, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(lb)), UB: types.EncodeFixed(int32(ub))},
		{Op: function.LESS_EQUAL, Valid: true, Oid: types.T_int32, LB: types.EncodeFixed(int32(ub))},
	}

	mp := mpool.MustNewZeroNoFixed()

	vecs := make([]*vector.Vector, 0, len(baseFilters))
	for range baseFilters {
		vec := vector.NewVec(types.T_int32.ToType())
		vector.AppendFixed[int32](vec, int32(21), false, mp)
		vecs = append(vecs, vec)
	}

	tableDef := &plan.TableDef{
		Name: "test",
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a"},
		},
		Cols: []*plan.ColDef{
			{
				Name: "a",
				Typ: plan.Type{
					Id: int32(types.T_int64),
				},
			},
		},
	}

	ts := types.MaxTs().ToTimestamp()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer {
			return types.NewPacker()
		},
		func(packer *types.Packer) {
			packer.Reset()
		},
		func(packer *types.Packer) {
			packer.Close()
		},
	)

	var packer *types.Packer
	var skipMask objectio.Bitmap

	for i := range baseFilters {
		tableDef.Cols[0].Typ.Id = int32(baseFilters[i].Oid)
		filter, err := NewMemPKFilter(
			tableDef,
			ts,
			packerPool,
			baseFilters[i],
			engine.FilterHint{})
		assert.Nil(t, err)
		assert.True(t, filter.isValid)

		skipMask = objectio.GetReusableBitmap()
		put := packerPool.Get(&packer)

		filter.FilterVector(vecs[i], packer, &skipMask)
		put.Put()

		require.Equal(t, 1, skipMask.Count(), filter.String())

		skipMask.Release()
	}
}
