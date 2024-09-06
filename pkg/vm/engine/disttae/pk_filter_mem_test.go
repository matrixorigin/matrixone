package disttae

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/engine_util"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewMemPKFilter(t *testing.T) {

	lb, ub := 10, 20

	baseFilters := []engine_util.BasePKFilter{
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
			&plan.ColDef{
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
		filter, err := newMemPKFilter(tableDef, ts, packerPool, baseFilters[i])
		assert.Nil(t, err)
		assert.True(t, filter.isValid)
		assert.Equal(t, function.BETWEEN, filter.op)
	}
}
