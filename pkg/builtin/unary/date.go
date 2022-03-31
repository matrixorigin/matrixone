package unary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/date"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["date"] = builtin.Date
	overload.AppendFunctionRets(builtin.Date, []types.T{types.T_date}, types.T_date)
	overload.AppendFunctionRets(builtin.Date, []types.T{types.T_datetime}, types.T_date)
	extend.UnaryReturnTypes[builtin.Date] = func(e extend.Extend) types.T {
		return getUnaryReturnType(builtin.Date, e)
	}
	extend.UnaryStrings[builtin.Date] = func(e extend.Extend) string {
		return fmt.Sprintf("date(%s)", e)
	}
	overload.OpTypes[builtin.Date] = overload.Unary
	overload.UnaryOps[builtin.Abs] = []*overload.UnaryOp{
		{
			Typ:        types.T_date,
			ReturnType: types.T_date,
			Fn: func(lv *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]types.Date)
				size := types.T(types.T_date).TypeLen()
				vec, err := process.Get(proc, int64(size)*int64(len(lvs)), types.Type{Oid: types.T_date, Size: int32(size)})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeDateSlice(vec.Data)
				rs = rs[:len(lvs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, lv.Nsp)
				vector.SetCol(vec, date.DateToDate(lvs, rs))
				return vec, nil
			},
		},
		{
			Typ:        types.T_datetime,
			ReturnType: types.T_date,
			Fn: func(lv *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				lvs := lv.Col.([]types.Datetime)
				size := types.T(types.T_date).TypeLen()
				vec, err := process.Get(proc, int64(size)*int64(len(lvs)), types.Type{Oid: types.T_date, Size: int32(size)})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeDateSlice(vec.Data)
				rs = rs[:len(lvs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, lv.Nsp)
				vector.SetCol(vec, date.DateTimeToDate(lvs, rs))
				return vec, nil
			},
		},
	}
}
