package unary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ceil"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["ceil"] = builtin.Ceil
	for _, item := range ArgAndRets {
		// append function parameter types and return types
		overload.AppendFunctionRets(builtin.Ceil, item.args, item.ret)
	}
	overload.AppendFunctionRets(builtin.Ceil, []types.T{types.T_varchar}, types.T_float64)
	overload.AppendFunctionRets(builtin.Ceil, []types.T{types.T_char}, types.T_float64)
	extend.UnaryReturnTypes[builtin.Ceil] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Ceil, extend)
	}
	extend.UnaryStrings[builtin.Ceil] = func(e extend.Extend) string {
		return fmt.Sprintf("ceil(%s)", e)
	}
	overload.OpTypes[builtin.Ceil] = overload.Unary
	overload.UnaryOps[builtin.Ceil] = []*overload.UnaryOp{
		{
			Typ:        types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint8)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilUint8(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint16)
				resultVector, err := process.Get(proc, 2*int64(len(origVecCol)), types.Type{Oid: types.T_uint16, Size: 2})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint16Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilUint16(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint32)
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 4})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilUint32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_uint64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilUint64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int8)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_int8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilInt8(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int16)
				resultVector, err := process.Get(proc, 2*int64(len(origVecCol)), types.Type{Oid: types.T_int16, Size: 2})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt16Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilInt16(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int32)
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_int32, Size: 4})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilInt32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilInt64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilFloat32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilFloat64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_char,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]string)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_char, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilString(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_varchar,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]string)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ceil.CeilString(origVecCol, results))
				return resultVector, nil
			},
		},
	}
}
