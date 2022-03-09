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
	"github.com/matrixorigin/matrixone/pkg/vectorize/abs"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var argAndRets = []argsAndRet{
	{[]types.T{types.T_uint8}, types.T_uint8},
	{[]types.T{types.T_uint16}, types.T_uint16},
	{[]types.T{types.T_uint32}, types.T_uint32},
	{[]types.T{types.T_uint64}, types.T_uint64},
	{[]types.T{types.T_int8}, types.T_int8},
	{[]types.T{types.T_int16}, types.T_int16},
	{[]types.T{types.T_int32}, types.T_int32},
	{[]types.T{types.T_int64}, types.T_int64},
	{[]types.T{types.T_float32}, types.T_float32},
	{[]types.T{types.T_float64}, types.T_float64},
}

func init() {
	extend.FunctionRegistry["abs"] = builtin.Abs

	for _, item := range argAndRets {
		// append function parameter types and return types
		overload.AppendFunctionRets(builtin.Abs, item.args, item.ret)
	}

	// define a get return type function for abs function
	extend.UnaryReturnTypes[builtin.Abs] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Abs, extend)
	}

	// define a stringify function for abs
	extend.UnaryStrings[builtin.Abs] = func(e extend.Extend) string {
		return fmt.Sprintf("abs(%s)", e)
	}

	// register abs function type
	overload.OpTypes[builtin.Abs] = overload.Unary

	// Preparation for function calling
	overload.UnaryOps[builtin.Abs] = []*overload.UnaryOp{
		{ // T_uint8
			Typ:        types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint8)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsUint8(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsUint8(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint16
			Typ:        types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint16)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsUint16(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 2*int64(len(origVecCol)), types.Type{Oid: types.T_int16, Size: 2})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint16Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsUint16(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint32
			Typ:        types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint32)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsUint32(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_uint32, Size: 4})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsUint32(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint64
			Typ:        types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint64)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsUint64(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_uint64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsUint64(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int8
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int8)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsInt8(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_int8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsInt8(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int16
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int16)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsInt16(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 2*int64(len(origVecCol)), types.Type{Oid: types.T_int16, Size: 2})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt16Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsInt16(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int32
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int32)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsInt32(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_int32, Size: 4})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt32Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsInt32(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int64
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsInt64(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeInt64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsInt64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				if origVec.Ref == 1 || origVec.Ref == 0 { // reuse the original vector when we don't need the original one anymore
					origVec.Ref = 0
					abs.AbsFloat32(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 4*int64(len(origVecCol)), types.Type{Oid: types.T_float32, Size: 4}) // get a new types.T_float32 vector to store the result vector
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat32Slice(resultVector.Data) // decode the vector's data to float32 type
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)                         // the new vector's nulls are the same as the original vector
				vector.SetCol(resultVector, abs.AbsFloat32(origVecCol, results)) // set the vector col with the return value from abs.AbsFloat32 function
				return resultVector, nil
			},
		},
		{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)
				if origVec.Ref == 1 || origVec.Ref == 0 {
					origVec.Ref = 0
					abs.AbsFloat64(origVecCol, origVecCol)
					return origVec, nil
				}
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, abs.AbsFloat64(origVecCol, results))
				return resultVector, nil
			},
		},
	}
}
