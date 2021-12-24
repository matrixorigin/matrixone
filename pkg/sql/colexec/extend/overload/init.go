// Copyright 2021 Matrix Origin
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

package overload

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	// OperatorCastRules records implicit type conversion rule for all operators and built-in functions
	// map's key is operator id or function id (eg: overload.Plus)
	OperatorCastRules = make(map[int][]castRule)

	// OperatorReturnType records return type for all operator and built-in functions
	// first map : key is operator or function id (eg: overload.Minus), value is second map :
	// key is numbers of args, and value is a slice of struct retType (contains information about arg types and return type)
	OperatorReturnType = make(map[int]map[uint8][]retType)
)

// set init here to make sure this function will be the last executed init function at this package
func init() {
	// init overload.BinOps and overload.UnaryOps
	initOperatorFunctions()
	// init cast-rule from ops
	initCastRulesForBinaryOps()
	initCastRulesForUnaryOps()
	initCastRulesForMulti()
	// init return type map from ops and cast-rule
	initReturnTypeFromUnary()
	initReturnTypeFromBinary()
	initReturnTypeFromMulti()
}

type castRule struct {
	NumArgs uint8 			 // numbers of arguments for this operator or functions
	sourceTypes []types.T    // source type for each argument
	targetTypes []types.Type // cast type for each argument
}

type retType struct {
	argTypes []types.T
	ret  	 types.T
}

// GetUnaryOpReturnType returns the returnType of unary ops or binary functions
func GetUnaryOpReturnType(op int, arg types.T) types.T {
	if m, ok := OperatorReturnType[op]; ok {
		if rs, okk := m[1]; okk {
			for _, r := range rs {
				if r.argTypes[0] == arg {
					return r.ret
				}
			}
		}
	}
	return arg
}

// GetBinOpReturnType returns the returnType of binary ops or binary functions
func GetBinOpReturnType(op int, lt, rt types.T) types.T {
	if m, ok := OperatorReturnType[op]; ok {
		if rs, okk := m[2]; okk {
			for _, r := range rs {
				if r.argTypes[0] == lt && r.argTypes[1] == rt {
					return r.ret
				}
			}
		}
	}
	return lt
}

// GetMultiReturnType returns the returnType of multi ops or multi functions
func GetMultiReturnType(op int, ts []types.T) types.T {
	return types.T_sel
}

// appendOperatorRets will add operator-return-type information into related structure
// op is operator id
// args is a slice of argument types
// ret is return type of this operator
func appendOperatorRets(op int, args []types.T, ret types.T) {
	appendRets(op, args, ret)
}

// AppendFunctionRets will add function-return-type information into related structure
// op is function id
// args is a slice of argument types
// ret is return type of this function
func AppendFunctionRets(op int, args []types.T, ret types.T) {
	appendRets(op, args, ret)
}

// TODO: this function should only be call at init function
func appendRets(op int, args []types.T, ret types.T) {
	if _, ok := OperatorReturnType[op]; !ok {
		OperatorReturnType[op] = make(map[uint8][]retType)
	}
	nArg := uint8(len(args))
	if _, ok := OperatorReturnType[op][nArg]; !ok {
		OperatorReturnType[op][nArg] = []retType{}
	}
	OperatorReturnType[op][nArg] = append(OperatorReturnType[op][nArg], retType{argTypes: args, ret: ret})
}

func initOperatorFunctions() {
	// unary
	initUnary()
	// binary
	{ // compute
		initPlus()
		initMinus()
		initMult()
		initDiv()
		initMod()
	}
	{ // compare
		initEq()
		initNe()
		initGt()
		initGe()
		initLt()
		initLe()
	}
	{ // logical
		initAnd()
		initOr()
	}
	// multi
	// others
	initCast()
	initLike()
}

func initReturnTypeFromBinary() {
	// from binOps
	for op, bs := range BinOps {
		for _, b := range bs {
			appendOperatorRets(op, []types.T{b.LeftType, b.RightType}, b.ReturnType)
		}
	}
	// from CastRules
	for op, rs := range OperatorCastRules {
		for _, r := range rs {
			if r.NumArgs != 2 {
				continue
			}
			var retType types.T
			found := false
			// find ret type
			for _, o := range BinOps[op] {
				if binaryCheck(op, o.LeftType, o.RightType, r.targetTypes[0].Oid, r.targetTypes[1].Oid) {
					retType = o.ReturnType
					found = true
					break
				}
			}
			if !found {
				panic(fmt.Sprintf("init return type from binary error after type conversion, operator/function id is %d, sourceArgTypes is %v, targetArgTypes is %v",
					op, r.sourceTypes, r.targetTypes))
			}
			appendOperatorRets(op, r.sourceTypes, retType)
		}
	}
}

func initReturnTypeFromUnary() {
	// from unaryOps
	for op, us := range UnaryOps {
		for _, u := range us {
			appendOperatorRets(op, []types.T{u.Typ}, u.ReturnType)
		}
	}
	// from cast rules
	for op, rs := range OperatorCastRules {
		for _, r := range rs {
			if r.NumArgs != 1 {
				continue
			}
			var retType types.T
			found := false
			// find ret type
			for _, o := range UnaryOps[op] {
				if unaryCheck(op, o.Typ, r.targetTypes[0].Oid) {
					retType = o.ReturnType
					found = true
					break
				}
			}
			if !found {
				panic(fmt.Sprintf("init return type from unary error after type conversion, operator/function id is %d, sourceArgTypes is %v, targetArgTypes is %v",
					op, r.sourceTypes, r.targetTypes))
			}
			appendOperatorRets(op, r.sourceTypes, retType)
		}
	}
}

func initReturnTypeFromMulti() {}

func initCastRulesForBinaryOps() {
	ints := []types.T{types.T_int8, types.T_int16, types.T_int32, types.T_int64}
	uints := []types.T{types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64}
	floats := []types.T{types.T_float32, types.T_float64}
	chars := []types.T{types.T_char, types.T_varchar}
	// dates := []types.T{types.T_date, types.T_datetime}

	// PLUS cast rule / Minus cast rule / Multiplication cast rule
	{
		ops := []int{Plus, Minus, Mult}
		for _, op := range ops {
			{
				// cast to int64 op int64 : +, -, * between int and uint
				targetType := []types.Type{
					{Oid: types.T_int64, Size: 8},
					{Oid: types.T_int64, Size: 8},
				}
				for _, l := range ints {
					for _, r := range uints {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
			}
			{
				/*
					cast to float64 op float64:
					1. +, -, * between int and uint
					2. +, -, * between uint and float
				*/
				targetType := []types.Type{
					{Oid: types.T_float64, Size: 8},
					{Oid: types.T_float64, Size: 8},
				}
				for _, l := range ints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				for _, l := range uints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
			}
		}
	}
	// Div cast rule / IntegerDiv cast rule
	{
		{
			/*
				cast to float64 / float64 (integerDiv will return int64):
				1. div between int and int
				2. div between uint and uint
				3. div between int and uint
				4. div between int and float
				5. div between uint and float
				6. div between float32 and float64
			*/
			targetType := []types.Type{
				{Oid: types.T_float64, Size: 8},
				{Oid: types.T_float64, Size: 8},
			}
			ops := []int{Div, IntegerDiv}
			for _, op := range ops {
				for _, l := range ints {
					for _, r := range ints {
							OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
								{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							}...)
					}
				}
				for _, l := range uints {
					for _, r := range uints {
							OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
								{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							}...)
					}
				}
				for _, l := range ints {
					for _, r := range uints {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				for _, l := range ints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				for _, l := range uints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
					{NumArgs: 2, sourceTypes: []types.T{types.T_float32, types.T_float32}, targetTypes: targetType},
					{NumArgs: 2, sourceTypes: []types.T{types.T_float32, types.T_float64}, targetTypes: targetType},
					{NumArgs: 2, sourceTypes: []types.T{types.T_float64, types.T_float32}, targetTypes: targetType},
				}...)
			}
		}
	}
	// Mod cast rule
	{
		{
			/*
				cast to int64 mod int64 :
				1. mod between int and int
				2. mod between int and uint
			*/
			targetType := []types.Type{
				{Oid: types.T_int64, Size: 8},
				{Oid: types.T_int64, Size: 8},
			}
			for _, l := range ints {
				for _, r := range ints {
					if l != r {
						OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
						}...)
					}
				}
			}
			for _, l := range ints {
				for _, r := range uints {
					OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
						{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
						{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
					}...)
				}
			}
		}
		{
			/*
				cast to uint64 mod uint64 : mod between uint and uint
			*/
			targetType := []types.Type{
				{Oid: types.T_uint64, Size: 8},
				{Oid: types.T_uint64, Size: 8},
			}
			for _, l := range uints {
				for _, r := range uints {
					if l != r {
						OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
						}...)
					}
				}
			}
		}
		{
			/*
				cast to float64 mod float64 :
				1. mod between int and float
				2. mod between uint and float
				3. mod between float32 and float64
			*/
			targetType := []types.Type{
				{Oid: types.T_float64, Size: 8},
				{Oid: types.T_float64, Size: 8},
			}
			for _, l := range ints {
				for _, r := range floats {
					OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
						{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
						{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
					}...)
				}
			}
			for _, l := range uints {
				for _, r := range floats {
					OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
						{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
						{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
					}...)
				}
			}
			OperatorCastRules[Mod] = append(OperatorCastRules[Mod], []castRule{
				{NumArgs: 2, sourceTypes: []types.T{types.T_float32, types.T_float64}, targetTypes: targetType},
				{NumArgs: 2, sourceTypes: []types.T{types.T_float64, types.T_float32}, targetTypes: targetType},
			}...)
		}
	}

	// EQ / NE / GE / GT / LE / LT
	{
		ops := []int{EQ, NE, GT, GE, LT, LE}
		for _, op := range ops {
			{
				/*
					cast to int64 op int64 :
					1. op between int and int
					2. op between uint and int
				*/
				targetType := []types.Type{
					{Oid: types.T_int64, Size: 8},
					{Oid: types.T_int64, Size: 8},
				}
				for _, l := range ints {
					for _, r := range ints {
						if l != r {
							OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
								{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							}...)
						}
					}
				}
				for _, l := range uints {
					for _, r := range ints {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
			}
			{
				/*
					cast to uint64 op uint64 :
					1. op between uint and uint
				*/
				targetType := []types.Type{
					{Oid: types.T_uint64, Size: 8},
					{Oid: types.T_uint64, Size: 8},
				}
				for _, l := range uints {
					for _, r := range uints {
						if l != r {
							OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
								{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							}...)
						}
					}
				}
			}
			{
				/*
					cast to float64 op float64 :
					1. op between int and float
					2. op between uint and float
					3. op between char and numeric
					4. op between float32 and float64
				*/
				targetType := []types.Type{
					{Oid: types.T_float64, Size: 8},
					{Oid: types.T_float64, Size: 8},
				}
				for _, l := range ints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				for _, l := range uints {
					for _, r := range floats {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				for _, l := range chars {
					var numeric []types.T
					numeric = append(numeric, floats...)
					numeric = append(numeric, ints...)
					numeric = append(numeric, uints...)
					for _, r := range numeric {
						OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
							{NumArgs: 2, sourceTypes: []types.T{l, r}, targetTypes: targetType},
							{NumArgs: 2, sourceTypes: []types.T{r, l}, targetTypes: targetType},
						}...)
					}
				}
				OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
					{NumArgs: 2, sourceTypes: []types.T{types.T_float32, types.T_float64}, targetTypes: targetType},
					{NumArgs: 2, sourceTypes: []types.T{types.T_float64, types.T_float32}, targetTypes: targetType},
				}...)
			}
			{
				/*
					cast to date op date :
					1. op between date and char / varchar
				*/
				targetType := []types.Type{
					{Oid: types.T_date, Size: 4},
					{Oid: types.T_date, Size: 4},
				}
				for _, l := range chars {
					OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
						{NumArgs: 2, sourceTypes: []types.T{l, types.T_date}, targetTypes: targetType},
						{NumArgs: 2, sourceTypes: []types.T{types.T_date, l}, targetTypes: targetType},
					}...)
				}
			}
			{
				/*
					cast to datetime op datetime :
					1. op between datetime and char / varchar
				*/
				targetType := []types.Type{
					{Oid: types.T_datetime, Size: 8},
					{Oid: types.T_datetime, Size: 8},
				}
				for _, l := range chars {
					OperatorCastRules[op] = append(OperatorCastRules[op], []castRule{
						{NumArgs: 2, sourceTypes: []types.T{l, types.T_datetime}, targetTypes: targetType},
						{NumArgs: 2, sourceTypes: []types.T{types.T_datetime, l}, targetTypes: targetType},
					}...)
				}
			}
		}
	}
}

func initCastRulesForUnaryOps() {
	// Not Operator
	{
		// cast to Not Float64 :
		// 1. not char / varchar
		targetType := []types.Type{{Oid: types.T_float64, Size: 8}}
		OperatorCastRules[Not] = []castRule{
			{NumArgs: 1, sourceTypes: []types.T{types.T_char}, targetTypes: targetType},
			{NumArgs: 1, sourceTypes: []types.T{types.T_varchar}, targetTypes: targetType},
		}
	}
}

func initCastRulesForMulti() {}