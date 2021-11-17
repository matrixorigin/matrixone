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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/vm/process"
)

const (
	Unary = iota
	Multi
	Binary
)

const (
	// unary operator
	UnaryMinus = iota
	Not

	// binary operator
	Or
	And
	Plus
	Minus
	Mult
	Div
	IntegerDiv
	Mod
	Like
	NotLike
	Typecast

	// binary operator - comparison operator
	EQ
	LT
	LE
	GT
	GE
	NE
)

var OpName = map[int]string{
	UnaryMinus: "-",

	Or:    "or",
	And:   "and",
	Plus:  "+",
	Minus: "-",
	Mult:  "*",
	Div:   "/",
	Mod:   "%",

	Like:    "like",
	NotLike: "notLike",

	Typecast: "cast",

	EQ: "=",
	LT: "<",
	LE: "<=",
	GT: ">",
	GE: ">=",
	NE: "<>",
}

var SelsType = types.Type{Oid: types.T_sel, Size: 8}

// UnaryOp is a unary operator.
type UnaryOp struct {
	Typ        types.T
	ReturnType types.T
	Fn         func(*vector.Vector, *process.Process, bool) (*vector.Vector, error)
}

// BinOp is a binary operator.
type BinOp struct {
	LeftType   types.T
	RightType  types.T
	ReturnType types.T

	Fn func(*vector.Vector, *vector.Vector, *process.Process, bool, bool) (*vector.Vector, error)
}

// MultiOp is a multiple operator.
type MultiOp struct {
	Min        int // minimum number of parameters
	Max        int // maximum number of parameters, -1 means unlimited
	Typ        types.T
	ReturnType types.T

	Fn func([]*vector.Vector, *process.Process, []bool) (*vector.Vector, error)
}
