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

//go:generate go run overloadGenerate.go
package overload

const (
	NotLogical = iota
	MayLogical
	MustLogical
)

var LogicalOps = map[int]uint8{
	Or:      MayLogical,
	And:     MayLogical,
	Like:    MustLogical,
	NotLike: MustLogical,
	EQ:      MustLogical,
	LT:      MustLogical,
	LE:      MustLogical,
	GT:      MustLogical,
	GE:      MustLogical,
	NE:      MustLogical,
}

var NegOps = map[int]int{
	Or:   And,
	And:  Or,
	EQ:   NE,
	LT:   GE,
	LE:   GT,
	GT:   LE,
	GE:   LT,
	Like: NotLike,
}

var OpTypes = map[int]int{
	UnaryMinus: Unary,
	Or:         Binary,
	And:        Binary,
	Plus:       Binary,
	Minus:      Binary,
	Mult:       Binary,
	Div:        Binary,
	Mod:        Binary,
	Like:       Binary,
	NotLike:    Binary,
	Typecast:   Binary,
	EQ:         Binary,
	LT:         Binary,
	LE:         Binary,
	GT:         Binary,
	GE:         Binary,
	NE:         Binary,
}

func IsLogical(op int) uint8 {
	if typ, ok := LogicalOps[op]; ok {
		return typ
	}
	return NotLogical
}

func OperatorType(op int) int {
	if typ, ok := OpTypes[op]; ok {
		return typ
	}
	return -1
}
