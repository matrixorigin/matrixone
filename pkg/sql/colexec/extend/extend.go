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

package extend

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
)

var UnaryReturnTypes = map[int]func(Extend) types.T{
	overload.UnaryMinus: func(e Extend) types.T {
		return e.ReturnType()
	},
	overload.Not: func(e Extend) types.T {
		return overload.GetUnaryOpReturnType(overload.Not, e.ReturnType())
	},
}

var BinaryReturnTypes = map[int]func(Extend, Extend) types.T{
	overload.Or: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.And: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.EQ: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.NE: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.LT: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.LE: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.GT: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.GE: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.Like: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.NotLike: func(_ Extend, _ Extend) types.T {
		return types.T_sel
	},
	overload.Typecast: func(_ Extend, r Extend) types.T {
		return r.ReturnType()
	},
	overload.Plus: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.Plus, l.ReturnType(), r.ReturnType())
	},
	overload.Minus: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.Minus, l.ReturnType(), r.ReturnType())
	},
	overload.Mult: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.Mult, l.ReturnType(), r.ReturnType())
	},
	overload.Div: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.Div, l.ReturnType(), r.ReturnType())
	},
	overload.IntegerDiv: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.IntegerDiv, l.ReturnType(), r.ReturnType())
	},
	overload.Mod: func(l Extend, r Extend) types.T {
		return overload.GetBinOpReturnType(overload.Mod, l.ReturnType(), r.ReturnType())
	},
}

var MultiReturnTypes = map[int]func([]Extend) types.T{}

var UnaryStrings = map[int]func(Extend) string{
	overload.UnaryMinus: func(e Extend) string {
		return "-" + e.String()
	},
	overload.Not: func(e Extend) string {
		return fmt.Sprintf("not(%s)", e)
	},
}

var BinaryStrings = map[int]func(Extend, Extend) string{
	overload.Like: func(l Extend, r Extend) string {
		return fmt.Sprintf("like(%s, %s)", l.String(), r.String())
	},
	overload.NotLike: func(l Extend, r Extend) string {
		return fmt.Sprintf("notLike(%s, %s)", l.String(), r.String())
	},
	overload.EQ: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s = %s", l.String(), r.String())
	},
	overload.LT: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s < %s", l.String(), r.String())
	},
	overload.GT: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s > %s", l.String(), r.String())
	},
	overload.LE: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s <= %s", l.String(), r.String())
	},
	overload.GE: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s >= %s", l.String(), r.String())
	},
	overload.NE: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s <> %s", l.String(), r.String())
	},
	overload.Or: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s or %s", l.String(), r.String())
	},
	overload.And: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s and %s", l.String(), r.String())
	},
	overload.Div: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s / %s", l.String(), r.String())
	},
	overload.IntegerDiv: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s div %s", l.String(), r.String())
	},
	overload.Mod: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s %% %s", l.String(), r.String())
	},
	overload.Plus: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s + %s", l.String(), r.String())
	},
	overload.Mult: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s * %s", l.String(), r.String())
	},
	overload.Minus: func(l Extend, r Extend) string {
		return fmt.Sprintf("%s - %s", l.String(), r.String())
	},
	overload.Typecast: func(l Extend, r Extend) string {
		return fmt.Sprintf("cast(%s as %s)", l.String(), r.ReturnType())
	},
}

var MultiStrings = map[int]func([]Extend) string{}

func AndExtends(e Extend, es []Extend) []Extend {
	switch v := e.(type) {
	case *UnaryExtend:
		return nil
	case *ParenExtend:
		return AndExtends(v.E, es)
	case *Attribute:
		return es
	case *ValueExtend:
		return es
	case *BinaryExtend:
		switch v.Op {
		case overload.EQ:
			return append(es, v)
		case overload.NE:
			return append(es, v)
		case overload.LT:
			return append(es, v)
		case overload.LE:
			return append(es, v)
		case overload.GT:
			return append(es, v)
		case overload.GE:
			return append(es, v)
		case overload.And:
			left, right := AndExtends(v.Left, es), AndExtends(v.Right, es)
			if left == nil || right == nil {
				return nil
			}
			return append(left, right...)
		}
	}
	return nil
}
