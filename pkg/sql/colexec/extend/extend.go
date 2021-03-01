package extend

import (
	"fmt"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/sql/colexec/extend/overload"
)

var UnaryReturnTypes = map[int]func(Extend) types.T{
	overload.UnaryMinus: func(e Extend) types.T {
		return e.ReturnType()
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
	overload.Plus: func(l Extend, _ Extend) types.T {
		return l.ReturnType()
	},
	overload.Minus: func(l Extend, _ Extend) types.T {
		return l.ReturnType()
	},
	overload.Mult: func(l Extend, _ Extend) types.T {
		return l.ReturnType()
	},
	overload.Div: func(l Extend, _ Extend) types.T {
		return l.ReturnType()
	},
	overload.Mod: func(l Extend, _ Extend) types.T {
		return l.ReturnType()
	},
}

var MultiReturnTypes = map[int]func([]Extend) types.T{}

var UnaryStrings = map[int]func(Extend) string{
	overload.UnaryMinus: func(e Extend) string {
		return "-" + e.String()
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
