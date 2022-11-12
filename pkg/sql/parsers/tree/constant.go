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

package tree

import (
	"go/constant"
	"strings"
)

// the AST for literals like string,numeric,bool and etc.
type Constant interface {
	Expr
}

type P_TYPE uint8

const (
	P_any P_TYPE = iota
	P_hexnum
	P_null
	P_bool
	P_int64
	P_uint64
	P_float64
	P_char
	P_decimal
	P_bit
	P_ScoreBinary
)

// the AST for the constant numeric value.
type NumVal struct {
	Constant
	Value constant.Value

	// negative is the sign label
	negative bool

	// origString is the "original" string literals that should remain sign-less.
	origString string

	//converted result
	resInt   int64
	resFloat float64
	ValType  P_TYPE
}

func (n *NumVal) Format(ctx *FmtCtx) {
	if n.origString != "" {
		ctx.WriteValue(n.ValType, FormatString(n.origString))
		return
	}
	switch n.Value.Kind() {
	case constant.String:
		ctx.WriteValue(n.ValType, n.origString)
	case constant.Bool:
		ctx.WriteString(strings.ToLower(n.Value.String()))
	case constant.Unknown:
		ctx.WriteString("null")
	}
}

func FormatString(str string) string {
	var buffer strings.Builder
	for i, ch := range str {
		if ch == '\n' {
			buffer.WriteString("\\n")
		} else if ch == '\x00' {
			buffer.WriteString("\\0")
		} else if ch == '\r' {
			buffer.WriteString("\\r")
		} else if ch == '\\' {
			if (i + 1) < len(str) {
				if str[i+1] == '_' || str[i+1] == '%' {
					buffer.WriteByte('\\')
					continue
				}
			}
			buffer.WriteString("\\\\")
		} else if ch == 8 {
			buffer.WriteString("\\b")
		} else if ch == 26 {
			buffer.WriteString("\\Z")
		} else if ch == '\t' {
			buffer.WriteString("\\t")
		} else {
			// buffer.WriteByte(byte(ch))
			buffer.WriteRune(ch)
		}
	}
	res := buffer.String()
	return res
}

func (n *NumVal) String() string {
	return n.origString
}

func (n *NumVal) Negative() bool {
	return n.negative
}

func NewNumVal(value constant.Value, origString string, negative bool) *NumVal {
	return &NumVal{
		Value:      value,
		origString: origString,
		negative:   negative,
	}
}

func NewNumValWithType(value constant.Value, origString string, negative bool, typ P_TYPE) *NumVal {
	numVal := &NumVal{
		Value:      value,
		origString: origString,
		negative:   negative,
		ValType:    typ,
	}
	return numVal
}

func NewNumValWithResInt(value constant.Value, origString string, negative bool, resInt int64) *NumVal {
	return &NumVal{
		Value:      value,
		origString: origString,
		negative:   negative,
		resInt:     resInt,
	}
}

func NewNumValWithResFoalt(value constant.Value, origString string, negative bool, resFloat float64) *NumVal {
	return &NumVal{
		Value:      value,
		origString: origString,
		negative:   negative,
		resFloat:   resFloat,
	}
}

// StrVal represents a constant string value.
type StrVal struct {
	Constant
	str string
}

func (node *StrVal) Format(ctx *FmtCtx) {
	ctx.WriteString(node.str)
}

func NewStrVal(s string) *StrVal {
	return &StrVal{str: s}
}
