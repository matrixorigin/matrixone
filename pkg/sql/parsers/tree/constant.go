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
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/logutil"
)

// the AST for literals like string,numeric,bool and etc.
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
	P_nulltext
)

type P_KIND uint8

const (
	Unknown P_KIND = iota
	Bool
	Str
	Int
	Float
)

// the AST for the constant numeric value.
type NumVal struct {
	ValType P_TYPE

	// negative is the sign label
	negative bool

	// origString is the "original" string literals that should remain sign-less.
	origString string

	// converted result
	resBool    bool
	resInt64   int64
	resUint64  uint64
	resFloat64 float64
}

func (node *NumVal) Kind() P_KIND {
	switch node.ValType {
	case P_null:
		return Unknown
	case P_int64, P_uint64:
		return Int
	case P_float64:
		return Float
	default:
		return Str
	}
}

func (node *NumVal) Bool() bool {
	switch node.ValType {
	case P_bool:
		return node.resBool
	case P_null:
		return false
	default:
		panic(fmt.Sprintf("%v not a Bool", node.ValType))
	}
}

func (node *NumVal) String() string {
	return node.origString
}

// follow package constant Uint64Val
func (node *NumVal) Uint64() (uint64, bool) {
	switch node.ValType {
	case P_int64:
		return uint64(node.resInt64), true
	case P_uint64:
		return node.resUint64, true
	case P_null:
		return 0, false
	default:
		panic(fmt.Sprintf("%v not a uint64", node.ValType))
	}
}

// follow package constant Int64Val
func (node *NumVal) Int64() (int64, bool) {
	switch node.ValType {
	case P_int64:
		return node.resInt64, true
	case P_null:
		return 0, false
	default:
		panic(fmt.Sprintf("%v not a int64", node.ValType))
	}
}

// follow package constant Float64Val
// Float64Val returns the nearest Go float64 value of x and whether the result is exact;
// x must be numeric or an [Unknown], but not [Complex]. For values too small (too close to 0)
// to represent as float64, [Float64Val] silently underflows to 0. The result sign always
// matches the sign of x, even for 0.
// If x is [Unknown], the result is (0, false).
func (node *NumVal) Float64() (float64, bool) {
	switch node.ValType {
	case P_int64:
		f := float64(node.resInt64)
		return f, int64(f) == node.resInt64
	case P_uint64:
		f := float64(node.resUint64)
		return f, uint64(f) == node.resUint64
	case P_float64:
		return node.resFloat64, true
	case P_null:
		return 0, false
	default:
		panic(fmt.Sprintf("%v not a float", node.ValType))
	}
}

func (node *NumVal) Negative() bool {
	return node.negative
}

func NewNumVal[T bool | int64 | uint64 | float64 | string](val T, originString string, negative bool, typ P_TYPE) *NumVal {
	nv := &NumVal{
		ValType:    typ,
		negative:   negative,
		origString: originString,
	}

	switch v := any(val).(type) {
	case bool:
		if typ != P_bool {
			logutil.Fatalf("unexpected type %T", v)
		}
		nv.resBool = v
	case int64:
		if typ != P_int64 {
			logutil.Fatalf("unexpected type %T", v)
		}
		nv.resInt64 = v
	case uint64:
		if typ != P_uint64 {
			logutil.Fatalf("unexpected type %T", v)
		}
		nv.resUint64 = v
	case float64:
		if typ != P_float64 {
			logutil.Fatalf("unexpected type %T", v)
		}
		nv.resFloat64 = v
	case string:
		// do nothing as val already store in origString
	default:
		logutil.Fatalf("unexpected type %T", v)
	}

	return nv
}

func (node *NumVal) Format(ctx *FmtCtx) {
	if node.origString != "" {
		ctx.WriteValue(node.ValType, FormatString(node.origString))
		return
	}

	switch node.ValType {
	case P_null:
		ctx.WriteString("null")
	case P_bool:
		ctx.WriteString(strconv.FormatBool(node.resBool))
	// case P_int64:
	// 	ctx.WriteString(strconv.FormatInt(node.resInt64, 10))
	// case P_uint64:
	// 	ctx.WriteString(strconv.FormatUint(node.resUint64, 10))
	default:
		ctx.WriteValue(node.ValType, node.origString)
	}
}

// Accept implements NodeChecker Accept interface.
func (node *NumVal) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	return v.Exit(node)
}

// StrVal represents a constant string value.
type StrVal struct {
	str string
}

func (s *StrVal) Format(ctx *FmtCtx) {
	ctx.WriteString(s.str)
}

// Accept implements NodeChecker Accept interface.
func (s *StrVal) Accept(v Visitor) (Expr, bool) {
	panic("unimplement StrVal Accept")
}

func NewStrVal(s string) *StrVal {
	return &StrVal{str: s}
}

func (s *StrVal) String() string {
	return s.str
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
