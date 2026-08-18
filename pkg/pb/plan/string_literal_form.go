// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func isPlanMySQLStringType(id int32) bool {
	switch id {
	case 60, 61, 64, 65, 70, 71:
		return true
	default:
		return false
	}
}

// ValidateStringLiteralForms rejects unknown wire enum values at a plan owner
// boundary. Protobuf intentionally preserves unknown enum integers, so callers
// must validate a decoded expression before treating its literal provenance as
// executable state.
func (m *Expr) ValidateStringLiteralForms() error {
	return m.walkStringLiterals(func(expr *Expr, lit *Literal) error {
		return expr.validateStringLiteralForm(lit)
	})
}

func (m *Expr) validateOwnStringLiteralForm() error {
	if m == nil || m.GetLit() == nil {
		return nil
	}
	return m.validateStringLiteralForm(m.GetLit())
}

func (m *Expr) validateStringLiteralForm(lit *Literal) error {
	if lit.LiteralForm < StringLiteralForm_STRING_LITERAL_NONE ||
		lit.LiteralForm > StringLiteralForm_STRING_LITERAL_BIT {
		return moerr.NewInvalidInputNoCtxf("invalid string literal form %d", lit.LiteralForm)
	}
	if lit.LiteralForm == StringLiteralForm_STRING_LITERAL_NONE {
		return nil
	}
	if lit.Isnull || lit.Value == nil {
		return moerr.NewInvalidInputNoCtx("string literal form requires a non-NULL literal value")
	}
	if _, ok := lit.Value.(*Literal_Sval); !ok || !isPlanMySQLStringType(m.Typ.Id) {
		return moerr.NewInvalidInputNoCtx("string literal form requires a string literal and string type")
	}
	binarySyntax := lit.LiteralForm == StringLiteralForm_STRING_LITERAL_HEX ||
		lit.LiteralForm == StringLiteralForm_STRING_LITERAL_BIT
	if lit.IsBin != binarySyntax {
		return moerr.NewInvalidInputNoCtx("string literal form and isBin disagree")
	}
	return nil
}

// NormalizeTextLiteralFormsForCompatibility maps the explicit ordinary TEXT
// spelling to the zero value used by older serialized plans. Semantic forms
// such as HEX, BIT, and BINARY_INTRODUCER remain distinct.
func (m *Expr) NormalizeTextLiteralFormsForCompatibility() error {
	if err := m.ValidateStringLiteralForms(); err != nil {
		return err
	}
	return m.walkStringLiterals(func(_ *Expr, lit *Literal) error {
		if lit.LiteralForm == StringLiteralForm_STRING_LITERAL_TEXT {
			lit.LiteralForm = StringLiteralForm_STRING_LITERAL_NONE
		}
		return nil
	})
}

func (m *Expr) walkStringLiterals(visitor func(*Expr, *Literal) error) error {
	if m == nil {
		return nil
	}
	if lit := m.GetLit(); lit != nil {
		if err := visitor(m, lit); err != nil {
			return err
		}
		return lit.Src.walkStringLiterals(visitor)
	}
	if fn := m.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := arg.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
	}
	if list := m.GetList(); list != nil {
		for _, item := range list.List {
			if err := item.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
	}
	if subquery := m.GetSub(); subquery != nil {
		if err := subquery.Child.walkStringLiterals(visitor); err != nil {
			return err
		}
	}
	if window := m.GetW(); window != nil {
		if err := window.WindowFunc.walkStringLiterals(visitor); err != nil {
			return err
		}
		for _, item := range window.PartitionBy {
			if err := item.walkStringLiterals(visitor); err != nil {
				return err
			}
		}
		for _, order := range window.OrderBy {
			if order != nil {
				if err := order.Expr.walkStringLiterals(visitor); err != nil {
					return err
				}
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				if err := window.Frame.Start.Val.walkStringLiterals(visitor); err != nil {
					return err
				}
			}
			if window.Frame.End != nil {
				return window.Frame.End.Val.walkStringLiterals(visitor)
			}
		}
	}
	return nil
}

func (p *Plan) ValidateStringLiteralForms() error {
	return validateStringLiteralFormsInOwner(p)
}

func ValidateStringLiteralFormsInOwner(owner any) error {
	return validateStringLiteralFormsInOwner(owner)
}

// validateStringLiteralFormsInOwner validates every expression nested in a
// decoded plan without coupling this boundary check to every plan node shape.
func validateStringLiteralFormsInOwner(owner any) error {
	seen := make(map[uintptr]struct{})
	var walk func(reflect.Value) error
	walk = func(value reflect.Value) error {
		if !value.IsValid() {
			return nil
		}
		if value.Kind() == reflect.Interface {
			if value.IsNil() {
				return nil
			}
			return walk(value.Elem())
		}
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return nil
			}
			if expr, ok := value.Interface().(*Expr); ok {
				return expr.ValidateStringLiteralForms()
			}
			pointer := value.Pointer()
			if _, ok := seen[pointer]; ok {
				return nil
			}
			seen[pointer] = struct{}{}
			return walk(value.Elem())
		}
		switch value.Kind() {
		case reflect.Struct:
			for field := 0; field < value.NumField(); field++ {
				if value.Type().Field(field).PkgPath == "" {
					if err := walk(value.Field(field)); err != nil {
						return err
					}
				}
			}
		case reflect.Slice, reflect.Array:
			if value.Type().Elem().Kind() == reflect.Uint8 {
				return nil
			}
			for item := 0; item < value.Len(); item++ {
				if err := walk(value.Index(item)); err != nil {
					return err
				}
			}
		case reflect.Map:
			iterator := value.MapRange()
			for iterator.Next() {
				if err := walk(iterator.Value()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(reflect.ValueOf(owner))
}
