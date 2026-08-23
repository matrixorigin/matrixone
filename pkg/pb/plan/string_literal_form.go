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
	"strings"

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
	return m.walkStringLiterals(func(expr *Expr, lit *Literal) error {
		if lit.LiteralForm == StringLiteralForm_STRING_LITERAL_TEXT &&
			staticStringDomainForPlanType(expr.Typ) == planStringDomainText {
			lit.LiteralForm = StringLiteralForm_STRING_LITERAL_NONE
		}
		return nil
	})
}

const (
	planStringDomainNone uint8 = iota
	planStringDomainText
	planStringDomainBinary
)

func staticStringDomainForPlanType(typ Type) uint8 {
	if !isPlanMySQLStringType(typ.Id) {
		return planStringDomainNone
	}
	// CharsetBinary is 1 in the plan wire contract and is authoritative even
	// for CHAR/VARCHAR/TEXT-shaped OIDs.
	if typ.Charset == 1 {
		return planStringDomainBinary
	}
	switch typ.Id {
	case 64, 65, 70: // BINARY, VARBINARY, BLOB
		return planStringDomainBinary
	default:
		return planStringDomainText
	}
}

const (
	possibleStringDomainText uint8 = 1 << iota
	possibleStringDomainBinary
)

func possibleStringDomainForStaticType(typ Type) uint8 {
	switch staticStringDomainForPlanType(typ) {
	case planStringDomainText:
		return possibleStringDomainText
	case planStringDomainBinary:
		return possibleStringDomainBinary
	default:
		return 0
	}
}

// RequiresMORPCVersion23StringProvenance reports whether an owner can produce
// runtime string provenance that differs from an expression's static domain.
// Besides cross-domain literals, IF/CASE/COALESCE preserve the domain of their
// selected value through binder-inserted casts. Older workers cannot represent
// that dynamic provenance, so such plans cannot cross a remote owner boundary
// before MORPC version 23.
func RequiresMORPCVersion23StringProvenance(owner any) (bool, error) {
	required := false
	err := walkExpressionsInOwner(owner, func(expr *Expr) error {
		_, exprRequired, err := expr.possibleRuntimeStringDomains()
		required = required || exprRequired
		return err
	})
	return required, err
}

// RequiresMORPCVersion23StringLiterals is retained for callers built against
// the original literal-only API. Its result now includes dynamic provenance.
func RequiresMORPCVersion23StringLiterals(owner any) (bool, error) {
	return RequiresMORPCVersion23StringProvenance(owner)
}

// RequiresMORPCVersion27NumericPrefix reports whether an owner contains a
// planner-injected CAST that uses the numeric-prefix sentinel. Charset=255 was
// deliberately unused before this contract, so older workers must not execute
// any plan that carries it.
func RequiresMORPCVersion27NumericPrefix(owner any) (bool, error) {
	required := false
	err := walkExpressionsInOwner(owner, func(expr *Expr) error {
		if !required {
			required = expr.requiresMORPCVersion27NumericPrefix()
		}
		return nil
	})
	return required, err
}

func (m *Expr) requiresMORPCVersion27NumericPrefix() bool {
	if m == nil {
		return false
	}
	if m.Typ.Charset == 255 {
		fn := m.GetF()
		if fn != nil && fn.Func != nil && strings.EqualFold(fn.Func.GetObjName(), "cast") {
			return true
		}
	}
	if lit := m.GetLit(); lit != nil && lit.Src.requiresMORPCVersion27NumericPrefix() {
		return true
	}
	if fn := m.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if arg.requiresMORPCVersion27NumericPrefix() {
				return true
			}
		}
	}
	if list := m.GetList(); list != nil {
		for _, item := range list.List {
			if item.requiresMORPCVersion27NumericPrefix() {
				return true
			}
		}
	}
	if subquery := m.GetSub(); subquery != nil && subquery.Child.requiresMORPCVersion27NumericPrefix() {
		return true
	}
	if window := m.GetW(); window != nil {
		if window.WindowFunc.requiresMORPCVersion27NumericPrefix() {
			return true
		}
		for _, item := range window.PartitionBy {
			if item.requiresMORPCVersion27NumericPrefix() {
				return true
			}
		}
		for _, order := range window.OrderBy {
			if order != nil && order.Expr.requiresMORPCVersion27NumericPrefix() {
				return true
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil && window.Frame.Start.Val.requiresMORPCVersion27NumericPrefix() {
				return true
			}
			if window.Frame.End != nil && window.Frame.End.Val.requiresMORPCVersion27NumericPrefix() {
				return true
			}
		}
	}
	return false
}

func (m *Expr) possibleRuntimeStringDomains() (uint8, bool, error) {
	if m == nil {
		return 0, false, nil
	}
	staticDomains := possibleStringDomainForStaticType(m.Typ)
	if lit := m.GetLit(); lit != nil {
		if err := m.validateStringLiteralForm(lit); err != nil {
			return 0, false, err
		}
		domains := staticDomains
		ownRequired := false
		switch lit.LiteralForm {
		case StringLiteralForm_STRING_LITERAL_TEXT:
			domains = possibleStringDomainText
			ownRequired = staticDomains == possibleStringDomainBinary
		case StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
			StringLiteralForm_STRING_LITERAL_HEX,
			StringLiteralForm_STRING_LITERAL_BIT:
			domains = possibleStringDomainBinary
			ownRequired = lit.LiteralForm == StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER &&
				staticDomains == possibleStringDomainText
		}
		_, childRequired, err := lit.Src.possibleRuntimeStringDomains()
		return domains, childRequired || ownRequired, err
	}

	fn := m.GetF()
	if fn == nil {
		required := false
		visit := func(expr *Expr) error {
			_, childRequired, err := expr.possibleRuntimeStringDomains()
			required = required || childRequired
			return err
		}
		if list := m.GetList(); list != nil {
			for _, item := range list.List {
				if err := visit(item); err != nil {
					return 0, false, err
				}
			}
		}
		if subquery := m.GetSub(); subquery != nil {
			if err := visit(subquery.Child); err != nil {
				return 0, false, err
			}
		}
		if window := m.GetW(); window != nil {
			if err := visit(window.WindowFunc); err != nil {
				return 0, false, err
			}
			for _, item := range window.PartitionBy {
				if err := visit(item); err != nil {
					return 0, false, err
				}
			}
			for _, order := range window.OrderBy {
				if order != nil {
					if err := visit(order.Expr); err != nil {
						return 0, false, err
					}
				}
			}
			if window.Frame != nil {
				if window.Frame.Start != nil {
					if err := visit(window.Frame.Start.Val); err != nil {
						return 0, false, err
					}
				}
				if window.Frame.End != nil {
					if err := visit(window.Frame.End.Val); err != nil {
						return 0, false, err
					}
				}
			}
		}
		return staticDomains, required, nil
	}
	required := false
	argDomains := make([]uint8, len(fn.Args))
	for i, arg := range fn.Args {
		domains, argRequired, err := arg.possibleRuntimeStringDomains()
		if err != nil {
			return 0, false, err
		}
		argDomains[i] = domains
		required = required || argRequired
	}
	name := ""
	functionID := int32(0)
	if fn.Func != nil {
		name = strings.ToLower(fn.Func.ObjName)
		functionID = int32(fn.Func.Obj >> 32)
	}
	if (name == "cast" || functionID == 21) &&
		len(argDomains) != 0 && fn.Func != nil && int32(fn.Func.Obj) == 0 {
		// The low 32 bits encode the overload. Overload zero is the binder's
		// implicit cast and is transparent to flow-control selected values.
		return argDomains[0], required, nil
	}

	selectedDomains := uint8(0)
	switch {
	case name == "if" || name == "iff" || functionID == 113:
		for i := 1; i < len(argDomains); i++ {
			selectedDomains |= argDomains[i]
		}
	case name == "case" || functionID == 71:
		for i := 1; i < len(argDomains); i += 2 {
			selectedDomains |= argDomains[i]
		}
		if len(argDomains)%2 == 1 {
			selectedDomains |= argDomains[len(argDomains)-1]
		}
	case name == "coalesce" || functionID == 74:
		for _, domains := range argDomains {
			selectedDomains |= domains
		}
	default:
		return staticDomains, required, nil
	}
	if selectedDomains != 0 && selectedDomains&^staticDomains != 0 {
		required = true
	}
	return selectedDomains, required, nil
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

// VisitExprTree visits expr and every nested expression exactly once in
// deterministic pre-order. It is the canonical traversal for Expr variants;
// callers should not maintain partial per-variant recursion.
func VisitExprTree(expr *Expr, visitor func(*Expr) error) error {
	if expr == nil {
		return nil
	}
	if err := visitor(expr); err != nil {
		return err
	}
	if lit := expr.GetLit(); lit != nil {
		return VisitExprTree(lit.Src, visitor)
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if err := VisitExprTree(arg, visitor); err != nil {
				return err
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if err := VisitExprTree(item, visitor); err != nil {
				return err
			}
		}
	}
	if subquery := expr.GetSub(); subquery != nil {
		if err := VisitExprTree(subquery.Child, visitor); err != nil {
			return err
		}
	}
	if window := expr.GetW(); window != nil {
		if err := VisitExprTree(window.WindowFunc, visitor); err != nil {
			return err
		}
		for _, item := range window.PartitionBy {
			if err := VisitExprTree(item, visitor); err != nil {
				return err
			}
		}
		for _, order := range window.OrderBy {
			if order != nil {
				if err := VisitExprTree(order.Expr, visitor); err != nil {
					return err
				}
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				if err := VisitExprTree(window.Frame.Start.Val, visitor); err != nil {
					return err
				}
			}
			if window.Frame.End != nil {
				if err := VisitExprTree(window.Frame.End.Val, visitor); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// VisitExpressionsInOwner calls visitor once for every expression root nested
// in owner. The visitor owns traversal inside each Expr; the reflective walk
// deliberately stops at *Expr so expression subtrees are not visited twice.
func VisitExpressionsInOwner(owner any, visitor func(*Expr) error) error {
	return walkExpressionsInOwner(owner, visitor)
}

// validateStringLiteralFormsInOwner validates every expression nested in a
// decoded plan without coupling this boundary check to every plan node shape.
func validateStringLiteralFormsInOwner(owner any) error {
	return walkExpressionsInOwner(owner, func(expr *Expr) error {
		return expr.ValidateStringLiteralForms()
	})
}

func walkExpressionsInOwner(owner any, visitor func(*Expr) error) error {
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
				return visitor(expr)
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
