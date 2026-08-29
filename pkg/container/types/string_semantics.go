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

package types

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

// StringDomain is the statically resolved SQL string domain. The Type remains
// the owner of the concrete OID, width, charset, and collation identity.
type StringDomain uint8

const (
	StringDomainNone StringDomain = iota
	StringDomainText
	StringDomainBinary
	stringDomainMax = StringDomainBinary
)

// RuntimeStringDomain describes only a value's row-level override. Inherit is
// deliberately distinct from Text: a selected text value must be able to
// override a statically binary common result type.
type RuntimeStringDomain uint8

const (
	RuntimeStringInherit RuntimeStringDomain = iota
	RuntimeStringText
	RuntimeStringBinary
	runtimeStringDomainMax = RuntimeStringBinary
)

// StringSource identifies the owner that introduced a runtime value. It does
// not encode the value's string domain or conversion behavior.
type StringSource uint8

const (
	StringSourceExpression StringSource = iota
	StringSourceLiteral
	StringSourceUserVariable
	StringSourceSQLPrepare
	StringSourceCOMStmt
	stringSourceMax = StringSourceCOMStmt
)

// Valid reports whether source is a value defined by the wire-stable enum.
// Decoders must reject unknown values instead of silently treating them as
// expression-owned data.
func (source StringSource) Valid() bool {
	return source <= stringSourceMax
}

// MergeStringSources is the contributing-values ownership rule. Equal owners
// remain exact; values contributed by different owners become expression
// results. The rule is deliberately commutative and associative.
func MergeStringSources(left, right StringSource) (StringSource, error) {
	if !left.Valid() || !right.Valid() {
		return StringSourceExpression, moerr.NewInvalidInputNoCtxf(
			"invalid string source merge %d and %d", left, right)
	}
	if left == right {
		return left, nil
	}
	return StringSourceExpression, nil
}

// StringLiteralForm records binder-visible literal syntax. Raw hexadecimal and
// bit forms remain separate from binary-string domain provenance because they
// also control numeric conversion.
type StringLiteralForm uint8

const (
	StringLiteralNone StringLiteralForm = iota
	StringLiteralText
	StringLiteralBinaryIntroducer
	StringLiteralHex
	StringLiteralBit
	stringLiteralFormMax = StringLiteralBit
)

// StringConversionKind is the conversion domain of a dynamic value. It is not
// a source category; adding a protocol or variable source must never add a new
// conversion kind.
type StringConversionKind uint8

const (
	StringConversionString StringConversionKind = iota
	StringConversionInteger
	StringConversionFloat
	StringConversionDecimal
	StringConversionBoolean
	stringConversionKindMax = StringConversionBoolean
)

// StringNullKind distinguishes ordinary untyped NULL from a NULL carrying a
// resolved SQL type. Parameter-marker source is retained independently.
type StringNullKind uint8

const (
	StringNotNull StringNullKind = iota
	StringUntypedNull
	StringTypedNull
	stringNullKindMax = StringTypedNull
)

// StringMergePolicy makes an expression's ownership rule explicit. Different
// policies intentionally do not share an implicit generic merge behavior.
type StringMergePolicy uint8

const (
	StringMergeSelectedValue StringMergePolicy = iota
	StringMergeCommonDomain
	StringMergeContributingValues
	stringMergePolicyMax = StringMergeContributingValues
)

// StringSemanticState is the logical contract shared by binder, frontend, and
// vector owners. Fields are private so invalid cross-axis combinations cannot
// be assembled without validation.
type StringSemanticState struct {
	staticType Type
	runtime    RuntimeStringDomain
	source     StringSource
	literal    StringLiteralForm
	conversion StringConversionKind
	nullKind   StringNullKind
}

func NewStringSemanticState(
	staticType Type,
	runtime RuntimeStringDomain,
	source StringSource,
	literal StringLiteralForm,
	conversion StringConversionKind,
	nullKind StringNullKind,
) (StringSemanticState, error) {
	state := StringSemanticState{
		staticType: staticType,
		runtime:    runtime,
		source:     source,
		literal:    literal,
		conversion: conversion,
		nullKind:   nullKind,
	}
	if err := state.Validate(); err != nil {
		return StringSemanticState{}, err
	}
	return state, nil
}

func (s StringSemanticState) StaticType() Type                     { return s.staticType }
func (s StringSemanticState) RuntimeDomain() RuntimeStringDomain   { return s.runtime }
func (s StringSemanticState) Source() StringSource                 { return s.source }
func (s StringSemanticState) LiteralForm() StringLiteralForm       { return s.literal }
func (s StringSemanticState) ConversionKind() StringConversionKind { return s.conversion }
func (s StringSemanticState) NullKind() StringNullKind             { return s.nullKind }

func (s StringSemanticState) Validate() error {
	if StaticStringDomain(s.staticType) > stringDomainMax || s.runtime > runtimeStringDomainMax ||
		s.source > stringSourceMax || s.literal > stringLiteralFormMax ||
		s.conversion > stringConversionKindMax || s.nullKind > stringNullKindMax {
		return moerr.NewInvalidInputNoCtx("invalid string semantic enum value")
	}

	staticDomain := StaticStringDomain(s.staticType)
	if staticDomain == StringDomainNone && s.staticType.Oid != T_any &&
		s.runtime != RuntimeStringInherit {
		return moerr.NewInvalidInputNoCtx("runtime string domain requires a string or unresolved static type")
	}
	if s.source == StringSourceLiteral {
		if s.literal == StringLiteralNone && s.nullKind == StringNotNull {
			return moerr.NewInvalidInputNoCtx("non-NULL literal requires an explicit literal form")
		}
	} else if s.literal != StringLiteralNone {
		return moerr.NewInvalidInputNoCtx("literal form requires literal source")
	}
	if (s.literal == StringLiteralText || s.literal == StringLiteralBinaryIntroducer) &&
		staticDomain == StringDomainNone && s.staticType.Oid != T_any {
		return moerr.NewInvalidInputNoCtx("text literal form requires a string or unresolved static type")
	}
	if s.literal == StringLiteralBinaryIntroducer && s.EffectiveStringDomain() != StringDomainBinary {
		return moerr.NewInvalidInputNoCtx("binary introducer requires an effective binary domain")
	}
	if s.conversion != StringConversionString &&
		s.source != StringSourceUserVariable &&
		s.source != StringSourceSQLPrepare &&
		s.source != StringSourceCOMStmt {
		return moerr.NewInvalidInputNoCtx("dynamic conversion kind requires a dynamic value source")
	}
	switch s.nullKind {
	case StringUntypedNull:
		if s.staticType.Oid != T_any {
			return moerr.NewInvalidInputNoCtx("untyped NULL requires T_any")
		}
	case StringTypedNull:
		if s.staticType.Oid == T_any {
			return moerr.NewInvalidInputNoCtx("typed NULL requires a resolved type")
		}
	}
	return nil
}

// StaticStringDomain resolves the SQL domain without consulting runtime row
// provenance. Binary charset is authoritative even for a text-shaped OID.
func StaticStringDomain(typ Type) StringDomain {
	if !typ.Oid.IsMySQLString() {
		return StringDomainNone
	}
	if typ.Charset == CharsetBinary {
		return StringDomainBinary
	}
	return StringDomainText
}

func (s StringSemanticState) EffectiveStringDomain() StringDomain {
	switch s.runtime {
	case RuntimeStringText:
		return StringDomainText
	case RuntimeStringBinary:
		return StringDomainBinary
	default:
		return StaticStringDomain(s.staticType)
	}
}

func StringNullKindForType(typ Type, isNull bool) StringNullKind {
	if !isNull {
		return StringNotNull
	}
	if typ.Oid == T_any {
		return StringUntypedNull
	}
	return StringTypedNull
}

// MergeStringSemanticStates applies one explicit ownership policy. resultType
// is already resolved by the binder and remains the sole static-domain owner.
func MergeStringSemanticStates(
	policy StringMergePolicy,
	resultType Type,
	states ...StringSemanticState,
) (StringSemanticState, error) {
	if policy > stringMergePolicyMax {
		return StringSemanticState{}, moerr.NewInvalidInputNoCtxf("invalid string merge policy %d", policy)
	}
	for i := range states {
		if err := states[i].Validate(); err != nil {
			return StringSemanticState{}, moerr.NewInvalidInputNoCtxf("invalid string state %d: %v", i, err)
		}
	}
	if len(states) == 0 {
		return StringSemanticState{}, moerr.NewInvalidInputNoCtx("string merge requires at least one state")
	}

	switch policy {
	case StringMergeSelectedValue:
		if len(states) != 1 {
			return StringSemanticState{}, moerr.NewInvalidInputNoCtx("selected-value merge requires exactly one selected state")
		}
		selected := states[0]
		selectedDomain := selected.EffectiveStringDomain()
		inheritedDomain := selected.runtime == RuntimeStringInherit
		selected.staticType = resultType
		if inheritedDomain && selectedDomain != StringDomainNone && selectedDomain != StaticStringDomain(resultType) {
			if selectedDomain == StringDomainBinary {
				selected.runtime = RuntimeStringBinary
			} else {
				selected.runtime = RuntimeStringText
			}
		}
		selected.nullKind = StringNullKindForType(resultType, selected.nullKind != StringNotNull)
		if err := selected.Validate(); err != nil {
			return StringSemanticState{}, err
		}
		return selected, nil

	case StringMergeCommonDomain:
		return expressionMergeState(resultType, states, RuntimeStringInherit)

	case StringMergeContributingValues:
		runtime := RuntimeStringInherit
		seenValue := false
		effective := StringDomainNone
		for _, state := range states {
			if state.nullKind != StringNotNull {
				continue
			}
			seenValue = true
			domain := state.EffectiveStringDomain()
			if domain == StringDomainBinary {
				effective = StringDomainBinary
			} else if effective == StringDomainNone && domain == StringDomainText {
				effective = StringDomainText
			}
		}
		if seenValue && effective != StringDomainNone && effective != StaticStringDomain(resultType) {
			if effective == StringDomainBinary {
				runtime = RuntimeStringBinary
			} else {
				runtime = RuntimeStringText
			}
		}
		return expressionMergeState(resultType, states, runtime)
	}
	panic("unreachable string merge policy")
}

func expressionMergeState(
	resultType Type,
	states []StringSemanticState,
	runtime RuntimeStringDomain,
) (StringSemanticState, error) {
	allNull := len(states) != 0
	for _, state := range states {
		if state.nullKind == StringNotNull {
			allNull = false
			break
		}
	}
	return NewStringSemanticState(
		resultType,
		runtime,
		StringSourceExpression,
		StringLiteralNone,
		StringConversionString,
		StringNullKindForType(resultType, allNull),
	)
}
