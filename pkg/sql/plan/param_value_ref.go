// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/matrixorigin/matrixone/pkg/container/types"

// A prepared execution's parameter values are an []any whose elements are
// *ParamValue pointing into storage the prepared statement owns and rewrites
// in place every execution (boxing a pointer does not allocate; boxing the
// struct would, once per parameter per execution). Values built elsewhere,
// such as tests or single-shot callers, may still be ParamValue structs.
// Read them through AsParamValue and write them through SetParamValueAt.

// AsParamValue returns a copy of the prepared parameter value v holds, by
// pointer or by value. The copy keeps value semantics for callers that adjust
// it locally: changing it never changes the statement's storage.
func AsParamValue(v any) (ParamValue, bool) {
	switch p := v.(type) {
	case *ParamValue:
		if p != nil {
			return *p, true
		}
	case ParamValue:
		return p, true
	}
	return ParamValue{}, false
}

// SetParamValueAt stores p as parameter i, writing through the statement's
// storage when values[i] points into it.
func SetParamValueAt(values []any, i int, p ParamValue) {
	if ref, ok := values[i].(*ParamValue); ok && ref != nil {
		*ref = p
		return
	}
	values[i] = p
}

// ParamValueStore is the per-statement storage behind an execution's []any of
// parameter values. Fill returns values whose elements point into it, reusing
// its memory once it is large enough.
type ParamValueStore struct {
	params []ParamValue
	values []any
}

// Reset readies the store for n parameters and returns the []any view; each
// element points at a zeroed ParamValue the caller fills through Ref.
func (s *ParamValueStore) Reset(n int) []any {
	if cap(s.params) < n {
		s.params = make([]ParamValue, n)
		s.values = make([]any, n)
	}
	s.params = s.params[:n]
	s.values = s.values[:n]
	for i := range s.params {
		s.params[i] = ParamValue{}
		s.values[i] = &s.params[i]
	}
	return s.values
}

// Ref returns parameter i's storage.
func (s *ParamValueStore) Ref(i int) *ParamValue {
	return &s.params[i]
}

// CopyParamValues returns values with every prepared parameter value copied
// out of the statement's storage, for a snapshot that must not change when the
// storage is rewritten (a retry of the same execution, for example).
func CopyParamValues(values []any) []any {
	if values == nil {
		return nil
	}
	out := make([]any, len(values))
	for i, v := range values {
		if param, ok := AsParamValue(v); ok {
			out[i] = param
		} else {
			out[i] = v
		}
	}
	return out
}

// ParamValueType returns the SQL type a prepared parameter value carries: its
// protocol RuntimeType (COM_STMT_EXECUTE) or else its SourceType (SQL EXECUTE
// ... USING). ok is false for a value that carries neither.
func ParamValueType(p ParamValue) (types.Type, bool) {
	if p.HasRuntimeType {
		return p.RuntimeType, true
	}
	if p.HasSourceType {
		return p.SourceType, true
	}
	return types.Type{}, false
}

// ParamValueKind is the conversion kind of a parameter: Integer, Float,
// Decimal or Boolean when its type is one of those. A parameter whose type is
// text, temporal or another non-numeric type, or that has no type (a NULL),
// keeps the kind its producer recorded: a user variable copied from a system
// variable (SET @v = @@var) holds the variable's text with the variable's
// numeric kind.
func ParamValueKind(p ParamValue) types.StringConversionKind {
	if typ, ok := ParamValueType(p); ok {
		if kind, _ := types.StringConversionKindForType(typ.Oid); kind != types.StringConversionString {
			return kind
		}
	}
	return p.PrepareParamKind
}
