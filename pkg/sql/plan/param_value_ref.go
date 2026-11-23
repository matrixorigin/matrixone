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
