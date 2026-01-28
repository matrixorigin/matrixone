// Copyright 2024 Matrix Origin
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

package tools

import (
	"sort"
)

type UnorderedMap[KT ~string | ~int, VT any] map[KT]VT

func (umap UnorderedMap[KT, VT]) Insert(k KT, v VT) {
	umap[k] = v
}

func (umap UnorderedMap[KT, VT]) Find(k KT) (ok bool, v VT) {
	v, ok = umap[k]
	return ok, v
}

func (umap UnorderedMap[KT, VT]) Count(k KT) int {
	_, ok := umap[k]
	if ok {
		return 1
	}
	return 0
}

func (umap UnorderedMap[KT, VT]) Size() int {
	return len(umap)
}

func AssertFunc(b bool, errInfo string) {
	if !b {
		panic(errInfo)
	}
}

func NewAssignMap(pairs ...AssignPair) UnorderedMap[string, *ExprMatcher] {
	ret := make(UnorderedMap[string, *ExprMatcher])
	for _, pair := range pairs {
		ret.Insert(pair.Key, pair.Value)
	}
	return ret
}

func NewAggrMap(pairs ...AggrPair) UnorderedMap[string, *AggrFuncMatcher] {
	ret := make(UnorderedMap[string, *AggrFuncMatcher])
	for _, pair := range pairs {
		ret.Insert(pair.Key, pair.Value)
	}
	return ret
}

func NewStringMap(pairs ...StringPair) UnorderedMap[string, string] {
	ret := make(UnorderedMap[string, string])
	for _, pair := range pairs {
		ret[pair.Key] = pair.Value
	}
	return ret
}

func StringsEqual(a, b []string) bool {
	alen := len(a)
	blen := len(b)
	if alen != blen {
		return false
	}
	if alen == 0 {
		return true
	}
	sort.Strings(a)
	sort.Strings(b)
	for i, s := range a {
		if s != b[i] {
			return false
		}
	}
	return true
}

func VarRefsEqual(a, b []VarRef) bool {
	if len(a) != len(b) {
		return false
	}
	alen := len(a)
	if alen == 0 {
		return true
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Name < a[j].Name })
	sort.Slice(b, func(i, j int) bool { return b[i].Name < b[j].Name })
	for i := 0; i < alen; i++ {
		if a[i].Name != b[i].Name {
			return false
		}
		if a[i].Type.Id != b[i].Type.Id {
			return false
		}
	}
	return true
}
