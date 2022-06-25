// Copyright 2022 Matrix Origin
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

package engine

const (
	FileterNone = iota
	FileterEq
	FileterNe
	FileterLt
	FileterLe
	FileterGt
	FileterGe
	FileterBtw
)

func NewAoeSparseFilter(s *store, reader *aoeReader) *AoeSparseFilter {
	return &AoeSparseFilter{reader: reader, storeReader: s}
}

/*func (a AoeSparseFilter) Eq(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterEq,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Ne(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterNe,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Lt(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterLt,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Le(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterLe,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Gt(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterGt,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Ge(s string, i interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterGe,
		attr: s,
		param1: i,
		param2: nil,
	})
	return a.reader, nil
}

func (a AoeSparseFilter) Btw(s string, i interface{}, i2 interface{}) (engine.Reader, error) {
	a.reader.filter = append(a.reader.filter, filterContext{
		filterType: FileterBtw,
		attr: s,
		param1: i,
		param2: i2,
	})
	return a.reader, nil
}*/
