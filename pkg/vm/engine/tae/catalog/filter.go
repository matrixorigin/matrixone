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

package catalog

func ActiveObjectWithNoTxnFilter(be *ObjectEntry) bool {
	return !be.HasDropCommitted() && !be.IsCreatingOrAborted()
}

func AppendableBlkFilter(be *ObjectEntry) bool {
	return be.IsAppendable()
}

func NonAppendableBlkFilter(be *ObjectEntry) bool {
	return !be.IsAppendable()
}

type ComposedFilter struct {
	CommitFilters []func(*ObjectEntry) bool
	BlockFilters  []func(*ObjectEntry) bool
}

func NewComposedFilter() *ComposedFilter {
	return &ComposedFilter{
		CommitFilters: make([]func(*ObjectEntry) bool, 0),
		BlockFilters:  make([]func(*ObjectEntry) bool, 0),
	}
}

func (filter *ComposedFilter) AddCommitFilter(f func(*ObjectEntry) bool) {
	filter.CommitFilters = append(filter.CommitFilters, f)
}

func (filter *ComposedFilter) AddBlockFilter(f func(*ObjectEntry) bool) {
	filter.BlockFilters = append(filter.BlockFilters, f)
}

func (filter *ComposedFilter) FilteCommit(be *ObjectEntry) bool {
	ret := false
	for _, f := range filter.CommitFilters {
		if !f(be) {
			ret = false
			break
		}
		ret = true
	}
	return ret
}

func (filter *ComposedFilter) FilteBlock(be *ObjectEntry) bool {
	ret := false
	for _, f := range filter.BlockFilters {
		if !f(be) {
			ret = false
			break
		}
		ret = true
	}
	return ret
}
