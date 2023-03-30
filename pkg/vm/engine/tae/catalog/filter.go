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

func ActiveWithNoTxnFilter(be *BaseEntryImpl[*MetadataMVCCNode]) bool {
	return !be.HasDropCommittedLocked() && !be.IsCreatingOrAborted()
}

func AppendableBlkFilter(be *BlockEntry) bool {
	return be.IsAppendable()
}

func NonAppendableBlkFilter(be *BlockEntry) bool {
	return !be.IsAppendable()
}

type ComposedFilter struct {
	CommitFilters []func(*BaseEntryImpl[*MetadataMVCCNode]) bool
	BlockFilters  []func(*BlockEntry) bool
}

func NewComposedFilter() *ComposedFilter {
	return &ComposedFilter{
		CommitFilters: make([]func(*BaseEntryImpl[*MetadataMVCCNode]) bool, 0),
		BlockFilters:  make([]func(*BlockEntry) bool, 0),
	}
}

func (filter *ComposedFilter) AddCommitFilter(f func(*BaseEntryImpl[*MetadataMVCCNode]) bool) {
	filter.CommitFilters = append(filter.CommitFilters, f)
}

func (filter *ComposedFilter) AddBlockFilter(f func(*BlockEntry) bool) {
	filter.BlockFilters = append(filter.BlockFilters, f)
}

func (filter *ComposedFilter) FilteCommit(be *BaseEntryImpl[*MetadataMVCCNode]) bool {
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

func (filter *ComposedFilter) FilteBlock(be *BlockEntry) bool {
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
