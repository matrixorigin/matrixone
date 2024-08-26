// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

//type RowsIter interface {
//	Next() bool
//	Close() error
//	Entry() RowEntry
//}

//type rowsIter struct {
//	ts           types.TS
//	iter         btree.IterG[RowEntry]
//	firstCalled  bool
//	lastRowID    types.Rowid
//	checkBlockID bool
//	blockID      types.Blockid
//	iterDeleted  bool
//}

func (p *PartitionStateInProgress) NewRowsIter(ts types.TS, blockID *types.Blockid, iterDeleted bool) *rowsIter {
	iter := p.rows.Copy().Iter()
	ret := &rowsIter{
		ts:          ts,
		iter:        iter,
		iterDeleted: iterDeleted,
	}
	if blockID != nil {
		ret.checkBlockID = true
		ret.blockID = *blockID
	}
	return ret
}

func (p *PartitionStateInProgress) NewPrimaryKeyIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
) *primaryKeyIter {
	index := p.primaryIndex.Copy()
	return &primaryKeyIter{
		ts:           ts,
		spec:         spec,
		iter:         index.Iter(),
		primaryIndex: index,
		rows:         p.rows.Copy(),
	}
}

//type primaryKeyDelIter struct {
//	primaryKeyIter
//	bid types.Blockid
//}

func (p *PartitionStateInProgress) NewPrimaryKeyDelIter(
	ts types.TS,
	spec PrimaryKeyMatchSpec,
	bid types.Blockid,
) *primaryKeyDelIter {
	index := p.primaryIndex.Copy()
	return &primaryKeyDelIter{
		primaryKeyIter: primaryKeyIter{
			ts:           ts,
			spec:         spec,
			primaryIndex: index,
			iter:         index.Iter(),
			rows:         p.rows.Copy(),
		},
		bid: bid,
	}
}
