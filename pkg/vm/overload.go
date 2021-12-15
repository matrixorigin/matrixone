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

package vm

import (
	"bytes"
	binner "github.com/matrixorigin/matrixone/pkg/sql/colexec/bag/inner"
	bunion "github.com/matrixorigin/matrixone/pkg/sql/colexec/bag/union"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergededup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergegroup"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergesum"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/offset"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/output"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/restrict"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/summarize"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/transfer"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var formatFunc = [...]func(interface{}, *bytes.Buffer){
	Top:               top.String,
	Dedup:             dedup.String,
	Limit:             limit.String,
	Group:             group.String,
	Order:             order.String,
	Offset:            offset.String,
	Transfer:          transfer.String,
	Restrict:          restrict.String,
	Summarize:         summarize.String,
	Projection:        projection.String,
	SetUnion:          nil,
	SetIntersect:      nil,
	SetDifference:     nil,
	SetDifferenceR:    nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      nil,
	SetRightJoin:      nil,
	SetNaturalJoin:    nil,
	SetSemiDifference: nil,
	BagUnion:          bunion.String,
	BagIntersect:      nil,
	BagDifference:     nil,
	BagDifferenceR:    nil,
	BagInnerJoin:      binner.String,
	BagNaturalJoin:    nil,
	Output:            output.String,
	Merge:             merge.String,
	MergeTop:          mergetop.String,
	MergeDedup:        mergededup.String,
	MergeOrder:        mergeorder.String,
	MergeGroup:        mergegroup.String,
	MergeSummarize:    mergesum.String,
}

var prepareFunc = [...]func(*process.Process, interface{}) error{
	Top:               top.Prepare,
	Dedup:             dedup.Prepare,
	Limit:             limit.Prepare,
	Group:             group.Prepare,
	Order:             order.Prepare,
	Offset:            offset.Prepare,
	Transfer:          transfer.Prepare,
	Restrict:          restrict.Prepare,
	Summarize:         summarize.Prepare,
	Projection:        projection.Prepare,
	SetUnion:          nil,
	SetIntersect:      nil,
	SetDifference:     nil,
	SetDifferenceR:    nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      nil,
	SetRightJoin:      nil,
	SetNaturalJoin:    nil,
	SetSemiDifference: nil,
	BagUnion:          bunion.Prepare,
	BagIntersect:      nil,
	BagDifference:     nil,
	BagDifferenceR:    nil,
	BagInnerJoin:      binner.Prepare,
	BagNaturalJoin:    nil,
	Output:            output.Prepare,
	Merge:             merge.Prepare,
	MergeTop:          mergetop.Prepare,
	MergeDedup:        mergededup.Prepare,
	MergeOrder:        mergeorder.Prepare,
	MergeGroup:        mergegroup.Prepare,
	MergeSummarize:    mergesum.Prepare,
}

var execFunc = [...]func(*process.Process, interface{}) (bool, error){
	Top:               top.Call,
	Dedup:             dedup.Call,
	Limit:             limit.Call,
	Group:             group.Call,
	Order:             order.Call,
	Offset:            offset.Call,
	Transfer:          transfer.Call,
	Restrict:          restrict.Call,
	Summarize:         summarize.Call,
	Projection:        projection.Call,
	SetUnion:          nil,
	SetIntersect:      nil,
	SetDifference:     nil,
	SetDifferenceR:    nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      nil,
	SetRightJoin:      nil,
	SetNaturalJoin:    nil,
	SetSemiDifference: nil,
	BagUnion:          bunion.Call,
	BagIntersect:      nil,
	BagDifference:     nil,
	BagDifferenceR:    nil,
	BagInnerJoin:      binner.Call,
	BagNaturalJoin:    nil,
	Output:            output.Call,
	Merge:             merge.Call,
	MergeTop:          mergetop.Call,
	MergeDedup:        mergededup.Call,
	MergeOrder:        mergeorder.Call,
	MergeGroup:        mergegroup.Call,
	MergeSummarize:    mergesum.Call,
}
