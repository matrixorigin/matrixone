package vm

import (
	"bytes"
	binner "matrixone/pkg/sql/colexec/bag/inner"
	bunion "matrixone/pkg/sql/colexec/bag/union"
	"matrixone/pkg/sql/colexec/dedup"
	"matrixone/pkg/sql/colexec/group"
	"matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/mergededup"
	"matrixone/pkg/sql/colexec/mergegroup"
	"matrixone/pkg/sql/colexec/mergeorder"
	"matrixone/pkg/sql/colexec/mergesum"
	"matrixone/pkg/sql/colexec/mergetop"
	"matrixone/pkg/sql/colexec/offset"
	"matrixone/pkg/sql/colexec/order"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/colexec/summarize"
	"matrixone/pkg/sql/colexec/top"
	"matrixone/pkg/sql/colexec/transfer"
	"matrixone/pkg/vm/process"
)

var sFuncs = [...]func(interface{}, *bytes.Buffer){
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

var pFuncs = [...]func(*process.Process, interface{}) error{
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

var rFuncs = [...]func(*process.Process, interface{}) (bool, error){
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
