package vm

import (
	"bytes"
	bdifference "matrixone/pkg/sql/colexec/bag/difference"
	bdifferenceR "matrixone/pkg/sql/colexec/bag/differenceR"
	binner "matrixone/pkg/sql/colexec/bag/inner"
	bintersect "matrixone/pkg/sql/colexec/bag/intersect"
	bnatural "matrixone/pkg/sql/colexec/bag/natural"
	bunion "matrixone/pkg/sql/colexec/bag/union"
	"matrixone/pkg/sql/colexec/dedup"
	"matrixone/pkg/sql/colexec/exchange"
	"matrixone/pkg/sql/colexec/group"
	"matrixone/pkg/sql/colexec/limit"
	"matrixone/pkg/sql/colexec/merge"
	"matrixone/pkg/sql/colexec/mergededup"
	"matrixone/pkg/sql/colexec/mergegroup"
	"matrixone/pkg/sql/colexec/mergeorder"
	"matrixone/pkg/sql/colexec/mergesum"
	"matrixone/pkg/sql/colexec/mergetop"
	"matrixone/pkg/sql/colexec/myoutput"
	"matrixone/pkg/sql/colexec/offset"
	"matrixone/pkg/sql/colexec/order"
	"matrixone/pkg/sql/colexec/output"
	"matrixone/pkg/sql/colexec/projection"
	"matrixone/pkg/sql/colexec/restrict"
	"matrixone/pkg/sql/colexec/set/difference"
	"matrixone/pkg/sql/colexec/set/differenceR"
	"matrixone/pkg/sql/colexec/set/inner"
	"matrixone/pkg/sql/colexec/set/intersect"
	"matrixone/pkg/sql/colexec/set/natural"
	"matrixone/pkg/sql/colexec/set/union"
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
	SetUnion:          union.String,
	SetIntersect:      intersect.String,
	SetDifference:     difference.String,
	SetDifferenceR:    differenceR.String,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.String,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.String,
	SetSemiDifference: nil,
	BagUnion:          bunion.String,
	BagIntersect:      bintersect.String,
	BagDifference:     bdifference.String,
	BagDifferenceR:    bdifferenceR.String,
	BagInnerJoin:      binner.String,
	BagNaturalJoin:    bnatural.String,
	Output:            output.String,
	MyOutput:          myoutput.String,
	Exchange:          exchange.String,
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
	SetUnion:          union.Prepare,
	SetIntersect:      intersect.Prepare,
	SetDifference:     difference.Prepare,
	SetDifferenceR:    differenceR.Prepare,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.Prepare,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.Prepare,
	SetSemiDifference: nil,
	BagUnion:          bunion.Prepare,
	BagIntersect:      bintersect.Prepare,
	BagDifference:     bdifference.Prepare,
	BagDifferenceR:    bdifferenceR.Prepare,
	BagInnerJoin:      binner.Prepare,
	BagNaturalJoin:    bnatural.Prepare,
	Output:            output.Prepare,
	MyOutput:          myoutput.Prepare,
	Exchange:          exchange.Prepare,
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
	SetUnion:          union.Call,
	SetIntersect:      intersect.Call,
	SetDifference:     difference.Call,
	SetDifferenceR:    differenceR.Call,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.Call,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.Call,
	SetSemiDifference: nil,
	BagUnion:          bunion.Call,
	BagIntersect:      bintersect.Call,
	BagDifference:     bdifference.Call,
	BagDifferenceR:    bdifferenceR.Call,
	BagInnerJoin:      binner.Call,
	BagNaturalJoin:    bnatural.Call,
	Output:            output.Call,
	MyOutput:          myoutput.Call,
	Exchange:          exchange.Call,
	Merge:             merge.Call,
	MergeTop:          mergetop.Call,
	MergeDedup:        mergededup.Call,
	MergeOrder:        mergeorder.Call,
	MergeGroup:        mergegroup.Call,
	MergeSummarize:    mergesum.Call,
}
