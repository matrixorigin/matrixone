package vm

import (
	"bytes"
	bdifference "matrixbase/pkg/sql/colexec/bag/difference"
	bdifferenceR "matrixbase/pkg/sql/colexec/bag/differenceR"
	binner "matrixbase/pkg/sql/colexec/bag/inner"
	bintersect "matrixbase/pkg/sql/colexec/bag/intersect"
	bnatural "matrixbase/pkg/sql/colexec/bag/natural"
	bunion "matrixbase/pkg/sql/colexec/bag/union"
	"matrixbase/pkg/sql/colexec/dedup"
	"matrixbase/pkg/sql/colexec/exchange"
	"matrixbase/pkg/sql/colexec/group"
	"matrixbase/pkg/sql/colexec/limit"
	"matrixbase/pkg/sql/colexec/mergededup"
	"matrixbase/pkg/sql/colexec/mergegroup"
	"matrixbase/pkg/sql/colexec/mergesum"
	"matrixbase/pkg/sql/colexec/mergetop"
	"matrixbase/pkg/sql/colexec/offset"
	"matrixbase/pkg/sql/colexec/output"
	"matrixbase/pkg/sql/colexec/projection"
	"matrixbase/pkg/sql/colexec/restrict"
	"matrixbase/pkg/sql/colexec/set/difference"
	"matrixbase/pkg/sql/colexec/set/differenceR"
	"matrixbase/pkg/sql/colexec/set/inner"
	"matrixbase/pkg/sql/colexec/set/intersect"
	"matrixbase/pkg/sql/colexec/set/natural"
	"matrixbase/pkg/sql/colexec/set/union"
	"matrixbase/pkg/sql/colexec/summarize"
	"matrixbase/pkg/sql/colexec/top"
	"matrixbase/pkg/sql/colexec/transfer"
	"matrixbase/pkg/vm/process"
)

var sFuncs = [...]func(interface{}, *bytes.Buffer){
	Top:               top.String,
	Dedup:             dedup.String,
	Limit:             limit.String,
	Group:             group.String,
	Order:             nil,
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
	Exchange:          exchange.String,
	MergeTop:          mergetop.String,
	MergeDedup:        mergededup.String,
	MergeGroup:        mergegroup.String,
	MergeSummarize:    mergesum.String,
}

var pFuncs = [...]func(*process.Process, interface{}) error{
	Top:               top.Prepare,
	Dedup:             dedup.Prepare,
	Limit:             limit.Prepare,
	Group:             group.Prepare,
	Order:             nil,
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
	Exchange:          exchange.Prepare,
	MergeTop:          mergetop.Prepare,
	MergeDedup:        mergededup.Prepare,
	MergeGroup:        mergegroup.Prepare,
	MergeSummarize:    mergesum.Prepare,
}

var rFuncs = [...]func(*process.Process, interface{}) (bool, error){
	Top:               top.Call,
	Dedup:             dedup.Call,
	Limit:             limit.Call,
	Group:             group.Call,
	Order:             nil,
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
	Exchange:          exchange.Call,
	MergeTop:          mergetop.Call,
	MergeDedup:        mergededup.Call,
	MergeGroup:        mergegroup.Call,
	MergeSummarize:    mergesum.Call,
}
