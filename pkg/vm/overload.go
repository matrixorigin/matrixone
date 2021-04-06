package vm

import (
	"bytes"
	binner "matrixbase/pkg/sql/colexec/bag/inner"
	bnatural "matrixbase/pkg/sql/colexec/bag/natural"
	bunion "matrixbase/pkg/sql/colexec/bag/union"
	"matrixbase/pkg/sql/colexec/dedup"
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
	"matrixbase/pkg/sql/colexec/set/inner"
	"matrixbase/pkg/sql/colexec/set/intersect"
	"matrixbase/pkg/sql/colexec/set/natural"
	"matrixbase/pkg/sql/colexec/shuffle"
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
	SetUnion:          nil,
	SetIntersect:      intersect.String,
	SetDifference:     nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.String,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.String,
	SetSemiDifference: nil,
	BagUnion:          bunion.String,
	BagInnerJoin:      binner.String,
	BagNaturalJoin:    bnatural.String,
	Output:            output.String,
	Shuffle:           shuffle.String,
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
	SetUnion:          nil,
	SetIntersect:      intersect.Prepare,
	SetDifference:     nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.Prepare,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.Prepare,
	SetSemiDifference: nil,
	BagUnion:          bunion.Prepare,
	BagInnerJoin:      binner.Prepare,
	BagNaturalJoin:    bnatural.Prepare,
	Output:            output.Prepare,
	Shuffle:           shuffle.Prepare,
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
	SetUnion:          nil,
	SetIntersect:      intersect.Call,
	SetDifference:     nil,
	SetFullJoin:       nil,
	SetLeftJoin:       nil,
	SetSemiJoin:       nil,
	SetInnerJoin:      inner.Call,
	SetRightJoin:      nil,
	SetNaturalJoin:    natural.Call,
	SetSemiDifference: nil,
	BagUnion:          bunion.Call,
	BagInnerJoin:      binner.Call,
	BagNaturalJoin:    bnatural.Call,
	Output:            output.Call,
	Shuffle:           shuffle.Call,
	MergeTop:          mergetop.Call,
	MergeDedup:        mergededup.Call,
	MergeGroup:        mergegroup.Call,
	MergeSummarize:    mergesum.Call,
}
