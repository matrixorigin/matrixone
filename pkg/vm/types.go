package vm

const (
	Top = iota
	Dedup
	Limit
	Group
	Order
	Offset
	Transfer
	Restrict
	Summarize
	Projection
	SetUnion
	SetIntersect
	SetDifference
	SetDifferenceR
	SetFullJoin
	SetLeftJoin
	SetSemiJoin
	SetInnerJoin
	SetRightJoin
	SetNaturalJoin
	SetSemiDifference // unsuitable name is anti join
	BagUnion
	BagIntersect
	BagDifference
	BagDifferenceR
	BagInnerJoin
	BagNaturalJoin
	Output
	Exchange
	Merge
	MergeTop
	MergeDedup
	MergeOrder
	MergeGroup
	MergeSummarize
)

type Instruction struct {
	Op  int
	Arg interface{}
}

type Instructions []Instruction
