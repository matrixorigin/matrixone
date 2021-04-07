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
	SetFullJoin
	SetLeftJoin
	SetSemiJoin
	SetInnerJoin
	SetRightJoin
	SetNaturalJoin
	SetSemiDifference // unsuitable name is anti join
	BagUnion
	BagIntersect
	BagInnerJoin
	BagNaturalJoin
	Output
	Exchange
	MergeTop
	MergeDedup
	MergeGroup
	MergeSummarize
)

type Instruction struct {
	Op  int
	Arg interface{}
}

type Instructions []Instruction
