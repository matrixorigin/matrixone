package vm

const (
	Nub = iota
	Top
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
	MultisetUnion
	MultisetIntersect
	MultisetDifference
	EqJoin
	SemiJoin
	InnerJoin
	NaturalJoin
	Output
	MergeSummarize
)

type Instruction struct {
	Op  int
	Arg interface{}
}

type Instructions []Instruction
