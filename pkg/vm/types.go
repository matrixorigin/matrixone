package vm

const (
	Nub = iota
	Top
	Limit
	Merge
	Group
	Order
	Transfer
	Restrict
	Summarize
	Projection
	ExtendProjection
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
)

type Instruction struct {
	Op int
}

type Instructions []Instruction
