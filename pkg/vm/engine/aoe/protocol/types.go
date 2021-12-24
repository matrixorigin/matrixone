package protocol

const ( // extend type
	Attr = iota
	Unary
	Multi
	Paren
	Value
	Binary
)


const ( // partition type
	List = iota
	Range
	ListWithSub
	RangeWithSub
)
