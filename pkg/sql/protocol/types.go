package protocol

const ( // extend type
	Attr = iota
	Unary
	Multi
	Paren
	Value
	Binary
)

const ( // table define type
	Index = iota
	Attribute
)

const ( // partition type
	List = iota
	Range
	ListWithSub
	RangeWithSub
)
