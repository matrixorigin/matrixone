package overload

var LogicalOps = map[int]uint8{
	Or:      0,
	And:     0,
	Like:    0,
	NotLike: 0,
	EQ:      0,
	LT:      0,
	LE:      0,
	GT:      0,
	GE:      0,
	NE:      0,
}

var NegOps = map[int]int{
	Or:   And,
	And:  Or,
	EQ:   NE,
	LT:   GE,
	LE:   GT,
	GT:   LE,
	GE:   LT,
	Like: NotLike,
}

var OpTypes = map[int]int{
	UnaryMinus: Unary,
	Or:         Binary,
	And:        Binary,
	Plus:       Binary,
	Minus:      Binary,
	Mult:       Binary,
	Div:        Binary,
	Mod:        Binary,
	Like:       Binary,
	NotLike:    Binary,
	Typecast:   Binary,
	EQ:         Binary,
	LT:         Binary,
	LE:         Binary,
	GT:         Binary,
	GE:         Binary,
	NE:         Binary,
}

func IsLogical(op int) bool {
	if _, ok := LogicalOps[op]; ok {
		return true
	}
	return false
}

func OperatorType(op int) int {
	if typ, ok := OpTypes[op]; ok {
		return typ
	}
	return -1
}
