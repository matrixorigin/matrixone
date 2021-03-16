package join

const (
	Inner = iota
	Left
	Right
	Full
	Semi
	Anti
	ExceptAll
	IntersectAll
)

type JoinType uint8
