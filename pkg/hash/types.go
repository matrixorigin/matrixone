package hash

type Group struct {
	Sel  int64
	Data []byte
	Sels []int64
}

type SetGroup struct {
	Idx int64
	Sel int64
}
