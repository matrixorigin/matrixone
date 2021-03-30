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

type BagGroup struct {
	Idx   int64
	Sel   int64
	Idata []byte
	Sdata []byte
	Is    []int64
	Sels  []int64
}

type DedupGroup struct {
	Sel int64
}
