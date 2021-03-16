package hash

type Join struct {
	Idx   int64
	Sel   int64
	Data  []byte
	Idata []byte
	Is    []int64
	Sels  []int64
}

type Group struct {
	Sel  int64
	Data []byte
	Sels []int64
}
