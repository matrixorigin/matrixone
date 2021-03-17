package hash

type Group struct {
	Sel  int64
	Data []byte
	Sels []int64
}

type JoinGroup struct {
	Idx   int64
	Sel   int64
	Data  []byte
	Idata []byte
	Is    []int64
	Sels  []int64
}
