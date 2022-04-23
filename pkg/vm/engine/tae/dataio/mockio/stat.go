package mockio

type fileStat struct {
	size int64
}

func (stat *fileStat) Name() string      { return "" }
func (stat *fileStat) Size() int64       { return stat.size }
func (stat *fileStat) OriginSize() int64 { return stat.size }
func (stat *fileStat) CompressAlgo() int { return 0 }
