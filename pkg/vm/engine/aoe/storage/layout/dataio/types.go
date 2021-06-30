package dataio

type fileStat struct {
	size int64
	name string
}

func (info *fileStat) Size() int64 {
	return info.size
}

func (info *fileStat) Name() string {
	return info.name
}
