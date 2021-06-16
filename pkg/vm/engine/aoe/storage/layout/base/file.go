package base

type Pointer struct {
	Offset int64
	Len    uint64
}

type IndexesMeta struct {
	Map map[string]*IndexMeta
}

type IndexMeta struct {
	Type IndexType
	Ptr  *Pointer
}

func NewIndexesMeta() *IndexesMeta {
	return &IndexesMeta{
		Map: make(map[string]*IndexMeta),
	}
}
