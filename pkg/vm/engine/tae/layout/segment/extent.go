package segment

type ExtentType uint8

const (
	APPEND ExtentType = iota
	UPDATE ExtentType = iota
)

type entry struct {
	offset uint32
	length uint32
}

type Extent struct {
	typ    ExtentType
	offset uint32
	length uint32
	data   []entry
}

func (ex *Extent) End() uint32 {
	return ex.offset + ex.length
}

func (ex *Extent) Offset() uint32 {
	return ex.offset
}

func (ex *Extent) Length() uint32 {
	return ex.length
}

func (ex *Extent) GetData() []entry {
	return ex.data
}

func (en *entry) GetOffset() uint32 {
	return en.offset
}

func (en *entry) GetLength() uint32 {
	return en.length
}
