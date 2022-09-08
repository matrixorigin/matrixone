package objectio

type Extent struct {
	id         uint64
	offset     uint32
	length     uint32
	originSize uint32
}

func (ex *Extent) Id() uint64 { return ex.id }

func (ex *Extent) End() uint32 { return ex.offset + ex.length }

func (ex *Extent) Offset() uint32 { return ex.offset }

func (ex *Extent) Length() uint32 { return ex.length }

func (ex *Extent) OriginSize() uint32 { return ex.originSize }
