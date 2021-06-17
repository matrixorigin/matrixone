package base

import (
	"fmt"
	"io"
)

type IVirtaulFile interface {
	io.Reader
	io.Writer
	Ref()
	Unref()
}

type Pointer struct {
	Offset int64
	Len    uint64
}

type IndexesMeta struct {
	Data []*IndexMeta
}

func (m *IndexesMeta) String() string {
	s := fmt.Sprintf("<IndexesMeta>[Cnt=%d]", len(m.Data))
	for _, meta := range m.Data {
		s = fmt.Sprintf("%s\n\t%s", s, meta.String())
	}
	return s
}

type IndexMeta struct {
	Type IndexType
	Ptr  *Pointer
}

func NewIndexesMeta() *IndexesMeta {
	return &IndexesMeta{
		Data: make([]*IndexMeta, 0),
	}
}

func (m *IndexMeta) String() string {
	s := fmt.Sprintf("<IndexMeta>[Ty=%d](Off: %d, Len:%d)", m.Type, m.Ptr.Offset, m.Ptr.Len)
	return s
}
