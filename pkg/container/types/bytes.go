package types

import (
	"bytes"
)

func (a *Bytes) Reset() {
	a.Os = a.Os[:0]
	a.Ns = a.Ns[:0]
	a.Data = a.Data[:0]
}

func (a *Bytes) Set(aidx int64, b *Bytes, bidx int64) int {
	return -1
}

func (a *Bytes) Window(start, end int) *Bytes {
	return &Bytes{
		Data: a.Data,
		Os:   a.Os[start:end],
		Ns:   a.Ns[start:end],
	}
}

func (a *Bytes) Append(vs [][]byte) error {
	o := uint32(len(a.Data))
	for _, v := range vs {
		a.Os = append(a.Os, o)
		a.Data = append(a.Data, v...)
		o += uint32(len(v))
		a.Ns = append(a.Ns, uint32(len(v)))
	}
	return nil
}

func (a *Bytes) String() string {
	var buf bytes.Buffer

	buf.WriteByte('[')
	j := len(a.Os) - 1
	for i, o := range a.Os {
		buf.Write(a.Data[o : o+a.Ns[i]])
		if i != j {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	return buf.String()
}
