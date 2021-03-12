package types

import (
	"bytes"
)

func (a *Bytes) Reset() {
	a.Offsets = a.Offsets[:0]
	a.Lengths = a.Lengths[:0]
	a.Data = a.Data[:0]
}

func (a *Bytes) Set(aidx int64, b *Bytes, bidx int64) int {
	return -1
}

func (a *Bytes) Window(start, end int) *Bytes {
	return &Bytes{
		Data:    a.Data,
		Offsets: a.Offsets[start:end],
		Lengths: a.Lengths[start:end],
	}
}

func (a *Bytes) Append(vs [][]byte) error {
	o := uint32(len(a.Data))
	for _, v := range vs {
		a.Offsets = append(a.Offsets, o)
		a.Data = append(a.Data, v...)
		o += uint32(len(v))
		a.Lengths = append(a.Lengths, uint32(len(v)))
	}
	return nil
}

func (a *Bytes) Get(n int) []byte {
	offset := a.Offsets[n]
	return a.Data[offset : offset+a.Lengths[n]]
}

func (a *Bytes) Swap(i, j int) {
	a.Offsets[i], a.Offsets[j] = a.Offsets[j], a.Offsets[i]
	a.Lengths[i], a.Lengths[j] = a.Lengths[j], a.Lengths[i]
}

func (a *Bytes) String() string {
	var buf bytes.Buffer

	buf.WriteByte('[')
	j := len(a.Offsets) - 1
	for i, o := range a.Offsets {
		buf.Write(a.Data[o : o+a.Lengths[i]])
		if i != j {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	return buf.String()
}
