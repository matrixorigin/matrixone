package tae

import (
	"encoding/binary"
	"fmt"
	io "io"
)

func (d *TableEntryDelta) CompactString() string {
	return fmt.Sprintf("d[n-%s,cstr-%d]", d.Name, len(d.Constraints))
}

func (d *TableEntryDelta) Clone() TableEntryDelta {
	return TableEntryDelta{
		Name:        d.Name,
		Constraints: d.Constraints,
	}
}

func (d *TableEntryDelta) MarshalToWriter(w io.Writer) (int, error) {
	if bytes, err := d.Marshal(); err != nil {
		return 0, err
	} else {
		if err = binary.Write(w, binary.BigEndian, uint32(len(bytes))); err != nil {
			return 0, err
		}
		n, err := w.Write(bytes)
		return n + 4, err
	}
}

func (d *TableEntryDelta) UnmarshalFromReader(r io.Reader) (int, error) {
	size := uint32(0)
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}
	buf := make([]byte, size)
	if x, err := r.Read(buf); err != nil {
		return x + 4, err
	}
	err := d.Unmarshal(buf)
	return int(size + 4), err
}
