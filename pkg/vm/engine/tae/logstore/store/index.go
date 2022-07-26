package store
import (
	
	"encoding/binary"
	"fmt"
	"io"
)

type Index struct {
	LSN  uint64
	CSN  uint32
	Size uint32
}

func NewIndex(lsn uint64, csn, size uint32) *Index {
	return &Index{
		LSN:  lsn,
		CSN:  csn,
		Size: size,
	}
}

func (index *Index) Compare(o *Index) int {
	if index.LSN > o.LSN {
		return 1
	} else if index.LSN < o.LSN {
		return -1
	}
	if index.CSN > o.CSN {
		return 1
	} else if index.CSN < o.CSN {
		return -1
	}
	return 0
}

func (index *Index) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, index.LSN); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, index.CSN); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, index.Size); err != nil {
		return
	}
	n = 16
	return
}

func (index *Index) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &index.LSN); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &index.CSN); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &index.Size); err != nil {
		return
	}
	n = 16
	return
}

func (index *Index) Clone() *Index {
	if index == nil {
		return nil
	}
	return &Index{
		LSN:  index.LSN,
		CSN:  index.CSN,
		Size: index.Size,
	}
}
func (index *Index) String() string {
	if index == nil {
		return "<nil index>"
	}
	return fmt.Sprintf("<Index[%d:%d/%d]>", index.LSN, index.CSN, index.Size)
}