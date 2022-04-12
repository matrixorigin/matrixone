package common

import (
	"encoding/binary"
	"io"
)

func WriteString(str string, w io.Writer) (n int64, err error) {
	buf := []byte(str)
	if err = binary.Write(w, binary.BigEndian, uint16(len(buf))); err != nil {
		return
	}
	wn, err := w.Write(buf)
	return int64(wn + 2), err
}

func ReadString(r io.Reader) (str string, err error) {
	strLen := uint16(0)
	if err = binary.Read(r, binary.BigEndian, &strLen); err != nil {
		return
	}
	buf := make([]byte, strLen)
	if _, err = r.Read(buf); err != nil {
		return
	}
	str = string(buf)
	return
}
