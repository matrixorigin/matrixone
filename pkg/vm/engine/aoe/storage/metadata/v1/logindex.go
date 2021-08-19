package metadata

import (
	"bytes"
	"fmt"
	"matrixone/pkg/encoding"
)

func (idx *LogIndex) String() string {
	return fmt.Sprintf("(%d,%d,%d,%d)", idx.ID, idx.Start, idx.Count, idx.Capacity)
}

func (idx *LogIndex) IsApplied() bool {
	return idx.Capacity == idx.Start+idx.Count
}

func (idx *LogIndex) Marshall() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(encoding.EncodeUint64(idx.ID))
	buf.Write(encoding.EncodeUint64(idx.Count))
	buf.Write(encoding.EncodeUint64(idx.Start))
	buf.Write(encoding.EncodeUint64(idx.Capacity))
	return buf.Bytes(), nil
}

func (idx *LogIndex) UnMarshall(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	idx.ID = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Count = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Start = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	idx.Capacity = encoding.DecodeUint64(buf[:8])
	buf = buf[8:]
	return nil
}
