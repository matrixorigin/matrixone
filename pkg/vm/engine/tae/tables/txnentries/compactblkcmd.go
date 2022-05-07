package txnentries

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type compactBlockCmd struct {
	from *common.ID
	to   *common.ID
}

func newCompactBlockCmd(from, to *common.ID) *compactBlockCmd {
	return &compactBlockCmd{
		from: from,
		to:   to,
	}
}
func (cmd *compactBlockCmd) GetType() int16 { return CmdCompactBlock }
func (cmd *compactBlockCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, CmdCompactBlock); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.from.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.from.SegmentID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.from.BlockID); err != nil {
		return
	}

	if err = binary.Write(w, binary.BigEndian, cmd.to.TableID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.to.SegmentID); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, cmd.to.BlockID); err != nil {
		return
	}
	return
}
func (cmd *compactBlockCmd) ReadFrom(r io.Reader) (err error) {
	cmd.from = &common.ID{}
	if err = binary.Read(r, binary.BigEndian, &cmd.from.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &cmd.from.SegmentID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &cmd.from.BlockID); err != nil {
		return
	}
	cmd.to = &common.ID{}
	if err = binary.Read(r, binary.BigEndian, &cmd.to.TableID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &cmd.to.SegmentID); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &cmd.to.BlockID); err != nil {
		return
	}
	return
}
func (cmd *compactBlockCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *compactBlockCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cmd.ReadFrom(bbuf)
	return
}
func (cmd *compactBlockCmd) String() string { return "" }
