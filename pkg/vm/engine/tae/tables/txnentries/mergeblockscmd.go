package txnentries

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type mergeBlocksCmd struct {
	from []*common.ID
	to   []*common.ID
}

func newMergeBlocksCmd(from, to []*common.ID) *mergeBlocksCmd {
	return &mergeBlocksCmd{
		from: from,
		to:   to,
	}
}
func (cmd *mergeBlocksCmd) GetType() int16 { return CmdMergeBlocks }
func (cmd *mergeBlocksCmd) WriteTo(w io.Writer) (err error) {
	if err = binary.Write(w, binary.BigEndian, CmdMergeBlocks); err != nil {
		return
	}
	fromLength := uint32(len(cmd.from))
	if err = binary.Write(w, binary.BigEndian, fromLength); err != nil {
		return
	}
	for _, from := range cmd.from {
		if err = binary.Write(w, binary.BigEndian, from.TableID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, from.SegmentID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, from.BlockID); err != nil {
			return
		}
	}

	toLength := uint32(len(cmd.to))
	if err = binary.Write(w, binary.BigEndian, toLength); err != nil {
		return
	}
	for _, to := range cmd.to {
		if err = binary.Write(w, binary.BigEndian, to.TableID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, to.SegmentID); err != nil {
			return
		}
		if err = binary.Write(w, binary.BigEndian, to.BlockID); err != nil {
			return
		}
	}
	return
}
func (cmd *mergeBlocksCmd) ReadFrom(r io.Reader) (err error) {
	fromLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &fromLength); err != nil {
		return
	}
	cmd.from = make([]*common.ID, fromLength)
	for i := 0; i < int(fromLength); i++ {
		id := &common.ID{}
		if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &id.SegmentID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &id.BlockID); err != nil {
			return
		}
		cmd.from[i] = id
	}
	toLength := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &toLength); err != nil {
		return
	}
	cmd.to = make([]*common.ID, toLength)
	for i := 0; i < int(toLength); i++ {
		id := &common.ID{}
		if err = binary.Read(r, binary.BigEndian, &id.TableID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &id.SegmentID); err != nil {
			return
		}
		if err = binary.Read(r, binary.BigEndian, &id.BlockID); err != nil {
			return
		}
		cmd.to[i] = id
	}
	return
}
func (cmd *mergeBlocksCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *mergeBlocksCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	err = cmd.ReadFrom(bbuf)
	return
}
func (cmd *mergeBlocksCmd) String() string { return "" }
