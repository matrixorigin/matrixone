// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnentries

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
func (cmd *compactBlockCmd) WriteTo(w io.Writer) (n int64, err error) {
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
	n = 2 + 8 + 8 + 8 + 8 + 8 + 8
	return
}
func (cmd *compactBlockCmd) ReadFrom(r io.Reader) (n int64, err error) {
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
	n = 8 + 8 + 8 + 8 + 8 + 8
	return
}
func (cmd *compactBlockCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cmd.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (cmd *compactBlockCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cmd.ReadFrom(bbuf)
	return
}
func (cmd *compactBlockCmd) String() string { return "" }
