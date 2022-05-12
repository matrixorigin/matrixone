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

package txnbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	CmdPointer int16 = iota
	CmdDeleteBitmap
	CmdBatch
	CmdAppend
	CmdUpdate
	CmdDelete
	CmdComposed
	CmdCustomized
)

func init() {
	txnif.RegisterCmdFactory(CmdPointer, func(int16) txnif.TxnCmd {
		return new(PointerCmd)
	})
	txnif.RegisterCmdFactory(CmdDeleteBitmap, func(int16) txnif.TxnCmd {
		return new(DeleteBitmapCmd)
	})
	txnif.RegisterCmdFactory(CmdBatch, func(int16) txnif.TxnCmd {
		return new(BatchCmd)
	})
	txnif.RegisterCmdFactory(CmdComposed, func(int16) txnif.TxnCmd {
		return new(ComposedCmd)
	})
}

type CustomizedCmd interface {
	GetID() uint32
}

func IsCustomizedCmd(cmd txnif.TxnCmd) bool {
	ctype := cmd.GetType()
	return ctype >= CmdCustomized
}

type BaseCmd struct{}
type PointerCmd struct {
	BaseCmd
	Group uint32
	Lsn   uint64
}

type DeleteBitmapCmd struct {
	BaseCmd
	Bitmap *roaring.Bitmap
}

type BatchCmd struct {
	BaseCmd
	Bat   batch.IBatch
	Types []types.Type
}

type ComposedCmd struct {
	BaseCmd
	Cmds []txnif.TxnCmd
}

type BaseCustomizedCmd struct {
	ID   uint32
	Impl txnif.TxnCmd
}

func NewBaseCustomizedCmd(id uint32, impl txnif.TxnCmd) *BaseCustomizedCmd {
	return &BaseCustomizedCmd{
		ID:   id,
		Impl: impl,
	}
}

func NewDeleteBitmapCmd(bitmap *roaring.Bitmap) *DeleteBitmapCmd {
	return &DeleteBitmapCmd{
		Bitmap: bitmap,
	}
}

func NewBatchCmd(bat batch.IBatch, colTypes []types.Type) *BatchCmd {
	return &BatchCmd{
		Bat:   bat,
		Types: colTypes,
	}
}

func NewComposedCmd() *ComposedCmd {
	return &ComposedCmd{
		Cmds: make([]txnif.TxnCmd, 0),
	}
}

func (c *BaseCustomizedCmd) GetID() uint32 {
	return c.ID
}

func (e *PointerCmd) GetType() int16 {
	return CmdPointer
}

func (e *PointerCmd) String() string {
	s := fmt.Sprintf("PointerCmd: Group=%d, Lsn=%d", e.Group, e.Lsn)
	return s
}

func (e *PointerCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.Group); err != nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.Lsn); err != nil {
		return
	}
	n = 14
	return
}

func (e *PointerCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *PointerCmd) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &e.Group); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &e.Lsn); err != nil {
		return
	}
	n = 12
	return
}

func (e *PointerCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := e.ReadFrom(bbuf)
	return err
}

func (e *DeleteBitmapCmd) GetType() int16 {
	return CmdDeleteBitmap
}

func (e *DeleteBitmapCmd) ReadFrom(r io.Reader) (n int64, err error) {
	e.Bitmap = roaring.NewBitmap()
	n, err = e.Bitmap.ReadFrom(r)
	return
}

func (e *DeleteBitmapCmd) WriteTo(w io.Writer) (n int64, err error) {
	if e == nil {
		return
	}
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	n, err = e.Bitmap.WriteTo(w)
	n += 2
	return
}

func (e *DeleteBitmapCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *DeleteBitmapCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := e.ReadFrom(bbuf)
	return err
}

func (e *DeleteBitmapCmd) String() string {
	s := fmt.Sprintf("DeleteBitmapCmd: Cardinality=%d", e.Bitmap.GetCardinality())
	return s
}

func (e *BatchCmd) GetType() int16 {
	return CmdBatch
}

func (e *BatchCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = e.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (e *BatchCmd) Unmarshal(buf []byte) error {
	bbuf := bytes.NewBuffer(buf)
	_, err := e.ReadFrom(bbuf)
	return err
}

func (e *BatchCmd) ReadFrom(r io.Reader) (n int64, err error) {
	e.Types, e.Bat, n, err = UnmarshalBatchFrom(r)
	return
}

func (e *BatchCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	colsBuf, err := MarshalBatch(e.Types, e.Bat)
	if err != nil {
		return
	}
	in, err := w.Write(colsBuf)
	n = int64(in) + 2
	return
}

func (e *BatchCmd) String() string {
	s := fmt.Sprintf("BatchCmd: Rows=%d", e.Bat.Length())
	return s
}

func (e *ComposedCmd) GetType() int16 {
	return CmdComposed
}

func (cc *ComposedCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = cc.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}

func (cc *ComposedCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = cc.ReadFrom(bbuf)
	return err
}

func (cc *ComposedCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, cc.GetType()); err != nil {
		return
	}
	n += 2
	cmds := uint32(len(cc.Cmds))
	if err = binary.Write(w, binary.BigEndian, cmds); err != nil {
		return
	}
	n += 4
	var cn int64
	for _, cmd := range cc.Cmds {
		if cn, err = cmd.WriteTo(w); err != nil {
			break
		} else {
			n += cn
		}
	}
	return
}

func (cc *ComposedCmd) ReadFrom(r io.Reader) (n int64, err error) {
	cmds := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &cmds); err != nil {
		return
	}
	n += 4
	var cn int64
	cc.Cmds = make([]txnif.TxnCmd, cmds)
	for i := 0; i < int(cmds); i++ {
		if cc.Cmds[i], cn, err = BuildCommandFrom(r); err != nil {
			break
		} else {
			n += cn
		}
	}
	return
}

func (cc *ComposedCmd) AddCmd(cmd txnif.TxnCmd) {
	cc.Cmds = append(cc.Cmds, cmd)
}

func (cc *ComposedCmd) ToString(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d", prefix, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.String())
	}
	return s
}

func (cc *ComposedCmd) String() string {
	return cc.ToString("")
}

func BuildCommandFrom(r io.Reader) (cmd txnif.TxnCmd, n int64, err error) {
	var cmdType int16
	if err = binary.Read(r, binary.BigEndian, &cmdType); err != nil {
		return
	}

	factory := txnif.GetCmdFactory(cmdType)

	cmd = factory(cmdType)
	n, err = cmd.ReadFrom(r)
	n += 2
	return
}
