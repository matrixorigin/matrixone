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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
	CmdTxn
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
	txnif.RegisterCmdFactory(CmdTxn, func(int16) txnif.TxnCmd {
		return NewEmptyTxnCmd()
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

func (base *BaseCmd) Close() {}

type PointerCmd struct {
	BaseCmd
	Group uint32
	Lsn   uint64
}

type DeleteBitmapCmd struct {
	BaseCmd
	Bitmap *roaring.Bitmap
}

type TxnCmd struct {
	*ComposedCmd
	*TxnCtx
	Txn txnif.AsyncTxn
}

type BatchCmd struct {
	BaseCmd
	Bat *containers.Batch
}

type ComposedCmd struct {
	BaseCmd
	Cmds    []txnif.TxnCmd
	CmdSize uint32
}

type BaseCustomizedCmd struct {
	BaseCmd
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

func NewBatchCmd(bat *containers.Batch) *BatchCmd {
	return &BatchCmd{
		Bat: bat,
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

func NewTxnCmd() *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(),
	}
}
func NewEmptyTxnCmd() *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(),
		TxnCtx:      &TxnCtx{},
	}
}
func (c *TxnCmd) SetTxn(txn txnif.AsyncTxn) {
	c.Txn = txn
}
func (c *TxnCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
		return
	}
	n += 2
	var sn int64
	sn, err = c.ComposedCmd.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	if sn, err = common.WriteString(c.Txn.GetID(), w); err != nil {
		return
	}
	n += sn
	is2PC := uint8(0)
	if c.Txn.Is2PC() {
		is2PC = 1
	}
	if err = binary.Write(w, binary.BigEndian, is2PC); err != nil {
		return
	}
	n += 1
	return
}
func (c *TxnCmd) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	var cmd txnif.TxnCmd
	cmd, sn, err = BuildCommandFrom(r)
	if err != nil {
		return
	}
	c.ComposedCmd = cmd.(*ComposedCmd)
	n += sn
	if c.ID, sn, err = common.ReadString(r); err != nil {
		return
	}
	n += sn
	is2PC := uint8(0)
	if err = binary.Read(r, binary.BigEndian, &is2PC); err != nil {
		return
	}
	n += 1
	if is2PC == 1 {
		c.Kind2PC = true
	}
	return
}
func (c *TxnCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (c *TxnCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = c.ReadFrom(bbuf)
	return err
}
func (c *TxnCmd) GetType() int16 { return CmdTxn }
func (c *TxnCmd) Desc() string {
	return fmt.Sprintf("Tid=%s,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.Desc())
}
func (c *TxnCmd) String() string {
	return fmt.Sprintf("Tid=%s,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.String())
}
func (c *TxnCmd) VerboseString() string {
	return fmt.Sprintf("Tid=%s,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.VerboseString())
}
func (c *TxnCmd) Close() {
	c.ComposedCmd.Close()
}
func (e *PointerCmd) GetType() int16 {
	return CmdPointer
}
func (e *PointerCmd) Desc() string {
	s := fmt.Sprintf("CmdName=Ptr;Group=%d;Lsn=%d", e.Group, e.Lsn)
	return s
}
func (e *PointerCmd) String() string {
	s := fmt.Sprintf("CmdName=Ptr;Group=%d;Lsn=%d]", e.Group, e.Lsn)
	return s
}

func (e *PointerCmd) VerboseString() string {
	s := fmt.Sprintf("CmdName=Ptr;Group=%d;Lsn=%d]", e.Group, e.Lsn)
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

func (e *DeleteBitmapCmd) Desc() string {
	s := fmt.Sprintf("CmdName=DEL;Cardinality=%d", e.Bitmap.GetCardinality())
	return s
}

func (e *DeleteBitmapCmd) String() string {
	s := fmt.Sprintf("CmdName=DEL;Cardinality=%d", e.Bitmap.GetCardinality())
	return s
}

func (e *DeleteBitmapCmd) VerboseString() string {
	s := fmt.Sprintf("CmdName=DEL;Cardinality=%d;Deletes=%v", e.Bitmap.GetCardinality(), e.Bitmap.String())
	return s
}
func (e *BatchCmd) GetType() int16 {
	return CmdBatch
}

func (e *BatchCmd) Close() {
	if e.Bat != nil {
		e.Bat.Close()
		e.Bat = nil
	}
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
	e.Bat = containers.NewBatch()
	n, err = e.Bat.ReadFrom(r)
	return
}

func (e *BatchCmd) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
		return
	}
	if n, err = e.Bat.WriteTo(w); err != nil {
		return
	}
	n += 2
	return
}

func (e *BatchCmd) Desc() string {
	s := fmt.Sprintf("CmdName=BAT;Rows=%d", e.Bat.Length())
	if e.Bat.HasDelete() {
		s = fmt.Sprintf("%s;DelCnt=%d", s, e.Bat.DeleteCnt())
	}
	return s
}

func (e *BatchCmd) String() string {
	return e.Desc()
}

func (e *BatchCmd) VerboseString() string {
	s := fmt.Sprintf("CmdName=BAT;Rows=%d;Data=%v", e.Bat.Length(), e.Bat)
	return s
}

func (cc *ComposedCmd) Close() {
	for _, cmd := range cc.Cmds {
		cmd.Close()
	}
}
func (cc *ComposedCmd) GetType() int16 {
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
	if err = binary.Write(w, binary.BigEndian, cc.CmdSize); err != nil {
		return
	}
	n += 4
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
	if err = binary.Read(r, binary.BigEndian, &cc.CmdSize); err != nil {
		return
	}
	n += 4
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

func (cc *ComposedCmd) SetCmdSize(size uint32) {
	cc.CmdSize = size
}

func (cc *ComposedCmd) ToString(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d/%d", prefix, cc.CmdSize, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.String())
	}
	return s
}

func (cc *ComposedCmd) ToDesc(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d/%d", prefix, cc.CmdSize, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.Desc())
	}
	return s
}
func (cc *ComposedCmd) ToVerboseString(prefix string) string {
	s := fmt.Sprintf("%sComposedCmd: Cnt=%d/%d", prefix, cc.CmdSize, len(cc.Cmds))
	for _, cmd := range cc.Cmds {
		s = fmt.Sprintf("%s\n%s\t%s", s, prefix, cmd.VerboseString())
	}
	return s
}

func (cc *ComposedCmd) VerboseString() string {
	return cc.ToVerboseString("")
}
func (cc *ComposedCmd) String() string {
	return cc.ToString("")
}
func (cc *ComposedCmd) Desc() string {
	return cc.ToDesc("")
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
