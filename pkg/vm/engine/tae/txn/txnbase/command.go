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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

const (
	CmdPointer int16 = iota
	CmdDeleteBitmap
	CmdBatch
	CmdAppend
	CmdDelete
	CmdComposed
	CmdTxn
	CmdTxnState
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
	txnif.RegisterCmdFactory(CmdTxnState, func(int16) txnif.TxnCmd {
		return NewEmptyTxnStateCmd()
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

type TxnStateCmd struct {
	ID       string
	State    txnif.TxnState
	CommitTs types.TS
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

func NewEmptyTxnStateCmd() *TxnStateCmd {
	return &TxnStateCmd{}
}

func NewTxnStateCmd(id string, state txnif.TxnState, cts types.TS) *TxnStateCmd {
	return &TxnStateCmd{
		ID:       id,
		State:    state,
		CommitTs: cts,
	}
}

func NewTxnCmd() *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(),
		TxnCtx:      &TxnCtx{},
	}
}

func NewEmptyTxnCmd() *TxnCmd {
	return &TxnCmd{
		ComposedCmd: NewComposedCmd(),
		TxnCtx:      NewEmptyTxnCtx(),
	}
}
func (c *TxnStateCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := c.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	// if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
	// 	return
	// }
	n += 2
	var sn int64
	if sn, err = objectio.WriteString(c.ID, w); err != nil {
		return
	}
	n += sn
	state := int32(c.State)
	if _, err = w.Write(types.EncodeInt32(&state)); err != nil {
		return
	}
	n += 4
	if _, err = w.Write(c.CommitTs[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	return
}
func (c *TxnStateCmd) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	if c.ID, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	state := int32(0)
	if _, err = r.Read(types.EncodeInt32(&state)); err != nil {
		return
	}
	c.State = txnif.TxnState(state)
	n += 4
	if _, err = r.Read(c.CommitTs[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	return
}
func (c *TxnStateCmd) Marshal() (buf []byte, err error) {
	var bbuf bytes.Buffer
	if _, err = c.WriteTo(&bbuf); err != nil {
		return
	}
	buf = bbuf.Bytes()
	return
}
func (c *TxnStateCmd) ApplyCommit()                  {}
func (c *TxnStateCmd) ApplyRollback()                {}
func (c *TxnStateCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
func (c *TxnStateCmd) Unmarshal(buf []byte) (err error) {
	bbuf := bytes.NewBuffer(buf)
	_, err = c.ReadFrom(bbuf)
	return err
}
func (c *TxnStateCmd) GetType() int16 { return CmdTxnState }
func (c *TxnStateCmd) Desc() string {
	return fmt.Sprintf("Tid=%s,State=%s,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) String() string {
	return fmt.Sprintf("Tid=%s,State=%v,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) VerboseString() string {
	return fmt.Sprintf("Tid=%s,State=%v,Cts=%s", c.ID, txnif.TxnStrState(c.State), c.CommitTs.ToString())
}
func (c *TxnStateCmd) Close() {
}
func (c *TxnCmd) ApplyCommit() {
	c.ComposedCmd.ApplyCommit()
}
func (c *TxnCmd) ApplyRollback() {
	c.ComposedCmd.ApplyRollback()
}
func (c *TxnCmd) SetReplayTxn(txn txnif.AsyncTxn) {
	c.ComposedCmd.SetReplayTxn(txn)
}
func (c *TxnCmd) SetTxn(txn txnif.AsyncTxn) {
	c.Txn = txn
	c.ID = txn.GetID()
	c.StartTS = txn.GetStartTS()
	c.PrepareTS = txn.GetPrepareTS()
	c.Participants = txn.GetParticipants()
	c.Memo = txn.GetMemo()
}
func (c *TxnCmd) WriteTo(w io.Writer) (n int64, err error) {
	t := c.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	// if err = binary.Write(w, binary.BigEndian, c.GetType()); err != nil {
	// 	return
	// }
	n += 2
	var sn int64
	sn, err = c.ComposedCmd.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	if sn, err = objectio.WriteString(c.ID, w); err != nil {
		return
	}
	n += sn
	//start ts
	if _, err = w.Write(c.StartTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	//prepare ts
	if _, err = w.Write(c.PrepareTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	//participants
	length := uint32(len(c.Participants))
	if _, err = w.Write(types.EncodeUint32(&length)); err != nil {
		return
	}
	n += 4
	for _, p := range c.Participants {
		if _, err = w.Write(types.EncodeUint64(&p)); err != nil {
			return
		}
		n += 8
	}
	if sn, err = c.Memo.WriteTo(w); err != nil {
		return
	}
	n += sn
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
	if c.ID, sn, err = objectio.ReadString(r); err != nil {
		return
	}
	n += sn
	// start timestamp
	if _, err = r.Read(c.StartTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	// prepare timestamp
	if _, err = r.Read(c.PrepareTS[:]); err != nil {
		return
	}
	n += types.TxnTsSize
	// participants
	num := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&num)); err != nil {
		return
	}
	n += 4
	c.Participants = make([]uint64, num)
	for i := 0; i < int(num); i++ {
		id := uint64(0)
		if _, err = r.Read(types.EncodeUint64(&id)); err != nil {
			break
		} else {
			c.Participants = append(c.Participants, id)
			n += 8
		}
	}
	if sn, err = c.Memo.ReadFrom(r); err != nil {
		return
	}
	n += sn
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
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.Desc())
}
func (c *TxnCmd) String() string {
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.String())
}
func (c *TxnCmd) VerboseString() string {
	return fmt.Sprintf("Tid=%X,Is2PC=%v,%s", c.ID, c.Is2PC(), c.ComposedCmd.VerboseString())
}
func (c *TxnCmd) Close() {
	c.ComposedCmd.Close()
}
func (e *PointerCmd) ApplyCommit()                  {}
func (e *PointerCmd) ApplyRollback()                {}
func (e *PointerCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
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
	t := e.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint32(&e.Group)); err != nil {
		return
	}
	if _, err = w.Write(types.EncodeUint64(&e.Lsn)); err != nil {
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
	if _, err = r.Read(types.EncodeUint32(&e.Group)); err != nil {
		return
	}
	if _, err = r.Read(types.EncodeUint64(&e.Lsn)); err != nil {
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
func (e *DeleteBitmapCmd) ApplyCommit()                  {}
func (e *DeleteBitmapCmd) ApplyRollback()                {}
func (e *DeleteBitmapCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
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
	t := e.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	// if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
	// 	return
	// }
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
func (e *BatchCmd) ApplyCommit()                  {}
func (e *BatchCmd) ApplyRollback()                {}
func (e *BatchCmd) SetReplayTxn(_ txnif.AsyncTxn) {}
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
	t := e.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	// if err = binary.Write(w, binary.BigEndian, e.GetType()); err != nil {
	// 	return
	// }
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
func (cc *ComposedCmd) ApplyCommit() {
	for _, c := range cc.Cmds {
		c.ApplyCommit()
	}
}
func (cc *ComposedCmd) ApplyRollback() {
	for _, c := range cc.Cmds {
		c.ApplyRollback()
	}
}
func (cc *ComposedCmd) SetReplayTxn(txn txnif.AsyncTxn) {
	for _, c := range cc.Cmds {
		c.SetReplayTxn(txn)
	}
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
	t := cc.GetType()
	if _, err = w.Write(types.EncodeInt16(&t)); err != nil {
		return
	}
	// if err = binary.Write(w, binary.BigEndian, cc.GetType()); err != nil {
	// 	return
	// }
	n += 2
	if _, err = w.Write(types.EncodeUint32(&cc.CmdSize)); err != nil {
		return
	}
	n += 4
	cmds := uint32(len(cc.Cmds))
	if _, err = w.Write(types.EncodeUint32(&cmds)); err != nil {
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
	if _, err = r.Read(types.EncodeUint32(&cc.CmdSize)); err != nil {
		return
	}
	n += 4
	cmds := uint32(0)
	if _, err = r.Read(types.EncodeUint32(&cmds)); err != nil {
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
	if _, err = r.Read(types.EncodeInt16(&cmdType)); err != nil {
		return
	}

	factory := txnif.GetCmdFactory(cmdType)

	cmd = factory(cmdType)
	n, err = cmd.ReadFrom(r)
	n += 2
	return
}
