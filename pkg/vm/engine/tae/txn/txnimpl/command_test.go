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

package txnimpl

import (
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestPointerCmd(t *testing.T) {
	groups := uint32(10)
	maxLsn := uint64(10)
	// cmds := make([]TxnCmd, int(groups)*int(maxLsn))
	// mashalled := make([][]byte, int(groups)*int(maxLsn))
	for group := uint32(1); group <= groups; group++ {
		for lsn := uint64(1); lsn <= maxLsn; lsn++ {
			cmd := new(txnbase.PointerCmd)
			cmd.Group = group
			cmd.Lsn = lsn
			mashalled, err := cmd.Marshal()
			assert.Nil(t, err)
			r := bytes.NewBuffer(mashalled)
			cmd2, _, err := txnbase.BuildCommandFrom(r)
			assert.Nil(t, err)
			assert.Equal(t, cmd.Group, cmd2.(*txnbase.PointerCmd).Group)
			assert.Equal(t, cmd.Lsn, cmd2.(*txnbase.PointerCmd).Lsn)
		}
	}
}

// func TestDeletesCmd(t *testing.T) {
// 	deletes := make(map[uint32]*roaring.Bitmap)
// 	for i := 0; i < 10; i++ {
// 		deletes[uint32(i)] = roaring.NewBitmap()
// 		deletes[uint32(i)].Add(uint64(i)*2 + 1)
// 	}
// 	cmd := MakeDeletesCmd(deletes)
// 	var w bytes.Buffer
// 	err := cmd.WriteTo(&w)
// 	assert.Nil(t, err)

// 	buf := w.Bytes()
// 	r := bytes.NewBuffer(buf)
// 	cmd2, err := BuildCommandFrom(r)
// 	assert.Nil(t, err)
// 	for k, v := range cmd2.(*LocalDeletesCmd).Deletes {
// 		assert.True(t, v.Contains(2*uint64(k)+1))
// 	}
// }

func TestComposedCmd(t *testing.T) {
	composed := txnbase.NewComposedCmd()
	groups := uint32(10)
	maxLsn := uint64(10)
	// pts := make([]TxnCmd, int(groups)*int(maxLsn))
	for group := uint32(1); group <= groups; group++ {
		for lsn := uint64(1); lsn <= maxLsn; lsn++ {
			cmd := new(txnbase.PointerCmd)
			cmd.Group = group
			cmd.Lsn = lsn
			composed.AddCmd(cmd)
			// pts = append(pts, cmd)
		}
	}
	batCnt := 5

	schema := catalog.MockSchema(4, 0)
	for i := 0; i < batCnt; i++ {
		data := catalog.MockData(schema, uint32((i+1)*5))
		bat, err := compute.CopyToIBatch(data, uint64(txnbase.MaxNodeRows))
		assert.Nil(t, err)
		batCmd := txnbase.NewBatchCmd(bat, schema.Types())
		del := roaring.NewBitmap()
		del.Add(uint32(i))
		delCmd := txnbase.NewDeleteBitmapCmd(del)
		comp := txnbase.NewComposedCmd()
		comp.AddCmd(batCmd)
		comp.AddCmd(delCmd)
		composed.AddCmd(comp)
	}
	var w bytes.Buffer
	_, err := composed.WriteTo(&w)
	assert.Nil(t, err)

	buf := w.Bytes()

	r := bytes.NewBuffer(buf)
	composed2, _, err := txnbase.BuildCommandFrom(r)
	assert.Nil(t, err)
	cmd1 := composed.Cmds
	cmd2 := composed2.(*txnbase.ComposedCmd).Cmds

	assert.Equal(t, len(cmd1), len(cmd2))
	for i, c1 := range cmd1 {
		c2 := cmd2[i]
		assert.Equal(t, c1.GetType(), c2.GetType())
		switch c1.GetType() {
		case txnbase.CmdPointer:
			assert.Equal(t, c1.(*txnbase.PointerCmd).Group, c2.(*txnbase.PointerCmd).Group)
			assert.Equal(t, c1.(*txnbase.PointerCmd).Group, c2.(*txnbase.PointerCmd).Group)
		case txnbase.CmdComposed:
			comp1 := c1.(*txnbase.ComposedCmd)
			comp2 := c2.(*txnbase.ComposedCmd)
			for j, cc1 := range comp1.Cmds {
				cc2 := comp2.Cmds[j]
				assert.Equal(t, cc1.GetType(), cc2.GetType())
				switch cc1.GetType() {
				case txnbase.CmdPointer:
					assert.Equal(t, cc1.(*txnbase.PointerCmd).Group, cc2.(*txnbase.PointerCmd).Group)
					assert.Equal(t, cc1.(*txnbase.PointerCmd).Group, cc2.(*txnbase.PointerCmd).Group)
				case txnbase.CmdDeleteBitmap:
					assert.True(t, cc1.(*txnbase.DeleteBitmapCmd).Bitmap.Equals(cc1.(*txnbase.DeleteBitmapCmd).Bitmap))
				case txnbase.CmdBatch:
					b1 := cc1.(*txnbase.BatchCmd)
					b2 := cc2.(*txnbase.BatchCmd)
					assert.Equal(t, b1.Types, b2.Types)
					assert.Equal(t, b1.Bat.Length(), b2.Bat.Length())
				}
			}
		}
	}
}

func checkAppendCmdIsEqual(t *testing.T, cmd1, cmd2 *AppendCmd) {
	assert.Equal(t, cmd1.ID, cmd2.ID)
	assert.Equal(t, len(cmd1.Cmds), len(cmd2.Cmds))
	for i, subcmd1 := range cmd1.Cmds {
		assert.Equal(t, subcmd1.GetType(), cmd2.Cmds[i].GetType())
	}
	assert.Equal(t, len(cmd1.Infos), len(cmd2.Infos))
	for i, info1 := range cmd1.Infos {
		checkAppendInfoIsEqual(t, info1, cmd2.Infos[i])
	}
}

func checkAppendInfoIsEqual(t *testing.T, info1, info2 *appendInfo) {
	assert.Equal(t, info1.seq, info2.seq)
	assert.Equal(t, info1.destLen, info2.destLen)
}
