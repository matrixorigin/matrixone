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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
)

func TestPointerCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	groups := uint32(10)
	maxLsn := uint64(10)
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

func TestComposedCmd(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	composed := txnbase.NewComposedCmd()
	defer composed.Close()
	groups := uint32(10)
	maxLsn := uint64(10)
	for group := uint32(1); group <= groups; group++ {
		for lsn := uint64(1); lsn <= maxLsn; lsn++ {
			cmd := new(txnbase.PointerCmd)
			cmd.Group = group
			cmd.Lsn = lsn
			composed.AddCmd(cmd)
		}
	}
	batCnt := 5

	schema := catalog.MockSchema(4, 0)
	for i := 0; i < batCnt; i++ {
		bat := catalog.MockBatch(schema, (i+1)*5)
		defer bat.Close()
		batCmd := txnbase.NewBatchCmd(bat)
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
	defer composed2.Close()
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
					assert.True(t, b1.Bat.Equals(b2.Bat))
				}
			}
		}
	}
}
