// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/tidwall/btree"
)

type BoundTableOperator struct {
	from, to types.TS
	tbl      *catalog.TableEntry
	visitor  *TableLogtailRespBuilder

	dAScanCnt, dSScanCnt int
	tAScanCnt, tSScanCnt int
}

func (c *BoundTableOperator) Report() string {
	return fmt.Sprintf("dAScanCnt: %d, dSScanCnt: %d, tAScanCnt: %d, tSScanCnt: %d", c.dAScanCnt, c.dSScanCnt, c.tAScanCnt, c.tSScanCnt)
}

func (c *BoundTableOperator) recordReport(isTombstone bool, appendable bool) {
	if isTombstone {
		if appendable {
			c.tAScanCnt++
		} else {
			c.tSScanCnt++
		}
	} else {
		if appendable {
			c.dAScanCnt++
		} else {
			c.dSScanCnt++
		}
	}
}

// iterObject is allowed to yield false positive results, because ForeachMVCCNodeInRange will check the accuracy of the result.
func (c *BoundTableOperator) iterObject(from, to types.TS, isTombstone bool) error {
	var it btree.IterG[*catalog.ObjectEntry]
	if isTombstone {
		it = c.tbl.MakeTombstoneObjectIt()
	} else {
		it = c.tbl.MakeDataObjectIt()
	}
	key := &catalog.ObjectEntry{EntryMVCCNode: catalog.EntryMVCCNode{DeletedAt: to.Next()}}
	var ok bool
	if ok = it.Seek(key); !ok {
		ok = it.Last()
	}

	// after seeking, the first object could be out of the range, but false positive is allowed.
	earlybreak := false
	for ; ok; ok = it.Prev() {
		if earlybreak {
			break
		}
		obj := it.Item()
		c.recordReport(isTombstone, obj.GetAppendable())
		if obj.IsAppendable() && obj.IsCEntry() && obj.CreatedAt.LT(&from) {
			earlybreak = true
		}

		if next := obj.GetNextVersion(); obj.IsCEntry() && next != nil && next.DeletedAt.LE(&to) {
			continue
		}

		if err := c.visitor.VisitObj(obj); err != nil {
			return err
		}
	}
	return nil
}

func (c *BoundTableOperator) Run() error {
	c.tbl.WaitDataObjectCommitted(c.to)
	if err := c.iterObject(c.from, c.to, false); err != nil {
		return err
	}
	c.tbl.WaitTombstoneObjectCommitted(c.to)
	if err := c.iterObject(c.from, c.to, true); err != nil {
		return err
	}
	return nil
}
