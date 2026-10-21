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
	var snapshot catalog.ObjectListSnapshot
	if isTombstone {
		snapshot = c.tbl.MakeTombstoneObjectSnapshot()
	} else {
		snapshot = c.tbl.MakeDataObjectSnapshot()
	}
	visit := func(obj *catalog.ObjectEntry) error {
		c.recordReport(isTombstone, obj.GetAppendable())
		if next := obj.GetNextVersion(); obj.IsCEntry() && next != nil && next.DeletedAt.LE(&to) {
			return nil
		}
		return c.visitor.VisitObj(obj)
	}
	return visitLogtailObjects(snapshot, from, to, visit)
}

func visitLogtailObjects(
	snapshot catalog.ObjectListSnapshot, from, to types.TS,
	visit func(*catalog.ObjectEntry) error,
) error {
	for group := catalog.ObjectListGroupAppendableCreate; group <= catalog.ObjectListGroupNonAppendableDrop; group++ {
		appendableCreate := group == catalog.ObjectListGroupAppendableCreate ||
			group == catalog.ObjectListGroupAppendableCreateWithDrop
		var bookmarkStart *catalog.ObjectEntry
		var bookmarkValid bool
		if appendableCreate {
			// An append on an older object can commit after a newer object is
			// created. Only a finalized max-commit prefix may be omitted.
			bookmarkStart, bookmarkValid = snapshot.CommitBookmarkStart(group, from)
		}
		bookmarkBeforeFrom := false
		if bookmarkStart != nil {
			commitTS := bookmarkStart.ObjectListCommitTS()
			bookmarkBeforeFrom = commitTS.LT(&from)
		}
		var last *catalog.ObjectEntry
		for {
			groupIt := snapshot.Group(group)
			var ok bool
			switch {
			case last != nil:
				// The pinned group tree is immutable. Resume after the last
				// fetched key, even if its visitor waited for a transaction.
				ok = groupIt.Seek(last)
				if ok {
					ok = groupIt.Next()
				}
			case appendableCreate && !bookmarkValid:
				ok = groupIt.First()
			case appendableCreate && bookmarkBeforeFrom:
				ok = groupIt.Seek(bookmarkStart)
			default:
				// A finalized append-max below from says nothing about
				// catalog creation in [from,to]. Never seek past from.
				ok = catalog.SeekObjectListGroup(&groupIt, group, from)
			}

			// The visitor can wait for append transactions whose completion
			// updates ObjectList. Release the B-tree read lock before visiting.
			var batch [64]*catalog.ObjectEntry
			count := 0
			for ok && count < len(batch) {
				obj := groupIt.Item()
				commitTS := obj.ObjectListCommitTS()
				if commitTS.GT(&to) {
					break
				}
				batch[count] = obj
				count++
				last = obj
				ok = groupIt.Next()
			}
			finished := !ok || count < len(batch)
			groupIt.Release()
			for _, obj := range batch[:count] {
				commitTS := obj.ObjectListCommitTS()
				if appendableCreate && commitTS.LT(&from) {
					if data := obj.GetObjectData(); data != nil {
						if maxCommitter, ok := data.(interface{ GetAppendMaxCommitTS() (types.TS, bool) }); ok {
							if maxCommit, finalized := maxCommitter.GetAppendMaxCommitTS(); finalized && maxCommit.LT(&from) {
								continue
							}
						}
					}
				}
				// Catalog creations in [from, to] are visited even when their
				// append max is below from or no rows were appended.
				if err := visit(obj); err != nil {
					return err
				}
			}
			if finished {
				break
			}
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
