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

package catalog

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

func (catalog *Catalog) CheckMetadata() {
	logutil.Infof("[MetadataCheck] Start")
	p := &LoopProcessor{}
	p.ObjectFn = catalog.checkObject
	p.TombstoneFn = catalog.checkTombstone
	catalog.RecurLoop(p)
	logutil.Infof("[MetadataCheck] End")
}
func (catalog *Catalog) checkTombstone(t data.Tombstone) error {
	obj := t.GetObject().(*ObjectEntry)
	_, err := obj.GetTable().GetObjectByID(&obj.ID)
	if err != nil {
		logutil.Warnf("[MetadataCheck] tombstone and object doesn't match, err %v, obj %v, tombstone %v",
			err,
			obj.PPString(3, 0, ""),
			t.String(3, 0, ""))
	}
	t.CheckTombstone()
	return nil
}
func (catalog *Catalog) checkObject(o *ObjectEntry) error {
	o.RLock()
	defer o.RUnlock()
	if o.Depth() > 2 {
		logutil.Warnf("[MetadataCheck] object mvcc link is too long, depth %d, obj %v", o.Depth(), o.PPStringLocked(3, 0, ""))
	}
	if o.IsAppendable() && o.HasDropCommittedLocked() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !o.IsAppendable() && !o.IsCreatingOrAborted() {
		if o.GetLatestNodeLocked().BaseNode.IsEmpty() {
			logutil.Warnf("[MetadataCheck] object should have stats, obj %v", o.PPStringLocked(3, 0, ""))
		}
	}
	if !catalog.gcTS.IsEmpty() {
		if o.HasDropCommittedLocked() && o.DeleteBeforeLocked(catalog.gcTS) && !o.InMemoryDeletesExistedLocked() {
			logutil.Warnf("[MetadataCheck] object should not exist, gcTS %v, obj %v", catalog.gcTS.ToString(), o.PPStringLocked(3, 0, ""))
		}
	}

	duration := time.Minute * 10
	ts := types.BuildTS(time.Now().UTC().UnixNano()-duration.Nanoseconds(), 0)
	if o.HasDropCommittedLocked() && o.DeleteBeforeLocked(ts) {
		if o.InMemoryDeletesExistedLocked() {
			logutil.Warnf("[MetadataCheck] object has in memory deletes %v after deleted, obj %v, tombstone %v",
				duration,
				o.PPStringLocked(3, 0, ""),
				o.GetTable().TryGetTombstone(o.ID).StringLocked(3, 0, ""))
		}
	}

	lastNode := o.GetLatestNodeLocked()
	if lastNode == nil {
		logutil.Warnf("[MetadataCheck] object MVCC Chain is empty, obj %v", o.ID.String())
	} else {
		duration := time.Minute * 10
		ts := types.BuildTS(time.Now().UTC().UnixNano()-duration.Nanoseconds(), 0)
		if !lastNode.IsCommitted() {
			if lastNode.Start.Less(&ts) {
				logutil.Warnf("[MetadataCheck] object MVCC Node hasn't committed %v after it starts, obj %v",
					duration,
					o.PPStringLocked(3, 0, ""))
			}
		} else {
			if lastNode.End.Equal(&txnif.UncommitTS) || lastNode.Prepare.Equal(&txnif.UncommitTS) {
				logutil.Warnf("[MetadataCheck] object MVCC Node hasn't committed but node.Txn is nil, obj %v",
					o.PPStringLocked(3, 0, ""))
			}
		}
	}
	return nil
}
