// Copyright 2021 - 2024 Matrix Origin
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

package service

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	ckpLocationDivider = ';'
)

// logtailMerger is the merger tool to merge multiple
// LogtailPhase instances.
type logtailMerger struct {
	logtails []*LogtailPhase
}

func newLogtailMerger(l ...*LogtailPhase) *logtailMerger {
	var lm logtailMerger
	lm.logtails = append(lm.logtails, l...)
	return &lm
}

// Merge merges all instances and return one logtail.TableLogtail
// and a callback function.
func (m *logtailMerger) Merge() (logtail.TableLogtail, func()) {
	var tableID api.TableID
	var entryCount int
	var ts timestamp.Timestamp
	for _, t := range m.logtails {
		entryCount += len(t.tail.Commands)

		// check the table ID.
		if tableID.TbId == 0 {
			tableID = *t.tail.Table
		} else if tableID.TbId != t.tail.Table.TbId {
			panic(fmt.Sprintf("cannot merge logtails with different table: %d, %d",
				tableID.TbId, t.tail.Table.TbId))
		}

		// get the max timestamp.
		if ts.Less(*t.tail.Ts) {
			ts = *t.tail.Ts
		}
	}

	// create the callbacks.
	callbacks := make([]func(), 0, entryCount)
	// create a new table logtail with the entry number.
	tail := logtail.TableLogtail{
		Ts:       &ts,
		Table:    &tableID,
		Commands: make([]api.Entry, 0, entryCount),
	}
	for _, t := range m.logtails {
		ckpLocLen := len(tail.CkpLocation)
		if ckpLocLen > 0 &&
			tail.CkpLocation[ckpLocLen-1] != ckpLocationDivider {
			tail.CkpLocation += string(ckpLocationDivider)
		}
		tail.CkpLocation += t.tail.CkpLocation
		tail.Commands = append(tail.Commands, t.tail.Commands...)
		callbacks = append(callbacks, t.closeCB)
	}

	// remove the last ';'
	ckpLocLen := len(tail.CkpLocation)
	if ckpLocLen > 0 &&
		tail.CkpLocation[ckpLocLen-1] == ckpLocationDivider {
		tail.CkpLocation = tail.CkpLocation[:ckpLocLen-1]
	}
	return tail, func() {
		for _, cb := range callbacks {
			if cb != nil {
				cb()
			}
		}
	}
}
