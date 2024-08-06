// Copyright 2022 Matrix Origin
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

package cdc

import (
	"sync"

	"github.com/panjf2000/ants/v2"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
)

var _ Decoder = new(decoder)

type decoder struct {
}

func (m *decoder) Decode(
	cdcCtx *TableCtx,
	input *DecoderInput) *DecoderOutput {
	//parallel step1:decode rows
	wg := sync.WaitGroup{}
	wg.Add(1)
	err := ants.Submit(func() {
		defer wg.Done()
		it := input.state.NewRowsIterInCdc()
		decodeRows(input.ts, it)
		defer it.Close()
	})
	if err != nil {
		panic(err)
	}
	//parallel step2:decode objects
	wg.Add(1)
	err = ants.Submit(func() {
		defer wg.Done()

	})
	if err != nil {
		panic(err)
	}
	//parallel step3:decode deltas
	wg.Add(1)
	err = ants.Submit(func() {
		defer wg.Done()

	})
	if err != nil {
		panic(err)
	}
	return nil
}

func decodeRows(
	ts timestamp.Timestamp,
	rowsIter logtailreplay.RowsIter) error {
	for rowsIter.Next() {
		ent := rowsIter.Entry()
		if ent.Deleted {
			//to delete
		} else {
			//to insert
		}
	}
	return nil
}

func decodeObjects(
	ts timestamp.Timestamp,
	objects *btree.BTreeG[logtailreplay.ObjectEntry],
) error {
	return nil
}

func decodeDeltas(
	ts timestamp.Timestamp,
	deltas *btree.BTreeG[logtailreplay.BlockDeltaEntry],
) error {
	return nil
}
