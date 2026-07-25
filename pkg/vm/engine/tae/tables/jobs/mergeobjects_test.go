// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/stretchr/testify/require"
)

func TestValidateCNOriginOutputs(t *testing.T) {
	makeEntry := func(flags ...bool) *api.MergeCommitEntry {
		entry := &api.MergeCommitEntry{}
		for _, flag := range flags {
			id := objectio.NewObjectid()
			stats := objectio.NewObjectStatsWithObjectID(
				&id, false, true, false,
			)
			if flag {
				objectio.WithCNOrigin()(stats)
			}
			entry.CreatedObjs = append(
				entry.CreatedObjs, stats.Clone().Marshal(),
			)
		}
		return entry
	}
	transferTable := func(
		maps ...api.TransferMap,
	) *mergesort.TransferTable {
		return mergesort.NewTransferTableFromMaps(maps)
	}

	t.Run("mixed outputs", func(t *testing.T) {
		// Source 0 is pure TN and lands in output 0. Source 1 carries CN
		// lineage and lands in output 1.
		table := transferTable(
			api.TransferMap{{ObjIdx: 0}},
			api.TransferMap{{ObjIdx: 1}},
		)
		err := validateCNOriginOutputs(
			makeEntry(false, true),
			table,
			[]bool{false, true},
			[]uint32{1, 1},
		)
		require.NoError(t, err)
	})

	t.Run("reject blanket marker", func(t *testing.T) {
		table := transferTable(
			api.TransferMap{{ObjIdx: 0}},
			api.TransferMap{{ObjIdx: 1}},
		)
		err := validateCNOriginOutputs(
			makeEntry(true, true),
			table,
			[]bool{false, true},
			[]uint32{1, 1},
		)
		require.ErrorContains(t, err, "incorrectly marked pure-TN output")
	})

	t.Run("reject omitted marker", func(t *testing.T) {
		table := transferTable(
			api.TransferMap{{ObjIdx: 0}},
			api.TransferMap{{ObjIdx: 1}},
		)
		err := validateCNOriginOutputs(
			makeEntry(false, false),
			table,
			[]bool{false, true},
			[]uint32{1, 1},
		)
		require.ErrorContains(t, err, "omitted CN-origin lineage")
	})

	t.Run("deleted lineage rows do not mark output", func(t *testing.T) {
		table := transferTable(
			api.TransferMap{{ObjIdx: 0}},
			api.TransferMap{{ObjIdx: api.NoTransfer}},
		)
		err := validateCNOriginOutputs(
			makeEntry(false),
			table,
			[]bool{false, true},
			[]uint32{1, 1},
		)
		require.NoError(t, err)
	})

	t.Run("transfer disabled", func(t *testing.T) {
		err := validateCNOriginOutputs(
			makeEntry(true),
			mergesort.NewTransferTableFromMaps(nil),
			[]bool{true},
			[]uint32{1},
		)
		require.NoError(t, err)
	})

	t.Run("reject source metadata mismatch", func(t *testing.T) {
		table := transferTable(api.TransferMap{{ObjIdx: 0}})
		err := validateCNOriginOutputs(
			makeEntry(false),
			table,
			[]bool{false},
			nil,
		)
		require.ErrorContains(t, err, "source lineage metadata mismatch")
	})

	t.Run("reject block metadata mismatch", func(t *testing.T) {
		table := transferTable(api.TransferMap{{ObjIdx: 0}})
		err := validateCNOriginOutputs(
			makeEntry(false),
			table,
			[]bool{false},
			[]uint32{2},
		)
		require.ErrorContains(t, err, "transfer metadata mismatch")
	})

	t.Run("reject invalid destination", func(t *testing.T) {
		table := transferTable(
			api.TransferMap{{ObjIdx: 1}},
		)
		err := validateCNOriginOutputs(
			makeEntry(false),
			table,
			[]bool{true},
			[]uint32{1},
		)
		require.ErrorContains(t, err, "out of range")
	})
}
