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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sql/features"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAppendCmd_MarshalUnmarshal_RoundTrip(t *testing.T) {
	bat := containers.NewBatch()
	defer bat.Close()

	cmd := NewEmptyAppendCmd()
	cmd.Data = bat
	cmd.Ts = types.BuildTS(1, 1)

	buf, err := cmd.MarshalBinary()
	require.NoError(t, err)
	require.NotNil(t, buf)

	// Use BuildCommandFrom which handles the header
	cmdInterface, err := txnbase.BuildCommandFrom(buf)
	require.NoError(t, err)
	cmd2, ok := cmdInterface.(*AppendCmd)
	require.True(t, ok)
	assert.Equal(t, cmd.Ts, cmd2.Ts)
}

func TestLegacyRenameAppendRequiresExactLayout(t *testing.T) {
	previous := catalog.NewEmptySchema("legacy_rename")
	typ := types.T_int32.ToType()
	require.NoError(t, previous.AppendPKCol("c", typ, 0))
	require.NoError(t, previous.AppendCol("d", typ))
	require.NoError(t, previous.Finalize(false))
	current := previous.Clone()
	require.NoError(t, current.ApplyAlterTable(api.NewRenameColumnReq(0, 0, "c", "new_key", 0)))

	attrs := previous.AllNames()
	require.True(t, isLegacyRenameAppend(attrs, previous, current))
	require.False(t, isLegacyRenameAppend(current.AllNames(), previous, current))
	require.False(t, isLegacyRenameAppend(attrs[:len(attrs)-1], previous, current))
	wrongNames := previous.AllNames()
	wrongNames[0] = "unknown"
	require.False(t, isLegacyRenameAppend(wrongNames, previous, current))
	wrongNames[0], wrongNames[1] = attrs[1], attrs[0]
	require.False(t, isLegacyRenameAppend(wrongNames, previous, current))

	changedType := current.Clone()
	changedType.ColDefs[0].Type = types.T_int64.ToType()
	require.False(t, isLegacyRenameAppend(attrs, previous, changedType))
	changedSeq := current.Clone()
	changedSeq.ColDefs[0], changedSeq.ColDefs[1] = changedSeq.ColDefs[1], changedSeq.ColDefs[0]
	require.False(t, isLegacyRenameAppend(attrs, previous, changedSeq))
}

func TestRecoverCheckpointAppendNames(t *testing.T) {
	typ := types.T_int64.ToType()
	for _, scenario := range []string{"rename", "rename_then_comment", "swap", "swap_after_drop", "sparse", "evolved", "wrong_type", "duplicate_name", "incomplete"} {
		t.Run(scenario, func(t *testing.T) {
			previous := catalog.NewEmptySchema("checkpoint_rename")
			require.NoError(t, previous.AppendPKCol("id", typ, 0))
			if scenario == "sparse" {
				previous.Extra.NextColSeqnum = 8
				require.NoError(t, previous.AppendColWithAttribute(engine.Attribute{Name: "price", Seqnum: 7, Type: typ}))
			} else {
				require.NoError(t, previous.AppendCol("price", typ))
			}
			if scenario == "swap_after_drop" {
				require.NoError(t, previous.AppendCol("retired", typ))
			}
			require.NoError(t, previous.Finalize(false))
			if scenario == "swap_after_drop" {
				require.NoError(t, previous.ApplyAlterTable(api.NewRemoveColumnReq(0, 0, 2, 2)))
				require.EqualValues(t, 3, previous.Extra.NextColSeqnum)
			}
			current := previous.Clone()
			rename := func(old, new string, seq uint16) {
				t.Helper()
				require.NoError(t, current.ApplyAlterTable(api.NewRenameColumnReq(0, 0, old, new, uint32(seq))))
			}
			if scenario == "swap" || scenario == "swap_after_drop" {
				rename("id", "tmp", 0)
				rename("price", "id", 1)
				rename("tmp", "price", 0)
			} else {
				rename("price", "amount", previous.GetSeqnum("price"))
			}
			current.Version++
			if scenario == "rename_then_comment" {
				current.Version++
				current.Extra.ColumnChanged = false
			}
			if scenario == "evolved" {
				current.Extra.FeatureFlag = features.SchemaEvolved
			}

			attrs, typs := previous.AllNames(), previous.AllTypes()
			if scenario == "wrong_type" {
				typs[1] = types.T_int32.ToType()
			}
			if scenario == "incomplete" {
				attrs, typs = attrs[:2], typs[:2]
			}
			original := append([]string(nil), attrs...)
			bat := containers.BuildBatch(attrs, typs, containers.Options{Allocator: common.DefaultAllocator})
			defer bat.Close()
			require.NoError(t, vector.AppendAny(bat.Vecs[0], int64(1), false, bat.Vecs[0].MPool()))
			if scenario != "wrong_type" {
				require.NoError(t, vector.AppendAny(bat.Vecs[1], int64(42), false, bat.Vecs[1].MPool()))
			}
			if scenario == "duplicate_name" {
				bat.Attrs[1] = "id"
				delete(bat.Nameidx, "price")
				original[1] = "id"
			}
			err := recoverCheckpointAppendNames(bat, current)
			if scenario == "rename" || scenario == "rename_then_comment" || scenario == "swap" {
				require.NoError(t, err)
				require.Equal(t, current.AllNames(), bat.Attrs)
				require.Equal(t, int64(1), bat.GetVectorByName(current.ColDefs[0].Name).GetValue(0))
				require.Equal(t, int64(42), bat.GetVectorByName(current.ColDefs[1].Name).GetValue(0))
			} else {
				require.Error(t, err)
				require.Equal(t, original, bat.Attrs)
				if scenario == "swap_after_drop" {
					// Both old names and types still look valid in the checkpoint.
					// Their positions expose the ambiguity before any row is installed.
					require.ElementsMatch(t, current.AllNames(), bat.Attrs)
					require.False(t, walColumnsInSchemaOrder(bat.Attrs, current))
					require.Equal(t, int64(1), bat.GetVectorByName("id").GetValue(0))
					require.Equal(t, int64(42), bat.GetVectorByName("price").GetValue(0))
				}
			}
		})
	}
}
