// Copyright 2026 Matrix Origin
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

package sidecar

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBuildManifestIsDeterministic(t *testing.T) {
	a := objectStats(t, 3)
	b := objectStats(t, 7)
	def := &planpb.TableDef{TblId: 42, DbName: "db", Name: "t", Cols: []*planpb.ColDef{{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64)}}}}
	one, names, err := buildManifest(def, "shared", []objectio.ObjectStats{b, a})
	require.NoError(t, err)
	two, names2, err := buildManifest(def, "shared", []objectio.ObjectStats{a, b})
	require.NoError(t, err)
	require.True(t, bytes.Equal(one, two))
	require.Equal(t, names, names2)
	require.Less(t, names[0], names[1])
	require.Contains(t, string(one), `"total_rows":10`)
}

func TestBuildManifestRejectsInvalidDefinitions(t *testing.T) {
	_, _, err := buildManifest(nil, "shared", nil)
	require.ErrorContains(t, err, "nil table definition")
	_, _, err = buildManifest(&planpb.TableDef{TblId: 42, Cols: []*planpb.ColDef{nil, {Hidden: true}}}, "shared", nil)
	require.ErrorContains(t, err, "no manifest columns")
}

func TestSnapshotProviderRejectsInvalidSetupBeforeStorageAccess(t *testing.T) {
	ctx := context.Background()
	read := substrait.Read{TableID: 42}
	_, err := (*SnapshotProvider)(nil).PrepareSnapshotRead(ctx, read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "invalid TAE snapshot provider")
	_, err = (&SnapshotProvider{}).PrepareSnapshotRead(ctx, read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "invalid TAE snapshot provider")
	provider := &SnapshotProvider{MPool: mpool.MustNewZero(), Relations: make(map[uint64]engine.Relation)}
	_, err = provider.PrepareSnapshotRead(ctx, read, []byte{1})
	require.ErrorContains(t, err, "invalid TAE snapshot provider")
	_, err = provider.PrepareSnapshotRead(ctx, read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "is not open")
}

type partitionedRelationStub struct{ engine.Relation }

func (*partitionedRelationStub) IsPartitionedRelation() bool { return true }

func TestSnapshotProviderRejectsPartitionedRelationBeforeStorageAccess(t *testing.T) {
	provider := &SnapshotProvider{
		MPool:     mpool.MustNewZero(),
		Relations: map[uint64]engine.Relation{42: new(partitionedRelationStub)},
	}
	facts, err := provider.PrepareSnapshotRead(
		context.Background(),
		substrait.Read{TableID: 42},
		make([]byte, types.TxnTsSize),
	)
	require.NoError(t, err)
	require.True(t, facts.NonTAE)
}

func objectStats(t *testing.T, rows uint32) objectio.ObjectStats {
	id := types.NewObjectid()
	s := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(s, rows))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(s, 1))
	return *s
}
