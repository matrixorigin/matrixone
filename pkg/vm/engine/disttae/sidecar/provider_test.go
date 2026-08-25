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
	"encoding/json"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBuildManifestIsDeterministic(t *testing.T) {
	a := objectStats(t, 3)
	b := objectStats(t, 7)
	def := testTableDef()
	one, names, err := buildManifest(def, 9, 7, "shared", []objectio.ObjectStats{b, a})
	require.NoError(t, err)
	two, names2, err := buildManifest(def, 9, 7, "shared", []objectio.ObjectStats{a, b})
	require.NoError(t, err)
	require.True(t, bytes.Equal(one, two))
	require.Equal(t, names, names2)
	require.Less(t, names[0], names[1])
	require.Contains(t, string(one), `"total_rows":10`)
	decoded := new(manifest)
	require.NoError(t, json.Unmarshal(one, decoded))
	require.Equal(t, 2, decoded.Version)
	require.Equal(t, uint64(9), decoded.AccountID)
	require.Equal(t, uint64(7), decoded.DatabaseID)
	require.Equal(t, uint32(3), decoded.SchemaVersion)
	require.Equal(t, uint64(11), decoded.Columns[0].ColumnID)
	require.Equal(t, uint32(5), decoded.Columns[0].SequenceNumber)
}

func TestBuildManifestRejectsInvalidDefinitions(t *testing.T) {
	_, _, err := buildManifest(nil, 9, 7, "shared", nil)
	require.ErrorContains(t, err, "nil table definition")
	_, _, err = buildManifest(&planpb.TableDef{TblId: 42, Cols: []*planpb.ColDef{nil, {Hidden: true}}}, 9, 7, "shared", nil)
	require.ErrorContains(t, err, "no manifest columns")
	_, _, err = buildManifest(testTableDef(), 0, 7, "shared", nil)
	require.NoError(t, err)
	_, _, err = buildManifest(testTableDef(), 9, 0, "shared", nil)
	require.ErrorContains(t, err, "identity")
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

type snapshotRelationStub struct {
	engine.Relation
	def             *planpb.TableDef
	stats           []objectio.ObjectStats
	visible         uint64
	visits          int
	starCalls       int
	tombstoneChecks int
	readView        client.WorkspaceReadView
	nonLocal        bool
	hasTombstones   bool
}

func (s *snapshotRelationStub) GetTableDef(context.Context) *planpb.TableDef { return s.def }

func (s *snapshotRelationStub) CanVisitSnapshotLocally() (bool, error) {
	return !s.nonLocal, nil
}

func (s *snapshotRelationStub) HasSnapshotTombstones(_ context.Context, readView client.WorkspaceReadView, _ types.TS) (bool, error) {
	s.tombstoneChecks++
	s.readView = readView
	return s.hasTombstones, nil
}

func (s *snapshotRelationStub) VisitSnapshotObjects(_ context.Context, _ types.TS, visit func(objectio.ObjectStats, bool) error) error {
	for i := range s.stats {
		s.visits++
		if err := visit(s.stats[i], false); err != nil {
			return err
		}
	}
	return nil
}

func (s *snapshotRelationStub) StarCount(context.Context) (uint64, error) {
	s.starCalls++
	return s.visible, nil
}

func TestSnapshotProviderRejectsDelegatedAndDeletedSnapshotsBeforeEnumeration(t *testing.T) {
	def := testTableDef()
	read := testRead(t, def)
	relation := &snapshotRelationStub{def: def, stats: []objectio.ObjectStats{objectStats(t, 1)}, visible: 1, nonLocal: true}
	readView := client.NewWorkspaceReadView(7, 8, 9)
	provider := &SnapshotProvider{MPool: mpool.MustNewZero(), DataDir: "shared", Relations: map[uint64]engine.Relation{42: relation}, ReadView: readView}

	facts, err := provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.NoError(t, err)
	require.True(t, facts.NonTAE)
	require.Zero(t, relation.tombstoneChecks)
	require.Zero(t, relation.visits)
	require.Zero(t, relation.starCalls)

	relation.nonLocal = false
	relation.hasTombstones = true
	facts, err = provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.NoError(t, err)
	require.True(t, facts.VisibleTombstones)
	require.Equal(t, 1, relation.tombstoneChecks)
	require.Equal(t, readView, relation.readView)
	require.Zero(t, relation.visits)
	require.Zero(t, relation.starCalls)
}

func TestSnapshotProviderPinsPhysicalSchemaAndRejectsAppendableObjects(t *testing.T) {
	def := testTableDef()
	read := testRead(t, def)
	stats := objectStats(t, 3)
	relation := &snapshotRelationStub{def: def, stats: []objectio.ObjectStats{stats}, visible: 3}
	provider := &SnapshotProvider{MPool: mpool.MustNewZero(), DataDir: "shared", Relations: map[uint64]engine.Relation{42: relation}}
	facts, err := provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.NoError(t, err)
	require.NotEmpty(t, facts.Manifest)
	require.Equal(t, 1, relation.starCalls)

	relation.visits = 0
	def.Version++
	_, err = provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "physical schema changed")
	require.Zero(t, relation.visits)
	def.Version--
	def.Cols[0].Seqnum++
	_, err = provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "physical schema changed")
	require.Zero(t, relation.visits)
	def.Cols[0].Seqnum--

	objectio.SetObjectStatsAppendable(&stats, true)
	relation.stats = []objectio.ObjectStats{stats, objectStats(t, 4)}
	relation.visible = 7
	relation.visits = 0
	relation.starCalls = 0
	facts, err = provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.NoError(t, err)
	require.True(t, facts.Uncommitted)
	require.Equal(t, 1, relation.visits)
	require.Zero(t, relation.starCalls)
}

func TestSnapshotProviderValidatesOptimizerProjectedColumnsAgainstLiveSchema(t *testing.T) {
	def := testTableDef()
	def.Cols = append(def.Cols, &planpb.ColDef{
		Name: "b", ColId: 12, Seqnum: 6, Typ: planpb.Type{Id: int32(types.T_varchar), Width: 8},
	})
	projected := *def
	projected.Cols = []*planpb.ColDef{def.Cols[1]}
	schema, err := substrait.CanonicalSchema(&projected)
	require.NoError(t, err)
	read := substrait.Read{
		AccountID: 0, DatabaseID: def.DbId, TableID: def.TblId, SchemaVersion: def.Version,
		Columns: []substrait.ColumnMapping{{ColumnID: def.Cols[1].ColId, SequenceNumber: def.Cols[1].Seqnum}},
		Schema:  schema,
	}
	relation := &snapshotRelationStub{def: def, stats: []objectio.ObjectStats{objectStats(t, 1)}, visible: 1}
	provider := &SnapshotProvider{MPool: mpool.MustNewZero(), DataDir: "shared", Relations: map[uint64]engine.Relation{42: relation}}
	facts, err := provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.NoError(t, err)
	require.Equal(t, schema, facts.CanonicalSchema)

	read.Columns = append(read.Columns, read.Columns[0])
	_, err = provider.PrepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize))
	require.ErrorContains(t, err, "physical schema changed")
}

func TestSnapshotProviderStopsObjectVisitationAtManifestBound(t *testing.T) {
	def := testTableDef()
	read := testRead(t, def)
	stats := []objectio.ObjectStats{objectStats(t, 1), objectStats(t, 2), objectStats(t, 3)}
	probe, err := newManifestBuilder(def, read.AccountID, read.DatabaseID, "shared", substrait.MaxManifestBytes)
	require.NoError(t, err)
	require.NoError(t, probe.add(stats[0]))
	one, _, err := probe.finish()
	require.NoError(t, err)
	relation := &snapshotRelationStub{def: def, stats: stats, visible: 6}
	provider := &SnapshotProvider{MPool: mpool.MustNewZero(), DataDir: "shared", Relations: map[uint64]engine.Relation{42: relation}}
	_, err = provider.prepareSnapshotRead(context.Background(), read, make([]byte, types.TxnTsSize), len(one))
	require.ErrorContains(t, err, "manifest exceeds maximum")
	require.True(t, substrait.IsNotEligible(err))
	reason, ok := substrait.NotEligibleReason(err)
	require.True(t, ok)
	require.Equal(t, substrait.EligibilitySnapshot, reason)
	require.Equal(t, 2, relation.visits)
	require.Zero(t, relation.starCalls)
}

func testTableDef() *planpb.TableDef {
	return &planpb.TableDef{DbId: 7, TblId: 42, Version: 3, DbName: "db", Name: "t", TableType: "r", Cols: []*planpb.ColDef{{Name: "a", ColId: 11, Seqnum: 5, Typ: planpb.Type{Id: int32(types.T_int64)}}}}
}

func testRead(t *testing.T, def *planpb.TableDef) substrait.Read {
	schema, err := substrait.CanonicalSchema(def)
	require.NoError(t, err)
	return substrait.Read{AccountID: 9, DatabaseID: 7, TableID: def.TblId, SchemaVersion: def.Version, Columns: []substrait.ColumnMapping{{ColumnID: def.Cols[0].ColId, SequenceNumber: def.Cols[0].Seqnum}}, Schema: schema}
}

func objectStats(t *testing.T, rows uint32) objectio.ObjectStats {
	id := types.NewObjectid()
	s := objectio.NewObjectStatsWithObjectID(&id, false, true, false)
	require.NoError(t, objectio.SetObjectStatsRowCnt(s, rows))
	require.NoError(t, objectio.SetObjectStatsBlkCnt(s, 1))
	objectio.SetObjectStatsAppendable(s, false)
	return *s
}
