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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestViewMetadataCacheRetainsScanSnapshot(t *testing.T) {
	snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 10}}
	ref := &plan.ObjectRef{SchemaName: "db", ObjName: "src", Obj: 7}
	def := &plan.TableDef{Name: "src", DbId: 2, TblId: 7, Version: 3}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		NodeType: plan.Node_TABLE_SCAN, ObjRef: ref, TableDef: def, ScanSnapshot: snapshot,
	}}}}}
	changed, err := checkModify(p, func(db, name string, got *plan2.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		require.Equal(t, "db", db)
		require.Equal(t, "src", name)
		require.Equal(t, snapshot, got, "a current object must not replace a captured historical dependency")
		return ref, def, nil
	})
	require.NoError(t, err)
	require.False(t, changed)
}

func TestViewMetadataCacheValidatesResolutionIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*plan.ObjectRef, *plan.TableDef)
		wantChange bool
	}{
		{name: "unchanged"},
		{name: "database replaced", mutate: func(_ *plan.ObjectRef, d *plan.TableDef) { d.DbId++ }, wantChange: true},
		{name: "publisher remapped", mutate: func(r *plan.ObjectRef, _ *plan.TableDef) {
			r.PubInfo = &plan.PubInfo{TenantId: r.PubInfo.TenantId + 1}
		}, wantChange: true},
		{name: "no longer a subscription", mutate: func(r *plan.ObjectRef, _ *plan.TableDef) { r.PubInfo = nil }, wantChange: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref := &plan.ObjectRef{SchemaName: "publisher_db", SubscriptionName: "sub", ObjName: "v",
				Db: 2, Obj: 7, Server: 3, PubInfo: &plan.PubInfo{TenantId: 42}}
			current := plan2.DeepCopyObjectRef(ref)
			def := &plan.TableDef{DbId: 2, TblId: 7, Version: 3}
			if tc.mutate != nil {
				tc.mutate(current, def)
			}
			p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{CatalogDependencies: []*plan.ObjectRef{ref}}}}
			changed, err := checkModify(p, func(db, name string, _ *plan2.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
				require.Equal(t, "sub", db)
				require.Equal(t, "v", name)
				return current, def, nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.wantChange, changed)
		})
	}
}

func TestViewMetadataCacheRebindsPublisherOnlyDependencies(t *testing.T) {
	// A SHOW description can bind a publisher source that is not itself in
	// the publication. Resolving its physical name in the subscriber is wrong;
	// without an account-aware resolver, rebuild in the isolated binder.
	ref := &plan.ObjectRef{SchemaName: "publisher_db", ObjName: "src", PubInfo: &plan.PubInfo{TenantId: 42}}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{CatalogDependencies: []*plan.ObjectRef{ref}}}}
	changed, err := checkModify(p, func(string, string, *plan2.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		t.Fatal("must not resolve the publisher's physical database in the subscriber")
		return nil, nil, nil
	})
	require.NoError(t, err)
	require.True(t, changed)
}

func TestViewMetadataCacheDeduplicatesResolutionNotEvidence(t *testing.T) {
	for _, tc := range []struct {
		name              string
		differentSnapshot bool
		differentVersion  bool
		wantCalls         int
	}{
		{name: "scan and copied dependency", wantCalls: 1},
		{name: "distinct historical snapshot", differentSnapshot: true, wantCalls: 2},
		{name: "same lookup but changed expected version", differentVersion: true, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := &plan.Snapshot{TS: &timestamp.Timestamp{PhysicalTime: 10}}
			ref := &plan.ObjectRef{SchemaName: "db", ObjName: "src", Db: 2, Obj: 7, Server: 3, Snapshot: snapshot}
			dependency := plan2.DeepCopyObjectRef(ref)
			if tc.differentSnapshot {
				dependency.Snapshot.TS.PhysicalTime++
			}
			if tc.differentVersion {
				dependency.Server++
			}
			def := &plan.TableDef{Name: "src", DbId: 2, TblId: 7, Version: 3}
			p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
				Nodes:               []*plan.Node{{NodeType: plan.Node_TABLE_SCAN, ObjRef: ref, TableDef: def, ScanSnapshot: snapshot}},
				CatalogDependencies: []*plan.ObjectRef{dependency},
			}}}
			calls := 0
			for i := 1; i <= 2; i++ {
				changed, err := checkModify(p, func(string, string, *plan2.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
					calls++
					return ref, def, nil
				})
				require.NoError(t, err)
				require.Equal(t, tc.differentVersion, changed)
				require.Equal(t, i*tc.wantCalls, calls, "lookup sharing is request-local and cannot erase a distinct snapshot or version")
			}
		})
	}
}

func TestViewMetadataCacheRejectsNewPublisherIdentity(t *testing.T) {
	ref := &plan.ObjectRef{SchemaName: "db", ObjName: "v", Db: 2, Obj: 7, Server: 3}
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{CatalogDependencies: []*plan.ObjectRef{ref}}}}
	changed, err := checkModify(p, func(string, string, *plan2.Snapshot) (*plan.ObjectRef, *plan.TableDef, error) {
		current := plan2.DeepCopyObjectRef(ref)
		current.PubInfo = &plan.PubInfo{TenantId: 42}
		return current, &plan.TableDef{DbId: 2, TblId: 7, Version: 3}, nil
	})
	require.NoError(t, err)
	require.True(t, changed, "matching numeric IDs cannot hide a publisher identity change")
}
