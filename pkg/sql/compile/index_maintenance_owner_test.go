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

package compile

import (
	"context"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

type maintenanceOwnerExecutor struct {
	executor.SQLExecutor
	t            *testing.T
	called       bool
	metadataName string
	storageName  string
	metadataID   uint64
	storageID    uint64
}

func (e *maintenanceOwnerExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	e.called = true
	spec := opts.IndexMaintenanceSpec()
	require.NotNil(e.t, spec)
	require.Len(e.t, spec.Targets, 2, "entries are not consumed by ivf_create")
	require.Equal(e.t, uint64(10), spec.BaseID)
	targets := make(map[string]executor.IndexMaintenanceTarget, len(spec.Targets))
	for _, target := range spec.Targets {
		targets[target.Role] = target
	}
	require.Equal(e.t, executor.IndexMaintenanceTarget{Role: catalog.SystemSI_IVFFLAT_TblType_Metadata, Name: e.metadataName, ID: e.metadataID}, targets[catalog.SystemSI_IVFFLAT_TblType_Metadata])
	require.Equal(e.t, executor.IndexMaintenanceTarget{Role: catalog.SystemSI_IVFFLAT_TblType_Centroids, Name: e.storageName, ID: e.storageID}, targets[catalog.SystemSI_IVFFLAT_TblType_Centroids])
	config := fmt.Sprintf(`{"db":"db","src":"source","metadata":%q,"index":%q}`, e.metadataName, e.storageName)
	require.NoError(e.t, opts.IndexMaintenanceValidator().ValidateIndexMaintenance("ivf_create", []string{"{}", config}, spec))
	require.Error(e.t, opts.IndexMaintenanceValidator().ValidateIndexMaintenance("ivf_create", []string{"{}", `{"db":"db","src":"source","metadata":"forged","index":"cent"}`}, spec))
	return executor.Result{}, nil
}

func TestIndexMaintenanceOwnerDerivesRoles(t *testing.T) {
	for _, scenario := range []string{"current", "create plan", "missing role", "duplicate role", "wrong parent", "cancelled", "mixed case", "wrong case", "distinct Unicode names"} {
		t.Run(scenario, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			db := mock_frontend.NewMockDatabase(ctrl)
			proc := testutil.NewProcess(t)
			proc.Ctx = defines.AttachAccountId(proc.Ctx, 0)
			if scenario == "cancelled" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				cancel()
				proc.Ctx = ctx
			}
			defs := []*plan.IndexDef{}
			for role, name := range map[string]string{catalog.SystemSI_IVFFLAT_TblType_Metadata: "meta", catalog.SystemSI_IVFFLAT_TblType_Centroids: "cent", catalog.SystemSI_IVFFLAT_TblType_Entries: "entries"} {
				defs = append(defs, &plan.IndexDef{IndexName: "idx", IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), IndexAlgoTableType: role, IndexTableName: name})
			}
			indexName, ownedName := "idx", "idx"
			if scenario == "mixed case" {
				indexName, ownedName = "MixedCaseIdx", "MixedCaseIdx"
			}
			if scenario == "wrong case" {
				indexName, ownedName = "mixedcaseidx", "MixedCaseIdx"
			}
			if scenario == "distinct Unicode names" {
				indexName, ownedName = "Σ", "Σ"
			}
			for _, def := range defs {
				def.IndexName = ownedName
			}
			if scenario == "distinct Unicode names" {
				for _, def := range append([]*plan.IndexDef(nil), defs...) {
					other := *def
					other.IndexName = "ς"
					other.IndexTableName = "other_" + def.IndexTableName
					defs = append(defs, &other)
				}
			}

			if scenario == "missing role" {
				kept := defs[:0]
				for _, def := range defs {
					if def.IndexAlgoTableType != catalog.SystemSI_IVFFLAT_TblType_Centroids {
						kept = append(kept, def)
					}
				}
				defs = kept
			}
			if scenario == "duplicate role" {
				defs = append(defs, defs[0])
			}
			owner := &pluginCompileCtx{c: &Compile{proc: proc}, dbSource: db, qryDatabase: "db", mainTableID: 10, originalTableDef: &plan.TableDef{Name: "source", Indexes: defs}}
			if scenario == "create plan" {
				owner.originalTableDef.Indexes = nil
				owner.indexInfo = &plan.CreateTable{TableDef: &plan.TableDef{Indexes: defs}}
			}
			db.EXPECT().GetDatabaseId(gomock.Any()).Return("8").AnyTimes()
			db.EXPECT().Relation(gomock.Any(), gomock.Any(), nil).DoAndReturn(func(ctx context.Context, name string, _ any) (engine.Relation, error) {
				require.Equal(t, proc.Ctx, ctx)
				if scenario == "cancelled" {
					return nil, ctx.Err()
				}
				rel := mock_frontend.NewMockRelation(ctrl)
				rel.EXPECT().GetTableID(gomock.Any()).Return(map[string]uint64{"source": 10, "meta": 11, "cent": 12, "other_meta": 21, "other_cent": 22}[name]).AnyTimes()
				rel.EXPECT().GetTableName().Return(name).AnyTimes()
				kind := catalog.SystemSI_IVFFLAT_TblType_Metadata
				if name == "cent" || name == "other_cent" {
					kind = catalog.SystemSI_IVFFLAT_TblType_Centroids
				}
				rel.EXPECT().GetTableDef(gomock.Any()).Return(&plan.TableDef{Name: name, TableType: kind}).AnyTimes()
				parent := uint64(10)
				if scenario == "wrong parent" {
					parent = 99
				}
				rel.EXPECT().GetExtraInfo().Return(&api.SchemaExtra{ParentTableID: parent}).AnyTimes()
				return rel, nil
			}).AnyTimes()
			exec := &maintenanceOwnerExecutor{t: t, metadataName: "meta", storageName: "cent", metadataID: 11, storageID: 12}
			installDropDDLExecutor(t, proc, exec)
			err := owner.RunIndexMaintenanceSQL("select maintenance", "ivf_create", indexName)
			if scenario == "current" || scenario == "create plan" || scenario == "mixed case" || scenario == "distinct Unicode names" {
				require.NoError(t, err)
				require.True(t, exec.called)
			} else {
				require.Error(t, err)
				require.False(t, exec.called)
			}
			if scenario == "cancelled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if scenario == "wrong case" {
				require.ErrorContains(t, err, "missing index maintenance plugin")
			}
			if scenario == "distinct Unicode names" {
				exec.called = false
				exec.metadataName, exec.storageName = "other_meta", "other_cent"
				exec.metadataID, exec.storageID = 21, 22
				require.NoError(t, owner.RunIndexMaintenanceSQL("select maintenance", "ivf_create", "ς"))
				require.True(t, exec.called)
			}
		})
	}
}
