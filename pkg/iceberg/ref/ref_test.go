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

package ref

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func TestParseNessieRefMapsBranchTagHashAndSnapshot(t *testing.T) {
	meta := &api.TableMetadata{Refs: map[string]api.SnapshotRef{
		"release": {SnapshotID: 10, Type: "tag"},
		"audit":   {SnapshotID: 11, Type: "branch"},
	}}
	spec, err := ParseNessieRef("release", meta)
	require.NoError(t, err)
	require.Equal(t, TypeTag, spec.Type)
	require.True(t, spec.ReadOnly)
	require.Equal(t, int64(10), spec.SnapshotID)

	spec, err = ParseNessieRef("branch:dev", meta)
	require.NoError(t, err)
	require.Equal(t, TypeBranch, spec.Type)
	require.Equal(t, "dev", spec.Name)

	spec, err = ParseNessieRef("hash:abc123", meta)
	require.NoError(t, err)
	require.Equal(t, TypeHash, spec.Type)
	require.True(t, spec.ReadOnly)

	spec, err = ParseNessieRef("snapshot:99", meta)
	require.NoError(t, err)
	require.Equal(t, TypeSnapshot, spec.Type)
	require.Equal(t, int64(99), spec.SnapshotID)
}

func TestValidateWriteAllowsBranchesAndRejectsReadOnlyRefs(t *testing.T) {
	require.NoError(t, ValidateWrite(Spec{Name: "dev", Type: TypeBranch}, api.CatalogCapabilities{}, false))

	err := ValidateWrite(Spec{Name: "release", Type: TypeTag}, api.CatalogCapabilities{BranchTag: true}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))

	require.NoError(t, ValidateWrite(Spec{Name: "release", Type: TypeTag}, api.CatalogCapabilities{BranchTag: true}, true))

	err = ValidateWrite(Spec{Name: "abc123", Type: TypeHash}, api.CatalogCapabilities{BranchTag: true}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")

	err = ValidateWrite(Spec{Name: "99", Type: TypeSnapshot}, api.CatalogCapabilities{BranchTag: true}, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only")
}

func TestApplyToAppendRequestAndRequirementUseTargetRef(t *testing.T) {
	req, err := ApplyToAppendRequest(api.AppendRequest{TargetRef: "main"}, Spec{Name: "publish", Type: TypeBranch}, api.CatalogCapabilities{}, false)
	require.NoError(t, err)
	require.Equal(t, "publish", req.TargetRef)

	requirement := CommitRequirement(Spec{Name: "publish", Type: TypeBranch}, 42)
	require.Equal(t, "assert-ref-snapshot-id", requirement.Type)
	require.Equal(t, "publish", requirement.Ref)
	require.Equal(t, int64(42), requirement.SnapshotID)
}

func TestRefreshCacheBuildsMOIRefRows(t *testing.T) {
	now := time.Unix(100, 0)
	rows := RefreshCache(1, 2, "sales", "orders", &api.TableMetadata{Refs: map[string]api.SnapshotRef{
		"main":    {SnapshotID: 7, Type: "branch"},
		"release": {SnapshotID: 6, Type: "tag"},
	}}, "nessie", now)
	require.Len(t, rows, 2)
	byName := map[string]string{}
	for _, row := range rows {
		byName[row.RefName] = row.RefType + ":" + row.SnapshotID
		require.Equal(t, uint32(1), row.AccountID)
		require.Equal(t, uint64(2), row.CatalogID)
		require.Equal(t, "nessie", row.Source)
		require.Equal(t, now, row.LastSeenAt)
	}
	require.Equal(t, "branch:7", byName["main"])
	require.Equal(t, "tag:6", byName["release"])
}
