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

package write

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/stretchr/testify/require"
)

func TestSelectManifestCommitAdapterAndUnsupported(t *testing.T) {
	require.Equal(t, ManifestCommitAdapterNative, SelectManifestCommitAdapter("").AdapterName())
	require.Equal(t, ManifestCommitAdapterNative, SelectManifestCommitAdapter("native").AdapterName())
	require.Equal(t, ManifestCommitAdapterNative, SelectManifestCommitAdapter("matrixone").AdapterName())

	custom := &fakeManifestAdapter{name: "custom"}
	require.Same(t, custom, SelectManifestCommitAdapter(" CUSTOM ", nil, custom))

	unsupported := SelectManifestCommitAdapter("missing")
	require.Equal(t, "missing", unsupported.AdapterName())
	_, err := unsupported.BuildAppendManifests(context.Background(), AppendManifestRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrUnsupportedFeature))
	require.Equal(t, "unsupported", UnsupportedManifestCommitAdapter{}.AdapterName())
}

func TestManifestAttemptBuilderUsesAdapterAndClonesPreservedManifests(t *testing.T) {
	adapter := &fakeManifestAdapter{name: "custom"}
	builder := &ManifestAttemptBuilder{
		Adapter:            adapter,
		ManifestPath:       "s3://warehouse/metadata/m.avro",
		ManifestListPath:   "s3://warehouse/metadata/snap.avro",
		SnapshotID:         99,
		SequenceNumber:     12,
		TimestampMS:        12345,
		PreservedManifests: []api.ManifestFile{{Path: "s3://warehouse/metadata/old.avro"}},
	}
	attempt, err := builder.BuildAppend(context.Background(), appendRequest())
	require.NoError(t, err)
	require.Equal(t, int64(99), attempt.BaseSnapshotID)
	require.Same(t, builder.LastResult.Attempt, attempt)
	require.Equal(t, "s3://warehouse/metadata/old.avro", adapter.request.PreservedManifests[0].Path)
	builder.PreservedManifests[0].Path = "mutated"
	require.Equal(t, "s3://warehouse/metadata/old.avro", adapter.request.PreservedManifests[0].Path)

	_, err = (*ManifestAttemptBuilder)(nil).BuildAppend(context.Background(), appendRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrConfigInvalid))
}

type fakeManifestAdapter struct {
	name    string
	request AppendManifestRequest
}

func (a *fakeManifestAdapter) AdapterName() string {
	return a.name
}

func (a *fakeManifestAdapter) BuildAppendManifests(ctx context.Context, req AppendManifestRequest) (*AppendManifestResult, error) {
	a.request = req
	return &AppendManifestResult{Attempt: &api.CommitAttempt{BaseSnapshotID: req.SnapshotID}}, nil
}
