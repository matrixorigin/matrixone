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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const (
	ManifestCommitAdapterNative = "matrixone"
	ManifestCommitAdapterMO     = "native"
)

type ManifestCommitAdapter interface {
	AdapterName() string
	BuildAppendManifests(ctx context.Context, req AppendManifestRequest) (*AppendManifestResult, error)
}

type NativeManifestCommitAdapter struct {
	Builder api.WriteBuilder
}

func (NativeManifestCommitAdapter) AdapterName() string {
	return ManifestCommitAdapterNative
}

func (a NativeManifestCommitAdapter) BuildAppendManifests(ctx context.Context, req AppendManifestRequest) (*AppendManifestResult, error) {
	return buildAppendManifests(ctx, req, a.Builder)
}

type UnsupportedManifestCommitAdapter struct {
	Name string
}

func (a UnsupportedManifestCommitAdapter) AdapterName() string {
	name := strings.TrimSpace(a.Name)
	if name == "" {
		return "unsupported"
	}
	return name
}

func (a UnsupportedManifestCommitAdapter) BuildAppendManifests(ctx context.Context, req AppendManifestRequest) (*AppendManifestResult, error) {
	return nil, api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest/commit adapter is not implemented", map[string]string{
		"adapter": a.AdapterName(),
	})
}

func SelectManifestCommitAdapter(name string, adapters ...ManifestCommitAdapter) ManifestCommitAdapter {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" || normalized == ManifestCommitAdapterNative || normalized == ManifestCommitAdapterMO {
		return NativeManifestCommitAdapter{}
	}
	for _, adapter := range adapters {
		if adapter == nil {
			continue
		}
		if strings.EqualFold(adapter.AdapterName(), normalized) {
			return adapter
		}
	}
	return UnsupportedManifestCommitAdapter{Name: normalized}
}

type ManifestAttemptBuilder struct {
	Adapter            ManifestCommitAdapter
	ManifestPath       string
	ManifestListPath   string
	SnapshotID         int64
	SequenceNumber     int64
	TimestampMS        int64
	PreservedManifests []api.ManifestFile
	LastResult         *AppendManifestResult
}

func (b *ManifestAttemptBuilder) BuildAppend(ctx context.Context, req api.AppendRequest) (*api.CommitAttempt, error) {
	if b == nil {
		return nil, api.NewError(api.ErrConfigInvalid, "Iceberg manifest attempt builder is nil", nil)
	}
	b.LastResult = nil
	adapter := b.Adapter
	if adapter == nil {
		adapter = NativeManifestCommitAdapter{}
	}
	result, err := adapter.BuildAppendManifests(ctx, AppendManifestRequest{
		Append:             req,
		SnapshotID:         b.SnapshotID,
		SequenceNumber:     b.SequenceNumber,
		TimestampMS:        b.TimestampMS,
		ManifestPath:       b.ManifestPath,
		ManifestListPath:   b.ManifestListPath,
		PreservedManifests: append([]api.ManifestFile(nil), b.PreservedManifests...),
	})
	if err != nil {
		return nil, err
	}
	b.LastResult = result
	return result.Attempt, nil
}

var _ ManifestCommitAdapter = NativeManifestCommitAdapter{}
var _ ManifestCommitAdapter = UnsupportedManifestCommitAdapter{}
var _ api.WriteBuilder = (*ManifestAttemptBuilder)(nil)
