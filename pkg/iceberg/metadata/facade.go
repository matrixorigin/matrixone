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

package metadata

import (
	"bytes"
	"context"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

const AdapterNativeMetadata = "native-metadata"

type NativeFacade struct{}

func (NativeFacade) AdapterName() string {
	return AdapterNativeMetadata
}

func (NativeFacade) ParseTableMetadata(ctx context.Context, data []byte, metadataLocation string) (*api.TableMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg metadata parse was canceled", map[string]string{"metadata_location": api.RedactPath(metadataLocation)}, err)
	}
	return ParseTableMetadata(data, metadataLocation)
}

func (NativeFacade) ReadManifestList(ctx context.Context, data []byte) ([]api.ManifestFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest list read was canceled", nil, err)
	}
	return ReadManifestList(data)
}

func (NativeFacade) ReadManifestListBounded(ctx context.Context, data []byte, maxRecords int) ([]api.ManifestFile, error) {
	return NativeFacade{}.ReadManifestListWithLimits(ctx, data, maxRecords, defaultOCFDecodedBytesLimit)
}

func (NativeFacade) ReadManifestListWithLimits(ctx context.Context, data []byte, maxRecords int, maxMemoryBytes int64) ([]api.ManifestFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest list read was canceled", nil, err)
	}
	allowance, err := ocfDecodedMemoryAllowance(maxMemoryBytes, len(data))
	if err != nil {
		return nil, err
	}
	return ReadManifestListFromReaderWithLimits(bytes.NewReader(data), maxRecords, allowance)
}

func (NativeFacade) ReadManifest(ctx context.Context, data []byte) ([]api.ManifestEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest read was canceled", nil, err)
	}
	return ReadManifest(data)
}

func (NativeFacade) ReadManifestBounded(ctx context.Context, data []byte, maxRecords int) ([]api.ManifestEntry, error) {
	return NativeFacade{}.ReadManifestWithLimits(ctx, data, maxRecords, defaultOCFDecodedBytesLimit)
}

func (NativeFacade) ReadManifestWithLimits(ctx context.Context, data []byte, maxRecords int, maxMemoryBytes int64) ([]api.ManifestEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg manifest read was canceled", nil, err)
	}
	allowance, err := ocfDecodedMemoryAllowance(maxMemoryBytes, len(data))
	if err != nil {
		return nil, err
	}
	return ReadManifestFromReaderWithLimits(bytes.NewReader(data), maxRecords, allowance)
}

func ocfDecodedMemoryAllowance(maxMemoryBytes int64, encodedBytes int) (int64, error) {
	if maxMemoryBytes <= 0 {
		maxMemoryBytes = defaultOCFDecodedBytesLimit
	}
	remaining := maxMemoryBytes - int64(encodedBytes)
	if remaining <= 1 {
		return 0, api.NewError(api.ErrPlanningLimitExceeded, "Iceberg Avro OCF has no memory available for decoding", nil)
	}
	// During decode the encoded object, one OCF block, and decoded Go values can
	// coexist. Spend only half the remaining budget on the block/record bytes;
	// the other half covers the decoded map/string/slice graph. A future facade
	// that visits typed records directly can replace this conservative split.
	return remaining / 2, nil
}

func (NativeFacade) ResolveSnapshot(ctx context.Context, meta *api.TableMetadata, selector api.SnapshotSelector) (api.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return api.Snapshot{}, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg snapshot resolve was canceled", nil, err)
	}
	return ResolveSnapshot(meta, selector)
}

func (NativeFacade) DetectUnsupportedP0(ctx context.Context, meta *api.TableMetadata, manifests []api.ManifestFile, files []api.DataFile) ([]api.UnsupportedFeature, error) {
	if err := ctx.Err(); err != nil {
		return nil, api.WrapError(api.ErrMetadataIOTimeout, "Iceberg feature detection was canceled", nil, err)
	}
	features := DetectUnsupportedP0Table(meta)
	for _, manifest := range manifests {
		if err := ValidateP0ManifestFile(manifest); err != nil {
			if icebergErr, ok := err.(*api.IcebergError); ok {
				features = append(features, api.UnsupportedFeature{Feature: icebergErr.Fields["features"], Reason: icebergErr.Message, Path: manifest.ManifestPathRedacted})
			} else {
				features = append(features, api.UnsupportedFeature{Feature: "manifest", Reason: err.Error(), Path: manifest.ManifestPathRedacted})
			}
		}
	}
	for _, file := range files {
		if err := ValidateP0DataFile(file); err != nil {
			if icebergErr, ok := err.(*api.IcebergError); ok {
				features = append(features, api.UnsupportedFeature{Feature: icebergErr.Fields["features"], Reason: icebergErr.Message, Path: file.FilePathRedacted})
			} else {
				features = append(features, api.UnsupportedFeature{Feature: "data-file", Reason: err.Error(), Path: file.FilePathRedacted})
			}
		}
	}
	return features, nil
}

var _ api.MetadataFacade = NativeFacade{}
var _ api.FeatureDetector = NativeFacade{}
