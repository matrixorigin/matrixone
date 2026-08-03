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

package maintenance

import (
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/metadata"
)

func maintenanceManifestWriteOptions(meta *api.TableMetadata, specID int, content api.ManifestContent) (metadata.ManifestWriteOptions, error) {
	schema, err := currentMaintenanceSchema(meta)
	if err != nil {
		return metadata.ManifestWriteOptions{}, err
	}
	for _, spec := range meta.PartitionSpecs {
		if spec.SpecID == specID {
			return metadata.ManifestWriteOptions{
				FormatVersion: meta.FormatVersion,
				Schema:        schema,
				PartitionSpec: spec,
				Content:       content,
			}, nil
		}
	}
	return metadata.ManifestWriteOptions{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance manifest partition spec is missing", map[string]string{
		"spec_id": strconv.Itoa(specID),
	})
}

func currentMaintenanceSchema(meta *api.TableMetadata) (api.Schema, error) {
	if meta == nil {
		return api.Schema{}, api.NewError(api.ErrConfigInvalid, "Iceberg maintenance manifest writer requires table metadata", nil)
	}
	if meta.FormatVersion != 2 {
		return api.Schema{}, api.NewError(api.ErrUnsupportedFeature, "Iceberg manifest-writing maintenance requires a format v2 table", map[string]string{
			"format_version": strconv.Itoa(meta.FormatVersion),
		})
	}
	schema, ok := meta.CurrentSchema()
	if !ok {
		return api.Schema{}, api.NewError(api.ErrMetadataInvalid, "Iceberg maintenance manifest writer could not resolve current schema", nil)
	}
	return schema, nil
}
