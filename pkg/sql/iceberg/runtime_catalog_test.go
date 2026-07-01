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

package iceberg

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

func TestResolveRuntimeCatalogRequestPrefixForWriteRefRewritesNessieBranch(t *testing.T) {
	client := &catalog.MockClient{
		GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
			return &api.ConfigResponse{
				Prefix:    "main|s3://warehouse",
				Overrides: map[string]string{"nessie.is-nessie-catalog": "true"},
			}, nil
		},
	}

	req, targetRef, targetRefType, err := resolveRuntimeCatalogRequestPrefixForWriteRef(context.Background(), client, api.CatalogRequest{
		Catalog: model.Catalog{Warehouse: "s3://warehouse"},
	}, "publish_branch", api.CatalogCapabilities{}, false)
	require.NoError(t, err)
	require.Equal(t, "publish_branch|s3://warehouse", req.Prefix)
	require.Equal(t, "main", targetRef)
	require.Equal(t, "branch", targetRefType)
}

func TestResolveRuntimeCatalogRequestPrefixForWriteRefRejectsTag(t *testing.T) {
	client := &catalog.MockClient{
		GetConfigFunc: func(ctx context.Context, req api.GetConfigRequest) (*api.ConfigResponse, error) {
			return &api.ConfigResponse{
				Prefix:    "main|s3://warehouse",
				Overrides: map[string]string{"nessie.is-nessie-catalog": "true"},
			}, nil
		},
	}

	_, _, _, err := resolveRuntimeCatalogRequestPrefixForWriteRef(context.Background(), client, api.CatalogRequest{
		Catalog: model.Catalog{Warehouse: "s3://warehouse"},
	}, "tag:release", api.CatalogCapabilities{}, false)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "read-only"), err.Error())
}
