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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
)

func TestValidatingCatalogClientForwardsCatalogMethods(t *testing.T) {
	ctx := context.Background()
	req := api.CatalogRequest{Catalog: model.Catalog{CatalogID: 7}}
	validatorCalls := 0
	upstream := &catalog.MockClient{}
	client := validatingCatalogClient{
		upstream: upstream,
		validator: func(ctx context.Context, got api.CatalogRequest) error {
			validatorCalls++
			require.Equal(t, uint64(7), got.Catalog.CatalogID)
			return nil
		},
	}

	_, err := client.GetConfig(ctx, api.GetConfigRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.ListNamespaces(ctx, api.ListNamespacesRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.ListTables(ctx, api.ListTablesRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.LoadTable(ctx, api.LoadTableRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.LoadCredentials(ctx, api.LoadCredentialsRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, api.CreateTableRequest{CatalogRequest: req})
	require.NoError(t, err)
	_, err = client.CommitTable(ctx, api.CommitRequest{CatalogRequest: req})
	require.NoError(t, err)

	require.Equal(t, 7, validatorCalls)
	require.Equal(t, []string{
		"GetConfig",
		"ListNamespaces",
		"ListTables",
		"LoadTable",
		"LoadCredentials",
		"CreateTable",
		"CommitTable",
	}, upstream.Calls)
}

func TestValidatingCatalogClientBlocksBeforeUpstream(t *testing.T) {
	ctx := context.Background()
	upstream := &catalog.MockClient{}
	client := validatingCatalogClient{
		upstream: upstream,
		validator: func(ctx context.Context, req api.CatalogRequest) error {
			return api.NewError(api.ErrResidencyDenied, "blocked", nil)
		},
	}

	_, err := client.LoadTable(ctx, api.LoadTableRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrResidencyDenied))
	require.Empty(t, upstream.Calls)
}

func TestValidatingCatalogClientNilValidatorAndRemoteSigner(t *testing.T) {
	ctx := context.Background()
	upstream := &catalog.MockClient{
		NewRemoteSignerFunc: func(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner {
			require.Equal(t, "rest", req.Catalog.Type)
			require.Equal(t, "v", config["k"])
			return recordingRemoteSigner{}
		},
	}
	client := validatingCatalogClient{upstream: upstream}
	_, err := client.GetConfig(ctx, api.GetConfigRequest{CatalogRequest: api.CatalogRequest{Catalog: model.Catalog{Type: "rest"}}})
	require.NoError(t, err)
	require.Equal(t, []string{"GetConfig"}, upstream.Calls)

	signer := client.NewRemoteSigner(api.CatalogRequest{Catalog: model.Catalog{Type: "rest"}}, map[string]string{"k": "v"})
	require.NotNil(t, signer)
	require.Equal(t, []string{"GetConfig", "NewRemoteSigner"}, upstream.Calls)

	noFactory := validatingCatalogClient{upstream: &catalog.MockClient{}}
	require.Nil(t, noFactory.NewRemoteSigner(api.CatalogRequest{}, nil))
}

func TestValidatingCatalogClientPlanScanRequiresScanPlanner(t *testing.T) {
	client := validatingCatalogClient{upstream: &catalog.MockClient{}}
	_, err := client.PlanScan(context.Background(), api.ScanPlanRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrServerPlanningRequired))
}

type recordingRemoteSigner struct{}

func (recordingRemoteSigner) Sign(context.Context, string, string) (icebergio.SignedRequest, error) {
	return icebergio.SignedRequest{}, nil
}
