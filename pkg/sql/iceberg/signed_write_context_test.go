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

package iceberg

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergcatalog "github.com/matrixorigin/matrixone/pkg/iceberg/catalog"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	"github.com/stretchr/testify/require"
)

type recordingDeniedSigner struct{ methods []string }

func (s *recordingDeniedSigner) Sign(_ context.Context, method, _ string) (icebergio.SignedRequest, error) {
	s.methods = append(s.methods, method)
	return icebergio.SignedRequest{}, errors.New("signing denied")
}

func TestSignedWriteObjectIOContextSelectsMethodsAndPropagatesDenial(t *testing.T) {
	signer := &recordingDeniedSigner{}
	scopeForLocation := func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{CatalogID: 7, Endpoint: "s3.example.com", Region: "us-east-1", Bucket: "bucket", Principal: "writer", StorageLocation: location}
	}
	ioctx := signedWriteObjectIOContext(signer, scopeForLocation, nil, nil, false)
	scope := ioctx.ScopeForLocation("s3://bucket/table/data.parquet")
	_, _, err := ioctx.WriterProvider.Resolve(context.Background(), scope)
	require.Error(t, err)
	_, _, err = ioctx.ReaderProvider.Resolve(context.Background(), scope)
	require.Error(t, err)
	require.Equal(t, []string{http.MethodPut, http.MethodGet}, signer.methods)
}

type recordingSigningCatalog struct {
	icebergcatalog.MockClient
	signer icebergio.RemoteSigner
}

func (c *recordingSigningCatalog) NewRemoteSigner(api.CatalogRequest, map[string]string) icebergio.RemoteSigner {
	return c.signer
}

func TestWriteFactoriesPreserveSignedAndMissingCredentialBehavior(t *testing.T) {
	ctx := context.Background()
	catalog := model.Catalog{AccountID: 42, CatalogID: 7, Name: "catalog", URI: "https://catalog.example.com/rest"}
	req := api.CatalogRequest{Catalog: catalog}
	scopeForLocation := func(location string) icebergio.ObjectScope {
		return icebergio.ObjectScope{AccountID: 42, CatalogID: 7, Endpoint: "catalog.example.com", Region: "us-east-1", Bucket: "bucket", Principal: "writer", StorageLocation: location}
	}
	appendFactory := NewAppendRuntimeCoordinatorFactory(AppendRuntimeCoordinatorFactoryOptions{Store: &fakeAppendRuntimeStore{}, ScopeForLocation: scopeForLocation})
	dmlFactory := NewDMLDeleteRuntimeCoordinatorFactory(DMLDeleteRuntimeCoordinatorFactoryOptions{Store: &fakeDMLDeleteRuntimeStore{}, ScopeForLocation: scopeForLocation})
	signer := &recordingDeniedSigner{}
	client := &recordingSigningCatalog{signer: signer}
	response := &api.LoadTableResponse{Capabilities: api.CatalogCapabilities{RemoteSigning: true}}
	appendIO, err := appendFactory.objectIOContext(ctx, client, req, response, api.Namespace{"db"}, "tbl", catalog)
	require.NoError(t, err)
	dmlIO, ok, err := dmlFactory.dmlWriteObjectIOContext(ctx, client, req, response, api.Namespace{"db"}, "tbl", catalog)
	require.NoError(t, err)
	require.True(t, ok)
	for _, ioctx := range []appendObjectIOContext{appendIO, dmlIO} {
		scope := ioctx.ScopeForLocation("s3://bucket/table/file.parquet")
		_, _, err = ioctx.WriterProvider.Resolve(ctx, scope)
		require.Error(t, err)
		_, _, err = ioctx.ReaderProvider.Resolve(ctx, scope)
		require.Error(t, err)
	}
	require.Equal(t, []string{http.MethodPut, http.MethodGet, http.MethodPut, http.MethodGet}, signer.methods)
	_, err = appendFactory.objectIOContext(ctx, client, req, &api.LoadTableResponse{}, api.Namespace{"db"}, "tbl", catalog)
	require.Error(t, err)
	_, ok, err = dmlFactory.dmlWriteObjectIOContext(ctx, client, req, nil, api.Namespace{"db"}, "tbl", catalog)
	require.NoError(t, err)
	require.False(t, ok)
}
