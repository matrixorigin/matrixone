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

	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
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
