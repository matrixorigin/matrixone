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

package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/stretchr/testify/require"
)

func TestRemoteSignerURLValidationAndJoin(t *testing.T) {
	url, err := remoteSignerURL(map[string]string{
		" uri ":                 " https://catalog.example.com/base?token=raw#frag ",
		"s3.signer.endpoint":    " /v1/sign ",
		"irrelevant_config_key": "ignored",
	}, false)
	require.NoError(t, err)
	require.Equal(t, "https://catalog.example.com/base/v1/sign", url)

	url, err = remoteSignerURL(map[string]string{"uri": "http://localhost:19120"}, true)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:19120/v1/aws/s3/sign", url)

	for _, cfg := range []map[string]string{
		{},
		{"uri": "://bad"},
		{"uri": "http://catalog.example.com"},
		{"uri": "ftp://catalog.example.com"},
		{"uri": "https://catalog.example.com", "s3.signer.endpoint": "https://evil.example.com/sign"},
	} {
		_, err := remoteSignerURL(cfg, false)
		require.Error(t, err, cfg)
	}
}

func TestFlattenHeaderMapAndRemoteSignExpiry(t *testing.T) {
	headers := flattenHeaderMap(map[string][]string{
		"X-Test": {" first ", "", "second"},
		"":       {"ignored"},
		"Empty":  {},
	})
	require.Equal(t, map[string]string{"X-Test": "first, second"}, headers)
	require.Nil(t, flattenHeaderMap(nil))

	now := time.Unix(100, 0).UTC()
	rfc := now.Add(time.Hour).Format(time.RFC3339Nano)
	require.Equal(t, now.Add(time.Hour), parseRemoteSignExpiresAt(remoteSignResponseWire{ExpiresAt: rfc}, now))
	require.Equal(t, time.UnixMilli(123456).UTC(), parseRemoteSignExpiresAt(remoteSignResponseWire{ExpiresAtAlt: "123456"}, now))
	require.Equal(t, now.Add(defaultRemoteSignedRequestTTL), parseRemoteSignExpiresAt(remoteSignResponseWire{Expiration: "bad"}, now))
}

func TestRESTClientNewRemoteSignerClonesConfigAndHandlesNilClient(t *testing.T) {
	client := NewRESTClient(WithAllowPlainHTTP(true))
	cfg := map[string]string{"uri": "http://localhost:19120"}
	signer := client.NewRemoteSigner(api.CatalogRequest{}, cfg).(restRemoteSigner)
	cfg["uri"] = "http://mutated.example.com"
	url, err := remoteSignerURL(signer.config, true)
	require.NoError(t, err)
	require.Equal(t, "http://localhost:19120/v1/aws/s3/sign", url)
	require.NotZero(t, signer.now())

	_, err = (restRemoteSigner{}).Sign(context.Background(), "GET", "s3://bucket/data.parquet")
	require.Error(t, err)
	require.Contains(t, err.Error(), string(api.ErrRemoteSigningDenied))
}
