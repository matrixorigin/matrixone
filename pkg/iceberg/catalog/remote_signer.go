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
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	icebergio "github.com/matrixorigin/matrixone/pkg/iceberg/io"
)

const (
	defaultS3SignerEndpoint       = "v1/aws/s3/sign"
	defaultRemoteSignedRequestTTL = 15 * time.Minute
)

type RemoteSignerFactory interface {
	NewRemoteSigner(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner
}

func (c *RESTClient) NewRemoteSigner(req api.CatalogRequest, config map[string]string) icebergio.RemoteSigner {
	return restRemoteSigner{
		client:  c,
		req:     req,
		config:  cloneStringMap(config),
		nowFunc: time.Now,
	}
}

type restRemoteSigner struct {
	client  *RESTClient
	req     api.CatalogRequest
	config  map[string]string
	nowFunc func() time.Time
}

func (s restRemoteSigner) Sign(ctx context.Context, method, location string) (icebergio.SignedRequest, error) {
	if s.client == nil {
		return icebergio.SignedRequest{}, api.NewError(api.ErrRemoteSigningDenied, "Iceberg REST remote signer requires client", nil)
	}
	target, err := remoteSignerURL(s.config, s.client.allowPlainHTTP)
	if err != nil {
		return icebergio.SignedRequest{}, err
	}
	uri, region, err := icebergio.BuildS3RemoteSigningRequestURI(ctx, location, s.config)
	if err != nil {
		return icebergio.SignedRequest{}, err
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		method = http.MethodGet
	}
	payload := remoteSignRequestWire{
		Method:  method,
		Region:  region,
		URI:     uri,
		Headers: map[string][]string{},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return icebergio.SignedRequest{}, api.WrapError(api.ErrRemoteSigningDenied, "Iceberg REST remote signing request could not be encoded", nil, err)
	}
	req := s.client.normalizeRequest(s.req)
	raw, err := s.client.doJSON(ctx, "remote_sign", req, http.MethodPost, target, nil, body)
	if err != nil {
		return icebergio.SignedRequest{}, err
	}
	var wire remoteSignResponseWire
	if err := decodeJSON(raw.body, &wire, "remote_sign"); err != nil {
		return icebergio.SignedRequest{}, err
	}
	signed := icebergio.SignedRequest{
		URL:       strings.TrimSpace(wire.URI),
		Headers:   flattenHeaderMap(wire.Headers),
		ExpiresAt: parseRemoteSignExpiresAt(wire, s.now()),
	}
	return signed, nil
}

func (s restRemoteSigner) now() time.Time {
	if s.nowFunc != nil {
		return s.nowFunc()
	}
	return time.Now()
}

func remoteSignerURL(config map[string]string, allowPlainHTTP bool) (string, error) {
	cfg := normalizeStringMap(config)
	rawBase := firstNonEmpty(cfg["s3.signer.uri"], cfg["uri"])
	if rawBase == "" {
		return "", api.NewError(api.ErrRemoteSigningDenied, "Iceberg remote signing config requires signer URI", nil)
	}
	base, err := url.Parse(rawBase)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", api.WrapError(api.ErrConfigInvalid, "Iceberg remote signing URI is invalid", map[string]string{"uri": rawBase}, err)
	}
	switch strings.ToLower(base.Scheme) {
	case "https":
	case "http":
		if !allowPlainHTTP {
			return "", api.NewError(api.ErrConfigInvalid, "Iceberg remote signing URI must use https unless plain HTTP is explicitly enabled", map[string]string{"uri": rawBase})
		}
	default:
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg remote signing URI must use https", map[string]string{"uri": rawBase})
	}
	endpoint := strings.TrimSpace(firstNonEmpty(cfg["s3.signer.endpoint"], defaultS3SignerEndpoint))
	if endpoint == "" {
		endpoint = defaultS3SignerEndpoint
	}
	if endpointURL, err := url.Parse(endpoint); err == nil && endpointURL.IsAbs() {
		return "", api.NewError(api.ErrConfigInvalid, "Iceberg remote signing endpoint must be relative", map[string]string{"endpoint": endpoint})
	}
	base.RawQuery = ""
	base.Fragment = ""
	joined := strings.TrimRight(base.String(), "/") + "/" + strings.TrimLeft(endpoint, "/")
	return joined, nil
}

func flattenHeaderMap(in map[string][]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, values := range in {
		if strings.TrimSpace(key) == "" || len(values) == 0 {
			continue
		}
		cleaned := make([]string, 0, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) != "" {
				cleaned = append(cleaned, strings.TrimSpace(value))
			}
		}
		if len(cleaned) > 0 {
			out[key] = strings.Join(cleaned, ", ")
		}
	}
	return out
}

func parseRemoteSignExpiresAt(wire remoteSignResponseWire, now time.Time) time.Time {
	for _, value := range []string{wire.ExpiresAt, wire.ExpiresAtAlt, wire.Expiration, wire.ExpirationAlt} {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ts, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return ts
		}
		if ms, err := strconv.ParseInt(value, 10, 64); err == nil && ms > 0 {
			return time.UnixMilli(ms).UTC()
		}
	}
	return now.Add(defaultRemoteSignedRequestTTL)
}

func normalizeStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return out
}

type remoteSignRequestWire struct {
	Method  string              `json:"method"`
	Region  string              `json:"region"`
	URI     string              `json:"uri"`
	Headers map[string][]string `json:"headers"`
}

type remoteSignResponseWire struct {
	URI           string              `json:"uri"`
	Headers       map[string][]string `json:"headers"`
	ExpiresAt     string              `json:"expires-at"`
	ExpiresAtAlt  string              `json:"expiresAt"`
	Expiration    string              `json:"expiration"`
	ExpirationAlt string              `json:"expires_at"`
}

var _ icebergio.RemoteSigner = restRemoteSigner{}
var _ RemoteSignerFactory = (*RESTClient)(nil)
