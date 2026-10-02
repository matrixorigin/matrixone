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

package foreigntvf

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	tpversion "github.com/elastic/elastic-transport-go/v8/elastictransport/version"
)

// newClient receives a config admitted by parseESQLConfig and selects only the two requests ESQL uses, instead of NewClient's
// registration of every Elasticsearch endpoint. connectESQL owns admission
// after the product check; elastictransport owns authentication and retries.
// The endpoint/header translation below matches go-elasticsearch v8.15.0 and
// is checked against its original constructor in esql_client_test.go.
func (c esqlConfig) newClient(transport *http.Transport) (*esqlHeaders, error) {
	addresses := c.Addresses
	if c.CloudID != "" {
		if len(addresses) != 0 {
			return nil, fmt.Errorf("cannot create client: both Addresses and CloudID are set")
		}
		values := strings.Split(c.CloudID, ":")
		if len(values) != 2 {
			return nil, fmt.Errorf("cannot parse CloudID: unexpected format: %q", c.CloudID)
		}
		data, err := base64.StdEncoding.DecodeString(values[1])
		if err != nil {
			return nil, fmt.Errorf("cannot parse CloudID: %w", err)
		}
		parts := strings.Split(string(data), "$")
		if len(parts) < 2 {
			return nil, fmt.Errorf("cannot parse CloudID: invalid encoded value: %s", parts)
		}
		addresses = []string{"https://" + parts[1] + "." + parts[0]}
	}
	urls := make([]*url.URL, 0, len(addresses))
	for _, address := range addresses {
		u, err := url.Parse(strings.TrimRight(address, "/"))
		if err != nil {
			return nil, fmt.Errorf("cannot parse url: %w", err)
		}
		urls = append(urls, u)
	}
	if urls[0].User != nil {
		c.Username = urls[0].User.Username()
		c.Password, _ = urls[0].User.Password()
	}
	goVersion := esqlGoVersion.ReplaceAllString(runtime.Version(), "$1")
	tp, err := elastictransport.New(elastictransport.Config{
		URLs:                   urls,
		Username:               c.Username,
		Password:               c.Password,
		APIKey:                 c.APIKey,
		ServiceToken:           c.ServiceToken,
		CertificateFingerprint: c.CertificateFingerprint,
		Transport:              transport,
		UserAgent:              fmt.Sprintf("go-elasticsearch/%s (%s %s; Go %s)", esqlClientVersion, runtime.GOOS, runtime.GOARCH, goVersion),
	})
	if err != nil {
		return nil, err
	}
	compatibility, _ := strconv.ParseBool(os.Getenv("ELASTIC_CLIENT_APIVERSIONING"))
	meta := fmt.Sprintf("es=%s,go=%s,t=%s,hc=%s", esqlStrippedVersion(esqlClientVersion), esqlStrippedVersion(runtime.Version()), esqlStrippedVersion(tpversion.Transport), esqlStrippedVersion(runtime.Version()))
	return &esqlHeaders{Client: tp, compatibility: compatibility, meta: meta}, nil
}

// Pinned with go.mod; wire-parity tests compare against the SDK version.
const esqlClientVersion = "8.15.0"

var (
	esqlGoVersion   = regexp.MustCompile(`go(\d+\.\d+\..+)`)
	esqlMetaVersion = regexp.MustCompile(`([0-9.]+)(.*)`)
)

func esqlStrippedVersion(version string) string {
	v := esqlMetaVersion.FindStringSubmatch(version)
	if len(v) != 3 || strings.Contains(version, "devel") {
		return "0.0p"
	}
	if v[2] != "" {
		return v[1] + "p"
	}
	return v[1]
}

// esqlHeaders preserves the SDK's constructor headers. All configuration is immutable; there is no additional request
// state, retry loop, connection pool, or product-check owner.
type esqlHeaders struct {
	*elastictransport.Client
	compatibility bool
	meta          string
}

func (t *esqlHeaders) Perform(req *http.Request) (*http.Response, error) {
	if t.compatibility {
		const compatibility = "application/vnd.elasticsearch+json;compatible-with=8"
		req.Header.Set("Accept", compatibility)
		if req.Body != nil {
			req.Header.Set("Content-Type", compatibility)
		}
	}
	meta := req.Header.Get("X-Elastic-Client-Meta")
	if meta != "" {
		meta = t.meta + "," + meta
	} else {
		meta = t.meta
	}
	req.Header.Set("X-Elastic-Client-Meta", meta)
	return t.Client.Perform(req)
}
