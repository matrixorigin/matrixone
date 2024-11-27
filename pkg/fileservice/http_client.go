// Copyright 2024 Matrix Origin
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

package fileservice

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/ncruces/go-dns"
	"go.uber.org/zap"
)

var (
	connectTimeout      = time.Second * 5
	readWriteTimeout    = time.Second * 20
	maxIdleConns        = 100
	maxIdleConnsPerHost = 100
	maxConnsPerHost     = 100
	idleConnTimeout     = 10 * time.Second
)

var dnsResolver = dns.NewCachingResolver(
	nil,
	dns.MaxCacheEntries(128),
)

func init() {
	net.DefaultResolver = dnsResolver
	http.DefaultTransport = httpTransport
}

var httpDialer = &net.Dialer{
	Timeout:  connectTimeout,
	Resolver: dnsResolver,
}

var httpTransport = wrapRoundTripper(&http.Transport{
	DialContext:           wrapDialContext(httpDialer.DialContext),
	MaxIdleConns:          maxIdleConns,
	IdleConnTimeout:       idleConnTimeout,
	MaxIdleConnsPerHost:   maxIdleConnsPerHost,
	MaxConnsPerHost:       maxConnsPerHost,
	TLSHandshakeTimeout:   connectTimeout,
	ResponseHeaderTimeout: readWriteTimeout,
	TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
		RootCAs:            caPool,
	},
	Proxy: http.ProxyFromEnvironment,
})

var caPool = func() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		panic(err)
	}
	return pool
}()

func newHTTPClient(args ObjectStorageArguments) *http.Client {

	// custom certs
	if len(args.CertFiles) > 0 {
		// custom certs
		for _, path := range args.CertFiles {
			content, err := os.ReadFile(path)
			if err != nil {
				logutil.Info("load cert file error",
					zap.Any("err", err),
				)
				// ignore
				continue
			}
			logutil.Info("file service: load cert file",
				zap.Any("path", path),
			)
			caPool.AppendCertsFromPEM(content)
		}
	}

	// client
	client := &http.Client{
		Transport: httpTransport,
	}

	return client
}
