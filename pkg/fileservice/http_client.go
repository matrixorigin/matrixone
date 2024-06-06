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
	connectTimeout      = time.Second * 3
	readWriteTimeout    = time.Second * 2
	maxIdleConns        = 100
	maxIdleConnsPerHost = 100
	maxConnsPerHost     = 1000
	idleConnTimeout     = 180 * time.Second
)

var dnsResolver = dns.NewCachingResolver(
	net.DefaultResolver,
	dns.MaxCacheEntries(128),
)

func newHTTPClient(args ObjectStorageArguments) *http.Client {

	// dialer
	dialer := &net.Dialer{
		Timeout:   connectTimeout,
		KeepAlive: 5 * time.Second,
		Resolver:  dnsResolver,
	}

	// transport
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		MaxIdleConns:          maxIdleConns,
		IdleConnTimeout:       idleConnTimeout,
		MaxIdleConnsPerHost:   maxIdleConnsPerHost,
		MaxConnsPerHost:       maxConnsPerHost,
		TLSHandshakeTimeout:   connectTimeout,
		ResponseHeaderTimeout: readWriteTimeout,
		ExpectContinueTimeout: readWriteTimeout,
		ForceAttemptHTTP2:     true,
	}

	// custom certs
	if len(args.CertFiles) > 0 {
		// custom certs
		pool, err := x509.SystemCertPool()
		if err != nil {
			panic(err)
		}
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
			pool.AppendCertsFromPEM(content)
		}
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
			RootCAs:            pool,
		}
		transport.TLSClientConfig = tlsConfig
	}

	// client
	client := &http.Client{
		Transport: transport,
	}

	return client
}
