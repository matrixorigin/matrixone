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
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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
	http.DefaultTransport = httpRoundTripper
}

var httpDialer = &net.Dialer{
	Timeout:  connectTimeout,
	Resolver: dnsResolver,
}

// activeConnMap tracks connections that are currently in use (active)
// Key: connection object (net.Conn), Value: last active time (time.Time)
// Used to deduplicate: same connection may be reused multiple times (GotConn called multiple times)
var activeConnMap sync.Map // map[net.Conn]time.Time

// trackConnActive marks a connection as active (in use)
// This is called when a connection is obtained and will be used
// Note: GotConn may be called multiple times for the same connection (when reused),
// so we use activeConnMap to deduplicate and only count each connection once
func trackConnActive(conn net.Conn) {
	if conn == nil {
		return
	}
	// Use connection object itself as key to identify unique connections
	// This is more accurate than using remote address (multiple connections can have same address)

	// Try to atomically load or store the connection
	_, loaded := activeConnMap.LoadOrStore(conn, time.Now())
	if !loaded {
		// New connection successfully stored
		v2.S3ConnActiveGauge.Inc()
		return
	}

	// Connection already exists, update timestamp atomically
	// Use LoadAndDelete + LoadOrStore to handle race condition where cleanupStaleConnections()
	// might delete the connection: if deleted, we re-increment metrics when re-adding.
	_, existed := activeConnMap.LoadAndDelete(conn)
	_, loaded = activeConnMap.LoadOrStore(conn, time.Now())
	if !loaded && !existed {
		// Connection was deleted by cleanup and re-added, increment metrics
		v2.S3ConnActiveGauge.Inc()
	} else if loaded {
		// Another goroutine added it, just update timestamp
		activeConnMap.Store(conn, time.Now())
	}
	// If !loaded && existed: we deleted and re-added existing connection, metrics already correct
}

// cleanupStaleConnections periodically cleans up connections that have been
// inactive for a long time (likely returned to idle pool or closed)
// This ensures the active connection count stays accurate
// staleThreshold should be aligned with the cleanup interval to catch connections
// that were closed by CloseIdleConnections() but still in activeConnMap
func cleanupStaleConnections() {
	now := time.Now()
	// Use a threshold slightly longer than the cleanup interval (5s) to catch
	// connections that were closed by CloseIdleConnections() but still tracked
	// This ensures metrics stay accurate after CloseIdleConnections() is called
	staleThreshold := 8 * time.Second // Slightly longer than cleanup interval (5s)

	activeConnMap.Range(func(key, value interface{}) bool {
		lastActive, ok := value.(time.Time)
		if !ok {
			activeConnMap.Delete(key)
			return true
		}

		// Check if connection is stale (inactive for too long)
		// Connections that have been inactive longer than the threshold are likely:
		// 1. Returned to idle pool and may have been closed by CloseIdleConnections()
		// 2. Actually closed by the transport's IdleConnTimeout
		// We can't directly check if a connection is closed, so we use inactivity time as a proxy
		if now.Sub(lastActive) > staleThreshold {
			// Connection has been inactive for too long, likely idle or closed
			activeConnMap.Delete(key)
			v2.S3ConnActiveGauge.Dec()
		}
		return true
	})
}

var httpTransport = &http.Transport{
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
}

func init() {
	// Note: Even though IdleConnTimeout, MaxIdleConnsPerHost, and MaxConnsPerHost
	// are configured, we still need to periodically close idle connections because:
	// 1. IdleConnTimeout only closes connections that have been idle for >10s
	// 2. MaxConnsPerHost limits total connections but doesn't actively close idle ones
	// 3. There may be edge cases where connections accumulate despite these settings
	//
	// We close idle connections and cleanup metrics together to ensure consistency:
	// - CloseIdleConnections() closes connections at the transport level
	// - cleanupStaleConnections() removes them from metrics tracking
	// This ensures metrics stay accurate after connections are closed.
	go func() {
		// Use a reasonable interval (5s) that's less aggressive than IdleConnTimeout (10s)
		// but frequent enough to control connection count
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			// Close idle connections to control connection count
			// This may close connections that are still in activeConnMap
			httpTransport.CloseIdleConnections()
			// Immediately cleanup stale connections to sync metrics with actual state
			// This ensures S3ConnActiveGauge stays accurate after CloseIdleConnections()
			cleanupStaleConnections()
		}
	}()
}

var httpRoundTripper = wrapRoundTripper(httpTransport)

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

	// use default transport if MaxConnsPerHost is not configured
	transport := httpRoundTripper
	if args.MaxConnsPerHost > 0 {
		// create a custom transport with configured MaxConnsPerHost
		customTransport := &http.Transport{
			DialContext:           wrapDialContext(httpDialer.DialContext),
			MaxIdleConns:          maxIdleConns,
			IdleConnTimeout:       idleConnTimeout,
			MaxIdleConnsPerHost:   maxIdleConnsPerHost,
			MaxConnsPerHost:       args.MaxConnsPerHost,
			TLSHandshakeTimeout:   connectTimeout,
			ResponseHeaderTimeout: readWriteTimeout,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
				RootCAs:            caPool,
			},
			Proxy: http.ProxyFromEnvironment,
		}
		transport = wrapRoundTripper(customTransport)
	}

	// client
	client := &http.Client{
		Transport: transport,
	}

	return client
}
