// Copyright 2021 - 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build darwin || linux

package frontend

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestConnectionProbeEstablishedTLS(t *testing.T) {
	server, client := newDisconnectTestTCPPair(t)
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	certificate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, certificate, certificate, public, private)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	serverTLS := tls.Server(server, &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}},
	})
	clientTLS := tls.Client(client, &tls.Config{RootCAs: roots, ServerName: "localhost"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error { return serverTLS.HandshakeContext(ctx) })
	group.Go(func() error { return clientTLS.HandshakeContext(ctx) })
	require.NoError(t, group.Wait())

	conn := &Conn{}
	conn.UseConn(serverTLS)
	payload := []byte{1, 0, 0, 0, byte(COM_PING)}
	_, err = clientTLS.Write(payload)
	require.NoError(t, err)
	closed, err := connectionPeerClosed(conn)
	require.NoError(t, err)
	require.False(t, closed)
	require.NoError(t, serverTLS.SetReadDeadline(time.Now().Add(5*time.Second)))
	got := make([]byte, len(payload))
	_, err = io.ReadFull(serverTLS, got)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	// Abort the transport like a killed client, without sending TLS close_notify.
	require.NoError(t, client.Close())
	requireDisconnectDetected(t, conn)
}
