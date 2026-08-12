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

package cnservice

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type siriusTestProtector struct{}

type siriusTestJournal struct{}

func (siriusTestJournal) StoreIfCapacity(_ context.Context, leases []*substrait.Lease, _ int) (int, error) {
	return len(leases), nil
}
func (siriusTestJournal) Active(context.Context, *substrait.Lease) (bool, error) { return true, nil }
func (siriusTestJournal) MarkReleased(context.Context, []byte) error             { return nil }
func (siriusTestJournal) Delete(context.Context, []byte) error                   { return nil }
func (siriusTestJournal) Load(context.Context, func(*substrait.Lease) error) error {
	return nil
}

type siriusTestFlightService interface{}

type siriusTestFlightAction struct {
	Type string `protobuf:"bytes,1,opt,name=type,proto3"`
	Body []byte `protobuf:"bytes,2,opt,name=body,proto3"`
}

func (m *siriusTestFlightAction) Reset()       { *m = siriusTestFlightAction{} }
func (*siriusTestFlightAction) String() string { return "SiriusTestFlightAction" }
func (*siriusTestFlightAction) ProtoMessage()  {}

type siriusTestFlightResult struct {
	Body []byte `protobuf:"bytes,1,opt,name=body,proto3"`
}

func (m *siriusTestFlightResult) Reset()       { *m = siriusTestFlightResult{} }
func (*siriusTestFlightResult) String() string { return "SiriusTestFlightResult" }
func (*siriusTestFlightResult) ProtoMessage()  {}

func (siriusTestProtector) Begin(context.Context) (
	func(context.Context, []byte, []string, time.Time) error,
	func(context.Context, []byte) error,
	func(),
	error,
) {
	return func(context.Context, []byte, []string, time.Time) error { return nil },
		func(context.Context, []byte) error { return nil }, func() {}, nil
}

func (siriusTestProtector) Unregister(context.Context, []byte) error { return nil }

func TestSiriusInternalErrorfUsesMoerrAndPreservesCause(t *testing.T) {
	plain := siriusInternalErrorf("configuration failure")
	require.True(t, moerr.IsMoErrCode(plain, moerr.ErrInternal))

	cause := errors.New("certificate failure")
	wrapped := siriusInternalErrorf("load certificate: %w", cause)
	require.ErrorIs(t, wrapped, cause)
	require.ErrorContains(t, wrapped, "load certificate: certificate failure")
}

func TestSiriusConfigIsOptInAndFailClosed(t *testing.T) {
	var disabled SiriusConfig
	require.NoError(t, disabled.validate())

	enabled := SiriusConfig{Enabled: true}
	err := enabled.validate()
	require.ErrorContains(t, err, "missing Sirius flight-address")
	require.Equal(t, uint64(64<<20), enabled.MaxBatchBytes)
	require.Equal(t, 15*time.Minute, enabled.RequestTimeout.Duration)
	require.Equal(t, 30*time.Second, enabled.CleanupTimeout.Duration)
	require.Equal(t, 15*time.Minute+30*time.Second, enabled.LeaseTTL.Duration)

	enabled.FlightAddress = "sidecar:32010"
	enabled.FlightServerName = "sidecar.internal"
	enabled.FlightClientCertPath = "client.crt"
	enabled.FlightClientKeyPath = "client.key"
	enabled.FlightServerCAPath = "sidecar-ca.crt"
	enabled.ResolverAddress = "127.0.0.1:32011"
	enabled.ResolverServerCertPath = "resolver.crt"
	enabled.ResolverServerKeyPath = "resolver.key"
	enabled.ResolverClientCAPath = "resolver-client-ca.crt"
	enabled.ResolverClientCertPath = "sidecar-client.crt"
	enabled.DataDir = "/var/lib/matrixone"
	require.NoError(t, enabled.validate())

	enabled.LeaseTTL.Duration = enabled.RequestTimeout.Duration
	require.ErrorContains(t, enabled.validate(), "invalid Sirius transport limits")
	enabled.RequestTimeout.Duration = time.Duration(1 << 62)
	enabled.CleanupTimeout.Duration = time.Duration(1 << 62)
	enabled.LeaseTTL.Duration = substrait.MaxLeaseTTL
	require.ErrorContains(t, enabled.validate(), "invalid Sirius transport limits")
}

func TestSiriusTLSLoadersAndStartupCleanup(t *testing.T) {
	certPath, keyPath, spkiHash := writeSiriusTestCertificate(t)
	config := SiriusConfig{
		Enabled: true, FlightAddress: "127.0.0.1:1", FlightServerName: "localhost",
		FlightClientCertPath: certPath, FlightClientKeyPath: keyPath, FlightServerCAPath: certPath,
		ResolverAddress: "127.0.0.1:0", ResolverServerCertPath: certPath, ResolverServerKeyPath: keyPath,
		ResolverClientCAPath: certPath, ResolverClientCertPath: certPath, DataDir: t.TempDir(),
		MaxBatchBytes: 1 << 20,
	}
	config.RequestTimeout.Duration = 20 * time.Millisecond
	config.CleanupTimeout.Duration = time.Second
	config.LeaseTTL.Duration = time.Minute

	clientTLS, err := loadSiriusClientTLS(config)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), clientTLS.MinVersion)
	require.Equal(t, "localhost", clientTLS.ServerName)
	require.Len(t, clientTLS.Certificates, 1)
	require.NotNil(t, clientTLS.RootCAs)

	resolverTLS, err := loadSiriusResolverTLS(config)
	require.NoError(t, err)
	require.Equal(t, uint16(tls.VersionTLS12), resolverTLS.MinVersion)
	require.Len(t, resolverTLS.Certificates, 1)
	require.NotNil(t, resolverTLS.ClientCAs)

	actualHash, err := loadCertificateSPKIHash(certPath)
	require.NoError(t, err)
	require.Equal(t, spkiHash, actualHash)

	leases := substrait.NewPersistentLeaseManager(1, siriusTestProtector{}, siriusTestJournal{})
	require.NoError(t, leases.Replay(context.Background()))
	auditor := substrait.ResolveAuditFunc(func(context.Context, substrait.ResolveAuditEvent) error { return nil })
	s := &service{cfg: &Config{UUID: "sirius-startup-test", Sirius: config}}
	WithSiriusReadDependencies(leases, auditor)(s)
	require.Same(t, leases, s.options.siriusLeases)
	require.NotNil(t, s.options.siriusAuditor)
	require.ErrorContains(t, s.startSiriusRuntime(context.Background()), "negotiate capabilities")
	require.Nil(t, s.siriusRuntime)
	require.NoError(t, s.closeSiriusRuntime())

	config.FlightAddress = startSiriusTestFlightServer(t, certPath, keyPath)
	serviceID := "sirius-startup-success-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.NewRuntime(metadata.ServiceType_CN, serviceID, nil))
	s = &service{cfg: &Config{UUID: serviceID, Sirius: config}}
	WithSiriusReadDependencies(leases, auditor)(s)
	require.NoError(t, s.startSiriusRuntime(context.Background()))
	require.NotNil(t, s.siriusRuntime)
	value, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(compile.SiriusRuntimeKey)
	require.True(t, ok)
	require.Same(t, s.siriusRuntime, value)
	require.NoError(t, s.closeSiriusRuntime())
	require.Nil(t, s.siriusRuntime)
	_, ok = moruntime.ServiceRuntime(serviceID).GetGlobalVariables(compile.SiriusRuntimeKey)
	require.False(t, ok)
}

func TestSiriusTLSLoadersRejectInvalidFilesAndDependencies(t *testing.T) {
	invalid := filepath.Join(t.TempDir(), "invalid.pem")
	require.NoError(t, os.WriteFile(invalid, []byte("not a certificate"), 0o600))
	_, err := loadCertificatePool(invalid)
	require.ErrorContains(t, err, "no CA certificates")
	_, err = loadCertificateSPKIHash(invalid)
	require.ErrorContains(t, err, "not PEM")

	missing := filepath.Join(t.TempDir(), "missing.pem")
	_, err = loadCertificatePool(missing)
	require.Error(t, err)
	_, err = loadCertificateSPKIHash(missing)
	require.ErrorContains(t, err, "load resolver client certificate")

	s := &service{cfg: &Config{Sirius: SiriusConfig{Enabled: true}}}
	require.ErrorContains(t, s.startSiriusRuntime(context.Background()), "GC-protected lease dependencies")
	s.cfg.Sirius.Enabled = false
	require.NoError(t, s.startSiriusRuntime(context.Background()))
}

func writeSiriusTestCertificate(t *testing.T) (string, string, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:        true, BasicConstraintsValid: true, DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	privateDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER}), 0o600))
	certificate, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return certPath, keyPath, digest[:]
}

func startSiriusTestFlightServer(t *testing.T, certPath, keyPath string) string {
	t.Helper()
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
	})))
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "arrow.flight.protocol.FlightService", HandlerType: (*siriusTestFlightService)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName: "DoAction", ServerStreams: true,
			Handler: func(_ any, stream grpc.ServerStream) error {
				action := new(siriusTestFlightAction)
				if err := stream.RecvMsg(action); err != nil {
					return err
				}
				if action.Type != "GetCapabilities" {
					return moerr.NewInternalErrorNoCtx("unexpected Flight action")
				}
				return stream.SendMsg(&siriusTestFlightResult{Body: []byte(substrait.CapabilityDocument)})
			},
		}},
	}, new(struct{}))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}
