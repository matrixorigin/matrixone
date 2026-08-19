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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/sidecarflight"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/substrait"
)

func siriusInternalErrorf(format string, args ...any) error {
	var cause error
	if strings.Contains(format, "%w") {
		format = strings.ReplaceAll(format, "%w", "%v")
		for _, arg := range args {
			if err, ok := arg.(error); ok {
				cause = err
				break
			}
		}
	}
	err := error(moerr.NewInternalErrorNoCtxf(format, args...))
	if cause != nil {
		err = errors.Join(err, cause)
	}
	return err
}

func (s *service) startSiriusRuntime(ctx context.Context) error {
	config := s.cfg.Sirius
	if !config.Enabled {
		return nil
	}
	if config.BenchmarkNoGC {
		if !config.benchmarkGCDisabled {
			return siriusInternalErrorf("substrait: Sirius benchmark-no-gc requires verified TN GC disablement")
		}
		if s.options.siriusLeases == nil && s.options.siriusAuditor == nil {
			s.options.siriusLeases, s.options.siriusAuditor = newSiriusBenchmarkDependencies()
		}
		if s.options.siriusLeases == nil || !s.options.siriusLeases.BenchmarkReady() || s.options.siriusAuditor == nil {
			return siriusInternalErrorf("substrait: Sirius benchmark-no-gc requires local lease dependencies")
		}
	} else if s.options.siriusLeases == nil || !s.options.siriusLeases.DurableReady() || s.options.siriusAuditor == nil {
		return siriusInternalErrorf("substrait: enabled Sirius runtime requires replayed GC-protected lease dependencies")
	}
	flightTLS, err := loadSiriusClientTLS(config)
	if err != nil {
		return err
	}
	resolverTLS, err := loadSiriusResolverTLS(config)
	if err != nil {
		return err
	}
	authorizedSPKI, err := loadCertificateSPKIHash(config.ResolverClientCertPath)
	if err != nil {
		return err
	}
	resolver, err := substrait.NewResolverServer(
		config.ResolverAddress, resolverTLS, s.options.siriusLeases, s.options.siriusAuditor,
	)
	if err != nil {
		return err
	}
	flight, err := sidecarflight.NewRuntime(ctx, sidecarflight.Config{
		Address: config.FlightAddress, TLSConfig: flightTLS, MaxBatchBytes: config.MaxBatchBytes,
		RequestTimeout: config.RequestTimeout.Duration, CleanupTimeout: config.CleanupTimeout.Duration,
	}, substrait.CapabilityDocument)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout.Duration)
		defer cancel()
		return errors.Join(err, resolver.Close(cleanupCtx))
	}
	runtime := &compile.SiriusRuntime{
		Flight: flight, Leases: s.options.siriusLeases, Resolver: resolver,
		AuthorizedClientSPKIHash: authorizedSPKI, DataDir: config.DataDir,
		LeaseTTL: config.LeaseTTL.Duration, CleanupTimeout: config.CleanupTimeout.Duration,
		BenchmarkNoGC: config.BenchmarkNoGC,
	}
	if err = runtime.Validate(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout.Duration)
		defer cancel()
		return errors.Join(err, runtime.Close(cleanupCtx))
	}
	// Make replayed lease resolution reachable before asking the sidecar to
	// quiesce orphaned executions. The runtime is not published yet, so no new
	// statement can enter during reconciliation setup.
	if err = resolver.Start(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout.Duration)
		defer cancel()
		return errors.Join(siriusInternalErrorf("substrait: start read resolver: %w", err), runtime.Close(cleanupCtx))
	}
	if err = runtime.ReconcileReplay(); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), config.CleanupTimeout.Duration)
		defer cancel()
		return errors.Join(err, runtime.Close(cleanupCtx))
	}
	s.siriusRuntime = runtime
	moruntime.ServiceRuntime(s.cfg.UUID).SetGlobalVariables(compile.SiriusRuntimeKey, runtime)
	return nil
}

func (s *service) closeSiriusRuntime() error {
	if s.siriusRuntime == nil {
		return nil
	}
	runtime := s.siriusRuntime
	s.siriusRuntime = nil
	moruntime.ServiceRuntime(s.cfg.UUID).CompareAndDeleteGlobalVariables(compile.SiriusRuntimeKey, runtime)
	timeout := s.cfg.Sirius.CleanupTimeout.Duration
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return runtime.Close(ctx)
}

func loadSiriusClientTLS(config SiriusConfig) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.FlightClientCertPath, config.FlightClientKeyPath)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: load Flight client certificate: %w", err)
	}
	roots, err := loadCertificatePool(config.FlightServerCAPath)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: load Flight server CA: %w", err)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12, ServerName: config.FlightServerName,
		Certificates: []tls.Certificate{certificate}, RootCAs: roots,
	}, nil
}

func loadSiriusResolverTLS(config SiriusConfig) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.ResolverServerCertPath, config.ResolverServerKeyPath)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: load resolver server certificate: %w", err)
	}
	clients, err := loadCertificatePool(config.ResolverClientCAPath)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: load resolver client CA: %w", err)
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clients,
	}, nil
}

func loadCertificatePool(path string) (*x509.CertPool, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(contents) {
		return nil, siriusInternalErrorf("certificate file contains no CA certificates")
	}
	return pool, nil
}

func loadCertificateSPKIHash(path string) ([]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: load resolver client certificate: %w", err)
	}
	block, _ := pem.Decode(contents)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, siriusInternalErrorf("substrait: resolver client certificate is not PEM")
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, siriusInternalErrorf("substrait: parse resolver client certificate: %w", err)
	}
	if len(certificate.RawSubjectPublicKeyInfo) == 0 {
		return nil, siriusInternalErrorf("substrait: resolver client certificate has no subject public key info")
	}
	// Hash the exact DER bytes used by tls.ConnectionState.VerifiedChains in
	// ResolveHandler so unusual but valid algorithm parameters cannot make the
	// configured identity differ after re-marshalling.
	digest := sha256.Sum256(certificate.RawSubjectPublicKeyInfo)
	return digest[:], nil
}
