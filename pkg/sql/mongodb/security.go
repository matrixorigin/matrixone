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

package mongodb

import (
	"context"
	"net"
	"regexp"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// HostResolver is injectable so DNS rebinding and SRV policy can be tested
// without depending on the machine resolver.
type HostResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
	LookupSRV(context.Context, string, string, string) (string, []*net.SRV, error)
}

type contextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

// policyDialer applies endpoint policy to every socket opened by the driver,
// including ReplicaSet members learned after seed discovery. It dials a
// validated resolved IP rather than asking the system resolver to resolve the
// hostname a second time, closing the DNS-rebinding gap. The MongoDB driver
// still retains the original hostname for TLS server-name verification.
type policyDialer struct {
	cfg      RuntimeConfig
	resolver HostResolver
	base     contextDialer
}

func newPolicyDialer(cfg RuntimeConfig, resolver HostResolver) *policyDialer {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	return &policyDialer{
		cfg:      cfg,
		resolver: resolver,
		base:     &net.Dialer{Timeout: cfg.ConnectTimeout},
	}
}

func (d *policyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB discovered an invalid endpoint")
	}
	if err = validateHost(ctx, host, d.cfg); err != nil {
		return nil, err
	}
	if ip := net.ParseIP(host); ip != nil {
		return d.base.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	addresses, err := d.resolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, moerr.NewInvalidInput(ctx, "MongoDB DNS lookup failed or returned no addresses")
	}
	for _, resolved := range addresses {
		if err = validateResolvedIP(ctx, resolved.IP, d.cfg, false); err != nil {
			return nil, err
		}
	}
	var lastErr error
	for _, resolved := range addresses {
		connection, dialErr := d.base.DialContext(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		lastErr = dialErr
	}
	return nil, lastErr
}

func ValidateSecretRef(ctx context.Context, value string, required bool) error {
	value = strings.TrimSpace(value)
	if value == "" && !required {
		return nil
	}
	if !strings.HasPrefix(strings.ToLower(value), "secret://") || len(value) <= len("secret://") {
		return moerr.NewInvalidInput(ctx, "MongoDB credentials and TLS material must use a secret:// reference")
	}
	return nil
}

func ValidateConnection(ctx context.Context, c Connection, cfg RuntimeConfig) error {
	if strings.TrimSpace(c.Name) == "" {
		return moerr.NewInvalidInput(ctx, "MongoDB connection name cannot be empty")
	}
	if err := ValidateSecretRef(ctx, c.CredentialSecretRef, true); err != nil {
		return err
	}
	if err := ValidateSecretRef(ctx, c.TLSCASecretRef, false); err != nil {
		return err
	}
	if _, err := decodeConnectorOptions(ctx, c.OptionsJSON); err != nil {
		return err
	}
	if !strings.EqualFold(c.AuthMechanism, "SCRAM-SHA-256") {
		return moerr.NewInvalidInput(ctx, "MongoDB MVP supports only SCRAM-SHA-256 authentication")
	}
	switch strings.ToLower(strings.TrimSpace(c.TLSMode)) {
	case "required", "disabled":
	default:
		return moerr.NewInvalidInput(ctx, "MongoDB tls_mode must be required or disabled")
	}
	if _, err := parseReadPreference(c.ReadPreference, c.MaxStalenessSeconds); err != nil {
		return moerr.NewInvalidInput(ctx, "unsupported MongoDB read preference or max staleness")
	}
	switch strings.ToLower(strings.TrimSpace(c.ReadConcern)) {
	case "majority", "local":
	default:
		return moerr.NewInvalidInput(ctx, "MongoDB read_concern must be majority or local")
	}
	hasHosts := strings.TrimSpace(c.Hosts) != ""
	hasSRV := strings.TrimSpace(c.SRVHost) != ""
	if hasHosts == hasSRV {
		return moerr.NewInvalidInput(ctx, "MongoDB connection requires exactly one of hosts or srv_host")
	}
	if hasSRV {
		if strings.ContainsAny(c.SRVHost, "/@:") {
			return moerr.NewInvalidInput(ctx, "MongoDB srv_host must be a DNS name without URI userinfo, path, or port")
		}
		return validateHost(ctx, c.SRVHost, cfg)
	}
	for _, seed := range strings.Split(c.Hosts, ",") {
		seed = strings.TrimSpace(seed)
		if seed == "" || strings.Contains(seed, "://") || strings.Contains(seed, "@") {
			return moerr.NewInvalidInput(ctx, "MongoDB hosts must contain host:port seeds without URI userinfo")
		}
		host, port, err := net.SplitHostPort(seed)
		if err != nil || port == "" {
			return moerr.NewInvalidInput(ctx, "MongoDB hosts must contain valid host:port seeds")
		}
		if err := validateHost(ctx, host, cfg); err != nil {
			return err
		}
	}
	return nil
}

// ValidateResolvedEndpoints rechecks every address immediately before client
// creation. Hostname policy alone is insufficient because DNS can resolve an
// allowed name to loopback, link-local, metadata, or an out-of-policy CIDR.
func ValidateResolvedEndpoints(ctx context.Context, c Connection, cfg RuntimeConfig, resolver HostResolver) error {
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	hosts := make([]string, 0, 4)
	if strings.TrimSpace(c.SRVHost) != "" {
		_, records, err := resolver.LookupSRV(ctx, "mongodb", "tcp", strings.TrimSuffix(c.SRVHost, "."))
		if err != nil || len(records) == 0 {
			return moerr.NewInvalidInput(ctx, "MongoDB SRV lookup failed or returned no endpoints")
		}
		for _, record := range records {
			hosts = append(hosts, strings.TrimSuffix(record.Target, "."))
		}
	} else {
		for _, seed := range strings.Split(c.Hosts, ",") {
			host, _, err := net.SplitHostPort(strings.TrimSpace(seed))
			if err != nil {
				return moerr.NewInvalidInput(ctx, "MongoDB seed endpoint is invalid")
			}
			hosts = append(hosts, host)
		}
	}
	for _, host := range hosts {
		if ip := net.ParseIP(host); ip != nil {
			if err := validateHost(ctx, ip.String(), cfg); err != nil {
				return err
			}
			continue
		}
		if err := validateHost(ctx, host, cfg); err != nil {
			return err
		}
		addresses, err := resolver.LookupIPAddr(ctx, host)
		if err != nil || len(addresses) == 0 {
			return moerr.NewInvalidInput(ctx, "MongoDB DNS lookup failed or returned no addresses")
		}
		for _, address := range addresses {
			if err := validateResolvedIP(ctx, address.IP, cfg, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateHost(ctx context.Context, host string, cfg RuntimeConfig) error {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return moerr.NewInvalidInput(ctx, "MongoDB host cannot be empty")
	}
	if ip := net.ParseIP(host); ip != nil {
		return validateResolvedIP(ctx, ip, cfg, true)
	}
	if len(cfg.AllowedHostSuffixes) == 0 {
		return moerr.NewInvalidInput(ctx, "MongoDB hostname endpoint requires an explicit suffix allowlist")
	}
	for _, suffix := range cfg.AllowedHostSuffixes {
		suffix = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(suffix)), ".")
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return nil
		}
	}
	return moerr.NewInvalidInput(ctx, "MongoDB endpoint is outside the configured hostname allowlist")
}

func validateResolvedIP(ctx context.Context, ip net.IP, cfg RuntimeConfig, requireCIDR bool) error {
	if ip == nil || ip.IsUnspecified() {
		return moerr.NewInvalidInput(ctx, "MongoDB unspecified endpoint is not allowed")
	}
	if isMetadataEndpoint(ip) {
		return moerr.NewInvalidInput(ctx, "MongoDB cloud metadata endpoint is not allowed")
	}
	if ip.IsLoopback() {
		if !cfg.AllowLoopback {
			return moerr.NewInvalidInput(ctx, "MongoDB loopback endpoint is not allowed")
		}
		return nil
	}
	if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return moerr.NewInvalidInput(ctx, "MongoDB link-local or multicast endpoint is not allowed")
	}
	if len(cfg.AllowedCIDRs) == 0 {
		if requireCIDR {
			return moerr.NewInvalidInput(ctx, "MongoDB IP endpoint requires an explicit CIDR allowlist")
		}
		// The originating DNS name was already checked against the hostname
		// allowlist. Still reject special IP classes above on every resolution.
		return nil
	}
	for _, raw := range cfg.AllowedCIDRs {
		_, network, err := net.ParseCIDR(strings.TrimSpace(raw))
		if err == nil && network.Contains(ip) {
			return nil
		}
	}
	return moerr.NewInvalidInput(ctx, "MongoDB endpoint is outside the configured CIDR allowlist")
}

func isMetadataEndpoint(ip net.IP) bool {
	// Link-local metadata addresses are rejected by the general link-local
	// policy below. These well-known non-link-local endpoints need an explicit
	// deny even when private MongoDB addresses are otherwise allowed.
	for _, raw := range []string{
		"100.100.100.200", // Alibaba Cloud ECS metadata
		"fd00:ec2::254",   // AWS IMDS IPv6
		"fd20:ce::254",    // Google Compute metadata IPv6
	} {
		if ip.Equal(net.ParseIP(raw)) {
			return true
		}
	}
	return false
}

var mongoDBUserInfoPattern = regexp.MustCompile(`(?i)(mongodb(?:\+srv)?://)[^/@\s]+@`)

func Redact(value string) string {
	if strings.TrimSpace(value) == "" {
		return value
	}
	// Error and trace messages often contain a URI as only one token. Redact
	// every embedded URI, not just inputs that parse as one complete URL.
	value = mongoDBUserInfoPattern.ReplaceAllString(value, "$1")
	for _, marker := range []string{"password=", "token=", "secret="} {
		for searchFrom := 0; searchFrom < len(value); {
			lower := strings.ToLower(value)
			relative := strings.Index(lower[searchFrom:], marker)
			if relative < 0 {
				break
			}
			idx := searchFrom + relative
			end := strings.IndexAny(value[idx+len(marker):], "&; ,")
			if end < 0 {
				end = len(value) - idx - len(marker)
			}
			value = value[:idx+len(marker)] + "******" + value[idx+len(marker)+end:]
			searchFrom = idx + len(marker) + len("******")
		}
	}
	return value
}
