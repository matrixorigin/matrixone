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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

type Capability string

const (
	CapabilityCredentialVending  Capability = "credential_vending"
	CapabilityRemoteSigning      Capability = "remote_signing"
	CapabilityServerSidePlanning Capability = "server_side_planning"
	CapabilityBranchTag          Capability = "branch_tag"
	CapabilityCommit             Capability = "commit"
	CapabilityCreateTable        Capability = "create_table"
	CapabilityMetricsReport      Capability = "metrics_report"
)

var orderedCapabilities = []Capability{
	CapabilityCredentialVending,
	CapabilityRemoteSigning,
	CapabilityServerSidePlanning,
	CapabilityBranchTag,
	CapabilityCommit,
	CapabilityCreateTable,
	CapabilityMetricsReport,
}

type CapabilityRegistry struct {
	caps api.CatalogCapabilities
}

func NewCapabilityRegistry(caps api.CatalogCapabilities) CapabilityRegistry {
	return CapabilityRegistry{caps: caps}
}

func CapabilityRegistryFromConfig(resp *api.ConfigResponse) CapabilityRegistry {
	if resp == nil {
		return CapabilityRegistry{}
	}
	return NewCapabilityRegistry(resp.Capabilities)
}

func CapabilityRegistryFromLoadTable(resp *api.LoadTableResponse) CapabilityRegistry {
	if resp == nil {
		return CapabilityRegistry{}
	}
	return NewCapabilityRegistry(resp.Capabilities)
}

func (r CapabilityRegistry) Supports(cap Capability) bool {
	switch cap {
	case CapabilityCredentialVending:
		return r.caps.CredentialVending
	case CapabilityRemoteSigning:
		return r.caps.RemoteSigning
	case CapabilityServerSidePlanning:
		return r.caps.ServerSidePlanning
	case CapabilityBranchTag:
		return r.caps.BranchTag
	case CapabilityCommit:
		return r.caps.Commit
	case CapabilityCreateTable:
		return r.caps.CreateTable
	case CapabilityMetricsReport:
		return r.caps.MetricsReport
	default:
		return false
	}
}

func (r CapabilityRegistry) Require(ctx context.Context, cap Capability, operation string) error {
	if r.Supports(cap) {
		return nil
	}
	fields := map[string]string{"capability": string(cap)}
	if strings.TrimSpace(operation) != "" {
		fields["operation"] = strings.TrimSpace(operation)
	}
	return api.ToMOErr(ctx, api.NewError(api.ErrUnsupportedFeature, "Iceberg catalog capability is required but not available", fields))
}

func (r CapabilityRegistry) Missing(required ...Capability) []Capability {
	missing := make([]Capability, 0)
	for _, cap := range required {
		if !r.Supports(cap) {
			missing = append(missing, cap)
		}
	}
	return missing
}

func (r CapabilityRegistry) Snapshot() map[Capability]bool {
	out := make(map[Capability]bool, len(orderedCapabilities))
	for _, cap := range orderedCapabilities {
		out[cap] = r.Supports(cap)
	}
	return out
}

func (r CapabilityRegistry) Enabled() []Capability {
	enabled := make([]Capability, 0, len(orderedCapabilities))
	for _, cap := range orderedCapabilities {
		if r.Supports(cap) {
			enabled = append(enabled, cap)
		}
	}
	return enabled
}

func NormalizeCapabilityName(name string) Capability {
	normalized := strings.ToLower(strings.TrimSpace(name))
	normalized = strings.ReplaceAll(normalized, "-", "_")
	normalized = strings.ReplaceAll(normalized, ".", "_")
	switch Capability(normalized) {
	case CapabilityCredentialVending, CapabilityRemoteSigning, CapabilityServerSidePlanning, CapabilityBranchTag, CapabilityCommit, CapabilityCreateTable, CapabilityMetricsReport:
		return Capability(normalized)
	default:
		return Capability(normalized)
	}
}

func CapabilityNames(caps []Capability) []string {
	names := make([]string, 0, len(caps))
	for _, cap := range caps {
		names = append(names, string(cap))
	}
	sort.Strings(names)
	return names
}

func ParseCapabilitiesJSON(raw string) (api.CatalogCapabilities, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return api.CatalogCapabilities{}, nil
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return api.CatalogCapabilities{}, api.NewError(api.ErrConfigInvalid, "Iceberg catalog capabilities_json is invalid", map[string]string{
			"error": err.Error(),
		})
	}
	caps := api.CatalogCapabilities{}
	if err := applyCapabilitiesValue(&caps, value); err != nil {
		return api.CatalogCapabilities{}, err
	}
	return caps, nil
}

func applyCapabilitiesValue(caps *api.CatalogCapabilities, value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, rawValue := range typed {
			if NormalizeCapabilityName(key) == "capabilities" {
				if err := applyCapabilitiesValue(caps, rawValue); err != nil {
					return err
				}
				continue
			}
			if err := applyCapabilityValue(caps, key, rawValue); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range typed {
			name, ok := item.(string)
			if !ok {
				return api.NewError(api.ErrConfigInvalid, "Iceberg catalog capabilities_json array entries must be capability names", nil)
			}
			setCapability(caps, NormalizeCapabilityName(name), true)
		}
	default:
		return api.NewError(api.ErrConfigInvalid, "Iceberg catalog capabilities_json must be an object or array", nil)
	}
	return nil
}

func applyCapabilityValue(caps *api.CatalogCapabilities, key string, value any) error {
	enabled, ok := capabilityBoolValue(value)
	if !ok {
		return api.NewError(api.ErrConfigInvalid, "Iceberg catalog capabilities_json values must be booleans", map[string]string{
			"capability": key,
		})
	}
	setCapability(caps, NormalizeCapabilityName(key), enabled)
	return nil
}

func capabilityBoolValue(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "on", "yes", "enabled":
			return true, true
		case "0", "false", "off", "no", "disabled":
			return false, true
		default:
			return false, false
		}
	case json.Number:
		return strings.TrimSpace(typed.String()) != "0", true
	default:
		return false, false
	}
}

func setCapability(caps *api.CatalogCapabilities, cap Capability, enabled bool) {
	switch cap {
	case CapabilityCredentialVending:
		caps.CredentialVending = enabled
	case CapabilityRemoteSigning:
		caps.RemoteSigning = enabled
	case CapabilityServerSidePlanning:
		caps.ServerSidePlanning = enabled
	case CapabilityBranchTag:
		caps.BranchTag = enabled
	case CapabilityCommit:
		caps.Commit = enabled
	case CapabilityCreateTable:
		caps.CreateTable = enabled
	case CapabilityMetricsReport:
		caps.MetricsReport = enabled
	}
}
