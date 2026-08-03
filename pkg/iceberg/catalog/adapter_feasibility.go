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

import "strings"

type AdapterFeasibilityStatus string

const (
	AdapterFeasibleViaFacade AdapterFeasibilityStatus = "facade_candidate"
	AdapterNeedsBridge       AdapterFeasibilityStatus = "needs_bridge"
	AdapterRESTPreferred     AdapterFeasibilityStatus = "rest_preferred"
	AdapterNotForV1Default   AdapterFeasibilityStatus = "not_v1_default"
)

type AdapterFeasibility struct {
	Name             string
	CatalogTypes     []string
	Status           AdapterFeasibilityStatus
	FacadeReusable   bool
	RequiresBuildTag bool
	Rationale        string
}

var nonRESTAdapterFeasibility = []AdapterFeasibility{
	{
		Name:           "glue",
		CatalogTypes:   []string{"glue", "aws-glue"},
		Status:         AdapterNeedsBridge,
		FacadeReusable: true,
		Rationale:      "Requires AWS SDK credential and region handling behind MO catalog facade; REST remains the V1 default.",
	},
	{
		Name:           "hive",
		CatalogTypes:   []string{"hive", "hms", "thrift-hive"},
		Status:         AdapterNeedsBridge,
		FacadeReusable: true,
		Rationale:      "Requires Hive Metastore Thrift client, Kerberos/TLS policy, and namespace/table mapping behind MO catalog facade.",
	},
	{
		Name:           "unity",
		CatalogTypes:   []string{"unity", "unity-catalog"},
		Status:         AdapterRESTPreferred,
		FacadeReusable: true,
		Rationale:      "Prefer REST/Open Catalog compatible surface where available; native SDK can be evaluated after auth/profile freeze.",
	},
}

var icebergGoAdapterFeasibility = AdapterFeasibility{
	Name:             AdapterIcebergGo,
	CatalogTypes:     []string{AdapterIcebergGo, "sql", "hadoop", "glue", "hive"},
	Status:           AdapterFeasibleViaFacade,
	FacadeReusable:   true,
	RequiresBuildTag: true,
	Rationale:        "Keep apache/iceberg-go inside the isolated adapter module and convert all results to MO stable facade types.",
}

func NonRESTAdapterFeasibility() []AdapterFeasibility {
	return cloneAdapterFeasibility(nonRESTAdapterFeasibility)
}

func IcebergGoAdapterFeasibility() AdapterFeasibility {
	return cloneOneAdapterFeasibility(icebergGoAdapterFeasibility)
}

func AdapterFeasibilityForType(name string) (AdapterFeasibility, bool) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	for _, item := range append(nonRESTAdapterFeasibility, icebergGoAdapterFeasibility) {
		for _, typ := range item.CatalogTypes {
			if normalized == strings.ToLower(strings.TrimSpace(typ)) {
				return cloneOneAdapterFeasibility(item), true
			}
		}
	}
	return AdapterFeasibility{}, false
}

func cloneAdapterFeasibility(in []AdapterFeasibility) []AdapterFeasibility {
	out := make([]AdapterFeasibility, len(in))
	for i := range in {
		out[i] = cloneOneAdapterFeasibility(in[i])
	}
	return out
}

func cloneOneAdapterFeasibility(in AdapterFeasibility) AdapterFeasibility {
	in.CatalogTypes = append([]string(nil), in.CatalogTypes...)
	return in
}
