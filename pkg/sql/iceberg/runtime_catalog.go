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

package iceberg

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
	"github.com/matrixorigin/matrixone/pkg/iceberg/model"
	icebergref "github.com/matrixorigin/matrixone/pkg/iceberg/ref"
)

func resolveRuntimeCatalogRequestPrefix(ctx context.Context, client api.CatalogClient, req api.CatalogRequest) (api.CatalogRequest, error) {
	if client == nil || strings.TrimSpace(req.Prefix) != "" {
		return req, nil
	}
	resp, err := client.GetConfig(ctx, api.GetConfigRequest{
		CatalogRequest: req,
		Warehouse:      req.Catalog.Warehouse,
	})
	if err != nil {
		return req, err
	}
	if resp != nil && strings.TrimSpace(resp.Prefix) != "" {
		req.Prefix = strings.TrimSpace(resp.Prefix)
	}
	return req, nil
}

func resolveRuntimeCatalogRequestPrefixForWriteRef(ctx context.Context, client api.CatalogClient, req api.CatalogRequest, rawRef string, caps api.CatalogCapabilities, allowTagMove bool) (api.CatalogRequest, string, string, error) {
	if client == nil || strings.TrimSpace(req.Prefix) != "" {
		return req, "", "", nil
	}
	resp, err := client.GetConfig(ctx, api.GetConfigRequest{
		CatalogRequest: req,
		Warehouse:      req.Catalog.Warehouse,
	})
	if err != nil {
		return req, "", "", err
	}
	if resp != nil && strings.TrimSpace(resp.Prefix) != "" {
		req.Prefix = strings.TrimSpace(resp.Prefix)
	}
	if isRuntimeNessieCatalogConfig(resp) {
		targetRef, _, err := normalizeRuntimeWriteRef(rawRef, caps, allowTagMove)
		if err != nil {
			return req, "", "", err
		}
		if rewritten, ok := rewriteRuntimeNessiePrefix(req.Prefix, targetRef); ok {
			req.Prefix = rewritten
			return req, model.DefaultRefMain, string(icebergref.TypeBranch), nil
		}
	}
	return req, "", "", nil
}

func normalizeRuntimeWriteRef(rawRef string, caps api.CatalogCapabilities, allowTagMove bool) (string, string, error) {
	spec, err := icebergref.ParseNessieRef(rawRef, nil)
	if err != nil {
		return "", "", err
	}
	if strings.TrimSpace(spec.Name) == "" {
		spec.Name = model.DefaultRefMain
	}
	if spec.Type == "" {
		spec.Type = icebergref.TypeBranch
	}
	if err := icebergref.ValidateWrite(spec, caps, allowTagMove); err != nil {
		return "", "", err
	}
	return spec.Name, string(spec.Type), nil
}

func isRuntimeNessieCatalogConfig(resp *api.ConfigResponse) bool {
	if resp == nil {
		return false
	}
	for _, cfg := range []map[string]string{resp.Overrides, resp.Defaults} {
		for key, value := range cfg {
			if strings.EqualFold(strings.TrimSpace(key), "nessie.is-nessie-catalog") &&
				strings.EqualFold(strings.TrimSpace(value), "true") {
				return true
			}
		}
	}
	return false
}

func rewriteRuntimeNessiePrefix(prefix, refName string) (string, bool) {
	refName = strings.TrimSpace(refName)
	if refName == "" || refName == model.DefaultRefMain {
		return prefix, false
	}
	parts := strings.SplitN(strings.TrimSpace(prefix), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		return prefix, false
	}
	return refName + "|" + parts[1], true
}
