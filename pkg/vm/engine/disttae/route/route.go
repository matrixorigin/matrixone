// Copyright 2021 -2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package route

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// RouteForSuperTenant is used to select CN servers for sys tenant.
// For sys tenant, there are some special strategies to select CN servers.
// First of all, the requested labels must be match with the ones on servers.
// Then, the following strategies are listed in order of priority:
//  1. The CN servers which are configured as sys account.
//  2. The CN servers which are configured as some labels whose key is not account.
//  3. The CN servers which are configured as no labels.
//  4. At last, if no CN servers are selected,
//     4.1 If the username is dump or root, we just select one randomly.
//     4.2 Else, no servers are selected.
func RouteForSuperTenant(
	service string,
	selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) {
	mc := clusterservice.GetMOCluster(service)
	_ = routeForSuperTenant(context.Background(), mc.GetCNService, selector, username, filter, appendFn)
}

// RouteForSuperTenantCandidates applies the super-tenant routing policy to an
// immutable candidate snapshot instead of reading cluster state itself.
func RouteForSuperTenantCandidates(
	ctx context.Context,
	candidates []metadata.CNService,
	selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return routeForSuperTenant(ctx, candidateGetter(ctx, candidates), selector, username, filter, appendFn)
}

type cnServiceGetter func(clusterservice.Selector, func(metadata.CNService) bool)

func routeForSuperTenant(
	ctx context.Context,
	getCNService cnServiceGetter,
	selector clusterservice.Selector,
	username string,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// found is true indicates that we have find some available CN services.
	var found bool
	var emptyCNs []*metadata.CNService

	// S1: Select servers that configured as sys account.
	getCNService(selector, func(s metadata.CNService) bool {
		if ctx.Err() != nil {
			return false
		}
		if filter != nil && filter(s.SQLAddress) {
			return true
		}
		// At this phase, only append non-empty servers.
		if len(s.Labels) == 0 {
			emptyCNs = append(emptyCNs, &s)
		} else {
			found = true
			appendFn(&s)
		}
		return true
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if found {
		return nil
	}

	// S2: If there are no servers that are configured as sys account.
	// There may be some performance issues, but we need to do this still.
	// (1) If there is only one request label, which is account:sys, we fetch all
	//   CN servers.
	// (2) Otherwise, there are other labels, we fetch the CN servers whose labels
	//   are matched with them.
	// In the apply function, we only append the CN servers who has no account key,
	// to filter out the CN servers who belongs to some common tenants.
	var se clusterservice.Selector
	if selector.LabelNum() == 1 {
		se = clusterservice.NewSelector()
	} else {
		se = selector.SelectWithoutLabel(map[string]string{"account": "sys"})
	}
	getCNService(se, func(s metadata.CNService) bool {
		if ctx.Err() != nil {
			return false
		}
		if filter != nil && filter(s.SQLAddress) {
			return true
		}
		// Append CN servers that are not configured as label with key "account".
		if _, ok := s.Labels["account"]; len(s.Labels) > 0 && !ok {
			found = true
			appendFn(&s)
		}
		return true
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if found {
		return nil
	}

	// S3: Select CN servers which has no labels.
	if len(emptyCNs) > 0 {
		for _, cn := range emptyCNs {
			if err := ctx.Err(); err != nil {
				return err
			}
			appendFn(cn)
		}
		return ctx.Err()
	}

	// S4.1: If the root is super, return all servers.
	username = strings.ToLower(username)
	if username == "dump" || username == "root" {
		getCNService(clusterservice.NewSelector(), func(s metadata.CNService) bool {
			if ctx.Err() != nil {
				return false
			}
			if filter != nil && filter(s.SQLAddress) {
				return true
			}
			appendFn(&s)
			return true
		})
		return ctx.Err()
	}

	// S4.2: No servers are returned.
	return ctx.Err()
}

// RouteForCommonTenant selects CN services for common tenant.
// If there are CN services for the selector, just select them,
// else, return CN services with empty labels if there are any.
func RouteForCommonTenant(
	service string,
	selector clusterservice.Selector,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) {
	mc := clusterservice.GetMOCluster(service)
	_ = routeForCommonTenant(context.Background(), mc.GetCNService, selector, filter, appendFn)
}

// RouteForCommonTenantCandidates applies the common-tenant routing policy to
// an immutable candidate snapshot instead of reading cluster state itself.
func RouteForCommonTenantCandidates(
	ctx context.Context,
	candidates []metadata.CNService,
	selector clusterservice.Selector,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return routeForCommonTenant(ctx, candidateGetter(ctx, candidates), selector, filter, appendFn)
}

func routeForCommonTenant(
	ctx context.Context,
	getCNService cnServiceGetter,
	selector clusterservice.Selector,
	filter func(string) bool,
	appendFn func(service *metadata.CNService),
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// found is true indicates that there are CN services for the selector.
	var found bool

	// preEmptyCNs keeps the CN services that has empty labels before we
	// find any CN service with non-empty label.
	var preEmptyCNs []*metadata.CNService

	getCNService(selector, func(s metadata.CNService) bool {
		if ctx.Err() != nil {
			return false
		}
		if filter != nil && filter(s.SQLAddress) {
			return true
		}
		if len(s.Labels) > 0 {
			// Find available CN, append it.
			found = true
			appendFn(&s)
		} else {
			if found {
				// If there are already CN services with non-empty labels,
				// then ignore those with empty labels.
				return true
			} else {
				// If there are no CN services with non-empty labels yet,
				// save the CNs to preEmptyCNs first.
				preEmptyCNs = append(preEmptyCNs, &s)
				return true
			}
		}
		return true
	})
	if err := ctx.Err(); err != nil {
		return err
	}

	// If there are no CN services with non-empty labels,
	// return those with empty labels.
	if !found && len(preEmptyCNs) > 0 {
		for _, cn := range preEmptyCNs {
			if err := ctx.Err(); err != nil {
				return err
			}
			appendFn(cn)
		}
	}
	return ctx.Err()
}

func candidateGetter(ctx context.Context, candidates []metadata.CNService) cnServiceGetter {
	matcher := new(clusterservice.SelectorMatcher)
	return func(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
		for _, candidate := range candidates {
			if ctx.Err() != nil {
				return
			}
			if candidate.WorkState != metadata.WorkState_Working &&
				candidate.WorkState != metadata.WorkState_Unknown {
				continue
			}
			if !matcher.MatchCN(selector, candidate) {
				continue
			}
			if !apply(candidate) {
				return
			}
		}
	}
}
