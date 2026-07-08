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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/iceberg/api"
)

func AccountConfigForFeatureGate(cfg api.Config, accountID uint32) api.AccountConfig {
	return api.AccountConfig{
		AccountID: accountID,
		// Account-level enablement is not backed by a persisted account setting yet.
		// When per-account mode is requested, fail closed until that source is wired.
		Enable: !cfg.EnablePerAccount,
	}
}

func EffectiveConfigForAccount(cfg api.Config, accountID uint32) api.Config {
	return cfg.EffectiveForAccount(AccountConfigForFeatureGate(cfg, accountID))
}

func EnsureFeatureEnabled(ctx context.Context, cfg api.Config, accountID uint32, surface string) error {
	if EffectiveConfigForAccount(cfg, accountID).Enable {
		return nil
	}
	if surface == "" {
		surface = "Iceberg connector"
	}
	return api.ToMOErr(ctx, api.NewError(api.ErrFeatureDisabled, surface+" is disabled by configuration", map[string]string{
		"account_id": strconv.FormatUint(uint64(accountID), 10),
	}))
}
