// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorindex

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

// MaintenanceOperand identifies a relation consumed for one index role. It is
// a config projection only; the caller must independently admit the relation.
type MaintenanceOperand struct {
	Role string
	Name string
}

// DecodeMaintenanceConfig decodes plugin literals without granting relation access.
func DecodeMaintenanceConfig(raw string) (IndexTableConfig, error) {
	var cfg IndexTableConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, err
	}
	if cfg.SrcTable == "" {
		return cfg, moerr.NewInvalidInputNoCtx("index maintenance source is empty")
	}
	return cfg, nil
}

// MaintenanceRoles selects required and present optional live definitions in order.
func MaintenanceRoles(defs map[string]*plan.IndexDef, required []string, optional ...string) ([]string, error) {
	for _, role := range required {
		if defs[role] == nil {
			return nil, moerr.NewInvalidInputNoCtx("missing index maintenance definition")
		}
	}
	for _, role := range optional {
		if defs[role] != nil {
			required = append(required, role)
		}
	}
	return required, nil
}

// ValidateMaintenanceConfig compares the plugin-decoded literal with its
// independently resolved owner and role identities. The plugin supplies role
// meanings; this helper does not infer algorithm semantics from SQL or names.
func ValidateMaintenanceConfig(spec *executor.IndexMaintenanceSpec, function, database, source string, targets []MaintenanceOperand) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	if !strings.EqualFold(spec.Function, function) || !strings.EqualFold(spec.Database, database) || (source != "" && !strings.EqualFold(spec.BaseName, source)) || len(spec.Targets) != len(targets) {
		return moerr.NewInvalidInputNoCtx("index maintenance config does not match its owner")
	}
	targetNames := make(map[string]string, len(targets))
	for _, target := range targets {
		if target.Role == "" {
			return moerr.NewInvalidInputNoCtx("index maintenance config target does not match its owner")
		}
		if _, exists := targetNames[target.Role]; exists {
			return moerr.NewInvalidInputNoCtx("index maintenance config target does not match its owner")
		}
		targetNames[target.Role] = target.Name
	}
	for _, target := range spec.Targets {
		name, ok := targetNames[target.Role]
		if !ok || !strings.EqualFold(name, target.Name) {
			return moerr.NewInvalidInputNoCtx("index maintenance config target does not match its owner")
		}
	}
	return nil
}
