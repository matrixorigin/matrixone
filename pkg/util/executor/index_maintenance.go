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

package executor

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// IndexMaintenanceTarget is one role in an already-authorized index group.
type IndexMaintenanceTarget struct {
	Role string
	Name string
	ID   uint64
}

// IndexMaintenanceSpec is server provenance for one generated maintenance
// operator. It is scoped to one statement and is never accepted from SQL.
type IndexMaintenanceSpec struct {
	Function   string
	AccountID  uint32
	Database   string
	DatabaseID uint64
	BaseName   string
	BaseID     uint64
	Targets    []IndexMaintenanceTarget
}

// IndexMaintenanceValidator belongs to an index plugin and checks literal
// arguments of its one generated TVF against the live owner provenance.
// It is local planning state and never serialized to a remote operator.
type IndexMaintenanceValidator interface {
	ValidateIndexMaintenance(function string, literalArgs []string, spec *IndexMaintenanceSpec) error
}

// Clone detaches the bounded target list from the caller and other operators.
func (s *IndexMaintenanceSpec) Clone() *IndexMaintenanceSpec {
	if s == nil {
		return nil
	}
	copy := *s
	copy.Targets = append([]IndexMaintenanceTarget(nil), s.Targets...)
	return &copy
}

// Validate checks the structural boundary; the owner additionally verifies
// the live account, group, relation definitions and IDs in its transaction.
func (s *IndexMaintenanceSpec) Validate() error {
	if s == nil || s.Function == "" || s.Database == "" || s.DatabaseID == 0 || s.BaseName == "" || s.BaseID == 0 || len(s.Targets) == 0 || len(s.Targets) > 16 {
		return moerr.NewInvalidInputNoCtx("invalid index maintenance provenance")
	}
	roles := make(map[string]struct{}, len(s.Targets))
	names := make(map[string]struct{}, len(s.Targets))
	ids := make(map[uint64]struct{}, len(s.Targets))
	for _, target := range s.Targets {
		name := strings.ToLower(target.Name)
		if target.Role == "" || name == "" || strings.EqualFold(name, s.BaseName) || target.ID == 0 || target.ID == s.BaseID {
			return moerr.NewInvalidInputNoCtx("invalid index maintenance target")
		}
		if _, ok := roles[target.Role]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate index maintenance role")
		}
		if _, ok := names[name]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate index maintenance name")
		}
		if _, ok := ids[target.ID]; ok {
			return moerr.NewInvalidInputNoCtx("duplicate index maintenance relation")
		}
		roles[target.Role], names[name], ids[target.ID] = struct{}{}, struct{}{}, struct{}{}
	}
	return nil
}

// IndexRelationPolicy limits relation resolution during SQL issued by an
// index-maintenance TVF. CallerMayAccess is derived from the live authenticated
// user, never from SQL or a numeric account identifier. Spec is optional and
// authorizes only explicitly marked generated AST nodes after ID verification.
type IndexRelationPolicy struct {
	CallerMayAccess bool
	Spec            *IndexMaintenanceSpec
}

func (p *IndexRelationPolicy) Clone() *IndexRelationPolicy {
	if p == nil {
		return nil
	}
	return &IndexRelationPolicy{CallerMayAccess: p.CallerMayAccess, Spec: p.Spec.Clone()}
}
