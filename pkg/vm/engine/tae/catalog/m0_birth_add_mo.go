//go:build mo_m0

// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalog

import (
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"strings"
)

func (s *Schema) prepareM0BirthAdd(add *api.AlterTableAddColumn) (*plan.AppendMissingState, error) {
	if add.AppendMissingState == nil && s.Extra.AppendMissingState == nil {
		if m0ColumnNeedsBackfill(add.Column.Default) {
			return nil, moerr.NewInternalErrorNoCtx("append missing state: non-NULL ADD needs an admitted descriptor")
		}
		return nil, nil
	}
	invalid := func() (*plan.AppendMissingState, error) {
		return nil, moerr.NewInternalErrorNoCtx("append missing state: rejected TN append transition")
	}
	if add.AppendMissingState == nil || add.ExpectedSchemaVersion != s.Version || add.ExpectedNextColSeqnum != s.Extra.NextColSeqnum ||
		add.InsertPosition != -1 || len(add.XXX_unrecognized) != 0 || s.Relkind != pkgcatalog.SystemOrdinaryRel ||
		s.Partitioned != 0 || s.FromPublication || len(s.Extra.Checks) != 0 {
		return nil, moerr.NewInternalErrorNoCtxf(
			"append missing state: TN source admission differs (state=%t version=%d/%d next=%d/%d position=%d kind=%q partition=%d publication=%t checks=%d unknown=%d)",
			add.AppendMissingState != nil, s.Version, add.ExpectedSchemaVersion,
			s.Extra.NextColSeqnum, add.ExpectedNextColSeqnum, add.InsertPosition, s.Relkind, s.Partitioned, s.FromPublication, len(s.Extra.Checks), len(add.XXX_unrecognized))
	}
	base, err := M0AppendBase(s)
	if err != nil {
		return nil, err
	}
	if len(s.Constraint) != 0 {
		var constraints engine.ConstraintDef
		if err := constraints.UnmarshalBinary(s.Constraint); err != nil {
			return nil, err
		}
		for _, c := range constraints.Cts {
			switch c := c.(type) {
			case *engine.PrimaryKeyDef:
			case *engine.StreamConfigsDef:
				if err := pkgcatalog.ValidateM0BirthProperties(c.Configs); err != nil {
					return nil, err
				}
			case *engine.IndexDef:
				if len(c.Indexes) != 0 {
					return invalid()
				}
			case *engine.ForeignKeyDef:
				if len(c.Fkeys) != 0 {
					return invalid()
				}
			case *engine.RefChildTableDef:
				if len(c.Tables) != 0 {
					return invalid()
				}
			default:
				return invalid()
			}
		}
	}
	existing := make([]plan.Type, s.Extra.NextColSeqnum)
	for i := range existing {
		col := s.ColDefs[i]
		if strings.EqualFold(col.Name, add.Column.Name) {
			return invalid()
		}
		existing[i] = plan.Type{Id: int32(col.Type.Oid), Width: col.Type.Width, Scale: col.Type.Scale, Enumvalues: col.EnumValues,
			Charset: uint32(col.Type.Charset), CollationVersion: uint32(col.Type.CollationVersion), NotNullable: !col.NullAbility, AutoIncr: col.AutoIncrement}
	}
	if err := plan.ValidateAppendMissingExtension(s.Extra.AppendMissingState, add.AppendMissingState, base, s.Version, existing, add.Column); err != nil {
		return nil, err
	}
	return plan.CloneAppendMissingState(add.AppendMissingState), nil
}
