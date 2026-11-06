// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package m0read

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// PlanEvolution reads explicit catalog metadata. A malformed or duplicated
// carrier is an error, never absence of authority.
func PlanEvolution(def *plan.TableDef) (*api.M0EvolutionContract, error) {
	if def == nil {
		return nil, moerr.NewInternalErrorNoCtx("M0: missing evolution schema")
	}
	var result *api.M0EvolutionContract
	seen := false
	for _, d := range def.Defs {
		for _, p := range d.GetProperties().GetProperties() {
			if p.Key != "schema_extra" {
				continue
			}
			if seen {
				return nil, moerr.NewInternalErrorNoCtx("M0: duplicate evolution carrier")
			}
			seen = true
			var extra api.SchemaExtra
			if err := extra.Unmarshal([]byte(p.Value)); err != nil {
				return nil, err
			}
			result = extra.M0Evolution
		}
	}
	return result, nil
}

type cloneEvolutionKey struct{}
type cloneEvolutionState struct{ contract *api.M0EvolutionContract }

// WithCloneEvolution is installed only by the clone owner after opening the
// source relation at its requested snapshot. It is not serialized authority.
func WithCloneEvolution(ctx context.Context, c *api.M0EvolutionContract) context.Context {
	state := cloneEvolutionState{}
	if c != nil {
		state.contract = proto.Clone(c).(*api.M0EvolutionContract)
	}
	return context.WithValue(ctx, cloneEvolutionKey{}, state)
}

func CloneEvolution(ctx context.Context) *api.M0EvolutionContract {
	c, _ := ctx.Value(cloneEvolutionKey{}).(cloneEvolutionState)
	return c.contract
}

func IsCloneEvolution(ctx context.Context) bool {
	_, ok := ctx.Value(cloneEvolutionKey{}).(cloneEvolutionState)
	return ok
}

// EvolutionBinding is supplied by the transaction-visible relation owner.
// It does not establish object membership or authorize an imported object.
type EvolutionBinding struct {
	Account            uint32
	Database, Relation uint64
}

// EvolutionDigest binds the ordered column identities and canonical semantics.
// Length prefixes avoid ambiguous concatenation; catalog version is deliberately
// excluded because metadata-only changes need not change physical layout.
func EvolutionDigest(columns []*api.M0EvolutionColumn) []byte {
	h := sha256.New()
	var n [8]byte
	for _, c := range columns {
		if c == nil {
			return nil
		}
		for _, v := range []uint64{c.ColumnId, uint64(c.Seqnum), uint64(c.IntroducedLayout), uint64(len(c.Semantics))} {
			binary.LittleEndian.PutUint64(n[:], v)
			_, _ = h.Write(n[:])
		}
		_, _ = h.Write(c.Semantics)
	}
	return h.Sum(nil)
}

// ValidateEvolution checks persisted authority independently of physical layout.
// A caller must additionally compare semantics with its authoritative schema and
// select objects from the relation's snapshot-visible catalog/workspace.
func ValidateEvolution(c *api.M0EvolutionContract, owner EvolutionBinding) error {
	bad := func() error { return moerr.NewInternalErrorNoCtx("M0: invalid relation evolution contract") }
	if c == nil || c.FormatVersion != 1 || len(c.RootId) != 16 || bytes.Equal(c.RootId, make([]byte, 16)) ||
		owner.Database == 0 || owner.Relation == 0 || c.AccountId != owner.Account || c.DatabaseId != owner.Database || c.RelationId != owner.Relation ||
		c.RootColumns == 0 || uint64(c.RootColumns)+uint64(c.LayoutGeneration) != uint64(len(c.Columns)) ||
		len(c.Columns) >= 65535 || len(c.SchemaDigest) != sha256.Size {
		return bad()
	}
	ids := make(map[uint64]struct{}, len(c.Columns))
	for i, col := range c.Columns {
		if col == nil || col.ColumnId == 0 || col.Seqnum != uint32(i) || len(col.Semantics) == 0 {
			return bad()
		}
		if _, exists := ids[col.ColumnId]; exists {
			return bad()
		}
		ids[col.ColumnId] = struct{}{}
		generation := uint32(0)
		if uint32(i) >= c.RootColumns {
			generation = uint32(i) - c.RootColumns + 1
		}
		if col.IntroducedLayout != generation {
			return bad()
		}
	}
	if !bytes.Equal(c.SchemaDigest, EvolutionDigest(c.Columns)) {
		return bad()
	}
	return nil
}

// EvolutionWidth requires the full prefix promised by an object's declaration;
// callers may fill NULL only after this width, never inside the prefix.
func EvolutionWidth(c *api.M0EvolutionContract, owner EvolutionBinding, generation uint32) (uint32, error) {
	if err := ValidateEvolution(c, owner); err != nil {
		return 0, err
	}
	if generation > c.LayoutGeneration {
		return 0, moerr.NewInternalErrorNoCtx("M0: object layout generation exceeds relation contract")
	}
	return c.RootColumns + generation, nil
}

// ValidatePlanEvolution checks catalog-carried authority against the selected
// schema. Relation membership/snapshot selection remain the reader owner's job.
func ValidatePlanEvolution(def *plan.TableDef, account uint32) (*api.M0EvolutionContract, error) {
	c, err := PlanEvolution(def)
	if err != nil || c == nil {
		return c, err
	}
	if err := ValidateEvolution(c, EvolutionBinding{Account: account, Database: def.DbId, Relation: def.TblId}); err != nil {
		return nil, err
	}
	var columns []*api.M0EvolutionColumn
	for _, col := range def.Cols {
		if col == nil {
			return nil, moerr.NewInternalErrorNoCtx("M0: missing authoritative evolution column")
		}
		if col.Name == "__mo_rowid" {
			continue
		}
		pos := len(columns)
		if pos >= len(c.Columns) {
			return nil, moerr.NewInternalErrorNoCtx("M0: schema exceeds evolution contract")
		}
		sem, err := PlanEvolutionColumnSemantic(def, col)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &api.M0EvolutionColumn{ColumnId: col.ColId, Seqnum: col.Seqnum, IntroducedLayout: c.Columns[pos].IntroducedLayout, Semantics: sem})
	}
	if len(columns) != len(c.Columns) || !bytes.Equal(EvolutionDigest(columns), c.SchemaDigest) {
		return nil, moerr.NewInternalErrorNoCtx("M0: authoritative schema disagrees with evolution contract")
	}
	return c, nil
}
