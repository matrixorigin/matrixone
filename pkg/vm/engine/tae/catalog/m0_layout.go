//go:build mo_m0

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

package catalog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var ErrM0Layout = errors.New("M0: unsupported column layout")

// ValidateM0Layout checks the restricted layout, including internal positions
// and cached maps. Comparing IDs alone would accept a clone that moved the
// hidden composite key. Rowid has a special TN sequence number; its catalog
// ordinal is not the physical identity of an ordinary column.
// The mo_m0 clone and read paths use this as a structural proof helper; caller
// admission remains responsible for selecting the restricted domain.
func ValidateM0Layout(cn *plan.TableDef, tn *Schema) error {
	if err := validateM0Schema(tn); err != nil {
		return err
	}
	if cn == nil || len(cn.Cols) != len(tn.ColDefs) || cn.Version != tn.Version {
		return fmt.Errorf("%w: CN/TN schema shape or version", ErrM0Layout)
	}
	for i, col := range cn.Cols {
		stored := tn.ColDefs[i]
		if col == nil || !strings.EqualFold(col.Name, stored.Name) || col.Hidden != stored.Hidden ||
			col.Primary != stored.Primary || vector.ProtoTypeToType(col.Typ) != stored.Type {
			return fmt.Errorf("%w: CN/TN column at %d", ErrM0Layout, i)
		}
		if pos, ok := cn.Name2ColIndex[col.Name]; !ok || pos != int32(i) {
			return fmt.Errorf("%w: CN name map at %d", ErrM0Layout, i)
		}
		if stored.IsPhyAddr() {
			continue
		}
		if col.ColId != uint64(i+1) || col.Seqnum != uint32(i) {
			return fmt.Errorf("%w: CN identity at %d", ErrM0Layout, i)
		}
	}
	pk := tn.GetPrimaryKey()
	if cn.Pkey == nil || !strings.EqualFold(cn.Pkey.PkeyColName, pk.Name) {
		return fmt.Errorf("%w: CN/TN primary key", ErrM0Layout)
	}
	if pk.IsCompositeColumn() {
		key := cn.Pkey.CompPkeyCol
		if key == nil || key.Name != cn.Cols[pk.Idx].Name || key.ColId != uint64(pk.Idx+1) ||
			key.Seqnum != uint32(pk.Idx) || !key.Hidden || !key.Primary {
			return fmt.Errorf("%w: CN composite key mapping", ErrM0Layout)
		}
	}
	return nil
}

// ValidateM0CloneLayout additionally compares against the source snapshot.
// Both independently rebuilt schemas can be dense and internally consistent
// while assigning different identities to the same column names.
func ValidateM0CloneLayout(sourceCN *plan.TableDef, sourceTN *Schema, targetCN *plan.TableDef, targetTN *Schema) error {
	if err := ValidateM0Layout(sourceCN, sourceTN); err != nil {
		return err
	}
	if err := ValidateM0Layout(targetCN, targetTN); err != nil {
		return err
	}
	if len(sourceCN.Cols) != len(targetCN.Cols) || sourceTN.Extra.NextColSeqnum != targetTN.Extra.NextColSeqnum ||
		sourceTN.Version != targetTN.Version {
		return fmt.Errorf("%w: clone width or version domain", ErrM0Layout)
	}
	for i, source := range sourceCN.Cols {
		target := targetCN.Cols[i]
		if source.Name != target.Name || source.Hidden != target.Hidden || source.Primary != target.Primary ||
			source.ColId != target.ColId || source.Seqnum != target.Seqnum ||
			vector.ProtoTypeToType(source.Typ) != vector.ProtoTypeToType(target.Typ) {
			return fmt.Errorf("%w: clone source mapping at %d", ErrM0Layout, i)
		}
	}
	return nil
}

func validateM0Schema(s *Schema) error {
	if s == nil || s.Extra == nil || len(s.ColDefs) < 2 || !s.HasPK() || s.HasFakePK() ||
		len(s.SortKey.Defs) != 1 || s.Extra.NextColSeqnum != uint32(len(s.ColDefs)-1) ||
		len(s.NameMap) != len(s.ColDefs) || len(s.SeqnumMap) != len(s.ColDefs) {
		return fmt.Errorf("%w: TN schema shape", ErrM0Layout)
	}
	for i, col := range s.ColDefs {
		if col == nil || col.Idx != i {
			return fmt.Errorf("%w: TN position at %d", ErrM0Layout, i)
		}
		if col.IsPhyAddr() {
			if i != len(s.ColDefs)-1 || col.SeqNum != objectio.SEQNUM_ROWID || !col.Hidden || s.PhyAddrKey != col {
				return fmt.Errorf("%w: rowid", ErrM0Layout)
			}
		} else if i == len(s.ColDefs)-1 || col.SeqNum != uint16(i) || col.SeqNum >= objectio.SEQNUM_UPPER {
			return fmt.Errorf("%w: TN identity at %d", ErrM0Layout, i)
		}
		if pos, ok := s.NameMap[col.Name]; !ok || pos != i {
			return fmt.Errorf("%w: TN name map at %d", ErrM0Layout, i)
		}
		if pos, ok := s.SeqnumMap[col.SeqNum]; !ok || pos != i {
			return fmt.Errorf("%w: TN sequence map at %d", ErrM0Layout, i)
		}
	}
	pk := s.GetPrimaryKey()
	if pk == nil || !pk.Primary || !pk.SortKey || pk.Idx < 0 || pk.Idx >= len(s.ColDefs) ||
		s.ColDefs[pk.Idx] != pk || !s.SortKey.HasColumn(pk.Idx) || pk.SeqNum != uint16(pk.Idx) {
		return fmt.Errorf("%w: TN primary key", ErrM0Layout)
	}
	return nil
}
