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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var ErrM0Layout error = moerr.NewInternalErrorNoCtx("M0: unsupported column layout")

type m0DetailError struct {
	cause  error
	detail string
}

func (e *m0DetailError) Error() string { return e.cause.Error() + ": " + e.detail }
func (e *m0DetailError) Unwrap() error { return e.cause }

func wrapM0Error(cause error, format string, args ...any) error {
	return &m0DetailError{cause: cause, detail: fmt.Sprintf(format, args...)}
}

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
		return wrapM0Error(ErrM0Layout, "CN/TN schema shape or version")
	}
	for i, col := range cn.Cols {
		stored := tn.ColDefs[i]
		if col == nil || !strings.EqualFold(col.Name, stored.Name) || col.Hidden != stored.Hidden ||
			col.Primary != stored.Primary || vector.ProtoTypeToType(col.Typ) != stored.Type {
			return wrapM0Error(ErrM0Layout, "CN/TN column at %d", i)
		}
		if pos, ok := cn.Name2ColIndex[col.Name]; !ok || pos != int32(i) {
			return wrapM0Error(ErrM0Layout, "CN name map at %d", i)
		}
		if stored.IsPhyAddr() {
			continue
		}
		if col.ColId != uint64(i+1) || col.Seqnum != uint32(i) {
			return wrapM0Error(ErrM0Layout, "CN identity at %d", i)
		}
	}
	pk := tn.GetPrimaryKey()
	if cn.Pkey == nil || !strings.EqualFold(cn.Pkey.PkeyColName, pk.Name) {
		return wrapM0Error(ErrM0Layout, "CN/TN primary key")
	}
	if pk.IsCompositeColumn() {
		key := cn.Pkey.CompPkeyCol
		if key == nil || key.Name != cn.Cols[pk.Idx].Name || key.ColId != uint64(pk.Idx+1) ||
			key.Seqnum != uint32(pk.Idx) || !key.Hidden || !key.Primary {
			return wrapM0Error(ErrM0Layout, "CN composite key mapping")
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
		return wrapM0Error(ErrM0Layout, "clone width or version domain")
	}
	for i, source := range sourceCN.Cols {
		target := targetCN.Cols[i]
		if source.Name != target.Name || source.Hidden != target.Hidden || source.Primary != target.Primary ||
			source.ColId != target.ColId || source.Seqnum != target.Seqnum ||
			vector.ProtoTypeToType(source.Typ) != vector.ProtoTypeToType(target.Typ) {
			return wrapM0Error(ErrM0Layout, "clone source mapping at %d", i)
		}
	}
	return nil
}

func validateM0Schema(s *Schema) error {
	if s == nil || s.Extra == nil || len(s.ColDefs) < 2 || !s.HasPK() || s.HasFakePK() ||
		len(s.SortKey.Defs) != 1 || s.Extra.NextColSeqnum != uint32(len(s.ColDefs)-1) ||
		len(s.NameMap) != len(s.ColDefs) || len(s.SeqnumMap) != len(s.ColDefs) {
		return wrapM0Error(ErrM0Layout, "TN schema shape")
	}
	for i, col := range s.ColDefs {
		if col == nil || col.Idx != i {
			return wrapM0Error(ErrM0Layout, "TN position at %d", i)
		}
		if col.IsPhyAddr() {
			if i != len(s.ColDefs)-1 || col.SeqNum != objectio.SEQNUM_ROWID || !col.Hidden || s.PhyAddrKey != col {
				return wrapM0Error(ErrM0Layout, "rowid")
			}
		} else if i == len(s.ColDefs)-1 || col.SeqNum != uint16(i) || col.SeqNum >= objectio.SEQNUM_UPPER {
			return wrapM0Error(ErrM0Layout, "TN identity at %d", i)
		}
		if pos, ok := s.NameMap[col.Name]; !ok || pos != i {
			return wrapM0Error(ErrM0Layout, "TN name map at %d", i)
		}
		if pos, ok := s.SeqnumMap[col.SeqNum]; !ok || pos != i {
			return wrapM0Error(ErrM0Layout, "TN sequence map at %d", i)
		}
	}
	pk := s.GetPrimaryKey()
	if pk == nil || !pk.Primary || !pk.SortKey || pk.Idx < 0 || pk.Idx >= len(s.ColDefs) ||
		s.ColDefs[pk.Idx] != pk || !s.SortKey.HasColumn(pk.Idx) || pk.SeqNum != uint16(pk.Idx) {
		return wrapM0Error(ErrM0Layout, "TN primary key")
	}
	return nil
}
