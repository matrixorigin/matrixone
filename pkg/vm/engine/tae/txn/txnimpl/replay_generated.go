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

package txnimpl

import (
	"bytes"
	"strings"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type replayGeneratedKey struct{ db, table uint64 }

type replayGeneratedTarget struct {
	schema    *catalog.Schema
	tableRows int
	invalid   bool
	seen      map[uint16]struct{}
	generated map[uint16][]byte
}

// restoreReplayGeneratedColumns uses only catalog rows in the same WAL
// transaction as the table schema. The schema codec does not carry generated
// expressions. Incomplete or ambiguous catalog rows must not be treated as
// proof that a column is not generated.
func restoreReplayGeneratedColumns(txn *txnbase.TxnCmd) {
	var targets map[replayGeneratedKey]*replayGeneratedTarget
	for _, raw := range txn.Cmds {
		cmd, ok := raw.(*catalog.EntryCommand[*catalog.TableMVCCNode, *catalog.TableNode])
		if !ok || cmd.ID == nil || cmd.ID.DbID == pkgcatalog.MO_CATALOG_ID || cmd.GetMVCCNode() == nil ||
			cmd.GetMVCCNode().BaseNode == nil || cmd.GetMVCCNode().BaseNode.Schema == nil {
			continue
		}
		if targets == nil {
			targets = make(map[replayGeneratedKey]*replayGeneratedTarget)
		}
		key := replayGeneratedKey{cmd.ID.DbID, cmd.ID.TableID}
		if prior := targets[key]; prior != nil {
			prior.invalid = true
			continue
		}
		targets[key] = &replayGeneratedTarget{schema: cmd.GetMVCCNode().BaseNode.Schema}
	}
	if len(targets) == 0 {
		return
	}
	for _, raw := range txn.Cmds {
		cmd, ok := raw.(*AppendCmd)
		if !ok || cmd.IsTombstone || cmd.Data == nil {
			continue
		}
		for _, info := range cmd.Infos {
			id := info.GetDest()
			if id.DbID != pkgcatalog.MO_CATALOG_ID || (id.TableID != pkgcatalog.MO_TABLES_ID && id.TableID != pkgcatalog.MO_COLUMNS_ID) {
				continue
			}
			start, end := int(info.GetSrcOff()), uint64(info.GetSrcOff())+uint64(info.GetSrcLen())
			if end > uint64(cmd.Data.Length()) {
				continue
			}
			for row := start; uint64(row) < end; row++ {
				if cmd.Data.HasDelete() && cmd.Data.IsDeleted(row) {
					continue
				}
				if id.TableID == pkgcatalog.MO_TABLES_ID {
					replayGeneratedTableRow(targets, cmd.Data, row)
				} else {
					replayGeneratedColumnRow(targets, cmd.Data, row)
				}
			}
		}
	}
	applyReplayGeneratedTargets(targets)
}

func applyReplayGeneratedTargets(targets map[replayGeneratedKey]*replayGeneratedTarget) {
	for _, target := range targets {
		if target.invalid || target.tableRows != 1 {
			continue
		}
		complete := true
		for _, col := range target.schema.ColDefs {
			if col == nil {
				complete = false
				break
			}
			if !col.IsPhyAddr() {
				if _, ok := target.seen[col.SeqNum]; !ok {
					complete = false
					break
				}
			}
		}
		if !complete {
			continue
		}
		for seq, expr := range target.generated {
			idx := target.schema.SeqnumMap[seq]
			target.schema.ColDefs[idx].GeneratedCol = expr
		}
	}
}

func replayGeneratedVector(bat *containers.Batch, name string, typ types.T, row int) *vector.Vector {
	idx, ok := bat.Nameidx[name]
	if !ok || idx < 0 || idx >= len(bat.Vecs) || bat.Vecs[idx] == nil {
		return nil
	}
	v := bat.Vecs[idx].GetDownstreamVector()
	if v == nil || v.GetType().Oid != typ || row < 0 || row >= v.Length() || v.IsNull(uint64(row)) {
		return nil
	}
	return v
}

func replayGeneratedTableRow(targets map[replayGeneratedKey]*replayGeneratedTarget, bat *containers.Batch, row int) {
	id := replayGeneratedVector(bat, pkgcatalog.SystemRelAttr_ID, types.T_uint64, row)
	db := replayGeneratedVector(bat, pkgcatalog.SystemRelAttr_DBID, types.T_uint64, row)
	account := replayGeneratedVector(bat, pkgcatalog.SystemRelAttr_AccID, types.T_uint32, row)
	version := replayGeneratedVector(bat, pkgcatalog.SystemRelAttr_Version, types.T_uint32, row)
	name := replayGeneratedVector(bat, pkgcatalog.SystemRelAttr_Name, types.T_varchar, row)
	if id == nil || db == nil || account == nil || version == nil || name == nil {
		return
	}
	key := replayGeneratedKey{vector.GetFixedAtNoTypeCheck[uint64](db, row), vector.GetFixedAtNoTypeCheck[uint64](id, row)}
	target := targets[key]
	if target == nil {
		return
	}
	target.tableRows++
	if target.schema.AcInfo.TenantID != vector.GetFixedAtNoTypeCheck[uint32](account, row) ||
		target.schema.Version != vector.GetFixedAtNoTypeCheck[uint32](version, row) ||
		!strings.EqualFold(target.schema.Name, name.GetStringAt(row)) {
		target.invalid = true
	}
}

func replayGeneratedColumnRow(targets map[replayGeneratedKey]*replayGeneratedTarget, bat *containers.Batch, row int) {
	id := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_RelID, types.T_uint64, row)
	db := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_DBID, types.T_uint64, row)
	account := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_AccID, types.T_uint32, row)
	seq := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Seqnum, types.T_uint16, row)
	name := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Name, types.T_varchar, row)
	nameOfTable := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_RelName, types.T_varchar, row)
	typ := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Type, types.T_varchar, row)
	ordinal := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Num, types.T_int32, row)
	notNull := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_NullAbility, types.T_int8, row)
	hidden := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_IsHidden, types.T_int8, row)
	cluster := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_IsClusterBy, types.T_int8, row)
	auto := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_IsAutoIncrement, types.T_int8, row)
	constraint := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_ConstraintType, types.T_char, row)
	hasDefault := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_HasExpr, types.T_int8, row)
	defaultExpr := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_DefaultExpr, types.T_varchar, row)
	hasUpdate := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_HasUpdate, types.T_int8, row)
	onUpdate := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Update, types.T_varchar, row)
	dropped := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_IsDropped, types.T_int8, row)
	enum := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_EnumValues, types.T_varchar, row)
	marker := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_HasGenerated, types.T_int8, row)
	expr := replayGeneratedVector(bat, pkgcatalog.SystemColAttr_Generated, types.T_varchar, row)
	if id == nil || db == nil || account == nil || seq == nil || name == nil || nameOfTable == nil || typ == nil ||
		ordinal == nil || notNull == nil || hidden == nil || cluster == nil || auto == nil || constraint == nil ||
		hasDefault == nil || defaultExpr == nil || hasUpdate == nil || onUpdate == nil || dropped == nil ||
		enum == nil || marker == nil || expr == nil {
		return
	}
	key := replayGeneratedKey{vector.GetFixedAtNoTypeCheck[uint64](db, row), vector.GetFixedAtNoTypeCheck[uint64](id, row)}
	target := targets[key]
	if target == nil {
		return
	}
	if name.GetStringAt(row) == pkgcatalog.Row_ID {
		return
	}
	sequence := vector.GetFixedAtNoTypeCheck[uint16](seq, row)
	idx, ok := target.schema.SeqnumMap[sequence]
	if !ok || idx < 0 || idx >= len(target.schema.ColDefs) || target.schema.AcInfo.TenantID != vector.GetFixedAtNoTypeCheck[uint32](account, row) {
		target.invalid = true
		return
	}
	col := target.schema.ColDefs[idx]
	var physicalType types.Type
	nullFlag := vector.GetFixedAtNoTypeCheck[int8](notNull, row)
	hiddenFlag := vector.GetFixedAtNoTypeCheck[int8](hidden, row)
	clusterFlag := vector.GetFixedAtNoTypeCheck[int8](cluster, row)
	autoFlag := vector.GetFixedAtNoTypeCheck[int8](auto, row)
	defaultFlag := vector.GetFixedAtNoTypeCheck[int8](hasDefault, row)
	updateFlag := vector.GetFixedAtNoTypeCheck[int8](hasUpdate, row)
	if col == nil || !strings.EqualFold(target.schema.Name, nameOfTable.GetStringAt(row)) ||
		!strings.EqualFold(col.Name, name.GetStringAt(row)) || vector.GetFixedAtNoTypeCheck[int32](ordinal, row) != int32(col.Idx+1) ||
		types.Decode(typ.GetBytesAt(row), &physicalType) != nil || physicalType != col.Type ||
		(nullFlag != 0 && nullFlag != 1) || (hiddenFlag != 0 && hiddenFlag != 1) ||
		(clusterFlag != 0 && clusterFlag != 1) || (autoFlag != 0 && autoFlag != 1) ||
		(defaultFlag != 0 && defaultFlag != 1) || (updateFlag != 0 && updateFlag != 1) ||
		vector.GetFixedAtNoTypeCheck[int8](dropped, row) != 0 ||
		(defaultFlag == 1) != (len(defaultExpr.GetBytesAt(row)) != 0) ||
		(updateFlag == 1) != (len(onUpdate.GetBytesAt(row)) != 0) ||
		col.NullAbility != (nullFlag == 0) || col.Hidden != (hiddenFlag == 1) ||
		col.ClusterBy != (clusterFlag == 1) || col.AutoIncrement != (autoFlag == 1) ||
		(constraint.GetStringAt(row) != pkgcatalog.SystemColPKConstraint && constraint.GetStringAt(row) != pkgcatalog.SystemColNoConstraint) ||
		col.Primary != (constraint.GetStringAt(row) == pkgcatalog.SystemColPKConstraint) ||
		!bytes.Equal(col.Default, defaultExpr.GetBytesAt(row)) || !bytes.Equal(col.OnUpdate, onUpdate.GetBytesAt(row)) ||
		col.EnumValues != enum.GetStringAt(row) {
		target.invalid = true
		return
	}
	if target.seen == nil {
		target.seen = make(map[uint16]struct{})
	}
	if _, duplicate := target.seen[sequence]; duplicate {
		target.invalid = true
		return
	}
	target.seen[sequence] = struct{}{}
	flag := vector.GetFixedAtNoTypeCheck[int8](marker, row)
	if flag == 0 {
		if len(expr.GetBytesAt(row)) != 0 {
			target.invalid = true
		}
		return
	}
	var generated plan.GeneratedCol
	data := expr.GetBytesAt(row)
	if flag != 1 || len(data) == 0 || types.Decode(data, &generated) != nil {
		target.invalid = true
		return
	}
	if target.generated == nil {
		target.generated = make(map[uint16][]byte)
	}
	target.generated[sequence] = bytes.Clone(data)
}
