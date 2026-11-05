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

package productl2

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var productl2_runSql = sqlexec.RunSql

// Ignored is how the operator treats the probe rows the IVF-FLAT index on its build side cannot
// store. Without a table, such a row fails the operator; with one, the row is left out of the
// result and recorded in the table.
type Ignored struct {
	Storage ivfflat.Storage `json:"storage"`
	Db      string          `json:"db,omitempty"`
	Table   string          `json:"table,omitempty"`
	// Pk is the probe position of the primary key, or -1.
	Pk int32 `json:"pk"`
	// Serial reports that the primary key is the serial of a composite key, written in hex.
	Serial bool `json:"serial,omitempty"`
	// Enum reports that the primary key is an ENUM, which a probe may carry as its serial.
	Enum bool `json:"enum,omitempty"`
	// EnumValues are the members of an ENUM primary key.
	EnumValues string `json:"enum_values,omitempty"`
	// Limit is the number of rows the table may hold.
	Limit int64 `json:"limit,omitempty"`
}

// EncodeIgnored returns ig as a string, "" for nil.
func EncodeIgnored(ig *Ignored) (string, error) {
	if ig == nil {
		return "", nil
	}
	b, err := json.Marshal(ig)
	return string(b), err
}

// DecodeIgnored returns the Ignored of s, nil for "".
func DecodeIgnored(s string) (*Ignored, error) {
	if s == "" {
		return nil, nil
	}
	ig := &Ignored{}
	if err := json.Unmarshal([]byte(s), ig); err != nil {
		return nil, moerr.NewInternalErrorNoCtxf("product_l2: invalid ignored config: %v", err)
	}
	return ig, nil
}

// leaveOut checks the non-NULL probe rows of vec and returns which of them are left out,
// recording them in the ignored table; it is nil when none is.
func (ctr *container) leaveOut(ap *Productl2, proc *process.Process, vec *vector.Vector) ([]bool, error) {
	ig := ap.Ignored
	if ig == nil {
		return nil, nil
	}
	var out []bool
	var rows []vectorindex.IgnoredRow
	ts := ctr.sqlproc.BuildSnapshotTS()
	for j := 0; j < vec.Length(); j++ {
		if vec.IsNull(uint64(j)) {
			continue
		}
		cerr := ig.Storage.Check(vec.GetBytesAt(j))
		if cerr == nil {
			continue
		}
		code, ok := vectorindex.UnstorableCode(cerr)
		if !ok || ig.Table == "" || ig.Pk < 0 {
			return nil, cerr
		}
		pk, err := ig.pkLiteral(proc, ctr.inBat.Vecs[ig.Pk], j)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = make([]bool, vec.Length())
		}
		out[j] = true
		rows = append(rows, vectorindex.IgnoredRow{Pk: pk, Code: code, Reason: vectorindex.IgnoredReason(cerr), Ts: ts})
		if ctr.ignored == 0 {
			ctr.firstPk, ctr.firstErr = ig.pkString(ctr.inBat.Vecs[ig.Pk], j, pk), cerr
		}
		ctr.ignored++
	}
	cols := vectorindex.IgnoredColumns{
		Pk:     catalog.IndexIgnored_TblCol_Pk,
		Code:   catalog.IndexIgnored_TblCol_Code,
		Reason: catalog.IndexIgnored_TblCol_Reason,
		Ts:     catalog.IndexIgnored_TblCol_Ts,
	}
	for _, sql := range vectorindex.IgnoredRowsSql(ig.Db, ig.Table, cols, rows) {
		res, err := productl2_runSql(ctr.sqlproc, sql)
		if err != nil {
			return nil, err
		}
		res.Close()
	}
	return out, nil
}

// pkLiteral returns the SQL literal of the primary key in row j of vec.
func (ig *Ignored) pkLiteral(proc *process.Process, vec *vector.Vector, j int) (string, error) {
	if ig.Serial {
		return vectorindex.SerialPkString(vec.GetBytesAt(j)), nil
	}
	if ig.Enum {
		e, err := enumKey(vec, j)
		if err != nil {
			return "", err
		}
		return vectorindex.EnumPkLiteral(e, ig.EnumValues), nil
	}
	return iscp.SqlLiteral(proc.Ctx, vec, j)
}

// enumKey returns the ENUM primary key in row j of vec, which carries the key or its serial.
func enumKey(vec *vector.Vector, j int) (types.Enum, error) {
	if vec.IsConst() {
		j = 0
	}
	if vec.GetType().Oid == types.T_enum {
		return vector.GetFixedAtNoTypeCheck[types.Enum](vec, j), nil
	}
	tp, err := types.Unpack(vec.GetBytesAt(j))
	if err != nil {
		return 0, err
	}
	var e types.Enum
	ok := len(tp) == 1
	if ok {
		e, ok = tp[0].(types.Enum)
	}
	if !ok {
		return 0, moerr.NewInternalErrorNoCtxf("product_l2: the ENUM primary key is serial %v", tp)
	}
	return e, nil
}

// pkString returns the primary key in row j of vec, whose literal is lit, as users read it.
func (ig *Ignored) pkString(vec *vector.Vector, j int, lit string) string {
	if ig.Enum {
		if e, err := enumKey(vec, j); err == nil {
			if label, err := types.ParseEnumIndex(ig.EnumValues, e); err == nil {
				return label
			}
			return strconv.FormatUint(uint64(e), 10)
		}
	}
	return lit
}

// endIgnored fails the operator when the table holds more rows than its limit after the rows the
// operator recorded, and logs them. An instance counts after its last write, so the last of the
// parallel instances to count sees every row the statement recorded.
func (ctr *container) endIgnored(ap *Productl2) error {
	if ctr.ignored == 0 {
		return nil
	}
	ig := ap.Ignored
	res, err := productl2_runSql(ctr.sqlproc, fmt.Sprintf("SELECT COUNT(*) FROM %s", sqlquote.QualifiedIdent(ig.Db, ig.Table)))
	if err != nil {
		return err
	}
	var n int64
	for _, b := range res.Batches {
		if b != nil && b.RowCount() > 0 {
			n = vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0)
		}
	}
	res.Close()
	ctr.logIgnored(ap)
	if n > ig.Limit {
		return moerr.NewIndexTooManyBadRecordsNoCtx(ig.Limit, ctr.firstPk, ctr.firstErr.Error())
	}
	return nil
}

// logIgnored writes one log line for the rows the operator left out.
func (ctr *container) logIgnored(ap *Productl2) {
	if ctr.ignored == 0 {
		return
	}
	logutil.Warn("ivfflat: rows the index cannot store are left out",
		zap.String("table", ap.Ignored.Table),
		zap.Int64("rows", ctr.ignored),
		zap.String("pk", ctr.firstPk),
		zap.Error(ctr.firstErr),
	)
	ctr.ignored = 0
}
