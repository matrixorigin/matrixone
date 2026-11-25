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

package compile

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/productl2"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat"
)

// productL2Ignored returns how the product_l2 join node treats the probe rows the IVF-FLAT index
// it assigns them to cannot store, or nil when its CENTROIDX option names no index.
func (c *Compile) productL2Ignored(node *plan.Node) (*productl2.Ignored, error) {
	opt, err := tree.ParseCentroidxOption(node.ExtraOptions)
	if err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("invalid CENTROIDX option: %v", err)
	}
	if opt.Params == "" || opt.Config == "" {
		return nil, nil
	}
	params, err := catalog.IndexParamsStringToMap(opt.Params)
	if err != nil {
		return nil, err
	}
	var cfg vectorindex.IndexTableConfig
	if err = json.Unmarshal([]byte(opt.Config), &cfg); err != nil {
		return nil, moerr.NewInvalidInputNoCtxf("invalid CENTROIDX index table config: %v", err)
	}

	q := strings.ToLower(params[catalog.Quantization])
	var min, max float64
	bounded := false
	if q == "int8" || q == "uint8" {
		if min, max, bounded, err = c.quantizeBounds(cfg.DbName, cfg.MetadataTable); err != nil {
			return nil, err
		}
	}
	ig := &productl2.Ignored{
		Storage:    ivfflat.NewStorage(types.T(cfg.KeyPartType), q, min, max, bounded),
		Pk:         -1,
		Serial:     cfg.PKey == catalog.CPrimaryKeyColName,
		Enum:       types.T(cfg.PKeyType) == types.T_enum,
		EnumValues: cfg.PKeyEnum,
	}
	if cfg.IgnoredTable != "" {
		ig.Db, ig.Table, ig.Limit = cfg.DbName, cfg.IgnoredTable, cfg.MaxBadRecords
	}
	ig.Pk = probePkPos(node.ProjectList, cfg.PKey, catalog.SystemSI_IVFFLAT_TblCol_Entries_pk)
	return ig, nil
}

// probePkPos returns the position of the probe column named one of names, or named one of them
// under one table qualifier, or -1. A name may itself hold dots, so an exact name wins.
func probePkPos(list []*plan.Expr, names ...string) int32 {
	match := func(qualified bool) int32 {
		for _, expr := range list {
			col := expr.GetCol()
			if col == nil || col.RelPos != 0 {
				continue
			}
			for _, name := range names {
				if col.Name == name {
					return col.ColPos
				}
				if qualified && strings.HasSuffix(col.Name, "."+name) &&
					!strings.Contains(col.Name[:len(col.Name)-len(name)-1], ".") {
					return col.ColPos
				}
			}
		}
		return -1
	}
	if pos := match(false); pos >= 0 {
		return pos
	}
	return match(true)
}

// quantizeBounds reads the trained int8/uint8 bounds from the IVF-FLAT metadata table.
func (c *Compile) quantizeBounds(db, metadata string) (min, max float64, ok bool, err error) {
	sql := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s IN (%s, %s)",
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_key),
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val),
		sqlquote.QualifiedIdent(db, metadata),
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_key),
		sqlquote.String(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin),
		sqlquote.String(catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax))
	res, err := c.runSqlWithResult(sql, NoAccountId)
	if err != nil {
		return 0, 0, false, err
	}
	defer res.Close()
	var hasMin, hasMax bool
	res.ReadRows(func(rows int, cols []*vector.Vector) bool {
		for i := 0; i < rows && err == nil; i++ {
			var v float64
			v, err = strconv.ParseFloat(strings.TrimSpace(cols[1].GetStringAt(i)), 64)
			switch cols[0].GetStringAt(i) {
			case catalog.SystemSI_IVFFLAT_Metadata_QuantizeMin:
				min, hasMin = v, true
			case catalog.SystemSI_IVFFLAT_Metadata_QuantizeMax:
				max, hasMax = v, true
			}
		}
		return err == nil
	})
	return min, max, hasMin && hasMax, err
}
