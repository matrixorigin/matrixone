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

package table_function

import (
	"bytes"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const maxFulltextAndTopK = uint64(4096)

type fulltextTermEstimate struct {
	term  string
	rows  uint64
	known bool
}

var ftEstimateAndOrderFulltextTerms = estimateAndOrderFulltextTerms

func splitFulltextTableName(name string) (string, string, error) {
	name = strings.TrimSpace(name)
	var parts []string
	if strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`") {
		parts = strings.SplitN(name[1:len(name)-1], "`.`", 2)
	} else {
		parts = strings.SplitN(name, ".", 2)
	}
	if len(parts) != 2 {
		return "", "", moerr.NewInvalidInputNoCtx("fulltext index table must be schema-qualified")
	}
	trim := func(s string) string { return strings.Trim(strings.TrimSpace(s), "`") }
	db, table := trim(parts[0]), trim(parts[1])
	if db == "" || table == "" {
		return "", "", moerr.NewInvalidInputNoCtx("invalid fulltext index table name")
	}
	return db, table, nil
}

// orderFulltextTerms uses an object-level upper bound. Since the hidden table
// is clustered by word, only objects whose word zone map can contain a term
// contribute rows. An uninitialized zone map makes all estimates incomplete;
// ordering then falls back to deterministic token properties.
func orderFulltextTerms(terms []string, infos []*plan.MetadataScanInfo) (ordered []string, driverRows uint64, fallback int64) {
	estimates := make([]fulltextTermEstimate, len(terms))
	metadataComplete := len(infos) > 0
	for _, info := range infos {
		if info == nil || len(info.ZoneMap) == 0 || !index.ZM(info.ZoneMap).IsInited() || !index.ZM(info.ZoneMap).IsString() {
			metadataComplete = false
			break
		}
	}
	for i, term := range terms {
		estimates[i] = fulltextTermEstimate{term: term, known: metadataComplete}
		if metadataComplete {
			for _, info := range infos {
				if index.ZM(info.ZoneMap).Contains([]byte(term)) && info.RowCnt > 0 {
					estimates[i].rows += uint64(info.RowCnt)
				}
			}
		} else {
			fallback++
		}
	}
	sort.SliceStable(estimates, func(i, j int) bool {
		if estimates[i].known != estimates[j].known {
			return estimates[i].known
		}
		if estimates[i].known && estimates[i].rows != estimates[j].rows {
			return estimates[i].rows < estimates[j].rows
		}
		if len(estimates[i].term) != len(estimates[j].term) {
			return len(estimates[i].term) > len(estimates[j].term)
		}
		return bytes.Compare([]byte(estimates[i].term), []byte(estimates[j].term)) < 0
	})
	ordered = make([]string, len(estimates))
	for i := range estimates {
		ordered[i] = estimates[i].term
	}
	if len(estimates) > 0 && estimates[0].known {
		driverRows = estimates[0].rows
	}
	return ordered, driverRows, fallback
}

func estimateAndOrderFulltextTerms(proc *process.Process, analyzer process.Analyzer, indexTable string, terms []string) ([]string, uint64, int64, error) {
	dbName, tableName, err := splitFulltextTableName(indexTable)
	if err != nil {
		return nil, 0, 0, err
	}
	e, ok := proc.Ctx.Value(defines.EngineKey{}).(engine.Engine)
	if !ok || e == nil {
		return nil, 0, 0, moerr.NewInternalError(proc.Ctx, "missing engine for fulltext term estimator")
	}
	db, err := e.Database(proc.Ctx, dbName, proc.GetTxnOperator())
	if err != nil {
		return nil, 0, 0, err
	}
	rel, err := db.Relation(proc.Ctx, tableName, nil)
	if err != nil {
		return nil, 0, 0, err
	}
	ctx := perfcounter.AttachS3RequestKey(proc.Ctx, analyzer.GetOpCounterSet())
	infos, err := process.MeasureFilesystemWait(analyzer, func() ([]*plan.MetadataScanInfo, error) {
		return rel.GetColumMetadataScanInfo(ctx, "word", false)
	})
	if err != nil {
		return nil, 0, 0, err
	}
	ordered, rows, fallback := orderFulltextTerms(terms, infos)
	return ordered, rows, fallback, nil
}
