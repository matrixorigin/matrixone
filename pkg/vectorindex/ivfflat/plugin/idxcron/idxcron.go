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

// Package idxcron is IVF-FLAT's idxcron hook implementation. It owns
// the lists / nsample heuristic that the executor used to host on
// (*IndexUpdateTaskInfo).checkIndexUpdatable, plus the
// kmeans_train_percent runtime adjustment that mutates task metadata.
//
// Decision tree mirrors the IVF-FLAT folklore (Faiss "30*nlist to
// 256*nlist training samples"):
//
//   - dsize < nlist                    : skip (k-means can't form
//     centroids with fewer points
//     than clusters)
//   - nsample < 30*nlist               : always reindex
//   - 30*nlist <= nsample < 256*nlist  : reindex every interval
//   - nsample >= 256*nlist             : reindex every 2*interval,
//     AND clamp
//     kmeans_train_percent to
//     256*nlist / dsize
//
// nsample = dsize * (kmeans_train_percent / 100), pulled from the
// task's persisted metadata blob.
package idxcron

import (
	"fmt"
	"strconv"
	"time"

	"github.com/bytedance/sonic"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	idxcronplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/idxcron"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	ivfflatrt "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/runtime"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// RunGetCountSql is the SELECT used to count source-table rows.
// Stubbed as a package-level var so tests can replace it.
var RunGetCountSql = sqlexec.RunSql

type Hooks struct{}

var _ idxcronplugin.Hooks = Hooks{}

// Updatable runs the IVF-FLAT-specific rebuild gate. The executor has
// already enforced auto_update on, currentHour matches, and
// createdAt + interval elapsed; everything below is IVF-FLAT-owned.
func (Hooks) Updatable(in idxcronplugin.UpdatableInput) (ok bool, reason string, err error) {
	logutil.Infof("[plugin] ivfflat Updatable: db=%s table=%s index=%s", in.TableDef.DbName, in.TableDef.Name, in.IndexName)
	nlist, err := lookupNlist(in.TableDef.Indexes, in.IndexName)
	if err != nil {
		return false, "", err
	}
	if nlist == 0 {
		return false, "", moerr.NewInternalErrorNoCtx("IVFFLAT index parameter LISTS not found")
	}

	dsize, err := countSourceRows(in.Sqlproc, in.TableDef.DbName, in.TableDef.Name)
	if err != nil {
		return false, "", err
	}

	// Fewer source rows than clusters — k-means can't form a
	// non-degenerate index, so skip and let brute-force handle queries.
	if dsize < nlist {
		return false, fmt.Sprintf("source data size < Nlist (%d < %d)", dsize, nlist), nil
	}

	lower := float64(30 * nlist)
	upper := float64(256 * nlist)

	// kmeans_train_percent comes from the index's algo_params (set as a CREATE
	// INDEX option), or the build default when absent — the same value
	// ivf_create resolves at rebuild time, so nsample tracks the real training
	// size. (Formerly read from, and clamped into, the idxcron task Metadata.)
	ivfTrainPercent, err := lookupKmeansTrainPercent(in.TableDef.Indexes, in.IndexName)
	if err != nil {
		return false, "", err
	}
	nsample := float64(dsize) * (ivfTrainPercent / 100)

	now := time.Now()

	switch {
	case nsample < lower:
		// Training sample too small to be representative — always reindex.
		return true, "", nil

	case nsample < upper:
		// Reindex every interval.
		if in.LastUpdateAt == nil {
			return true, "", nil
		}
		ts := time.Unix(in.LastUpdateAt.Unix(), 0).Add(in.Interval)
		if ts.After(now) {
			return false, fmt.Sprintf(
				"training sample size in between lower and upper limit (%f < %f < %f) AND current time < interval after lastUpdatedAt (%v < %v)",
				lower, nsample, upper, now.Format("2006-01-02 15:04:05"), ts.Format("2006-01-02 15:04:05")), nil
		}
		return true, "", nil

	default:
		// nsample >= upper — k-means over a large training sample is
		// expensive, so reindex on a slower 2*interval cadence.
		if in.LastUpdateAt != nil {
			ts := time.Unix(in.LastUpdateAt.Unix(), 0).Add(2 * in.Interval)
			if ts.After(now) {
				return false, fmt.Sprintf(
					"training sample size > upper limit ( %f > %f) AND current time < 2*interval after lastUpdatedAt (%v < %v)",
					nsample, upper, now.Format("2006-01-02 15:04:05"), ts.Format("2006-01-02 15:04:05")), nil
			}
		}
		return true, "", nil
	}
}

// lookupKmeansTrainPercent reads kmeans_train_percent from the named index's
// algo_params (present only when given as a CREATE INDEX option), falling back
// to the build default when absent — matching what ivf_create resolves at
// rebuild time.
func lookupKmeansTrainPercent(indexes []*plan.IndexDef, indexName string) (float64, error) {
	for _, idx := range indexes {
		if idx.IndexName != indexName {
			continue
		}
		ast, err := sonic.Get([]byte(idx.IndexAlgoParams), catalog.IndexAlgoParamKmeansTrainPercent)
		if err != nil {
			// key absent → not set in CREATE INDEX; use the build default.
			return ivfflatrt.DefaultKmeansTrainPercent, nil
		}
		s, err := ast.String()
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}
	return ivfflatrt.DefaultKmeansTrainPercent, nil
}

// lookupNlist reads the "lists" key from the named index's
// indexAlgoParams. Returns 0 (not an error) when the key is absent
// or the index isn't found — the caller surfaces missing-LISTS as a
// task error to match the executor's historical behaviour. A
// malformed JSON value for the lists key is surfaced as an error
// rather than silently treated as "missing".
func lookupNlist(indexes []*plan.IndexDef, indexName string) (int64, error) {
	for _, idx := range indexes {
		if idx.IndexName != indexName {
			continue
		}
		ast, err := sonic.Get([]byte(idx.IndexAlgoParams), catalog.IndexAlgoParamLists)
		if err != nil {
			return 0, nil
		}
		return ast.Int64()
	}
	return 0, nil
}

// countSourceRows runs the SELECT COUNT(*) used to drive the
// nsample heuristic. Returns 0 on an empty/absent result. COUNT
// aggregates are registered as types.T_int64 (see
// pkg/sql/plan/function/list_agg.go), so the result vector is int64;
// reading it as uint64 would panic the type check.
func countSourceRows(sqlproc *sqlexec.SqlProcess, dbName, tableName string) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dbName, tableName)
	res, err := RunGetCountSql(sqlproc, sql)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return 0, nil
	}
	bat := res.Batches[0]
	if bat.RowCount() == 0 {
		return 0, nil
	}
	return vector.GetFixedAtWithTypeCheck[int64](bat.Vecs[0], 0), nil
}
