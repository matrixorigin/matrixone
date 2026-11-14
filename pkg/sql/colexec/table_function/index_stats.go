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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/indexstats"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/diagnostic"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	indexStatsRunSQL       = sqlexec.RunSql
	indexStatsRunStreaming = sqlexec.RunStreamingSql
	indexStatsReadStorage  = readIndexStatsStorage
	indexStatsSourceCommit = readIndexStatsSourceCommit
)

type indexStatsState struct {
	config      indexstats.Config
	batch       *batch.Batch
	version     int64
	hasVersion  bool
	buckets     []*diagnostic.Bucket
	summary     diagnostic.Summary
	next        int
	reason      string
	untrained   bool
	storage     map[string]any
	maintenance map[string]any
	fulltext    *indexstats.FullTextSummary
	keySummary  *indexstats.KeySummary
}

func indexStatsPrepare(proc *process.Process, tf *TableFunction) (tvfState, error) {
	state := &indexStatsState{}
	if len(tf.Args) != 3 {
		return nil, moerr.NewInvalidInput(proc.Ctx, "mo_index_stats requires 3 arguments (database, table, index)")
	}
	if err := json.Unmarshal(tf.Params, &state.config); err != nil {
		return nil, err
	}
	return state, nil
}

func (s *indexStatsState) reset(_ *TableFunction, _ *process.Process) {
	if s.batch != nil {
		s.batch.CleanOnlyData()
	}
	s.buckets = nil
	s.storage = nil
	s.maintenance = nil
	s.fulltext = nil
	s.keySummary = nil
	s.next = 0
}

func (s *indexStatsState) free(_ *TableFunction, proc *process.Process, _ bool, _ error) {
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
	s.buckets = nil
}

func (s *indexStatsState) end(_ *TableFunction, _ *process.Process) error { return nil }

func (s *indexStatsState) start(tf *TableFunction, proc *process.Process, _ int, _ process.Analyzer) error {
	s.reset(tf, proc)
	s.reason = ""
	s.untrained = false
	s.hasVersion = false
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	}
	sqlproc := sqlexec.NewSqlProcess(proc)
	sqlproc.ApplyScanSnapshot(tf.ScanSnapshot)
	if s.config.KeyKind != "" {
		return s.startKeyIndex(proc, sqlproc)
	}
	if s.config.FullTextVersion > 0 {
		return s.startFullText(proc, sqlproc)
	}
	versionSQL := fmt.Sprintf("SELECT CAST(%s AS BIGINT) FROM %s WHERE %s = 'version'",
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_val),
		sqlquote.QualifiedIdent(s.config.Database, s.config.Metadata),
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Metadata_key))
	res, err := indexStatsRunSQL(sqlproc, versionSQL)
	if err != nil {
		return err
	}
	versionRows := 0
	for _, bat := range res.Batches {
		for row := 0; row < bat.RowCount(); row++ {
			if bat.Vecs[0].IsNull(uint64(row)) {
				continue
			}
			s.version = vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], row)
			versionRows++
		}
	}
	res.Close()
	if versionRows == 0 && s.config.Async {
		s.summary = diagnostic.Summary{}
		s.reason = "index_not_initialized"
		return s.readMetadata(proc, sqlproc)
	}
	if versionRows != 1 {
		return moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: current IVF version is missing or ambiguous")
	}
	s.hasVersion = true
	centerSQL := fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s = %d",
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Centroids_id),
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid),
		sqlquote.QualifiedIdent(s.config.Database, s.config.Centroids),
		sqlquote.Ident(catalog.SystemSI_IVFFLAT_TblCol_Centroids_version), s.version)
	centers, err := indexStatsRunSQL(sqlproc, centerSQL)
	if err != nil {
		return err
	}
	buckets, err := readIndexStatsCenters(centers)
	if err != nil {
		return err
	}
	geometry := strings.EqualFold(s.config.OpType, "vector_l2_ops")
	quantized := s.config.Quantization != "" && !strings.EqualFold(s.config.Quantization, "none")
	if !geometry {
		s.reason = "center_stats_unsupported_distance"
	} else if quantized && s.config.Async {
		// Current primary-table values can be ahead of CDC's indexed values.
		// Joining them would misreport maintenance lag as geometric drift.
		geometry = false
		s.reason = "center_stats_unavailable_async_quantized"
	}
	query := buildIndexStatsEntriesSQL(s.config, s.version, geometry, quantized)
	var scratch []float64
	err = streamIndexStats(proc, sqlproc, query, func(bat *batch.Batch) error {
		for row := 0; row < bat.RowCount(); row++ {
			id := vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], row)
			bucket, ok := buckets[id]
			if !ok {
				return moerr.NewInvalidInputf(proc.Ctx, "mo_index_stats: entry references missing centroid %d in version %d", id, s.version)
			}
			var values []float64
			if geometry {
				var decodeErr error
				values, decodeErr = indexStatsVectorInto(bat.Vecs[1], row, scratch)
				if decodeErr != nil {
					return decodeErr
				}
				if values != nil {
					scratch = values
				}
			}
			if addErr := bucket.Add(values); addErr != nil {
				return addErr
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.buckets = make([]*diagnostic.Bucket, 0, len(buckets))
	for _, bucket := range buckets {
		s.buckets = append(s.buckets, bucket)
		s.untrained = s.untrained || bucket.Center == nil
	}
	sort.Slice(s.buckets, func(i, j int) bool { return s.buckets[i].ID < s.buckets[j].ID })
	s.summary = diagnostic.Summarize(s.buckets)
	for _, value := range []float64{s.summary.CV, s.summary.Geometry.MeanResidual,
		s.summary.Geometry.RMSResidual, s.summary.Geometry.CenterShift, s.summary.Geometry.Mismatch} {
		if math.IsInf(value, 0) || math.IsNaN(value) {
			return moerr.NewInvalidInput(proc.Ctx, "mo_index_stats: summary residual overflow")
		}
	}
	return s.readMetadata(proc, sqlproc)
}

func (s *indexStatsState) readMetadata(proc *process.Process, sqlproc *sqlexec.SqlProcess) error {
	var err error
	s.storage, err = indexStatsReadStorage(proc, sqlproc, s.config)
	if err != nil {
		return err
	}
	if s.config.Async {
		s.maintenance, err = readIndexStatsMaintenance(proc, sqlproc, s.config)
		if err != nil {
			return err
		}
	}
	return nil
}

func buildIndexStatsEntriesSQL(cfg indexstats.Config, version int64, geometry, quantized bool) string {
	id := sqlquote.QualifiedIdent("e", catalog.SystemSI_IVFFLAT_TblCol_Entries_id)
	query := "SELECT " + id
	if geometry {
		if quantized {
			query += ", " + sqlquote.QualifiedIdent("src", cfg.Column)
		} else {
			query += ", " + sqlquote.QualifiedIdent("e", catalog.SystemSI_IVFFLAT_TblCol_Entries_entry)
		}
	}
	query += " FROM " + sqlquote.QualifiedIdent(cfg.Database, cfg.Entries) + " AS e"
	if geometry && quantized {
		// LEFT JOIN retains an index entry even if its primary row is missing.
		query += " LEFT JOIN " + sqlquote.QualifiedIdent(cfg.Database, cfg.Table) + " AS src ON " +
			sqlquote.QualifiedIdent("e", catalog.SystemSI_IVFFLAT_TblCol_Entries_pk) + " = " +
			sqlquote.QualifiedIdent("src", cfg.PrimaryKey)
	}
	return query + " WHERE " + sqlquote.QualifiedIdent("e", catalog.SystemSI_IVFFLAT_TblCol_Entries_version) + " = " + strconv.FormatInt(version, 10)
}

func readIndexStatsCenters(res executor.Result) (map[int64]*diagnostic.Bucket, error) {
	defer res.Close()
	buckets := make(map[int64]*diagnostic.Bucket)
	for _, bat := range res.Batches {
		for row := 0; row < bat.RowCount(); row++ {
			id := vector.GetFixedAtNoTypeCheck[int64](bat.Vecs[0], row)
			center, err := indexStatsVector(bat.Vecs[1], row)
			if err != nil {
				return nil, err
			}
			if _, exists := buckets[id]; exists {
				return nil, moerr.NewInvalidInputNoCtxf("mo_index_stats: duplicate centroid %d", id)
			}
			buckets[id] = diagnostic.NewBucket(id, center)
		}
	}
	return buckets, nil
}

// streamIndexStats releases each input batch immediately. Only O(centroids *
// dimensions) running statistics are retained, rather than all entry vectors.
func streamIndexStats(proc *process.Process, sqlproc *sqlexec.SqlProcess, query string, consume func(*batch.Batch) error) (err error) {
	ctx, cancel := context.WithCancel(proc.GetTopContext())
	results := make(chan executor.Result, 1)
	done := make(chan error, 1)
	go func() {
		defer close(results)
		executorErrors := make(chan error, 1)
		res, runErr := indexStatsRunStreaming(ctx, sqlproc, query, results, executorErrors)
		res.Close()
		select {
		case executorErr := <-executorErrors:
			if executorErr != nil {
				runErr = executorErr
			}
		default:
		}
		done <- runErr
	}()
	defer func() {
		cancel()
		for res := range results {
			res.Close()
		}
		if runErr := <-done; err == nil {
			err = runErr
		}
	}()
	for {
		select {
		case res, ok := <-results:
			if !ok {
				return nil
			}
			consumeErr := func() error {
				defer res.Close()
				for _, bat := range res.Batches {
					if err := consume(bat); err != nil {
						return err
					}
				}
				return nil
			}()
			if consumeErr != nil {
				return consumeErr
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func indexStatsVector(vec *vector.Vector, row int) ([]float64, error) {
	return indexStatsVectorInto(vec, row, nil)
}

func indexStatsVectorInto(vec *vector.Vector, row int, values []float64) ([]float64, error) {
	if vec.IsNull(uint64(row)) {
		return nil, nil
	}
	raw := vec.GetBytesAt(row)
	width := 0
	var cell types.BlockScaledCell
	switch vec.GetType().Oid {
	case types.T_array_float64:
		width = 8
	case types.T_array_float32:
		width = 4
	case types.T_array_float16, types.T_array_bf16:
		width = 2
	case types.T_array_int8, types.T_array_uint8:
		width = 1
	case types.T_array_float8, types.T_array_float4:
		var err error
		cell, err = types.ParseBlockScaledCell(raw)
		if err != nil {
			return nil, err
		}
		format, _ := vec.GetType().Oid.BlockScaledFormat()
		if cell.Format != format || vec.GetType().Width > 0 &&
			vec.GetType().Width != types.MaxArrayDimension && int(vec.GetType().Width) != cell.Dim {
			return nil, moerr.NewInvalidInputNoCtxf("mo_index_stats: block-scaled vector cell does not match %s", vec.GetType())
		}
	}
	dim := cell.Dim
	if dim == 0 {
		if width == 0 || len(raw) == 0 || len(raw)%width != 0 {
			return nil, moerr.NewInvalidInputNoCtxf("mo_index_stats: invalid vector encoding for %s", vec.GetType())
		}
		dim = len(raw) / width
	}
	if cap(values) < dim {
		values = make([]float64, dim)
	} else {
		values = values[:dim]
	}
	switch vec.GetType().Oid {
	case types.T_array_float64:
		copy(values, types.BytesToArray[float64](raw))
	case types.T_array_float32:
		for i, v := range types.BytesToArray[float32](raw) {
			values[i] = float64(v)
		}
	case types.T_array_float16:
		for i, v := range types.BytesToArray[types.Float16](raw) {
			values[i] = float64(v.ToFloat32())
		}
	case types.T_array_bf16:
		for i, v := range types.BytesToArray[types.BF16](raw) {
			values[i] = float64(v.ToFloat32())
		}
	case types.T_array_int8:
		for i, v := range types.BytesToArray[int8](raw) {
			values[i] = float64(v)
		}
	case types.T_array_uint8:
		for i, v := range types.BytesToArray[uint8](raw) {
			values[i] = float64(v)
		}
	case types.T_array_float8, types.T_array_float4:
		for i := range values {
			values[i] = float64(cell.At(i))
		}
	}
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, moerr.NewInvalidInputNoCtx("mo_index_stats: non-finite vector component")
		}
	}
	return values, nil
}

func (s *indexStatsState) call(tf *TableFunction, proc *process.Process) (vm.CallResult, error) {
	if s.next > len(s.buckets) {
		return vm.CancelResult, nil
	}
	s.batch.CleanOnlyData()
	count := 0
	for count < 128 && s.next <= len(s.buckets) {
		var bucket *diagnostic.Bucket
		rowType := "index"
		if s.next != 0 {
			bucket = s.buckets[s.next-1]
			rowType = "centroid"
		}
		for i, attr := range tf.Attrs {
			v := s.batch.Vecs[i]
			var err error
			switch strings.ToLower(attr) {
			case "index_type":
				indexType := "ivfflat"
				if s.config.FullTextVersion > 0 {
					indexType = "fulltext"
				} else if s.config.KeyKind != "" {
					indexType = s.config.KeyKind
				}
				err = vector.AppendBytes(v, []byte(indexType), false, proc.Mp())
			case "row_type":
				err = vector.AppendBytes(v, []byte(rowType), false, proc.Mp())
			case "object_id":
				var id string
				if bucket != nil {
					id = strconv.FormatInt(bucket.ID, 10)
				}
				err = vector.AppendBytes(v, []byte(id), bucket == nil, proc.Mp())
			case "metrics":
				encoded, encodeErr := json.Marshal(s.rowMetrics(bucket))
				if encodeErr != nil {
					return vm.CancelResult, encodeErr
				}
				value, parseErr := types.ParseSliceToByteJson(encoded)
				if parseErr != nil {
					return vm.CancelResult, parseErr
				}
				err = vector.AppendByteJson(v, value, false, proc.Mp())
			default:
				err = moerr.NewInvalidInputf(proc.Ctx, "mo_index_stats: unknown output column %s", attr)
			}
			if err != nil {
				return vm.CancelResult, err
			}
		}
		s.next++
		count++
	}
	s.batch.SetRowCount(count)
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}

// rowMetrics keeps each algorithm's payload separate from the stable public
// columns. Index-wide distribution fields have no meaning on a centroid row.
func (s *indexStatsState) rowMetrics(bucket *diagnostic.Bucket) map[string]any {
	if s.keySummary != nil {
		return s.keyIndexMetrics()
	}
	if s.fulltext != nil {
		return s.fullTextMetrics()
	}
	var count int64
	var geometry diagnostic.Geometry
	var hasGeometry bool
	var metrics map[string]any
	avg := float64(0)
	if s.summary.Centroids > 0 {
		avg = float64(s.summary.Count) / float64(s.summary.Centroids)
	}
	if bucket == nil {
		count, geometry, hasGeometry = s.summary.Count, s.summary.Geometry, s.summary.HasGeometry
		metrics = map[string]any{
			"index_version":        s.version,
			"vector_count":         count,
			"centroid_count":       s.summary.Centroids,
			"empty_centroid_count": s.summary.Empty,
			"min_bucket_size":      s.summary.Min,
			"max_bucket_size":      s.summary.Max,
			"bucket_size_cv":       s.summary.CV,
			"lists":                s.config.Lists,
			"op_type":              s.config.OpType,
			"quantization":         s.config.Quantization,
			"maintenance_mode":     "sync",
		}
		options := make(map[string]string)
		for name, value := range s.config.Options {
			switch strings.ToLower(name) {
			case catalog.IndexAlgoParamLists, catalog.IndexAlgoParamOpType, catalog.Quantization, catalog.Async:
				// These options already have typed summary fields.
			default:
				options[name] = value
			}
		}
		if len(options) > 0 {
			metrics["index_options"] = options
		}
		if !s.hasVersion {
			metrics["index_version"] = nil
		}
		if s.summary.Centroids > 0 {
			metrics["empty_centroid_ratio"] = float64(s.summary.Empty) / float64(s.summary.Centroids)
		}
		metrics["avg_bucket_size"] = avg
		if avg > 0 {
			metrics["max_bucket_size_multiple"] = float64(s.summary.Max) / avg
		}
		if s.storage != nil {
			metrics["storage"] = s.storage
		}
		if s.config.Async {
			metrics["maintenance_mode"] = "async"
			metrics["async"] = s.maintenance
		}
	} else {
		count = bucket.Count
		geometry, hasGeometry = bucket.Geometry()
		metrics = map[string]any{"vector_count": count}
		if avg > 0 {
			metrics["bucket_size_multiple"] = float64(count) / avg
		}
	}
	status := "ok"
	switch {
	case s.reason != "":
		status = s.reason
	case bucket != nil && bucket.Center == nil || bucket == nil && s.untrained:
		status = "untrained"
	case bucket == nil && s.summary.Centroids == 0:
		status = "no_centroids"
	case count == 0:
		status = "empty"
	case !hasGeometry:
		status = "incomplete_vectors"
	}
	if status != "ok" && status != "empty" {
		metrics["status"] = status
	}
	if hasGeometry {
		metrics["center_mismatch_ratio"] = geometry.Mismatch
	}
	return metrics
}
