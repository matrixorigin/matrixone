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

package mongoscan

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/mongodb"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (scan *MongoScan) String(buf *bytes.Buffer) {
	buf.WriteString("mongoscan: credential-free external source")
}

func (scan *MongoScan) OpType() vm.OpType { return vm.MongoScan }

func (scan *MongoScan) Prepare(proc *process.Process) (err error) {
	if scan.OpAnalyzer == nil {
		scan.OpAnalyzer = process.NewAnalyzer(scan.GetIdx(), scan.IsFirst, scan.IsLast, "mongoscan")
	} else {
		scan.OpAnalyzer.Reset()
	}
	if scan.Scan == nil {
		return moerr.NewInvalidInput(proc.Ctx, "MongoScan requires a scan specification")
	}
	if scan.Scan.MaxParallelism != 1 {
		return moerr.NewNotSupported(proc.Ctx, "MongoDB MVP requires max_parallelism=1")
	}
	if scan.Scan.Split != nil {
		return moerr.NewNotSupported(proc.Ctx, "MongoDB local split execution is not enabled in the MVP")
	}
	deps, err := scan.runtimeDependencies(proc)
	if err != nil {
		return err
	}
	accountID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return err
	}
	if !deps.Config.EnabledFor(accountID) {
		return moerr.NewNotSupported(proc.Ctx, "MongoDB external tables are disabled for this account")
	}
	if deps.Connections == nil || deps.Mappings == nil || deps.Secrets == nil || deps.Pool == nil {
		return moerr.NewInternalError(proc.Ctx, "MongoDB runtime dependencies are incomplete")
	}
	mapping, err := deps.Mappings.ResolveMongoDBMapping(
		proc.Ctx, accountID, scan.Scan.MappingId, scan.Scan.MappingVersion)
	if err != nil {
		return err
	}
	if !mongodb.MappingSnapshotMatchesPlan(mapping, scan.Scan) {
		return moerr.NewInvalidInput(proc.Ctx, "MongoDB table mapping changed after planning; retry the statement")
	}
	connection, err := deps.Connections.ResolveMongoDBConnection(
		proc.Ctx, accountID, scan.Scan.ConnectionId, scan.Scan.ConnectionVersion)
	if err != nil {
		return err
	}
	if connection.AccountID != accountID || connection.ConnectionID != scan.Scan.ConnectionId ||
		connection.Version != scan.Scan.ConnectionVersion || connection.Disabled {
		return moerr.NewInvalidInput(proc.Ctx, "MongoDB connection is disabled, stale, or belongs to another account")
	}
	if deps.Limiter != nil {
		scan.ctr.releaseLimit, err = deps.Limiter.Acquire(proc.Ctx, accountID, connection.ConnectionID)
		if err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			scan.closeResources(proc.Ctx)
		}
	}()
	credentials, err := deps.Secrets.ResolveMongoDBCredentials(
		proc.Ctx, accountID, connection.CredentialSecretRef, connection.TLSCASecretRef)
	if err != nil {
		return moerr.NewInternalError(proc.Ctx, "MongoDB credential resolution failed")
	}
	lease, err := deps.Pool.Acquire(proc.Ctx, connection, credentials, deps.Config)
	if err != nil {
		return err
	}
	scan.ctr.lease = lease

	columns := mongodb.ColumnsFromPlan(scan.Scan.Columns)
	scan.ctr.converter, err = mongodb.NewConverter(proc.Ctx, columns, deps.Config.MaxValueBytes)
	if err != nil {
		return err
	}
	scan.ctr.converter.SetConversionErrorLimits(deps.Config.MaxConversionErrors, deps.Config.MaxConversionErrorRate)
	predicate, err := mongodb.PredicateFromPlan(proc.Ctx, scan.Scan.PushedPredicate)
	if err != nil {
		return err
	}
	filter, err := mongodb.PredicateToBSON(proc.Ctx, predicate)
	if err != nil {
		return err
	}
	projection := mongodb.ProjectionDocument(columns)
	batchRows := deps.Config.BatchRows
	if batchRows <= 0 {
		batchRows = mongodb.DefaultRuntimeConfig().BatchRows
	}
	findStarted := time.Now()
	scan.ctr.cursor, err = lease.Client().Collection(scan.Scan.Database, scan.Scan.Collection).Find(
		proc.Ctx,
		mongodb.FindSpec{
			Filter:     filter,
			Projection: projection,
			BatchSize:  batchRows,
		},
	)
	metric.MongoDBPhaseDurationHistogram.WithLabelValues("find").Observe(time.Since(findStarted).Seconds())
	if err != nil {
		metric.MongoDBCursorEventCounter.WithLabelValues("find_error").Inc()
		return moerr.NewInternalError(proc.Ctx, "MongoDB find failed")
	}
	metric.MongoDBCursorEventCounter.WithLabelValues("open").Inc()
	scan.ctr.generation++
	scan.ctr.done = false
	scan.ctr.rows = 0
	scan.ctr.rawBytes = 0
	scan.ctr.pendingRaw = nil
	if scan.ProjectList != nil {
		return scan.PrepareProjection(proc)
	}
	return nil
}

func (scan *MongoScan) Call(proc *process.Process) (vm.CallResult, error) {
	scanStarted := time.Now()
	if scan.OpAnalyzer != nil {
		defer scan.OpAnalyzer.AddScanTime(scanStarted)
	}
	result := vm.NewCallResult()
	if scan.ctr.done {
		result.Status = vm.ExecStop
		return result, nil
	}
	if scan.ctr.cursor == nil || scan.ctr.converter == nil {
		result.Status = vm.ExecStop
		return result, moerr.NewInternalError(proc.Ctx, "MongoScan is not prepared")
	}
	if scan.ctr.buf != nil {
		scan.ctr.buf.Clean(proc.Mp())
		scan.ctr.buf = nil
	}

	deps, err := scan.runtimeDependencies(proc)
	if err != nil {
		return result, err
	}
	batchRows := int(deps.Config.BatchRows)
	if batchRows <= 0 {
		batchRows = int(mongodb.DefaultRuntimeConfig().BatchRows)
	}
	maxBatchBytes := deps.Config.MaxBatchBytes
	if maxBatchBytes <= 0 {
		maxBatchBytes = mongodb.DefaultRuntimeConfig().MaxBatchBytes
	}
	maxRows := firstPositive(scan.Scan.MaxScanRows, deps.Config.MaxScanRows)
	maxBytes := firstPositive(scan.Scan.MaxScanBytes, deps.Config.MaxScanBytes)

	bat := scan.ctr.converter.NewBatch()
	var currentBatchBytes int64
	for bat.RowCount() < batchRows {
		var raw []byte
		if scan.ctr.pendingRaw != nil {
			raw = scan.ctr.pendingRaw
			scan.ctr.pendingRaw = nil
		} else {
			waitStarted := time.Now()
			if !scan.ctr.cursor.Next(proc.Ctx) {
				if scan.OpAnalyzer != nil {
					scan.OpAnalyzer.WaitStop(waitStarted)
				}
				metric.MongoDBPhaseDurationHistogram.WithLabelValues("get_more_wait").Observe(time.Since(waitStarted).Seconds())
				if cursorErr := scan.ctr.cursor.Err(); cursorErr != nil {
					metric.MongoDBCursorEventCounter.WithLabelValues("error").Inc()
					bat.Clean(proc.Mp())
					scan.ctr.done = true
					scan.closeResources(proc.Ctx)
					return result, moerr.NewInternalError(proc.Ctx, "MongoDB cursor failed")
				}
				scan.ctr.done = true
				scan.closeResources(proc.Ctx)
				break
			}
			if scan.OpAnalyzer != nil {
				scan.OpAnalyzer.WaitStop(waitStarted)
			}
			metric.MongoDBPhaseDurationHistogram.WithLabelValues("get_more_wait").Observe(time.Since(waitStarted).Seconds())
			raw = scan.ctr.cursor.CurrentRaw()
		}
		if int64(len(raw)) > maxBatchBytes || currentBatchBytes+int64(len(raw)) > maxBatchBytes && bat.RowCount() == 0 {
			bat.Clean(proc.Mp())
			scan.ctr.done = true
			scan.closeResources(proc.Ctx)
			return result, moerr.NewInvalidInput(proc.Ctx, "MongoDB batch byte limit exceeded")
		}
		if currentBatchBytes+int64(len(raw)) > maxBatchBytes {
			// A cursor cannot unread, so retain exactly one bounded document for
			// the next Call. Copy because driver-owned Current is reused by Next.
			scan.ctr.pendingRaw = append(scan.ctr.pendingRaw[:0], raw...)
			break
		}
		decodeStarted := time.Now()
		if err = scan.ctr.converter.AppendDocument(proc.Ctx, bat, raw, proc.Mp()); err != nil {
			metric.MongoDBPhaseDurationHistogram.WithLabelValues("decode_append").Observe(time.Since(decodeStarted).Seconds())
			bat.Clean(proc.Mp())
			scan.ctr.done = true
			scan.closeResources(proc.Ctx)
			return result, err
		}
		metric.MongoDBPhaseDurationHistogram.WithLabelValues("decode_append").Observe(time.Since(decodeStarted).Seconds())
		metric.MongoDBScanDocumentsCounter.Inc()
		metric.MongoDBScanRawBytesCounter.Add(float64(len(raw)))
		currentBatchBytes += int64(len(raw))
		scan.ctr.rows++
		scan.ctr.rawBytes += int64(len(raw))
		if maxRows > 0 && scan.ctr.rows > maxRows || maxBytes > 0 && scan.ctr.rawBytes > maxBytes {
			bat.Clean(proc.Mp())
			scan.ctr.done = true
			scan.closeResources(proc.Ctx)
			return result, moerr.NewInvalidInput(proc.Ctx, "MongoDB statement scan limit exceeded")
		}
	}
	if bat.RowCount() == 0 {
		bat.Clean(proc.Mp())
		result.Status = vm.ExecStop
		return result, nil
	}
	scan.ctr.buf = bat
	scan.OpAnalyzer.Input(bat)
	result.Batch, err = scan.ExecProjection(proc, bat)
	if err != nil {
		scan.ctr.done = true
		scan.closeResources(proc.Ctx)
		return result, err
	}
	return result, nil
}

func (scan *MongoScan) runtimeDependencies(proc *process.Process) (*mongodb.RuntimeDependencies, error) {
	if scan.Dependencies != nil {
		return scan.Dependencies, nil
	}
	rt := moruntime.ServiceRuntime(proc.GetService())
	value, ok := rt.GetGlobalVariables(mongodb.RuntimeDependenciesKey)
	if !ok && proc.GetService() != "" {
		value, ok = moruntime.ServiceRuntime("").GetGlobalVariables(mongodb.RuntimeDependenciesKey)
	}
	deps, typeOK := value.(*mongodb.RuntimeDependencies)
	if !ok || !typeOK || deps == nil {
		return nil, moerr.NewInternalError(proc.Ctx, "MongoDB runtime dependencies are not installed on this CN")
	}
	return deps, nil
}

func firstPositive(first, fallback int64) int64 {
	if first > 0 {
		return first
	}
	return fallback
}
