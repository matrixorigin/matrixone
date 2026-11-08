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

package table_function

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/subscriptionmetadata"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	subscriptionTablesFunctionName  = "mo_subscription_tables"
	subscriptionColumnsFunctionName = "mo_subscription_columns"

	subscriptionCandidatePageSize = 64
	subscriptionTableFilterSize   = 256
	subscriptionResultBufferSize  = 8
	subscriptionOutputBatchSize   = 8192
)

var (
	subscriptionMetadataRunSQL          = sqlexec.RunSql
	subscriptionMetadataRunStreamingSQL = sqlexec.RunStreamingSql
)

type subscriptionMetadataConfig struct {
	functionName string
	columnNames  []string
	columnTypes  []types.Type
	buildQuery   func(subscriptionCandidate, []string) string
}

type subscriptionCandidate struct {
	subscriberID      uint32
	localDatabaseID   uint64
	localDatabaseName string
	localOwner        uint32
	subscriberName    string
	publisherID       uint32
	sourceAccountID   uint32
	publisherName     string
	sourceDatabase    string
	tableList         string
	accountList       string
}

type subscriptionMetadataState struct {
	batch  *batch.Batch
	config *subscriptionMetadataConfig

	outputSources []int
	called        bool
	streaming     bool
	streamEnded   bool
	limited       bool
	limit         uint64
	emitted       uint64

	streamCh     chan executor.Result
	errCh        chan error
	streamDone   chan struct{}
	streamCancel context.CancelFunc

	currentResult     *executor.Result
	currentBatchIndex int
	currentRowOffset  int
}

var subscriptionTablesConfig = subscriptionMetadataConfig{
	functionName: subscriptionTablesFunctionName,
	buildQuery:   buildSubscriptionTablesQuery,
}

var subscriptionColumnsConfig = subscriptionMetadataConfig{
	functionName: subscriptionColumnsFunctionName,
	buildQuery:   buildSubscriptionColumnsQuery,
}

func init() {
	subscriptionTablesConfig.columnNames, subscriptionTablesConfig.columnTypes = subscriptionmetadata.TablesSchema()
	subscriptionColumnsConfig.columnNames, subscriptionColumnsConfig.columnTypes = subscriptionmetadata.ColumnsSchema()
}

func subscriptionMetadataPrepare(_ *process.Process, tf *TableFunction) (tvfState, error) {
	var config *subscriptionMetadataConfig
	switch tf.FuncName {
	case subscriptionTablesFunctionName:
		config = &subscriptionTablesConfig
	case subscriptionColumnsFunctionName:
		config = &subscriptionColumnsConfig
	default:
		return nil, moerr.NewNotSupportedNoCtxf("table function %s is not supported", tf.FuncName)
	}
	return &subscriptionMetadataState{config: config}, nil
}

func (s *subscriptionMetadataState) reset(_ *TableFunction, proc *process.Process) {
	s.stopStreaming()
	if s.batch != nil {
		s.batch.CleanOnlyData()
		s.batch.SetRowCount(0)
	}
	s.outputSources = nil
	s.called = false
	s.streamEnded = false
	s.limited = false
	s.limit = 0
	s.emitted = 0
}

func (s *subscriptionMetadataState) free(
	_ *TableFunction,
	proc *process.Process,
	_ bool,
	_ error,
) {
	s.stopStreaming()
	if s.batch != nil {
		s.batch.Clean(proc.Mp())
		s.batch = nil
	}
	s.outputSources = nil
}

func (s *subscriptionMetadataState) end(_ *TableFunction, _ *process.Process) error {
	s.stopStreaming()
	return nil
}

func (s *subscriptionMetadataState) start(
	tf *TableFunction,
	proc *process.Process,
	nthRow int,
	_ process.Analyzer,
) error {
	s.stopStreaming()
	if s.batch == nil {
		s.batch = tf.createResultBatch()
	} else {
		s.batch.CleanOnlyData()
	}
	s.batch.SetRowCount(0)
	s.called = false
	s.streamEnded = false
	s.limited = false
	s.limit = 0
	s.emitted = 0

	var err error
	s.outputSources, err = subscriptionOutputSources(s.config, tf.Attrs)
	if err != nil {
		return err
	}
	if nthRow != 0 {
		return nil
	}
	if tf.Limit != nil {
		s.limit, err = evalLimitExpression(proc, tf.Limit, 0)
		if err != nil {
			return err
		}
		s.limited = true
		if s.limit == 0 {
			s.streamEnded = true
			return nil
		}
	}

	subscriberID, err := defines.GetAccountId(proc.Ctx)
	if err != nil {
		return err
	}
	// Snapshot catalog scans select a namespace, not an authorization principal.
	// Match their tenant for aliases while retaining proc.Ctx for admission A.
	if snapshot := tf.ScanSnapshot; snapshot != nil && snapshot.Tenant != nil &&
		snapshot.Tenant.TenantID != subscriberID {
		if subscriberID != catalog.System_Account {
			return moerr.NewInvalidInput(proc.Ctx, "cross-account subscription metadata requires the system account")
		}
		subscriberID = snapshot.Tenant.TenantID
	}
	ctx, cancel := context.WithCancel(proc.Ctx)
	s.streamCancel = cancel
	s.streamCh = make(chan executor.Result, subscriptionResultBufferSize)
	s.errCh = make(chan error, 1)
	s.streamDone = make(chan struct{})
	s.streaming = true

	sqlContext := sqlexec.NewSqlContext(
		ctx,
		proc.GetService(),
		proc.GetTxnOperator(),
		subscriberID,
		proc.GetResolveVariableFunc(),
	)
	// Source relations and the subscriber alias must exist at data T. The
	// producer separately borrows statement authorization A for publication
	// eligibility, scope and source routing; old grants cannot authorize T.
	if ts := sqlexec.NewSqlProcess(proc).ApplyScanSnapshot(tf.ScanSnapshot); ts != nil {
		sqlContext.TxnOperator = proc.GetTxnOperator().CloneSnapshotOp(*ts)
	}
	streamCh := s.streamCh
	errCh := s.errCh
	done := s.streamDone
	config := s.config
	go runSubscriptionMetadataProducer(ctx, sqlContext, subscriberID, config, streamCh, errCh, done)

	if err := s.fillBatch(proc); err != nil {
		s.stopStreaming()
		return err
	}
	return nil
}

func (s *subscriptionMetadataState) call(
	_ *TableFunction,
	proc *process.Process,
) (vm.CallResult, error) {
	if s.called {
		s.batch.CleanOnlyData()
		s.batch.SetRowCount(0)
		if err := s.fillBatch(proc); err != nil {
			s.stopStreaming()
			return vm.CancelResult, err
		}
	}
	s.called = true
	if s.batch == nil || s.batch.RowCount() == 0 {
		return vm.CancelResult, nil
	}
	return vm.CallResult{Status: vm.ExecNext, Batch: s.batch}, nil
}

func subscriptionOutputSources(config *subscriptionMetadataConfig, attrs []string) ([]int, error) {
	byName := make(map[string]int, len(config.columnNames))
	for i, name := range config.columnNames {
		byName[strings.ToLower(name)] = i
	}
	positions := make([]int, len(attrs))
	for i, attr := range attrs {
		position, ok := byName[strings.ToLower(attr)]
		if !ok {
			return nil, moerr.NewInternalErrorNoCtxf(
				"%s output column %s is unavailable", config.functionName, attr)
		}
		positions[i] = position
	}
	return positions, nil
}

func (s *subscriptionMetadataState) fillBatch(proc *process.Process) error {
	rowCount := 0
	for rowCount < subscriptionOutputBatchSize {
		if s.limited && s.emitted >= s.limit {
			s.streamEnded = true
			s.stopStreaming()
			break
		}
		source, err := s.currentSourceBatch(proc)
		if err != nil {
			return err
		}
		if source == nil {
			break
		}

		available := source.RowCount() - s.currentRowOffset
		count := min(available, subscriptionOutputBatchSize-rowCount)
		if s.limited {
			remaining := s.limit - s.emitted
			if uint64(count) > remaining {
				count = int(remaining)
			}
		}
		if count <= 0 {
			continue
		}
		for destination, sourceIndex := range s.outputSources {
			if err := s.batch.Vecs[destination].UnionBatch(
				source.Vecs[sourceIndex], int64(s.currentRowOffset), count, nil, proc.Mp()); err != nil {
				return err
			}
		}
		s.currentRowOffset += count
		rowCount += count
		s.emitted += uint64(count)
		if s.currentRowOffset == source.RowCount() {
			s.currentBatchIndex++
			s.currentRowOffset = 0
		}
	}
	s.batch.SetRowCount(rowCount)
	return nil
}

func (s *subscriptionMetadataState) currentSourceBatch(proc *process.Process) (*batch.Batch, error) {
	for {
		if s.currentResult != nil {
			for s.currentBatchIndex < len(s.currentResult.Batches) {
				source := s.currentResult.Batches[s.currentBatchIndex]
				if source == nil || source.RowCount() == 0 {
					s.currentBatchIndex++
					s.currentRowOffset = 0
					continue
				}
				if err := validateSubscriptionSourceBatch(s.config, source); err != nil {
					return nil, err
				}
				return source, nil
			}
			s.closeCurrentResult()
		}
		if s.streamEnded || !s.streaming {
			return nil, nil
		}
		if err := s.readStreamResult(proc); err != nil {
			return nil, err
		}
	}
}

func validateSubscriptionSourceBatch(config *subscriptionMetadataConfig, source *batch.Batch) error {
	if len(source.Vecs) != len(config.columnTypes) {
		return moerr.NewInternalErrorNoCtxf(
			"%s returned %d columns, expected %d",
			config.functionName, len(source.Vecs), len(config.columnTypes))
	}
	for i, expected := range config.columnTypes {
		if source.Vecs[i] == nil {
			return moerr.NewInternalErrorNoCtxf(
				"%s returned a nil vector for column %s", config.functionName, config.columnNames[i])
		}
		if source.Vecs[i].GetType().Oid != expected.Oid {
			return moerr.NewInternalErrorNoCtxf(
				"%s returned type %s for column %s, expected %s",
				config.functionName,
				source.Vecs[i].GetType().String(),
				config.columnNames[i],
				expected.String())
		}
		if source.Vecs[i].Length() < source.RowCount() {
			return moerr.NewInternalErrorNoCtxf(
				"%s returned %d values for %d rows in column %s",
				config.functionName,
				source.Vecs[i].Length(),
				source.RowCount(),
				config.columnNames[i])
		}
	}
	return nil
}

func (s *subscriptionMetadataState) readStreamResult(proc *process.Process) error {
	for {
		select {
		case err := <-s.errCh:
			if err != nil {
				return err
			}
		case result, ok := <-s.streamCh:
			if !ok {
				s.streamEnded = true
				select {
				case err := <-s.errCh:
					if err != nil {
						return err
					}
				default:
				}
				if err := subscriptionMetadataCallerCancellation(proc); err != nil {
					return err
				}
				return nil
			}
			s.currentResult = &result
			s.currentBatchIndex = 0
			s.currentRowOffset = 0
			return nil
		case <-proc.Ctx.Done():
			return subscriptionMetadataCallerCancellation(proc)
		}
	}
}

func subscriptionMetadataCallerCancellation(proc *process.Process) error {
	if cause := context.Cause(proc.Ctx); cause != nil {
		return cause
	}
	return proc.Ctx.Err()
}

func (s *subscriptionMetadataState) closeCurrentResult() {
	if s.currentResult == nil {
		return
	}
	s.currentResult.Close()
	s.currentResult = nil
	s.currentBatchIndex = 0
	s.currentRowOffset = 0
}

func (s *subscriptionMetadataState) stopStreaming() {
	if !s.streaming {
		s.closeCurrentResult()
		return
	}
	if s.streamCancel != nil {
		s.streamCancel()
	}
	s.closeCurrentResult()
	if s.streamCh != nil {
		for result := range s.streamCh {
			result.Close()
		}
	}
	if s.streamDone != nil {
		<-s.streamDone
	}
	if s.errCh != nil {
		for {
			select {
			case <-s.errCh:
			default:
				goto drained
			}
		}
	}

drained:
	s.streaming = false
	s.streamCh = nil
	s.errCh = nil
	s.streamDone = nil
	s.streamCancel = nil
}

func runSubscriptionMetadataProducer(
	ctx context.Context,
	sqlContext *sqlexec.SqlContext,
	subscriberID uint32,
	config *subscriptionMetadataConfig,
	streamCh chan executor.Result,
	errCh chan error,
	done chan struct{},
) {
	defer close(done)
	defer close(streamCh)

	admission, err := subscriptionmetadata.AuthorizationSnapshot(ctx)
	if err != nil {
		publishSubscriptionMetadataError(ctx, errCh, err)
		return
	}
	value, ok := moruntime.ServiceRuntime(sqlContext.GetService()).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		publishSubscriptionMetadataError(ctx, errCh, moerr.NewInternalError(ctx, "missing subscription metadata executor"))
		return
	}
	opts := executor.Options{}.WithAccountID(catalog.System_Account).
		WithDatabase(catalog.MO_CATALOG).WithTxnIsolation(txn.TxnIsolation_SI)
	if admission != nil {
		opts = opts.WithAuthorizationSnapshot(admission)
	}
	// One independent authorization transaction owns all candidate pages.
	// ExecTxn commits/rolls it back before the producer signals completion;
	// cancellation and early LIMIT still terminate through stopStreaming.
	err = value.(executor.SQLExecutor).ExecTxn(ctx, func(auth executor.TxnExecutor) error {
		authority := *sqlContext
		authority.TxnOperator = auth.Txn()
		return scanSubscriptionMetadata(ctx, &authority, sqlContext, subscriberID, config, streamCh)
	}, opts)
	if err != nil {
		publishSubscriptionMetadataError(ctx, errCh, err)
	}
}

func scanSubscriptionMetadata(
	ctx context.Context,
	authority, data *sqlexec.SqlContext,
	subscriberID uint32,
	config *subscriptionMetadataConfig,
	streamCh chan executor.Result,
) error {
	var lastDatabaseID uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		aliases, nextDatabaseID, err := readSubscriptionAliases(data, subscriberID, lastDatabaseID)
		if err != nil {
			return err
		}
		if len(aliases) == 0 {
			return nil
		}
		candidateSQL := buildAuthorizedSubscriptionQuery(subscriberID, aliases)
		sqlProcess := sqlexec.NewSqlProcessWithContext(authority).
			WithExecutionIdentity(catalog.System_Account, catalog.MO_CATALOG)
		result, err := subscriptionMetadataRunSQL(sqlProcess, candidateSQL)
		if err != nil {
			result.Close()
			return err
		}
		candidates, _, _, decodeErr := func() ([]subscriptionCandidate, int, uint64, error) {
			defer result.Close()
			return decodeSubscriptionCandidates(result, lastDatabaseID)
		}()
		if decodeErr != nil {
			return decodeErr
		}
		for _, candidate := range candidates {
			if err := ctx.Err(); err != nil {
				return err
			}
			candidate.subscriberID = subscriberID
			if !subscriptionCandidateAuthorized(candidate) {
				continue
			}
			if err := streamSubscriptionCandidate(ctx, data, candidate, config, streamCh); err != nil {
				return err
			}
		}
		if len(aliases) < subscriptionCandidatePageSize {
			return nil
		}
		lastDatabaseID = nextDatabaseID
	}
}

type subscriptionAlias struct {
	id                     uint64
	name                   string
	owner                  uint32
	publisher, publication string
}

// Alias membership is schema at T, not authorization. A dropped alias can
// still exist in a historical schema; a later-created alias cannot. Never use
// historical publication status or grants to decide eligibility.
func readSubscriptionAliases(data *sqlexec.SqlContext, subscriberID uint32, previousID uint64) ([]subscriptionAlias, uint64, error) {
	result, err := subscriptionMetadataRunSQL(sqlexec.NewSqlProcessWithContext(data).
		WithExecutionIdentity(catalog.System_Account, catalog.MO_CATALOG), buildSubscriptionAliasQuery(subscriberID, previousID))
	if err != nil {
		result.Close()
		return nil, previousID, err
	}
	defer result.Close()
	aliases := make([]subscriptionAlias, 0, subscriptionCandidatePageSize)
	lastID := previousID
	for _, source := range result.Batches {
		if source == nil || source.RowCount() == 0 {
			continue
		}
		if err := validateSubscriptionMetadataBatch(source, []types.T{types.T_uint64, types.T_varchar, types.T_uint32, types.T_varchar, types.T_varchar}); err != nil {
			return nil, previousID, err
		}
		for row := 0; row < source.RowCount(); row++ {
			id := vector.GetFixedAtWithTypeCheck[uint64](source.Vecs[0], row)
			if id <= lastID || len(aliases) == subscriptionCandidatePageSize {
				return nil, previousID, moerr.NewInternalErrorNoCtx("invalid subscription alias page")
			}
			lastID = id
			aliases = append(aliases, subscriptionAlias{id: id,
				name:        strings.Clone(source.Vecs[1].GetStringAt(row)),
				owner:       vector.GetFixedAtWithTypeCheck[uint32](source.Vecs[2], row),
				publisher:   strings.Clone(source.Vecs[3].GetStringAt(row)),
				publication: strings.Clone(source.Vecs[4].GetStringAt(row))})
		}
	}
	return aliases, lastID, nil
}

func buildSubscriptionAliasQuery(subscriberID uint32, lastDatabaseID uint64) string {
	return fmt.Sprintf("SELECT d.dat_id, d.datname, d.owner, s.pub_account_name, s.pub_name "+
		"FROM mo_catalog.mo_database d JOIN mo_catalog.mo_subs s "+
		"ON s.sub_account_id = d.account_id AND s.sub_name = d.datname "+
		"WHERE d.account_id = %d AND d.dat_type = %s AND d.dat_id > %d ORDER BY d.dat_id LIMIT %d",
		subscriberID, sqlquote.String(catalog.SystemDBTypeSubscription), lastDatabaseID, subscriptionCandidatePageSize)
}

// Only alias identity is transported from T. Publication membership, table
// scope and source routing are resolved at the single current admission A.
func buildAuthorizedSubscriptionQuery(subscriberID uint32, aliases []subscriptionAlias) string {
	rows := make([]string, len(aliases))
	for i, alias := range aliases {
		rows[i] = fmt.Sprintf("SELECT CAST(%d AS BIGINT UNSIGNED) AS dat_id, %s AS datname, "+
			"CAST(%d AS INT UNSIGNED) AS owner, %s AS pub_account_name, %s AS pub_name",
			alias.id, sqlquote.String(alias.name), alias.owner, sqlquote.String(alias.publisher), sqlquote.String(alias.publication))
	}
	return fmt.Sprintf("SELECT d.dat_id, d.datname, d.owner, subscriber.account_name, "+
		"CAST(p.account_id AS INT UNSIGNED), publisher.account_name, p.database_name, p.table_list, p.account_list, "+
		"CAST(CASE WHEN p.database_id = 0 THEN p.account_id ELSE source.account_id END AS INT UNSIGNED) "+
		"FROM (%s) d JOIN mo_catalog.mo_account publisher ON publisher.account_name = d.pub_account_name "+
		"JOIN mo_catalog.mo_pubs p ON p.account_id = publisher.account_id AND p.pub_name = d.pub_name "+
		"JOIN mo_catalog.mo_account subscriber ON subscriber.account_id = %d "+
		"LEFT JOIN mo_catalog.mo_database source ON source.dat_id = p.database_id AND source.datname = p.database_name "+
		"WHERE publisher.status <> %s AND (p.database_id = 0 OR source.dat_id IS NOT NULL) ORDER BY d.dat_id", strings.Join(rows, " UNION ALL "), subscriberID, sqlquote.String("suspend"))
}

func decodeSubscriptionCandidates(
	result executor.Result,
	previousDatabaseID uint64,
) ([]subscriptionCandidate, int, uint64, error) {
	candidates := make([]subscriptionCandidate, 0, subscriptionCandidatePageSize)
	rowCount := 0
	lastDatabaseID := previousDatabaseID
	for _, source := range result.Batches {
		if source == nil || source.RowCount() == 0 {
			continue
		}
		if err := validateSubscriptionCandidateBatch(source); err != nil {
			return nil, 0, previousDatabaseID, err
		}
		for row := 0; row < source.RowCount(); row++ {
			databaseID := vector.GetFixedAtWithTypeCheck[uint64](source.Vecs[0], row)
			if databaseID <= lastDatabaseID {
				return nil, 0, previousDatabaseID, moerr.NewInternalErrorNoCtxf(
					"subscription candidate database id %d did not follow %d", databaseID, lastDatabaseID)
			}
			lastDatabaseID = databaseID
			rowCount++
			candidates = append(candidates, subscriptionCandidate{
				localDatabaseID:   databaseID,
				localDatabaseName: strings.Clone(source.Vecs[1].GetStringAt(row)),
				localOwner:        vector.GetFixedAtWithTypeCheck[uint32](source.Vecs[2], row),
				subscriberName:    strings.Clone(source.Vecs[3].GetStringAt(row)),
				publisherID:       vector.GetFixedAtWithTypeCheck[uint32](source.Vecs[4], row),
				sourceAccountID:   vector.GetFixedAtWithTypeCheck[uint32](source.Vecs[9], row),
				publisherName:     strings.Clone(source.Vecs[5].GetStringAt(row)),
				sourceDatabase:    strings.Clone(source.Vecs[6].GetStringAt(row)),
				tableList:         strings.Clone(source.Vecs[7].GetStringAt(row)),
				accountList:       strings.Clone(source.Vecs[8].GetStringAt(row)),
			})
		}
	}
	return candidates, rowCount, lastDatabaseID, nil
}

func validateSubscriptionCandidateBatch(source *batch.Batch) error {
	expected := []types.T{
		types.T_uint64,
		types.T_varchar,
		types.T_uint32,
		types.T_varchar,
		types.T_uint32,
		types.T_varchar,
		types.T_varchar,
		types.T_text,
		types.T_text,
		types.T_uint32,
	}
	return validateSubscriptionMetadataBatch(source, expected)
}

func validateSubscriptionMetadataBatch(source *batch.Batch, expected []types.T) error {
	if len(source.Vecs) != len(expected) {
		return moerr.NewInternalErrorNoCtxf(
			"subscription candidate query returned %d columns, expected %d", len(source.Vecs), len(expected))
	}
	for i, expectedType := range expected {
		if source.Vecs[i] == nil || source.Vecs[i].GetType().Oid != expectedType {
			return moerr.NewInternalErrorNoCtxf(
				"subscription candidate query returned an invalid vector at column %d", i)
		}
		if source.Vecs[i].Length() < source.RowCount() {
			return moerr.NewInternalErrorNoCtxf(
				"subscription candidate query returned too few values at column %d", i)
		}
		for row := 0; row < source.RowCount(); row++ {
			if source.Vecs[i].IsNull(uint64(row)) {
				return moerr.NewInternalErrorNoCtxf(
					"subscription candidate query returned NULL at column %d", i)
			}
		}
	}
	return nil
}

func subscriptionCandidateAuthorized(candidate subscriptionCandidate) bool {
	if candidate.subscriberName == candidate.publisherName {
		return false
	}
	publication := pubsub.PubInfo{SubAccountsStr: candidate.accountList}
	if !publication.InSubAccounts(candidate.subscriberName) {
		return false
	}
	// An admitted general publisher's foreign source can target only its
	// owner. The page's current subscriber row already resolves that name to
	// subscriberID, so owner-only names establish the same ID set as canonical
	// subscription admission, without another query per candidate or target.
	ownerOnly := candidate.sourceAccountID == candidate.subscriberID &&
		!strings.EqualFold(candidate.accountList, pubsub.AccountAll)
	if ownerOnly {
		for name := range strings.SplitSeq(candidate.accountList, pubsub.Sep) {
			if strings.TrimSpace(name) != candidate.subscriberName {
				ownerOnly = false
				break
			}
		}
	}
	return pubsub.PublicationSourceOwnerAuthorized(candidate.publisherID, candidate.sourceAccountID, ownerOnly)
}

func streamSubscriptionCandidate(
	ctx context.Context,
	sqlContext *sqlexec.SqlContext,
	candidate subscriptionCandidate,
	config *subscriptionMetadataConfig,
	streamCh chan executor.Result,
) error {
	return forEachPublicationTableChunk(candidate, func(tableNames []string) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		sql := config.buildQuery(candidate, tableNames)
		sqlProcess := sqlexec.NewSqlProcessWithContext(sqlContext).
			WithExecutionIdentity(candidate.sourceAccountID, catalog.MO_CATALOG)
		executorErrCh := make(chan error, 1)
		_, err := subscriptionMetadataRunStreamingSQL(ctx, sqlProcess, sql, streamCh, executorErrCh)
		select {
		case executorErr := <-executorErrCh:
			if executorErr != nil {
				err = executorErr
			}
		default:
		}
		return err
	})
}

func forEachPublicationTableChunk(
	candidate subscriptionCandidate,
	visit func([]string) error,
) error {
	// table_list is the live publication scope used by SHOW PUBLICATIONS and
	// subscription resolution. all_table can remain stale after ALTER PUBLICATION.
	if strings.EqualFold(candidate.tableList, pubsub.TableAll) {
		return visit(nil)
	}
	if candidate.tableList == "" {
		return moerr.NewInternalErrorNoCtx("subscription publication contains an empty table list")
	}

	chunk := make([]string, 0, subscriptionTableFilterSize)
	for tableName := range strings.SplitSeq(candidate.tableList, pubsub.Sep) {
		if tableName == "" {
			return moerr.NewInternalErrorNoCtx("subscription publication contains an empty table name")
		}
		chunk = append(chunk, tableName)
		if len(chunk) == subscriptionTableFilterSize {
			if err := visit(chunk); err != nil {
				return err
			}
			chunk = make([]string, 0, subscriptionTableFilterSize)
		}
	}
	if len(chunk) != 0 {
		return visit(chunk)
	}
	return nil
}

func subscriptionTablePredicate(column string, tableNames []string) string {
	if tableNames == nil {
		return ""
	}
	quoted := make([]string, len(tableNames))
	for i, tableName := range tableNames {
		quoted[i] = sqlquote.String(tableName)
	}
	return " AND " + column + " IN (" + strings.Join(quoted, ",") + ")"
}

func buildSubscriptionTablesQuery(candidate subscriptionCandidate, tableNames []string) string {
	return "SELECT " +
		"CAST(" + strconv.FormatUint(uint64(candidate.subscriberID), 10) +
		" AS INT UNSIGNED) AS account_id, " +
		"tbl.rel_id, tbl.relname, " +
		"CAST(" + sqlquote.String(candidate.localDatabaseName) + " AS VARCHAR(5000)) AS reldatabase, " +
		"CAST(" + strconv.FormatUint(candidate.localDatabaseID, 10) +
		" AS BIGINT UNSIGNED) AS reldatabase_id, " +
		"tbl.relkind, tbl.rel_createsql, tbl.created_time, tbl.partitioned, tbl.rel_comment, tbl.extra_info, " +
		"tbl.rel_logical_id, CAST(" + strconv.FormatUint(uint64(candidate.localOwner), 10) +
		" AS INT UNSIGNED) AS owner, " +
		// The frozen View consumer calls this routing column
		// publisher_account_id, but it must resolve the physical relation.
		"CAST(" + strconv.FormatUint(uint64(candidate.sourceAccountID), 10) +
		" AS INT UNSIGNED) AS publisher_account_id " +
		"FROM mo_catalog.mo_tables tbl " +
		"WHERE tbl.account_id = current_account_id() AND tbl.reldatabase = " +
		sqlquote.String(candidate.sourceDatabase) +
		subscriptionTablePredicate("tbl.relname", tableNames)
}

func buildSubscriptionColumnsQuery(candidate subscriptionCandidate, tableNames []string) string {
	ordinary := "SELECT " +
		"CAST(" + strconv.FormatUint(uint64(candidate.subscriberID), 10) +
		" AS INT UNSIGNED) AS account_id, " +
		"CAST(" + strconv.FormatUint(candidate.localDatabaseID, 10) +
		" AS BIGINT UNSIGNED) AS att_database_id, " +
		"CAST(" + sqlquote.String(candidate.localDatabaseName) + " AS VARCHAR(256)) AS att_database, " +
		"mc.att_relname_id, mc.att_relname, mc.attname, mc.atttyp, mc.attnum, mc.attnotnull, mc.att_default, " +
		"mc.att_constraint_type, mc.att_is_auto_increment, mc.att_comment, mc.att_is_hidden, mc.attr_enum, " +
		"mc.attr_has_generated, mc.attr_generated, CAST(mk.key_priority AS BIGINT) AS key_priority, " +
		"mt.rel_id, mt.relkind, mt.rel_createsql, mt.partitioned, mt.extra_info, mt.rel_logical_id, " +
		"CAST(" + strconv.FormatUint(uint64(candidate.localOwner), 10) + " AS INT UNSIGNED) AS table_owner, " +
		"mc.att_is_unsigned " +
		"FROM mo_catalog.mo_columns mc " +
		"JOIN mo_catalog.mo_tables mt ON mt.account_id = mc.account_id " +
		"AND mt.reldatabase = mc.att_database AND mt.relname = mc.att_relname " +
		"LEFT JOIN (SELECT ki.table_id, ki.column_name, " +
		"MAX(CASE WHEN ki.type = 'PRIMARY' THEN 3 " +
		"WHEN ki.type = 'UNIQUE' AND kp.part_count = 1 THEN 2 ELSE 1 END) AS key_priority " +
		"FROM mo_catalog.mo_indexes ki " +
		"JOIN mo_catalog.mo_tables kt ON kt.rel_id = ki.table_id " +
		"AND kt.account_id = current_account_id() AND kt.reldatabase = " +
		sqlquote.String(candidate.sourceDatabase) +
		subscriptionTablePredicate("kt.relname", tableNames) +
		" JOIN (SELECT pi.id, COUNT(*) AS part_count FROM mo_catalog.mo_indexes pi " +
		"JOIN mo_catalog.mo_tables pt ON pt.rel_id = pi.table_id " +
		"AND pt.account_id = current_account_id() AND pt.reldatabase = " +
		sqlquote.String(candidate.sourceDatabase) +
		subscriptionTablePredicate("pt.relname", tableNames) +
		" GROUP BY pi.id) kp ON ki.id = kp.id " +
		"WHERE (ki.type = 'PRIMARY' OR ki.ordinal_position = 1) " +
		"AND ki.type IN ('PRIMARY','UNIQUE','MULTIPLE','FULLTEXT','SPATIAL') " +
		"GROUP BY ki.table_id, ki.column_name) mk " +
		"ON mk.table_id = mc.att_relname_id AND mk.column_name = mc.attname " +
		"WHERE mc.account_id = current_account_id() AND mc.att_database = " +
		sqlquote.String(candidate.sourceDatabase) +
		subscriptionTablePredicate("mc.att_relname", tableNames)
	return ordinary
}

func publishSubscriptionMetadataError(ctx context.Context, errCh chan<- error, err error) {
	if err == nil || ctx.Err() != nil {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}
