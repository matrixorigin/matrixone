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

package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/pubsub"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

// getPublicationRelationsFormat reads the relations of one publication source
// database inside the caller's transaction. Views are included: they are valid
// members of an explicit table list even though SHOW TABLES hides them inside
// subscription databases. rel_id separates row-replacing schema changes
// (ALTER TABLE column add/drop and ALTER VIEW allocate a new rel_id) and
// rel_version separates in-place ones (ALTER TABLE RENAME COLUMN preserves
// the rel_id and bumps rel_version), so the stability comparison sees every
// schema change that alters what a view binds.
const getPublicationRelationsFormat = `select relname, relkind, coalesce(viewdef, ''), rel_id, rel_version from mo_catalog.mo_tables where reldatabase = '%s' and account_id = %d`

// getPublicationSystemRelationFormat reads every catalog row of one relation of
// a system database under the system account, so account ownership and the
// cluster relation kind are visible.
const getPublicationSystemRelationFormat = `select relkind, account_id from mo_catalog.mo_tables where reldatabase = '%s' and relname = '%s'`

type publicationRelation struct {
	relID      uint64
	relVersion uint64
	relKind    string
	viewDef    string
}

// publicationRelationRef is one relation a published view needs. database is
// the physical database the relation was bound to; it is empty only for
// references that the publication database itself owns.
type publicationRelationRef struct {
	database string
	relation string
	// foreign marks bindings that a table list can never represent: another
	// account, a nested subscription, or a snapshot time travel.
	foreign bool
	// qualified marks references written with an explicit database prefix.
	// A subscriber routes only unqualified references through the
	// subscription, so a qualified reference to the publication database can
	// never resolve at query time.
	qualified bool
}

// publicationSessionLowerCaseTableNames reads the validating session's
// lower_case_table_names mode and refuses any context that cannot name one:
// a scope validation parses view definitions stored before the binder
// persisted their creation-time mode, and certifying their relations under a
// guessed mode would admit a scope the subscribers cannot bind - or reject a
// complete one - because the same instance-global mode is what the DDL
// session and every subscriber session resolve identifier case with. The
// variable is registered int-typed global and dynamic; a session unable to
// produce its int value fails the DDL explicitly instead of being answered
// with a fabricated mode.
func publicationSessionLowerCaseTableNames(ctx context.Context, ses FeSession) (int64, error) {
	value, err := ses.GetSessionSysVar("lower_case_table_names")
	if err != nil {
		return 0, err
	}
	mode, ok := value.(int64)
	if !ok {
		return 0, moerr.NewInternalError(ctx,
			"cannot validate publication view scope: session variable lower_case_table_names is unavailable")
	}
	return mode, nil
}

// publicationBackgroundLowerCaseTableNames resolves the case mode of a
// scope validation that runs without a user session in scope - the snapshot
// and PITR replays that re-create publications inside a background
// executor. The trustworthy context there is the replay executor's own
// session: it is the parse context the same executor replays the restored
// view definitions with, so certifying a publication's scope under exactly
// that mode keeps the validation on the resolution the replay itself uses.
func publicationBackgroundLowerCaseTableNames(ctx context.Context, bh BackgroundExec) (int64, error) {
	executor, ok := bh.(*backExec)
	if !ok || executor == nil || executor.backSes == nil {
		return 0, moerr.NewInternalError(ctx,
			"cannot validate publication view scope: session variable lower_case_table_names is unavailable")
	}
	return publicationSessionLowerCaseTableNames(ctx, executor.backSes)
}

// validatePublicationTableScope rejects an explicit table-level publication
// list whose views need relations outside that list. The subscriber resolves
// every relation inside the persisted view SQL against the publication's table
// list, so a view whose dependency closure is incomplete would accept CREATE
// PUBLICATION/CREATE DATABASE ... FROM and then fail on every query (issue
// #27767). tableNames are the deduplicated requested members; ctx is bound to
// the source account of dbName and bh is inside the publication transaction.
// targetAccountIDs are the accounts that may subscribe to the publication.
//
// Membership only proves that a relation of the requested name exists: after
// the closure walk every selected view is also bound through its stored
// definition - the same persisted-view binding a subscriber repeats on every
// query - inside the caller's transaction as pure metadata binding, so a view
// whose required columns were removed (a base table dropped and recreated
// with an incompatible schema) is rejected before any catalog write, no
// expression of the definition is ever evaluated, no function of it is ever
// executed, session temporary tables stay out of resolution, and the call
// the binder sends to user-defined functions is refused with the stable
// subscription-resolution error.
//
// The relation snapshot, the closure walk and each binding probe are separate
// statements, and RC advances the catalog snapshot per statement while ALTER
// VIEW and ALTER TABLE need only the shared database lock this transaction
// already holds. A view redefined between those statements could pass
// membership through its old definition and the probe through its new one,
// and schema changes are split across the catalog's own identity fields:
// row-replacing alters allocate a new rel_id while leaving kind and definition
// text untouched, and an in-place ALTER TABLE RENAME COLUMN preserves the
// rel_id and bumps rel_version instead. After the probe the relations are
// re-read and compared by their complete identity - rel_id, rel_version,
// relkind and the stored definition - so any definition or schema change
// revalidates the whole scope against the newer catalog, and a relation that
// keeps changing fails closed instead of admitting an unvalidated scope.
func validatePublicationTableScope(
	ctx context.Context,
	bh BackgroundExec,
	dbName string,
	tableNames []string,
	targetAccountIDs []uint32,
	sessionLowerCaseTableNames int64,
) error {
	if len(tableNames) == 0 {
		return nil
	}
	sourceAccountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	for attempt := 1; ; attempt++ {
		relations, err := getPublicationDatabaseRelations(ctx, bh, dbName)
		if err != nil {
			return err
		}
		boundViews, err := publicationScopeWalkViews(
			ctx, bh, dbName, tableNames, targetAccountIDs, sourceAccountID, relations,
			sessionLowerCaseTableNames)
		if err != nil {
			return err
		}
		if len(boundViews) > 0 {
			// Every relation of each view survived the name and membership
			// checks; now prove the stored definitions still bind the
			// current catalog.
			if err := validatePublicationViewBindings(ctx, bh, dbName, boundViews); err != nil {
				return err
			}
		}
		latest, err := getPublicationDatabaseRelations(ctx, bh, dbName)
		if err != nil {
			return err
		}
		changed := ""
		for _, name := range tableNames {
			if latest[name] != relations[name] {
				changed = name
				break
			}
		}
		if changed == "" {
			return nil
		}
		if attempt >= publicationScopeMaxValidationAttempts {
			return moerr.NewInternalErrorf(ctx,
				"table-level publication for database '%s' cannot publish relation '%s': its definition changed during validation, retry the statement",
				dbName, changed)
		}
	}
}

// publicationScopeMaxValidationAttempts bounds the revalidation loop of
// validatePublicationTableScope against a definition that keeps changing.
const publicationScopeMaxValidationAttempts = 3

// publicationScopeWalkViews proves every requested view's dependency closure
// stays inside the requested membership, using the relation snapshot read by
// the caller, and returns the views that must still pass the binding probe.
// sessionLowerCaseTableNames is the validating session's own case mode; it
// is the resolution context for definitions stored before relations recorded
// their creation-time mode, the same mode the DDL session and every
// subscriber session resolve identifier case with.
func publicationScopeWalkViews(
	ctx context.Context,
	bh BackgroundExec,
	dbName string,
	tableNames []string,
	targetAccountIDs []uint32,
	sourceAccountID uint32,
	relations map[string]publicationRelation,
	sessionLowerCaseTableNames int64,
) ([]string, error) {
	included := make(map[string]struct{}, len(tableNames))
	pendingViews := make([]string, 0, len(tableNames))
	for _, name := range tableNames {
		relation, ok := relations[name]
		if !ok {
			// The inherited table list of an ALTER that switches the source
			// database is checked here; CREATE/ALTER TABLE lists were already
			// checked against SHOW TABLES.
			return nil, moerr.NewInternalErrorf(ctx, "table '%s' not exists", name)
		}
		included[name] = struct{}{}
		if relation.relKind == catalog.SystemViewRel {
			pendingViews = append(pendingViews, name)
		}
	}

	visitedViews := make(map[string]struct{}, len(pendingViews))
	boundViews := make([]string, 0, len(pendingViews))
	for len(pendingViews) > 0 {
		viewName := pendingViews[0]
		pendingViews = pendingViews[1:]
		if _, done := visitedViews[viewName]; done {
			continue
		}
		visitedViews[viewName] = struct{}{}
		boundViews = append(boundViews, viewName)

		refs, err := publicationViewRelationRefs(ctx, relations[viewName].viewDef, dbName, sourceAccountID, viewName, sessionLowerCaseTableNames)
		if err != nil {
			return nil, err
		}
		for _, ref := range refs {
			if ref.foreign {
				return nil, moerr.NewInternalErrorf(ctx,
					"table-level publication for database '%s' cannot publish view '%s': it depends on relation '%s.%s' bound to another account, subscription, or snapshot",
					dbName, viewName, ref.database, ref.relation)
			}
			if isPublicationSystemDatabase(ref.database) {
				// The subscriber resolves qualified system references inside
				// its own account, so they are published only when the
				// subscriber accounts can actually resolve them.
				available, err := publicationSystemRelationAvailable(ctx, bh, ref.database, ref.relation, targetAccountIDs)
				if err != nil {
					return nil, err
				}
				if !available {
					return nil, moerr.NewInternalErrorf(ctx,
						"table-level publication for database '%s' cannot publish view '%s': system relation '%s.%s' is not available to its subscriber accounts",
						dbName, viewName, ref.database, ref.relation)
				}
				continue
			}
			if ref.database != dbName {
				return nil, moerr.NewInternalErrorf(ctx,
					"table-level publication for database '%s' cannot publish view '%s': it depends on relation '%s.%s' outside the publication database",
					dbName, viewName, ref.database, ref.relation)
			}
			if _, ok := relations[ref.relation]; !ok {
				return nil, moerr.NewInternalErrorf(ctx,
					"view '%s' depends on table '%s' which does not exist in database '%s'",
					viewName, ref.relation, dbName)
			}
			if _, ok := included[ref.relation]; !ok {
				return nil, moerr.NewInternalErrorf(ctx,
					"table-level publication for database '%s' must include table '%s' required by view '%s'",
					dbName, ref.relation, viewName)
			}
			if relations[ref.relation].relKind == catalog.SystemViewRel {
				pendingViews = append(pendingViews, ref.relation)
			}
		}
	}
	return boundViews, nil
}

// validatePublicationViewBindings binds one published view through its stored
// definition inside the caller's transaction, as pure metadata binding: the
// defines.PublicationViewBinding marker on the probe statement makes the
// wrapper-supplied statement keep identifier source case, keeps the build at
// the metadata boundary in front of the optimizer's rewrite pipeline, and
// gates the binder's eager-evaluation sites (the fold switch, LIMIT/OFFSET
// folding and window frame-bound evaluation: no expression of the definition
// is evaluated, no function of it is executed, and no pipeline is built or
// run), resolves persisted objects only (session temporary tables stay out
// of name resolution), and refuses user-defined function binding with a
// distinguishable error. The binding
// runs with the publication database as its current database - the
// creation-time context of the definition - so function calls are adjudicated
// by the same real binder precedence the view was created under. Binding
// alone is the probe's complete answer - the same persisted-view binding a
// subscriber's first query performs - so a view whose base table was dropped
// and recreated with an incompatible schema (its column missing) fails here
// instead of creating a subscription that fails on every query, and a view
// whose call the binder sends to user-defined functions fails here with the
// stable subscription-resolution error. Probe failures are classified before
// they reach that message: semantic bind failures keep the stable broken-view
// verdict, while cancellation, query interruption and the transaction
// retry/conflict family propagate unchanged, with their original identity
// and code, so the DDL layer can retry on them and report real interruptions
// instead of a misleading broken-view error.
func validatePublicationViewBindings(
	ctx context.Context,
	bh BackgroundExec,
	dbName string,
	viewNames []string,
) error {
	for _, viewName := range viewNames {
		bh.ClearExecResultSet()
		if err := bh.Exec(
			defines.WithPublicationViewBinding(ctx),
			publicationViewBindingSQL(dbName, viewName)); err != nil {
			var udf *plan.UdfDisallowedInPublicationViewBinding
			if errors.As(err, &udf) {
				return moerr.NewInternalErrorf(ctx,
					"table-level publication for database '%s' cannot publish view '%s': it calls user-defined function '%s', and user-defined functions do not resolve through subscriptions",
					dbName, viewName, udf.Name)
			}
			if isPublicationProbeInfrastructureError(err) {
				// Cancellation, query interruption and the transaction
				// retry/conflict family must keep their original identity and
				// code: the DDL layer retries on them and reports the real
				// interruptions to the session, while the wrap below would
				// turn an interrupted or conflicted validation into a
				// misleading broken-view error and close its retry path.
				return err
			}
			return moerr.NewInternalErrorf(ctx,
				"table-level publication for database '%s' cannot publish view '%s': its stored definition no longer binds the published relations: %s",
				dbName, viewName, err.Error())
		}
		bh.ClearExecResultSet()
	}
	return nil
}

// isPublicationProbeInfrastructureError classifies a binding-probe failure
// as an execution- or transaction-layer condition the probe's caller must
// handle with the error's own identity: context cancellation and deadlines,
// query interruption, the transaction retry and write-conflict family, and
// every code moerr.MustRollbackTxn already treats as fatal to the owning
// transaction (deadlocks, lock timeouts, backend loss). Everything else from
// the probe is a semantic property of the stored definition - a missing
// relation, a missing column, a resolution failure - and keeps receiving the
// stable broken-view verdict below; only forwarding infrastructure conditions
// with their own code keeps that message truthful.
func isPublicationProbeInfrastructureError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	// The probe fails inside the whole executor depth, so the code is matched
	// through wrapped chains, not only on the error the surface returns;
	// moerr.MustRollbackTxn already unwraps on its own.
	var moErr *moerr.Error
	if errors.As(err, &moErr) {
		switch moErr.ErrorCode() {
		case moerr.ErrQueryInterrupted, moerr.ErrTxnNeedRetry,
			moerr.ErrTxnNeedRetryWithDefChanged, moerr.ErrTAENeedRetry,
			moerr.ErrTxnWriteConflict:
			return true
		}
	}
	return moerr.MustRollbackTxn(err)
}

// publicationViewBindingSQL is the empty-result statement whose executed form
// is the metadata binding of one published view: under the
// defines.PublicationViewBinding marker the background executor never builds
// or runs a pipeline for it, it only binds the stored definition.
func publicationViewBindingSQL(dbName, viewName string) string {
	return "select * from " + sqlquote.QualifiedIdent(dbName, viewName) + " limit 0"
}

func getPublicationDatabaseRelations(
	ctx context.Context,
	bh BackgroundExec,
	dbName string,
) (map[string]publicationRelation, error) {
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return nil, err
	}
	sql := fmt.Sprintf(getPublicationRelationsFormat, sanitizeSQLInput(dbName), accountId)

	bh.ClearExecResultSet()
	if err = bh.Exec(ctx, sql); err != nil {
		return nil, err
	}

	erArray, err := getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	relations := make(map[string]publicationRelation)
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			name, err := result.GetString(ctx, i, 0)
			if err != nil {
				return nil, err
			}
			relKind, err := result.GetString(ctx, i, 1)
			if err != nil {
				return nil, err
			}
			viewDef, err := result.GetString(ctx, i, 2)
			if err != nil {
				return nil, err
			}
			relID, err := result.GetUint64(ctx, i, 3)
			if err != nil {
				return nil, err
			}
			relVersion, err := result.GetUint64(ctx, i, 4)
			if err != nil {
				return nil, err
			}
			relations[name] = publicationRelation{relID: relID, relVersion: relVersion, relKind: relKind, viewDef: viewDef}
		}
	}
	return relations, nil
}

// publicationSystemRelationAvailable reports whether a relation of a system
// database resolves inside the publication's subscriber accounts. A subscriber
// binds qualified system references in its own account: cluster relations
// resolve through the system account, the shared mo_catalog relation trio
// resolves for every account, and every other system relation resolves only
// for an account that owns its own catalog row - so every concrete target
// account must own a row of the relation. Relations like mo_catalog.mo_account
// and the whole mo_task database are created for the system account only, so
// publishing a view over them to a general account would reproduce the broken
// subscription this validator exists to reject.
func publicationSystemRelationAvailable(
	ctx context.Context,
	bh BackgroundExec,
	database string,
	relation string,
	targetAccountIDs []uint32,
) (bool, error) {
	if strings.EqualFold(database, catalog.MO_CATALOG) {
		switch strings.ToLower(relation) {
		case catalog.MO_DATABASE, catalog.MO_TABLES, catalog.MO_COLUMNS:
			return true, nil
		}
	}

	sql := fmt.Sprintf(
		getPublicationSystemRelationFormat,
		sanitizeSQLInput(database), sanitizeSQLInput(relation))
	systemCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	if err := bh.Exec(systemCtx, sql); err != nil {
		return false, err
	}

	erArray, err := getResultSet(systemCtx, bh)
	if err != nil {
		return false, err
	}

	hasGeneralRow := false
	ownedAccounts := make(map[uint64]struct{})
	for _, result := range erArray {
		for i := uint64(0); i < result.GetRowCount(); i++ {
			relKind, err := result.GetString(systemCtx, i, 0)
			if err != nil {
				return false, err
			}
			if relKind == catalog.SystemClusterRel {
				return true, nil
			}
			accountId, err := result.GetUint64(systemCtx, i, 1)
			if err != nil {
				return false, err
			}
			ownedAccounts[accountId] = struct{}{}
			if accountId != uint64(catalog.System_Account) {
				hasGeneralRow = true
			}
		}
	}
	for _, target := range targetAccountIDs {
		if target == publicationTargetAnyGeneralAccount {
			// ACCOUNT ALL includes accounts created after the publication:
			// judge the relation as a general subscriber resolves it. A row
			// owned by an existing general account shows an account that
			// already resolves it. A relation the account-initialization
			// contract provisions for every account CREATE ACCOUNT creates
			// resolves for future subscribers from the moment they exist, so
			// it stays publishable on a fresh system that has created no
			// general account yet; relations the system account keeps to
			// itself have no such evidence.
			return hasGeneralRow ||
				publicationSystemRelationProvisionedAtAccountCreation(bh.Service(), database, relation), nil
		}
		if _, owned := ownedAccounts[uint64(target)]; !owned {
			// The relation resolves inside the target's own catalog only;
			// another account owning a row cannot substitute for it.
			return false, nil
		}
	}
	return true, nil
}

// publicationProvisionedSystemRelations is built once per process from the
// account-initialization sources themselves and holds the "database.relation"
// keys (lowercase) of the system relations CREATE ACCOUNT provisions for
// every account it creates.
var (
	publicationProvisionedSystemRelationsOnce sync.Once
	publicationProvisionedSystemRelationsSet  map[string]struct{}
)

// publicationSystemRelationProvisionedAtAccountCreation reports whether the
// account-initialization contract provisions a row of this relation for every
// account CREATE ACCOUNT creates: the general-tenant initialization executes
// every createSqls statement except the sys-only set, installs the
// account-visible trace and metric relations, and installs the information_
// schema and mysql views. An ACCOUNT ALL publication extends to accounts
// created after the publication, so availability of such relations follows
// the contract rather than the accounts that happen to exist when the
// publication is created; relations the system account keeps to itself are
// provisioned for accounts it never creates and stay unavailable. service
// selects the information_schema generation the session's own tenant
// initialization installs.
func publicationSystemRelationProvisionedAtAccountCreation(
	service, database, relation string,
) bool {
	publicationProvisionedSystemRelationsOnce.Do(func() {
		publicationProvisionedSystemRelationsSet = buildPublicationProvisionedSystemRelations(service)
	})
	key := publicationProvisionedSymbolKey(database, relation)
	_, provisioned := publicationProvisionedSystemRelationsSet[key]
	return provisioned
}

func publicationProvisionedSymbolKey(database, relation string) string {
	return strings.ToLower(database) + "." + strings.ToLower(relation)
}

func buildPublicationProvisionedSystemRelations(service string) map[string]struct{} {
	provisioned := make(map[string]struct{})
	collect := func(sql string) {
		statements, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			// The sets are assembled from the same literals the initializer
			// executes; an unparsable element stays unconsumed instead of
			// broadening availability.
			return
		}
		defer freeStatements(statements)
		record := func(name *tree.TableName) {
			if name == nil || name.SchemaName == "" {
				return
			}
			provisioned[publicationProvisionedSymbolKey(string(name.SchemaName), string(name.ObjectName))] = struct{}{}
		}
		for _, statement := range statements {
			switch stmt := statement.(type) {
			case *tree.CreateTable:
				record(&stmt.Table)
			case *tree.CreateView:
				record(stmt.Name)
			}
		}
	}

	// mo_catalog: exactly what createTablesInMoCatalogOfGeneralTenant2
	// executes for each new account.
	for _, sql := range createSqls {
		if isSysOnlyCatalogRelationSQL(sql) {
			continue
		}
		collect(sql)
	}
	// system and system_metrics: the account-visible trace and metric
	// relations each account is initialized with.
	for _, sql := range motrace.GetSchemaForAccount(context.Background(), sysAccountName) {
		collect(sql)
	}
	for _, sql := range mometric.GetSchemaForAccount(context.Background(), sysAccountName) {
		collect(sql)
	}
	// information_schema and mysql: the views installed for each account.
	for _, sql := range sysview.InitInformationSchemaSysTablesForProtocol(
		protocolVersionForTenantInitialization(service)) {
		collect(sql)
	}
	for _, sql := range sysview.InitMysqlSysTables {
		collect(sql)
	}
	return provisioned
}

// publicationTargetAccountIDs returns the accounts that may subscribe to the
// publication being created or altered.
func publicationTargetAccountIDs(accounts map[int32]*pubsub.AccountInfo) []uint32 {
	if len(accounts) == 0 {
		return nil
	}
	targets := make([]uint32, 0, len(accounts))
	for accountId := range accounts {
		targets = append(targets, uint32(accountId))
	}
	return targets
}

// publicationAccountsReduceOnly reports whether an ALTER's final target set
// only removes authorization: every remaining account was already a
// subscriber, so the change cannot expose the published views to a subscriber
// that could not resolve them. finalAccountNamesStr is the ALTER's final
// account marker: switching to ACCOUNT ALL extends subscription rights to
// accounts created after the change, so ALL always counts as an expansion -
// even when the explicit list already names every existing account and the
// account maps compare equal.
func publicationAccountsReduceOnly(
	finalAccountNamesStr string,
	newSubAccounts, oldSubAccounts map[int32]*pubsub.AccountInfo,
) bool {
	if finalAccountNamesStr == pubsub.AccountAll {
		return false
	}
	for accountId := range newSubAccounts {
		if _, wasSubscriber := oldSubAccounts[accountId]; !wasSubscriber {
			return false
		}
	}
	return true
}

func isPublicationSystemDatabase(dbName string) bool {
	if dbName == "" {
		return false
	}
	_, ok := sysDatabases[strings.ToLower(dbName)]
	return ok
}

// publicationTargetAnyGeneralAccount marks the target set of an ACCOUNT ALL
// publication: every account, including ones created after the publication,
// may subscribe. System-relation availability is then judged for a general
// account, so a view over a system-owned relation cannot be published to all
// accounts while no general account exists and only later be revealed broken.
const publicationTargetAnyGeneralAccount = math.MaxUint32

// isPublicationSharedSystemRelation reports whether a relation is a system
// relation shared across accounts: the binder binds cluster relations and the
// switch-to-sys system relations through the system account even when a
// general account owns the view, so their persisted dependency account is the
// system account for every publisher. Such a relation is published through the
// system-database availability rule, not the account identity check.
func isPublicationSharedSystemRelation(database, relation string) bool {
	if database == "" {
		return false
	}
	return isClusterTable(database, relation) || ShouldSwitchToSysAccount(database, relation)
}

// publicationViewRelationRefs returns the direct relations of one published
// view. Views created since dependency snapshots were persisted carry their
// binder-resolved dependencies; older persisted definitions fall back to
// parsing the view SQL with the same CTE-aware collector used by database
// clone, so aliases and CTE names are never reported as catalog relations.
// The statement is parsed on both paths: a qualified reference to the
// publication database is accepted by the binder but a subscriber routes only
// unqualified references through the subscription (query_builder keeps the
// creation namespace for qualified names), so such a view would publish
// successfully and fail on every subscriber query.
func publicationViewRelationRefs(
	ctx context.Context,
	viewDef string,
	dbName string,
	sourceAccountID uint32,
	viewName string,
	sessionLowerCaseTableNames int64,
) ([]publicationRelationRef, error) {
	var viewData plan.ViewData
	if err := json.Unmarshal([]byte(viewDef), &viewData); err != nil {
		return nil, err
	}
	parsedRefs, err := parsePublicationViewRelationRefs(ctx, &viewData, dbName, viewName, sessionLowerCaseTableNames)
	if err != nil {
		return nil, err
	}
	for _, ref := range parsedRefs {
		if ref.qualified && ref.database == dbName {
			return nil, moerr.NewInternalErrorf(ctx,
				"table-level publication for database '%s' cannot publish view '%s': view references relation '%s.%s' with a qualified name, which its subscriber accounts cannot resolve",
				dbName, viewName, ref.database, ref.relation)
		}
	}
	if viewData.Dependencies == nil {
		return parsedRefs, nil
	}
	refs := make([]publicationRelationRef, 0, len(viewData.Dependencies))
	for _, dependency := range viewData.Dependencies {
		refs = append(refs, publicationRelationRef{
			database: dependency.DatabaseName,
			relation: dependency.RelationName,
			foreign: dependency.SubscriptionName != "" ||
				dependency.PublisherAccount != 0 ||
				dependency.Snapshot != nil ||
				dependency.SnapshotName != "" ||
				(dependency.AccountID != sourceAccountID &&
					!isPublicationSharedSystemRelation(dependency.DatabaseName, dependency.RelationName)),
		})
	}
	return refs, nil
}

func parsePublicationViewRelationRefs(
	ctx context.Context,
	viewData *plan.ViewData,
	dbName string,
	viewName string,
	sessionLowerCaseTableNames int64,
) ([]publicationRelationRef, error) {
	// The parse mode is the resolution context of the whole walk chain: the
	// creation-time mode recorded by the binder when it persisted the
	// definition, and for rows stored before that field existed the
	// validating session's own mode - the same instance-global value the DDL
	// session and every subscriber session resolve identifier case with.
	lowerCaseTableNames := sessionLowerCaseTableNames
	if viewData.LowerCaseTableNames != nil {
		lowerCaseTableNames = *viewData.LowerCaseTableNames
	}
	parserSQLMode := legacyViewParserSQLModeForRestore
	if viewData.SQLMode != nil {
		parserSQLMode = *viewData.SQLMode
	}
	// Unqualified references inside a view bind to the database that was
	// current when the view was created, exactly like the persisted binder.
	defaultDatabase := viewData.DefaultDatabase
	if defaultDatabase == "" {
		defaultDatabase = dbName
	}

	statements, err := parsers.ParseWithSQLMode(
		ctx, dialect.MYSQL, viewData.Stmt, lowerCaseTableNames, parserSQLMode)
	if err != nil {
		return nil, err
	}
	defer freeStatements(statements)
	if len(statements) != 1 {
		return nil, moerr.NewParseError(ctx, "persisted View must contain one statement")
	}
	var source *tree.Select
	switch statement := statements[0].(type) {
	case *tree.CreateView:
		source = statement.AsSource
	case *tree.AlterView:
		source = statement.AsSource
	default:
		return nil, moerr.NewParseError(ctx, "persisted View statement is not CREATE/ALTER VIEW")
	}

	// The collector must stay fail closed: a statement form the shared walker
	// cannot audit, or a rewrite conflict, rejects the publication instead of
	// accepting a view whose relations were never validated.
	collector := publicationViewRefCollector{
		defaultDatabase: defaultDatabase,
		refs:            make([]publicationRelationRef, 0, 4),
	}
	collector.remap = remapDbContext{
		lowerCaseTableNames: lowerCaseTableNames,
		unsupported:         &collector.unsupported,
		err:                 &collector.walkErr,
		collectTableName:    collector.collect,
		enterCTEs:           collector.enterCTEs,
		visitExpr:           collector.observeExpr,
	}
	remapDbInSelect(source, collector.remap)
	// WITH members the statement referenced are walked after the whole
	// statement has been traversed: usage can surface anywhere inside it,
	// including derived tables and subqueries.
	for len(collector.pending) > 0 {
		cte := collector.pending[0]
		collector.pending = collector.pending[1:]
		remapDbInStmt(cte.Stmt, collector.declRemap[cte])
	}
	if collector.walkErr != nil {
		return nil, collector.walkErr
	}
	if collector.unsupported {
		return nil, moerr.NewInternalErrorf(ctx,
			"cannot validate the relations of view '%s': its statement contains a form the dependency walker does not support",
			viewName)
	}
	if len(collector.sequenceFunctions) > 0 {
		// Sequence functions resolve their argument at execution time in the
		// executing account (baseBinder defers them past binding), and
		// sequence objects are never routed through a subscription, so a
		// published view calling one would subscribe successfully and fail
		// on every query. Detection is a pure syntax walk: the validation
		// never executes the function, so it can never consume a sequence
		// value.
		return nil, moerr.NewInternalErrorf(ctx,
			"table-level publication for database '%s' cannot publish view '%s': it calls sequence function '%s', and sequence objects do not resolve through subscriptions",
			dbName, viewName, collector.sequenceFunctions[0])
	}
	if len(collector.tableFunctions) > 0 {
		// Table functions that resolve a source relation from their arguments
		// bind that relation through the executing account - table_changes
		// through the compiler context at bind time, metadata_scan and
		// table_stats through the engine at execution time - so the subscriber
		// re-resolves it in its own account, through the subscription, and the
		// function fails there: table_changes is rejected by its own binder,
		// while the runtime scanners open the publisher's database name in the
		// subscriber's catalog where it does not exist (or silently names a
		// same-named subscriber database instead). Neither failure is recoverable
		// by including the base relations: the planner gives these functions
		// static schemas and captures no source dependency, so the scope
		// validator would otherwise admit a permanently unqueryable view after
		// publication and subscription creation both succeed. Reject the call
		// before any catalog write.
		return nil, moerr.NewInternalErrorf(ctx,
			"table-level publication for database '%s' cannot publish view '%s': it reads table function '%s', which resolves its source through the executing account and does not operate through subscriptions",
			dbName, viewName, collector.tableFunctions[0])
	}
	if len(collector.relationResolvingFunctions) > 0 {
		// The scalar relation-resolving builtins freeze their source as string
		// literals of the stored definition and resolve it only at execution,
		// through the executing account's engine context (func_mo.go's
		// e.Database(dbStr, ...)): the publisher-and-subscriber names never
		// meet, so a published view admitting this scope is permanently broken
		// from the subscription - every subscriber query either fails its
		// database lookup ("database ... not found") or, when the subscriber
		// owns a same-named database, silently reads the subscriber's own
		// catalog instead of the publisher's relation. No dependency is
		// captured - the literals never appear as relation references - so
		// including the base relations proves nothing. Reject the call before
		// any catalog write.
		return nil, moerr.NewInternalErrorf(ctx,
			"table-level publication for database '%s' cannot publish view '%s': it calls function '%s', which resolves its source relation through the executing account and does not operate through subscriptions",
			dbName, viewName, collector.relationResolvingFunctions[0])
	}
	return collector.refs, nil
}

// isPublicationSequenceFunction reports whether name is one of the public
// sequence functions whose argument the binder resolves at execution time.
func isPublicationSequenceFunction(name string) bool {
	switch name {
	case "nextval", "currval", "setval":
		return true
	default:
		return false
	}
}

// isPublicationIncompatibleTableFunction reports whether name is one of the
// table functions that resolve a catalog relation from their own arguments
// through the executing account. table_changes resolves (database, table)
// string literals through the compiler context while binding, so the
// publisher-side probe captures the relation as a dependency; metadata_scan
// and table_stats hand their (source, column) string literals to the
// function operator, which opens that relation through the executing
// account's engine only at execution, after binding exposed a static schema
// and captured no dependency. A subscriber replaying the stored definition
// re-resolves the argument inside its own account, where the source routes
// through the subscription - and the function fails there, in its own binder
// (table_changes) or at its first runtime lookup (metadata_scan,
// table_stats). Only relation-resolving table functions belong here: any
// execution account sees the same result for literal-driven ones (json_table,
// generate_series, unnest, ...) and account-local ones bind identically on
// the subscriber side. Extend this set when a new table function switches to
// argument-resolved sources.
func isPublicationIncompatibleTableFunction(name string) bool {
	switch name {
	case "table_changes":
		// buildTableChanges resolves (database, table) string literals
		// through the compiler context of the executing account, and
		// validateTableChangesSource rejects subscription sources.
		return true
	case "metadata_scan":
		// buildMetadataScan exposes a static schema without source
		// validation; the executor opens (database, table) of the first
		// argument through the executing account's engine, where a
		// subscriber account does not hold the publisher's database.
		return true
	case "table_stats":
		// buildTableStats exposes a static schema without source
		// validation; the executor resolves the table path of the first
		// argument through the executing account at runtime.
		return true
	default:
		return false
	}
}

// isPublicationRelationResolvingScalarFunction reports whether name is one of
// the scalar builtins that resolve a catalog relation from their own string
// literal arguments at execution time. mo_table_rows, mo_table_size,
// mo_table_col_max and mo_table_col_min all call the engine's
// e.Database(proc.Ctx, dbStr, ...) with the executing account's context
// (pkg/sql/plan/function/func_mo.go), and internal_auto_increment does the
// same for its database and table literals
// (pkg/sql/plan/function/auto_increase.go): the literals of a stored
// definition stay frozen through the subscriber's replay, so every subscriber
// query either fails the database lookup or - when the subscriber owns a
// same-named database - silently reads the subscriber's own catalog instead
// of the published relation (with internal_auto_increment the wrong source is
// observable as the subscriber's own auto-increment counter, not the
// publisher's). No catalog relation reference is ever captured for these
// calls, so the view's dependency list cannot speak for them and no
// membership can repair the subscription. internal_auto_increment is
// registered volatile, so no eager fold of a marked probe can execute it;
// its defect is purely the executing-account source resolution.
//
// mo_ctl belongs here as a whole despite being an administration dispatch.
// Its table-extra command resolves a frozen 'database.table' parameter through
// the executing account's engine
// (pkg/sql/plan/function/ctl/cmd_table_extra.go), and the mo_ctl access gate
// (pkg/sql/plan/function/ctl/ctl.go's verifyMoCtlAccess) only stops non-sys
// accounts: it requires the sys account with the moadmin role, and that exact
// identity can subscribe to an ordinary account's publication - 'account sys'
// is a legal account-list entry and the sys moadmin passes the same gate at
// execution. For a sys subscriber the frozen literal therefore executes and
// silently reads the sys account's own same-named relation or fails its
// executing-account lookup, while every ordinary subscriber fails the access
// gate; MoCtl additionally refuses row-correlated evaluation (its const-args
// length guard), so no subscription audience can rely on any published
// mo_ctl view - a wrong-source read for one account class, a stable failure
// for the rest. Extend this set when a new scalar builtin switches to
// argument-resolved sources or gains an account-gated executing-account
// catalog open like these. The other internal_* names are pure literal
// metadata helpers that never open a relation and stay publishable.
func isPublicationRelationResolvingScalarFunction(name string) bool {
	switch name {
	case "mo_table_rows", "mo_table_size", "mo_table_col_max", "mo_table_col_min",
		"internal_auto_increment", "mo_ctl":
		return true
	default:
		return false
	}
}

// publicationViewRefCollector gathers the catalog relations of one persisted
// view statement usage-aware, matching the binder: a WITH member's body is
// bound only when the statement references that member, and a member resolves
// names against the members declared before it, exactly like preprocessCte
// masks later-declared names. The shared remap walker is used for all
// traversal; its enterCTEs/collectCTERef hooks route every SELECT scope -
// top level, CTE bodies, derived tables and subqueries - through this
// collector, so an unused CTE never contributes relations and a reference
// that the binder would resolve to a physical table is never mistaken for a
// later-declared CTE of the same name.
type publicationViewRefCollector struct {
	remap           remapDbContext
	defaultDatabase string
	refs            []publicationRelationRef
	// sequenceFunctions records the sequence functions the statement calls.
	// The binder resolves their argument at execution time in the executing
	// account, so they can never bind inside a subscription and are rejected
	// by the publication scope validator.
	sequenceFunctions []string
	// tableFunctions records the relation-resolving table functions the
	// statement reads (FROM-position calls such as table_changes,
	// metadata_scan and table_stats). The subscriber re-resolves their source
	// in its own account - at bind time or at execution time - where the
	// function cannot operate through the subscription, so the validator
	// rejects the view before any catalog write.
	tableFunctions []string
	// relationResolvingFunctions records the scalar relation-resolving
	// builtins the statement calls (mo_table_rows, mo_table_size,
	// mo_table_col_max, mo_table_col_min, internal_auto_increment, and the
	// mo_ctl administration dispatch). Their source is frozen as string
	// literals and resolved at execution through the executing account, so
	// a subscriber either fails the lookup or silently reads its own
	// same-named catalog - the validator rejects the view before any catalog
	// write.
	relationResolvingFunctions []string
	// pending holds WITH members the statement referenced whose bodies have
	// not been walked yet.
	pending     []*tree.CTE
	visited     map[*tree.CTE]struct{}
	declRemap   map[*tree.CTE]remapDbContext
	walkErr     error
	unsupported bool
}

// observeExpr receives every expression node the shared walker reaches.
func (c *publicationViewRefCollector) observeExpr(expr tree.Expr) {
	if function, ok := expr.(*tree.FuncExpr); ok && function.FuncName != nil {
		name := strings.ToLower(function.FuncName.Origin())
		if isPublicationSequenceFunction(name) &&
			!slices.Contains(c.sequenceFunctions, name) {
			c.sequenceFunctions = append(c.sequenceFunctions, name)
		}
		// A table function's FROM-position call reaches this walk as the
		// function expression the shared table-expr walker descends into -
		// no valid view can call one as a scalar expression, since the
		// registry only binds relation-resolving names in FROM.
		if isPublicationIncompatibleTableFunction(name) &&
			!slices.Contains(c.tableFunctions, name) {
			c.tableFunctions = append(c.tableFunctions, name)
		}
		// The scalar relation-resolving builtins reach this walk in any
		// expression position: their source is string literals, so no
		// relation reference is collected anywhere else.
		if isPublicationRelationResolvingScalarFunction(name) &&
			!slices.Contains(c.relationResolvingFunctions, name) {
			c.relationResolvingFunctions = append(c.relationResolvingFunctions, name)
		}
	}
}

// collect classifies one catalog relation reference.
func (c *publicationViewRefCollector) collect(name *tree.TableName) {
	database := c.defaultDatabase
	if name.ExplicitSchema {
		database = string(name.SchemaName)
	} else if string(name.ObjectName) == "dual" {
		// The planner binds unqualified dual to a value scan instead of
		// resolving a catalog relation.
		return
	}
	c.refs = append(c.refs, publicationRelationRef{
		database:  database,
		relation:  string(name.ObjectName),
		foreign:   name.AtTsExpr != nil,
		qualified: name.ExplicitSchema,
	})
}

func (c *publicationViewRefCollector) markUsed(cte *tree.CTE) {
	if c.visited == nil {
		c.visited = make(map[*tree.CTE]struct{})
	}
	if _, done := c.visited[cte]; done {
		return
	}
	// Mark visited up front: a recursive CTE references itself, and its body
	// must be walked exactly once.
	c.visited[cte] = struct{}{}
	c.pending = append(c.pending, cte)
}

// enterCTEs registers the members of one WITH clause for the scope's main
// statement and remembers each member's declaration scope, mirroring the
// binder. preprocessCte masks every later-declared name for a member body,
// and findCTE stops the outward lookup at that mask, so a member resolves
// references against: the members declared before it (plus itself for
// RECURSIVE), and the inherited scope minus every name this WITH declares -
// the remaining names fall to the physical relation, exactly like the binder.
// The main statement sees every member; nested scopes layer on top through
// the collectCTERef closure chain.
func (c *publicationViewRefCollector) enterCTEs(w *tree.With, remap remapDbContext) remapDbContext {
	if c.declRemap == nil {
		c.declRemap = make(map[*tree.CTE]remapDbContext)
	}
	key := func(name string) string {
		return tree.NewCStr(name, remap.lowerCaseTableNames).Compare()
	}

	members := make([]*tree.CTE, 0, len(w.CTEs))
	declared := make(map[string]struct{}, len(w.CTEs))
	for _, cte := range w.CTEs {
		if cte == nil || cte.Name == nil || cte.Name.Alias == "" {
			continue
		}
		members = append(members, cte)
		declared[key(string(cte.Name.Alias))] = struct{}{}
	}

	// A member body never resolves a name this WITH declares through the
	// outer scopes; drop every declared name from the inherited visibility.
	base := remap
	if len(remap.cteNames) > 0 && len(declared) > 0 {
		filtered := make(map[string]struct{}, len(remap.cteNames))
		for name := range remap.cteNames {
			if _, masked := declared[name]; !masked {
				filtered[name] = struct{}{}
			}
		}
		base.cteNames = filtered
	}
	outerRef := remap.collectCTERef
	resolve := func(own map[string]*tree.CTE) func(*tree.TableName) {
		return func(name *tree.TableName) {
			if cte, ok := own[key(string(name.ObjectName))]; ok {
				c.markUsed(cte)
				return
			}
			if outerRef != nil {
				outerRef(name)
				return
			}
			// Every name the walker classifies as a visible CTE resolves in
			// the closure chain; fall back to catalog classification should a
			// future walker change that invariant.
			c.collect(name)
		}
	}

	// Declaration scopes: member i sees the snapshot of members declared
	// before it (plus itself for RECURSIVE); a later-declared sibling of the
	// same name is masked and its reference stays a physical relation.
	sequentialScope := base
	predecessors := make(map[string]*tree.CTE, len(members))
	for _, cte := range members {
		own := make(map[string]*tree.CTE, len(predecessors)+1)
		for name, visible := range predecessors {
			own[name] = visible
		}
		if w.IsRecursive {
			own[key(string(cte.Name.Alias))] = cte
		}
		scope := sequentialScope
		scope.collectCTERef = resolve(own)
		if w.IsRecursive {
			scope = scope.withVisibleCTEs(cte)
		}
		c.declRemap[cte] = scope
		predecessors[key(string(cte.Name.Alias))] = cte
		sequentialScope = sequentialScope.withVisibleCTEs(cte)
	}

	// The main statement sees every member through its own resolver.
	allMembers := make(map[string]*tree.CTE, len(members))
	mainScope := base
	for _, cte := range members {
		allMembers[key(string(cte.Name.Alias))] = cte
		mainScope = mainScope.withVisibleCTEs(cte)
	}
	mainScope.collectCTERef = resolve(allMembers)
	return mainScope
}
